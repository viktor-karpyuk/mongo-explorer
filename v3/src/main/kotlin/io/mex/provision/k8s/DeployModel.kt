package io.mex.provision.k8s

import io.mex.data.BackupChoice
import io.mex.data.K8sDeploySpec
import io.mex.data.K8sOperator
import io.mex.data.K8sProfile
import io.mex.data.K8sTopology
import io.mex.data.TlsChoice

/**
 * Validation is the single mechanical source of truth for the strict-Prod lock list
 * (K8P-PROF-3, ← PROV-3 verbatim): the wizard's enable logic, the renderer's
 * defence-in-depth throw, and the tests all consult this one function.
 */

private val DNS1123 = Regex("[a-z0-9]([-a-z0-9]{0,51}[a-z0-9])?")

/** Operator-supported image versions offered by the wizard. First = default. */
val K8S_MONGO_VERSIONS = listOf("8.0", "7.0", "6.0")

fun validate(spec: K8sDeploySpec): List<String> = buildList {
    if (!spec.name.matches(DNS1123)) {
        add("Name must be DNS-1123: lowercase alphanumerics and dashes, at most 53 characters.")
    }
    if (spec.namespace.isBlank()) add("Namespace is required.")
    if (spec.context.isBlank()) add("A kubeconfig context is required.")
    if (spec.mongoVersion !in K8S_MONGO_VERSIONS) add("Unsupported MongoDB version ${spec.mongoVersion}.")
    if (spec.storageGi < 1) add("Storage must be at least 1 Gi.")

    addAll(validateTopology(spec))
    addAll(validateTls(spec))
    addAll(validateBackup(spec))
    if (spec.profile == K8sProfile.prod) addAll(prodLocks(spec))
}

private fun validateTopology(spec: K8sDeploySpec): List<String> = buildList {
    when (val t = spec.topology) {
        is K8sTopology.ReplicaSet -> {
            val allowed = if (spec.profile == K8sProfile.prod) setOf(3, 5) else setOf(1, 3)
            if (t.members !in allowed) {
                add("Replica set members must be ${allowed.joinToString(" or ")} for the ${spec.profile.name} profile.")
            }
        }
        is K8sTopology.Sharded -> {
            if (spec.operator == K8sOperator.mco) {
                add("MCO cannot express sharded clusters — install the Percona operator for sharding.")
            }
            if (t.shards !in 1..8) add("Shards must be between 1 and 8.")
            val members = if (spec.profile == K8sProfile.prod) setOf(3, 5) else setOf(1, 3)
            if (t.membersPerShard !in members) {
                add("Members per shard must be ${members.joinToString(" or ")} for the ${spec.profile.name} profile.")
            }
            val mongosRange = if (spec.profile == K8sProfile.prod) 2..4 else 1..4
            if (t.mongos !in mongosRange) {
                add("Mongos routers must be between ${mongosRange.first} and ${mongosRange.last}.")
            }
            if (t.configServers != 3) add("Config servers are fixed at 3.")
        }
    }
}

private fun validateTls(spec: K8sDeploySpec): List<String> = buildList {
    when (val tls = spec.tls) {
        is TlsChoice.CertManager -> if (tls.issuer.isBlank()) add("cert-manager TLS needs an Issuer.")
        is TlsChoice.ByoCa -> if (tls.secretName.isBlank()) add("BYO-CA TLS needs the CA Secret name.")
        is TlsChoice.OperatorSelfSigned ->
            if (spec.operator == K8sOperator.mco) add("MCO has no operator-self-signed TLS mode.")
        TlsChoice.Off -> Unit
    }
}

private fun validateBackup(spec: K8sDeploySpec): List<String> = buildList {
    when (val b = spec.backup) {
        is BackupChoice.Pbm -> {
            if (spec.operator == K8sOperator.mco) add("PBM backups are PSMDB-only.")
            if (b.endpoint.isBlank()) add("PBM needs an S3-compatible endpoint.")
            if (b.bucket.isBlank()) add("PBM needs a bucket.")
            if (b.credentialsSecret.isBlank()) add("PBM needs a credentials Secret.")
        }
        else -> Unit
    }
}

/** The lock list, one message per violated lock (K8P-PROF-3). No individual overrides. */
private fun prodLocks(spec: K8sDeploySpec): List<String> = buildList {
    when (spec.tls) {
        TlsChoice.Off -> add("Prod locks TLS on — cert-manager Issuer or your own CA Secret.")
        is TlsChoice.OperatorSelfSigned -> add("Prod does not accept operator-self-signed TLS.")
        else -> Unit
    }
    if (spec.storageClass.isNullOrBlank()) add("Prod requires an explicit StorageClass.")
    when (val b = spec.backup) {
        BackupChoice.None -> add("Prod requires backups: PBM (PSMDB) or a declared external arrangement (MCO).")
        BackupChoice.ByoDeclared ->
            if (spec.operator == K8sOperator.psmdb) add("PSMDB Prod uses PBM, not a declared external arrangement.")
        is BackupChoice.Pbm -> Unit
    }
    // Member minimums are enforced in validateTopology via the profile-aware sets;
    // PDB, zone spread, resource limits and deletion protection are materialised
    // unconditionally by the renderer for prod — nothing to configure, nothing to forget.
}

/** One-line topology summary for cards (`sharded 2×3 · csrs 3 · mongos 2`). */
fun k8sSummary(t: K8sTopology): String = when (t) {
    is K8sTopology.ReplicaSet -> "replica set ×${t.members}"
    is K8sTopology.Sharded -> "sharded ${t.shards}×${t.membersPerShard} · csrs ${t.configServers} · mongos ${t.mongos}"
}

package io.mex.provision.k8s

import io.mex.data.BackupChoice
import io.mex.data.K8sDeploySpec
import io.mex.data.K8sOperator
import io.mex.data.K8sProfile
import io.mex.data.K8sTopology
import io.mex.data.TlsChoice

/**
 * Model → YAML bundle (K8P-CR-1). Pure and deterministic: same spec in, identical
 * bytes out, so the preview hash is stable and the output is golden-file testable.
 * Emission order is apply order.
 *
 * Prod locks are materialised *here*, not in the UI (K8P-PROF-3): PDB, zone spread,
 * resource limits and deletion protection are rendered unconditionally for prod, and
 * a spec that violates a lock throws rather than silently rendering something weaker.
 */

data class YamlDoc(
    val kind: String,
    val name: String,
    val yaml: String,
    /** True when secret values were replaced by placeholders for display. */
    val secretValuesElided: Boolean = false,
)

/** Placeholder shown in the preview instead of real credentials (K8P-SEC-4). */
const val ELIDED = "<generated — not shown>"

/**
 * @param secretValues real material, keyed `secretName/key`; empty renders elided
 *   placeholders (preview). The apply path passes the real values.
 */
fun render(spec: K8sDeploySpec, secretValues: Map<String, String> = emptyMap()): List<YamlDoc> {
    val violations = validate(spec)
    require(violations.isEmpty()) { "cannot render an invalid spec: ${violations.first()}" }
    val elide = secretValues.isEmpty()

    return buildList {
        if (spec.namespaceCreated) add(namespaceDoc(spec))
        add(usersSecretDoc(spec, secretValues, elide))
        (spec.tls as? TlsChoice.CertManager)?.let { add(certificateDoc(spec, it)) }
        add(crDoc(spec))
        if (spec.profile == K8sProfile.prod) add(pdbDoc(spec))
    }
}

/* ===================== documents ===================== */

private fun namespaceDoc(spec: K8sDeploySpec) = YamlDoc(
    kind = "Namespace",
    name = spec.namespace,
    yaml = """
        apiVersion: v1
        kind: Namespace
        metadata:
          name: ${spec.namespace}
          labels:
            ${LABEL_KEY}: ${spec.name}
    """.trimIndent(),
)

const val LABEL_KEY = "app.kubernetes.io/managed-by-mex"

fun usersSecretName(spec: K8sDeploySpec) = "${spec.name}-users"

private fun usersSecretDoc(spec: K8sDeploySpec, values: Map<String, String>, elide: Boolean): YamlDoc {
    val name = usersSecretName(spec)
    val password = if (elide) ELIDED else values["$name/password"].orEmpty()
    return YamlDoc(
        kind = "Secret",
        name = name,
        secretValuesElided = elide,
        yaml = """
            apiVersion: v1
            kind: Secret
            metadata:
              name: $name
              namespace: ${spec.namespace}
              labels:
                ${LABEL_KEY}: ${spec.name}
            type: Opaque
            stringData:
              # Root user consumed by the operator to bootstrap authentication.
              user: root
              password: "$password"
        """.trimIndent(),
    )
}

private fun certificateDoc(spec: K8sDeploySpec, tls: TlsChoice.CertManager) = YamlDoc(
    kind = "Certificate",
    name = "${spec.name}-tls",
    yaml = """
        apiVersion: cert-manager.io/v1
        kind: Certificate
        metadata:
          name: ${spec.name}-tls
          namespace: ${spec.namespace}
          labels:
            ${LABEL_KEY}: ${spec.name}
        spec:
          secretName: ${spec.name}-tls
          duration: 8760h
          renewBefore: 720h
          issuerRef:
            name: ${tls.issuer}
            kind: Issuer
          commonName: ${spec.name}
          dnsNames:
            - ${spec.name}
            - ${spec.name}.${spec.namespace}
            - "*.${spec.name}-svc.${spec.namespace}.svc.cluster.local"
    """.trimIndent(),
)

private fun pdbDoc(spec: K8sDeploySpec) = YamlDoc(
    kind = "PodDisruptionBudget",
    name = spec.name,
    yaml = """
        apiVersion: policy/v1
        kind: PodDisruptionBudget
        metadata:
          name: ${spec.name}
          namespace: ${spec.namespace}
          labels:
            ${LABEL_KEY}: ${spec.name}
        spec:
          maxUnavailable: 1
          selector:
            matchLabels:
              app.kubernetes.io/instance: ${spec.name}
    """.trimIndent(),
)

private fun crDoc(spec: K8sDeploySpec) = YamlDoc(
    kind = crKind(spec.operator),
    name = spec.name,
    yaml = when (spec.operator) {
        K8sOperator.mco -> mcoCr(spec)
        K8sOperator.psmdb -> psmdbCr(spec)
    },
)

/* ===================== shared fragments ===================== */

private fun resources(spec: K8sDeploySpec, indent: String): String {
    // Prod pins requests == limits: a burstable database is a database that gets
    // throttled at the worst possible moment.
    val i = indent
    return buildString {
        appendLine("${i}resources:")
        appendLine("$i  requests:")
        appendLine("$i    cpu: \"${spec.cpu}\"")
        appendLine("$i    memory: ${spec.memory}")
        appendLine("$i  limits:")
        appendLine("$i    cpu: \"${spec.cpu}\"")
        append("$i    memory: ${spec.memory}")
    }
}

private fun storage(spec: K8sDeploySpec, indent: String): String = buildString {
    val i = indent
    appendLine("${i}volumeClaimTemplates:")
    appendLine("$i  - metadata:")
    appendLine("$i      name: data-volume")
    appendLine("$i    spec:")
    spec.storageClass?.let { appendLine("$i      storageClassName: $it") }
    appendLine("$i      accessModes: [\"ReadWriteOnce\"]")
    appendLine("$i      resources:")
    appendLine("$i        requests:")
    append("$i          storage: ${spec.storageGi}Gi")
}

/** Zone + host spread with DoNotSchedule — the anti-affinity prod lock (K8P-PROF-3). */
private fun topologySpread(spec: K8sDeploySpec, indent: String, component: String): String = buildString {
    val i = indent
    appendLine("${i}topologySpreadConstraints:")
    for (key in listOf("topology.kubernetes.io/zone", "kubernetes.io/hostname")) {
        appendLine("$i  - maxSkew: 1")
        appendLine("$i    topologyKey: $key")
        appendLine("$i    whenUnsatisfiable: DoNotSchedule")
        appendLine("$i    labelSelector:")
        appendLine("$i      matchLabels:")
        appendLine("$i        app.kubernetes.io/instance: ${spec.name}")
        appendLine("$i        app.kubernetes.io/component: $component")
    }
    // Trim the trailing newline so document bytes stay deterministic.
    if (isNotEmpty()) setLength(length - 1)
}

/* ===================== MCO ===================== */

private fun mcoCr(spec: K8sDeploySpec): String {
    val members = (spec.topology as K8sTopology.ReplicaSet).members
    val prod = spec.profile == K8sProfile.prod
    val sb = StringBuilder()
    sb.appendLine("apiVersion: ${crApiVersion(spec.operator)}")
    sb.appendLine("kind: ${crKind(spec.operator)}")
    sb.appendLine("metadata:")
    sb.appendLine("  name: ${spec.name}")
    sb.appendLine("  namespace: ${spec.namespace}")
    sb.appendLine("  labels:")
    sb.appendLine("    $LABEL_KEY: ${spec.name}")
    sb.appendLine("spec:")
    sb.appendLine("  members: $members")
    sb.appendLine("  type: ReplicaSet")
    sb.appendLine("  version: \"${spec.mongoVersion}.0\"")
    sb.appendLine("  security:")
    sb.appendLine("    authentication:")
    sb.appendLine("      modes: [\"SCRAM\"]")
    when (val tls = spec.tls) {
        is TlsChoice.CertManager -> {
            sb.appendLine("    tls:")
            sb.appendLine("      enabled: true")
            sb.appendLine("      certificateKeySecretRef:")
            sb.appendLine("        name: ${spec.name}-tls")
            sb.appendLine("      caCertificateSecretRef:")
            sb.appendLine("        name: ${spec.name}-tls")
        }
        is TlsChoice.ByoCa -> {
            sb.appendLine("    tls:")
            sb.appendLine("      enabled: true")
            sb.appendLine("      certificateKeySecretRef:")
            sb.appendLine("        name: ${tls.secretName}")
            sb.appendLine("      caCertificateSecretRef:")
            sb.appendLine("        name: ${tls.secretName}")
        }
        else -> {
            sb.appendLine("    tls:")
            sb.appendLine("      enabled: false")
        }
    }
    sb.appendLine("  users:")
    sb.appendLine("    - name: root")
    sb.appendLine("      db: admin")
    sb.appendLine("      passwordSecretRef:")
    sb.appendLine("        name: ${usersSecretName(spec)}")
    sb.appendLine("        key: password")
    sb.appendLine("      roles:")
    sb.appendLine("        - name: root")
    sb.appendLine("          db: admin")
    sb.appendLine("      scramCredentialsSecretName: ${spec.name}-scram")
    sb.appendLine("  statefulSet:")
    sb.appendLine("    spec:")
    sb.appendLine(storage(spec, "      "))
    sb.appendLine("      template:")
    sb.appendLine("        spec:")
    if (prod) sb.appendLine(topologySpread(spec, "          ", "mongod"))
    sb.appendLine("          containers:")
    sb.appendLine("            - name: mongod")
    sb.append(resources(spec, "              "))
    return sb.toString()
}

/* ===================== PSMDB ===================== */

private fun psmdbCr(spec: K8sDeploySpec): String {
    val prod = spec.profile == K8sProfile.prod
    val sb = StringBuilder()
    sb.appendLine("apiVersion: ${crApiVersion(spec.operator)}")
    sb.appendLine("kind: ${crKind(spec.operator)}")
    sb.appendLine("metadata:")
    sb.appendLine("  name: ${spec.name}")
    sb.appendLine("  namespace: ${spec.namespace}")
    sb.appendLine("  labels:")
    sb.appendLine("    $LABEL_KEY: ${spec.name}")
    if (prod) {
        // Deletion protection: the operator refuses to remove data on CR delete unless
        // the finalizer is dropped deliberately (K8P-PROF-3, teardown honours it).
        sb.appendLine("  finalizers:")
        sb.appendLine("    - delete-psmdb-pods-in-order")
    }
    sb.appendLine("spec:")
    // Must track the operator the installer deploys — a crVersion ahead of the running
    // operator is rejected outright.
    sb.appendLine("  crVersion: $PSMDB_VERSION")
    sb.appendLine("  image: percona/percona-server-mongodb:${spec.mongoVersion}")
    sb.appendLine("  allowUnsafeConfigurations: ${!prod}")
    sb.appendLine("  updateStrategy: SmartUpdate")
    sb.appendLine("  secrets:")
    sb.appendLine("    users: ${usersSecretName(spec)}")
    when (val tls = spec.tls) {
        is TlsChoice.CertManager -> {
            sb.appendLine("  tls:")
            sb.appendLine("    mode: requireTLS")
            sb.appendLine("    issuerConf:")
            sb.appendLine("      name: ${tls.issuer}")
            sb.appendLine("      kind: Issuer")
        }
        is TlsChoice.ByoCa -> {
            sb.appendLine("  tls:")
            sb.appendLine("    mode: requireTLS")
            sb.appendLine("  secrets:")
            sb.appendLine("    ssl: ${tls.secretName}")
        }
        TlsChoice.OperatorSelfSigned -> {
            sb.appendLine("  tls:")
            sb.appendLine("    mode: preferTLS")
        }
        TlsChoice.Off -> {
            sb.appendLine("  unsafeFlags:")
            sb.appendLine("    tls: true")
            sb.appendLine("  tls:")
            sb.appendLine("    mode: disabled")
        }
    }

    when (val t = spec.topology) {
        is K8sTopology.ReplicaSet -> {
            sb.appendLine("  replsets:")
            sb.append(psmdbReplset(spec, "rs0", t.members, prod, "mongod"))
            sb.appendLine("  sharding:")
            sb.appendLine("    enabled: false")
        }
        is K8sTopology.Sharded -> {
            sb.appendLine("  replsets:")
            for (i in 1..t.shards) {
                sb.append(psmdbReplset(spec, "rs$i", t.membersPerShard, prod, "mongod"))
            }
            sb.appendLine("  sharding:")
            sb.appendLine("    enabled: true")
            sb.appendLine("    configsvrReplSet:")
            sb.appendLine("      size: ${t.configServers}")
            sb.appendLine(resources(spec, "      "))
            if (prod) sb.appendLine(topologySpread(spec, "      ", "cfg"))
            sb.appendLine(storage(spec, "      "))
            sb.appendLine("    mongos:")
            sb.appendLine("      size: ${t.mongos}")
            sb.appendLine(resources(spec, "      "))
            if (prod) sb.appendLine(topologySpread(spec, "      ", "mongos"))
        }
    }

    when (val b = spec.backup) {
        is BackupChoice.Pbm -> {
            sb.appendLine("  backup:")
            sb.appendLine("    enabled: true")
            sb.appendLine("    image: percona/percona-backup-mongodb:2.5.0")
            sb.appendLine("    storages:")
            sb.appendLine("      s3-primary:")
            sb.appendLine("        type: s3")
            sb.appendLine("        s3:")
            sb.appendLine("          bucket: ${b.bucket}")
            sb.appendLine("          endpointUrl: ${b.endpoint}")
            sb.appendLine("          credentialsSecret: ${b.credentialsSecret}")
            sb.appendLine("    pitr:")
            sb.appendLine("      enabled: ${b.pitrEnabled}")
            sb.appendLine("    tasks:")
            sb.appendLine("      - name: nightly-full")
            sb.appendLine("        enabled: true")
            sb.appendLine("        schedule: \"${b.fullSchedule}\"")
            sb.appendLine("        storageName: s3-primary")
            sb.append("        compressionType: gzip")
        }
        else -> {
            sb.appendLine("  backup:")
            sb.append("    enabled: false")
        }
    }
    return sb.toString()
}

private fun psmdbReplset(
    spec: K8sDeploySpec,
    name: String,
    size: Int,
    prod: Boolean,
    component: String,
): String = buildString {
    appendLine("    - name: $name")
    appendLine("      size: $size")
    appendLine(resources(spec, "      "))
    if (prod) appendLine(topologySpread(spec, "      ", component))
    appendLine(storage(spec, "      "))
}

package io.mex.data

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
enum class K8sOperator { mco, psmdb }

@Serializable
enum class K8sProfile { dev, prod }

/** K8P-PROF-4 — RS for both operators; sharded is PSMDB-only (capability-gated). */
@Serializable
sealed interface K8sTopology {
    @Serializable
    @SerialName("replicaSet")
    data class ReplicaSet(val members: Int) : K8sTopology

    @Serializable
    @SerialName("sharded")
    data class Sharded(
        val shards: Int,
        val membersPerShard: Int,
        val mongos: Int = 2,
        val configServers: Int = 3,
    ) : K8sTopology
}

@Serializable
sealed interface TlsChoice {
    @Serializable
    @SerialName("off")
    data object Off : TlsChoice // dev only

    @Serializable
    @SerialName("selfSigned")
    data object OperatorSelfSigned : TlsChoice // dev only, PSMDB only

    @Serializable
    @SerialName("certManager")
    data class CertManager(val issuer: String) : TlsChoice

    @Serializable
    @SerialName("byoCa")
    data class ByoCa(val secretName: String) : TlsChoice
}

@Serializable
sealed interface BackupChoice {
    @Serializable
    @SerialName("none")
    data object None : BackupChoice // dev only

    @Serializable
    @SerialName("pbm")
    data class Pbm(
        val endpoint: String,
        val bucket: String,
        val credentialsSecret: String,
        /** cron for the full backup; the PITR window rides alongside (K8P-BKP-1). */
        val fullSchedule: String = "0 1 * * *",
        val pitrEnabled: Boolean = true,
    ) : BackupChoice

    /** MCO Prod: backups declared as handled outside the app (K8P-BKP-2, ← PROV-13). */
    @Serializable
    @SerialName("byoDeclared")
    data object ByoDeclared : BackupChoice
}

@Serializable
data class K8sDeploySpec(
    val name: String, // CR name, DNS-1123
    val context: String,
    val kubeconfigPath: String? = null,
    val namespace: String,
    /** App-created namespaces may be removed at teardown when empty (K8P-CTX-6). */
    val namespaceCreated: Boolean = false,
    val operator: K8sOperator,
    val profile: K8sProfile,
    val topology: K8sTopology,
    val mongoVersion: String,
    val storageClass: String? = null, // required in prod (K8P-PROF-3)
    val storageGi: Int = 20,
    val cpu: String = "1",
    val memory: String = "2Gi",
    val tls: TlsChoice,
    val backup: BackupChoice,
    /** Secret name → sha256 of its material, for drift banners; never the material (K8P-SEC-2). */
    val secretFingerprints: Map<String, String> = emptyMap(),
)

enum class K8sDeployStatus { applying, pending, ready, degraded, failed, deleting, missing }

data class K8sDeployment(
    val id: String,
    val spec: K8sDeploySpec,
    val status: K8sDeployStatus,
    /** Raw CR state string — the app reports, never invents (K8P-NFR-3). */
    val statusDetail: String?,
    val bundleHash: String?,
    val connectionId: String?,
    val error: String?,
    val createdAt: Long,
    val appliedAt: Long?,
)

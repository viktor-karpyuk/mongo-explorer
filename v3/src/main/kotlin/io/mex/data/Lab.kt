package io.mex.data

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/** Stamped on each lab so a lab built by a newer app can refuse Start (PRV-LIFE-5). */
const val LAB_APP_MAJOR = 3

/** PRV-TOPO-1 — the three provisionable shapes; bounds enforced by [io.mex.provision.validate]. */
@Serializable
sealed interface LabTopology {
    @Serializable
    @SerialName("standalone")
    data object Standalone : LabTopology

    @Serializable
    @SerialName("replicaSet")
    data class ReplicaSet(val members: Int) : LabTopology

    @Serializable
    @SerialName("sharded")
    data class Sharded(
        val shards: Int,
        val membersPerShard: Int,
        val mongos: Int = 2,
        val configServers: Int = 3,
    ) : LabTopology
}

enum class LabStatus { provisioning, running, stopped, failed, missing }

/** PRV-BOOT-1 — pipeline phases, in execution order. */
enum class ProvisionPhase { render, up, wait, initiate, shards, auth, verify, register }

data class Lab(
    val id: String,
    val name: String,
    val topology: LabTopology,
    val status: LabStatus,
    val mongoTag: String,
    val auth: Boolean,
    /** compose service name → host port on 127.0.0.1 (PRV-RENDER-3). */
    val portMap: Map<String, Int>,
    /** Registered connection; nullable-on-read — the connection may be deleted independently (PRV-CONN-5). */
    val connectionId: String?,
    /** Managed directory holding compose.yaml (+ keyfile); all writes and deletes stay inside it (PRV-RENDER-1). */
    val dir: String,
    val appMajor: Int,
    val error: String?,
    val createdAt: Long,
) {
    /** Short id used in compose project/service names (PRV-RENDER-6). */
    val id8: String get() = id.takeLast(8).lowercase()
}

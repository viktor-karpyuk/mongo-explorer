package io.mex.data

import kotlinx.serialization.Serializable

@Serializable
data class MigrationNs(val db: String, val coll: String)

@Serializable
data class MigrationSpec(
    val sourceId: String,
    val targetId: String,
    val namespaces: List<MigrationNs>,
    val ensureCollections: Boolean = true,
)

enum class MigrationStatus { pending, running, paused, completed, failed }

@Serializable
data class MigrationCheckpoint(
    val nsIndex: Int = 0,
    val lastIdEjson: String? = null,
    val copiedInNs: Long = 0,
)

data class MigrationJob(
    val id: String,
    val spec: MigrationSpec,
    val status: MigrationStatus,
    val startedAt: Long?,
    val finishedAt: Long?,
    val error: String?,
    val checkpoint: MigrationCheckpoint?,
)

data class MigrationProgress(
    val jobId: String,
    val status: MigrationStatus,
    val currentNs: MigrationNs?,
    val copied: Long,
    val estimated: Long?,
    val error: String? = null,
)

data class PreflightCheck(val name: String, val ok: Boolean, val detail: String? = null)
data class PreflightResult(val ok: Boolean, val checks: List<PreflightCheck>)

package io.mex.data

import kotlinx.serialization.Serializable

@Serializable
data class MigrationNs(val db: String, val coll: String)

/** MIG-CONFLICT-1 — how the copy treats documents already present on the target. */
@Serializable
enum class ConflictPolicy { abort, append, upsert, drop }

@Serializable
data class MigrationSpec(
    val sourceId: String,
    val targetId: String,
    val namespaces: List<MigrationNs>,
    val ensureCollections: Boolean = true,
    val conflictPolicy: ConflictPolicy = ConflictPolicy.abort,
    val copyIndexes: Boolean = true,
    val verify: Boolean = true,
    val docErrorLimit: Long = 1_000,
)

enum class MigrationStatus { pending, running, paused, completed, failed }

enum class MigrationPhase { copy, indexes, verify }

@Serializable
data class MigrationCheckpoint(
    val nsIndex: Int = 0,
    /** Canonical EJSON of {"_id": …} — MIG-CKPT-1. Legacy values fail to parse and reset the namespace. */
    val lastIdEjson: String? = null,
    val copiedInNs: Long = 0,
    val skippedInNs: Long = 0,
    val errorsInNs: Long = 0,
)

/** MIG-VERIFY-2 — per-namespace verification outcome. */
@Serializable
data class NsReport(
    val db: String,
    val coll: String,
    val sourceCount: Long,
    val targetCount: Long,
    val skipped: Long,
    val errors: Long,
    val indexesCopied: Int,
    val missingIndexes: List<String> = emptyList(),
    val errorSamples: List<String> = emptyList(),
    val ok: Boolean,
)

@Serializable
data class VerificationReport(val ok: Boolean, val finishedAt: Long, val rows: List<NsReport>)

data class MigrationJob(
    val id: String,
    val spec: MigrationSpec,
    val status: MigrationStatus,
    val startedAt: Long?,
    val finishedAt: Long?,
    val error: String?,
    val checkpoint: MigrationCheckpoint?,
    val report: VerificationReport? = null,
)

data class MigrationProgress(
    val jobId: String,
    val status: MigrationStatus,
    val currentNs: MigrationNs?,
    val copied: Long,
    val estimated: Long?,
    val error: String? = null,
    val phase: MigrationPhase? = null,
    val skipped: Long = 0,
    val docErrors: Long = 0,
)

data class PreflightCheck(val name: String, val ok: Boolean, val detail: String? = null, val warn: Boolean = false)
data class PreflightResult(val ok: Boolean, val checks: List<PreflightCheck>)

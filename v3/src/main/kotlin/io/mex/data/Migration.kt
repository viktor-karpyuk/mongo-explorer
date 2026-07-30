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
    /** Replay source collection options (validator, collation, capped, timeseries) onto the target. */
    val copyCollectionOptions: Boolean = true,
    /** Read preference for the source cursor — `secondaryPreferred` offloads a live primary. */
    val readFromSecondary: Boolean = false,
    /** Soft cap on documents per bulk write; the byte cap applies independently. */
    val batchSize: Int = 1_000,
)

enum class MigrationStatus { pending, running, paused, completed, failed, cancelled }

enum class MigrationPhase { copy, indexes, verify }

@Serializable
data class MigrationCheckpoint(
    val nsIndex: Int = 0,
    /** Canonical EJSON of {"_id": …} — MIG-CKPT-1. Legacy values fail to parse and reset the namespace. */
    val lastIdEjson: String? = null,
    /**
     * Set immediately *before* a batch is written and cleared after the checkpoint advances.
     * A non-null value on resume means the process died mid-write, so the first batch of the
     * resumed namespace must be replayed idempotently (MIG-CKPT-4).
     */
    val pendingLastIdEjson: String? = null,
    val copiedInNs: Long = 0,
    val skippedInNs: Long = 0,
    val errorsInNs: Long = 0,
    /** Per-document write-error samples, carried across pause/resume so the report can show them. */
    val errorSamples: List<String> = emptyList(),
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
) {
    /** A job that stopped early but kept a usable checkpoint can be resumed instead of restarted. */
    val resumable: Boolean
        get() = (status == MigrationStatus.failed || status == MigrationStatus.cancelled) &&
            checkpoint != null &&
            (checkpoint.nsIndex > 0 || checkpoint.copiedInNs > 0 || checkpoint.lastIdEjson != null)
}

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
    /** Documents per second over the job's copy phase, for the progress bar's ETA. */
    val docsPerSecond: Double? = null,
    val nsIndex: Int = 0,
    val nsTotal: Int = 0,
)

data class PreflightCheck(val name: String, val ok: Boolean, val detail: String? = null, val warn: Boolean = false)
data class PreflightResult(val ok: Boolean, val checks: List<PreflightCheck>)

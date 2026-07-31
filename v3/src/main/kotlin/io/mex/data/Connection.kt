package io.mex.data

data class ConnectionRecord(
    val id: String,
    val name: String,
    val uri: String,
    val notes: String?,
    val createdAt: Long,
    val updatedAt: Long,
    val lastUsedAt: Long?,
    /** DBA-RO-1 — every mutating affordance in the UI is disabled for this connection. */
    val readOnly: Boolean = false,
)

data class ConnectionSummary(
    val id: String,
    val name: String,
    val notes: String?,
    val createdAt: Long,
    val updatedAt: Long,
    val lastUsedAt: Long?,
    val readOnly: Boolean = false,
)

data class ConnectionInput(
    val name: String,
    val uri: String,
    val notes: String? = null,
    val readOnly: Boolean = false,
)

data class UriHistoryEntry(
    val id: Long,
    val uri: String,
    val preview: String,
    val usedAt: Long,
)

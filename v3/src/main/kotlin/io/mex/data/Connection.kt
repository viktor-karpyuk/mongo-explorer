package io.mex.data

data class ConnectionRecord(
    val id: String,
    val name: String,
    val uri: String,
    val notes: String?,
    val createdAt: Long,
    val updatedAt: Long,
    val lastUsedAt: Long?,
)

data class ConnectionSummary(
    val id: String,
    val name: String,
    val notes: String?,
    val createdAt: Long,
    val updatedAt: Long,
    val lastUsedAt: Long?,
)

data class ConnectionInput(
    val name: String,
    val uri: String,
    val notes: String? = null,
)

data class UriHistoryEntry(
    val id: Long,
    val uri: String,
    val preview: String,
    val usedAt: Long,
)

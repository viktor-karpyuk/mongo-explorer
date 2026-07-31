package io.mex.data

enum class BackupStatus { running, completed, failed, cancelled }

/** What a backup covers: both null = full deployment, coll null = one database. */
data class BackupScope(val db: String? = null, val coll: String? = null) {
    val label: String
        get() = when {
            db == null -> "full deployment"
            coll == null -> db
            else -> "$db.$coll"
        }
}

data class BackupEntry(
    val id: String,
    val connectionId: String?,
    val connectionName: String,
    val scope: BackupScope,
    val gzip: Boolean,
    val path: String,
    val status: BackupStatus,
    val error: String?,
    val startedAt: Long,
    val finishedAt: Long?,
    val sizeBytes: Long?,
)

package io.mex.data

enum class QueryKind { find, aggregate, command }

data class QueryHistoryRow(
    val id: Long,
    val connectionId: String,
    val database: String,
    val collection: String,
    val kind: QueryKind,
    val body: String,
    val durationMs: Long?,
    val rowCount: Int?,
    val error: String?,
    val ranAt: Long,
)

data class QueryHistoryInput(
    val connectionId: String,
    val database: String,
    val collection: String,
    val kind: QueryKind,
    val body: String,
    val durationMs: Long? = null,
    val rowCount: Int? = null,
    val error: String? = null,
)

private const val MAX_PER_CONNECTION = 100

class QueryHistoryRepo(private val store: Store) {
    private val conn get() = store.conn

    fun list(connectionId: String, limit: Int = MAX_PER_CONNECTION): List<QueryHistoryRow> {
        val out = mutableListOf<QueryHistoryRow>()
        conn.prepareStatement(
            """
            SELECT * FROM query_history
            WHERE connection_id = ?
            ORDER BY ran_at DESC
            LIMIT ?
            """.trimIndent(),
        ).use { ps ->
            ps.setString(1, connectionId)
            ps.setInt(2, limit)
            ps.executeQuery().use { rs ->
                while (rs.next()) {
                    out += QueryHistoryRow(
                        id = rs.getLong("id"),
                        connectionId = rs.getString("connection_id"),
                        database = rs.getString("database_name"),
                        collection = rs.getString("collection"),
                        kind = QueryKind.valueOf(rs.getString("kind")),
                        body = rs.getString("body"),
                        durationMs = rs.getLong("duration_ms").takeIf { !rs.wasNull() },
                        rowCount = rs.getInt("row_count").takeIf { !rs.wasNull() },
                        error = rs.getString("error"),
                        ranAt = rs.getLong("ran_at"),
                    )
                }
            }
        }
        return out
    }

    fun record(input: QueryHistoryInput) {
        conn.prepareStatement(
            """
            INSERT INTO query_history
              (connection_id, database_name, collection, kind, body,
               duration_ms, row_count, error, ran_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """.trimIndent(),
        ).use { ps ->
            ps.setString(1, input.connectionId)
            ps.setString(2, input.database)
            ps.setString(3, input.collection)
            ps.setString(4, input.kind.name)
            ps.setString(5, input.body)
            input.durationMs?.also { ps.setLong(6, it) } ?: ps.setNull(6, java.sql.Types.INTEGER)
            input.rowCount?.also { ps.setInt(7, it) } ?: ps.setNull(7, java.sql.Types.INTEGER)
            ps.setString(8, input.error)
            ps.setLong(9, System.currentTimeMillis())
            ps.executeUpdate()
        }
        trim(input.connectionId)
    }

    private fun trim(connectionId: String) {
        conn.prepareStatement(
            """
            DELETE FROM query_history
            WHERE connection_id = ? AND id NOT IN (
              SELECT id FROM query_history
              WHERE connection_id = ?
              ORDER BY ran_at DESC
              LIMIT ?
            )
            """.trimIndent(),
        ).use { ps ->
            ps.setString(1, connectionId)
            ps.setString(2, connectionId)
            ps.setInt(3, MAX_PER_CONNECTION)
            ps.executeUpdate()
        }
    }
}

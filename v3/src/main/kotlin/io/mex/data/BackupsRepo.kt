package io.mex.data

import com.github.f4b6a3.ulid.UlidCreator

class BackupsRepo(private val store: Store) {
    private val conn get() = store.conn

    /** Backups mid-dump when the app died can never finish — surface them honestly. */
    fun reconcileOrphans() {
        conn.prepareStatement(
            "UPDATE backups SET status = 'failed', error = 'interrupted — the app closed during the dump' WHERE status = 'running'",
        ).use { it.executeUpdate() }
    }

    fun list(): List<BackupEntry> {
        val out = mutableListOf<BackupEntry>()
        conn.prepareStatement("SELECT * FROM backups ORDER BY started_at DESC").use { ps ->
            ps.executeQuery().use { rs ->
                while (rs.next()) out += parse(rs)
            }
        }
        return out
    }

    fun create(
        connectionId: String,
        connectionName: String,
        scope: BackupScope,
        gzip: Boolean,
        path: String,
    ): BackupEntry {
        val id = UlidCreator.getUlid().toString()
        val now = System.currentTimeMillis()
        conn.prepareStatement(
            """
            INSERT INTO backups (id, connection_id, connection_name, scope_db, scope_coll,
                                 gzip, path, status, started_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'running', ?)
            """.trimIndent(),
        ).use { ps ->
            ps.setString(1, id)
            ps.setString(2, connectionId)
            ps.setString(3, connectionName)
            ps.setString(4, scope.db)
            ps.setString(5, scope.coll)
            ps.setInt(6, if (gzip) 1 else 0)
            ps.setString(7, path)
            ps.setLong(8, now)
            ps.executeUpdate()
        }
        return BackupEntry(id, connectionId, connectionName, scope, gzip, path, BackupStatus.running, null, now, null, null)
    }

    fun finish(id: String, status: BackupStatus, error: String? = null, sizeBytes: Long? = null) {
        conn.prepareStatement(
            "UPDATE backups SET status = ?, error = ?, finished_at = ?, size_bytes = ? WHERE id = ?",
        ).use { ps ->
            ps.setString(1, status.name)
            ps.setString(2, error)
            ps.setLong(3, System.currentTimeMillis())
            if (sizeBytes != null) ps.setLong(4, sizeBytes) else ps.setNull(4, java.sql.Types.BIGINT)
            ps.setString(5, id)
            ps.executeUpdate()
        }
    }

    fun delete(id: String) {
        conn.prepareStatement("DELETE FROM backups WHERE id = ?").use { ps ->
            ps.setString(1, id)
            ps.executeUpdate()
        }
    }

    private fun parse(rs: java.sql.ResultSet): BackupEntry = BackupEntry(
        id = rs.getString("id"),
        connectionId = rs.getString("connection_id"),
        connectionName = rs.getString("connection_name"),
        scope = BackupScope(rs.getString("scope_db"), rs.getString("scope_coll")),
        gzip = rs.getInt("gzip") != 0,
        path = rs.getString("path"),
        status = BackupStatus.valueOf(rs.getString("status")),
        error = rs.getString("error"),
        startedAt = rs.getLong("started_at"),
        finishedAt = rs.getLong("finished_at").takeIf { !rs.wasNull() },
        sizeBytes = rs.getLong("size_bytes").takeIf { !rs.wasNull() },
    )
}

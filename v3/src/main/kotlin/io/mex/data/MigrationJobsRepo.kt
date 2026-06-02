package io.mex.data

import com.github.f4b6a3.ulid.UlidCreator
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json

private val json = Json { ignoreUnknownKeys = true }

class MigrationJobsRepo(private val store: Store) {
    private val conn get() = store.conn

    fun reconcileOrphans() {
        conn.prepareStatement(
            """
            UPDATE migration_jobs SET status = 'failed', error = 'orphaned at restart'
            WHERE status IN ('running','paused') AND heartbeat_at IS NOT NULL
              AND heartbeat_at < ?
            """.trimIndent(),
        ).use { ps ->
            ps.setLong(1, System.currentTimeMillis() - 60_000)
            ps.executeUpdate()
        }
    }

    fun list(): List<MigrationJob> {
        val out = mutableListOf<MigrationJob>()
        conn.prepareStatement("SELECT * FROM migration_jobs ORDER BY id DESC").use { ps ->
            ps.executeQuery().use { rs ->
                while (rs.next()) out += parse(rs)
            }
        }
        return out
    }

    fun get(id: String): MigrationJob? {
        conn.prepareStatement("SELECT * FROM migration_jobs WHERE id = ?").use { ps ->
            ps.setString(1, id)
            ps.executeQuery().use { rs ->
                return if (rs.next()) parse(rs) else null
            }
        }
    }

    fun create(spec: MigrationSpec): MigrationJob {
        val id = UlidCreator.getUlid().toString()
        conn.prepareStatement(
            "INSERT INTO migration_jobs (id, source_conn_id, target_conn_id, spec, status) VALUES (?, ?, ?, ?, 'pending')",
        ).use { ps ->
            ps.setString(1, id)
            ps.setString(2, spec.sourceId)
            ps.setString(3, spec.targetId)
            ps.setString(4, json.encodeToString(spec))
            ps.executeUpdate()
        }
        return get(id)!!
    }

    fun setStatus(id: String, status: MigrationStatus, error: String? = null) {
        val now = System.currentTimeMillis()
        conn.prepareStatement(
            """
            UPDATE migration_jobs SET status = ?,
              started_at = COALESCE(started_at, CASE WHEN ? = 'running' THEN ? ELSE NULL END),
              finished_at = CASE WHEN ? IN ('completed','failed') THEN ? ELSE finished_at END,
              error = COALESCE(?, error),
              heartbeat_at = ?
            WHERE id = ?
            """.trimIndent(),
        ).use { ps ->
            ps.setString(1, status.name)
            ps.setString(2, status.name)
            ps.setLong(3, now)
            ps.setString(4, status.name)
            ps.setLong(5, now)
            ps.setString(6, error)
            ps.setLong(7, now)
            ps.setString(8, id)
            ps.executeUpdate()
        }
    }

    fun setCheckpoint(id: String, cp: MigrationCheckpoint) {
        conn.prepareStatement("UPDATE migration_jobs SET checkpoint = ?, heartbeat_at = ? WHERE id = ?").use { ps ->
            ps.setString(1, json.encodeToString(cp))
            ps.setLong(2, System.currentTimeMillis())
            ps.setString(3, id)
            ps.executeUpdate()
        }
    }

    fun delete(id: String) {
        conn.prepareStatement("DELETE FROM migration_jobs WHERE id = ?").use { ps ->
            ps.setString(1, id)
            ps.executeUpdate()
        }
    }

    private fun parse(rs: java.sql.ResultSet): MigrationJob {
        val cp = rs.getString("checkpoint")?.let { json.decodeFromString<MigrationCheckpoint>(it) }
        return MigrationJob(
            id = rs.getString("id"),
            spec = json.decodeFromString(rs.getString("spec")),
            status = MigrationStatus.valueOf(rs.getString("status")),
            startedAt = rs.getLong("started_at").takeIf { !rs.wasNull() },
            finishedAt = rs.getLong("finished_at").takeIf { !rs.wasNull() },
            error = rs.getString("error"),
            checkpoint = cp,
        )
    }
}

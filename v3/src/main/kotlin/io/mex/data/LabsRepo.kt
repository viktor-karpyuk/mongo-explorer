package io.mex.data

import com.github.f4b6a3.ulid.UlidCreator
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json

private val json = Json { ignoreUnknownKeys = true }

class LabsRepo(private val store: Store) {
    private val conn get() = store.conn

    /** A lab mid-provision when the app died can never finish — surface it honestly (PRV-LIFE-4). */
    fun reconcileOrphans() {
        conn.prepareStatement(
            "UPDATE labs SET status = 'failed', error = 'interrupted — the app closed during provisioning' WHERE status = 'provisioning'",
        ).use { it.executeUpdate() }
    }

    fun list(): List<Lab> {
        val out = mutableListOf<Lab>()
        conn.prepareStatement("SELECT * FROM labs ORDER BY created_at DESC").use { ps ->
            ps.executeQuery().use { rs ->
                while (rs.next()) parse(rs)?.let { out += it }
            }
        }
        return out
    }

    fun get(id: String): Lab? {
        conn.prepareStatement("SELECT * FROM labs WHERE id = ?").use { ps ->
            ps.setString(1, id)
            ps.executeQuery().use { rs ->
                return if (rs.next()) parse(rs) else null
            }
        }
    }

    fun nameExists(name: String): Boolean {
        conn.prepareStatement("SELECT 1 FROM labs WHERE name = ?").use { ps ->
            ps.setString(1, name)
            ps.executeQuery().use { rs -> return rs.next() }
        }
    }

    /** [labsDir] is the managed root; the lab's own dir embeds the generated id (PRV-RENDER-1). */
    fun create(name: String, topology: LabTopology, mongoTag: String, auth: Boolean, labsDir: String): Lab {
        val id = UlidCreator.getUlid().toString()
        val now = System.currentTimeMillis()
        val dir = "$labsDir/$id"
        conn.prepareStatement(
            """
            INSERT INTO labs (id, name, topology, status, mongo_tag, auth, dir, app_major, created_at)
            VALUES (?, ?, ?, 'provisioning', ?, ?, ?, ?, ?)
            """.trimIndent(),
        ).use { ps ->
            ps.setString(1, id)
            ps.setString(2, name)
            ps.setString(3, json.encodeToString(topology))
            ps.setString(4, mongoTag)
            ps.setInt(5, if (auth) 1 else 0)
            ps.setString(6, dir)
            ps.setInt(7, LAB_APP_MAJOR)
            ps.setLong(8, now)
            ps.executeUpdate()
        }
        return get(id)!!
    }

    fun setStatus(id: String, status: LabStatus, error: String? = null) {
        conn.prepareStatement("UPDATE labs SET status = ?, error = ? WHERE id = ?").use { ps ->
            ps.setString(1, status.name)
            ps.setString(2, error)
            ps.setString(3, id)
            ps.executeUpdate()
        }
    }

    fun setPorts(id: String, portMap: Map<String, Int>) {
        conn.prepareStatement("UPDATE labs SET port_map = ? WHERE id = ?").use { ps ->
            ps.setString(1, json.encodeToString(portMap))
            ps.setString(2, id)
            ps.executeUpdate()
        }
    }

    fun setConnection(id: String, connectionId: String?) {
        conn.prepareStatement("UPDATE labs SET connection_id = ? WHERE id = ?").use { ps ->
            ps.setString(1, connectionId)
            ps.setString(2, id)
            ps.executeUpdate()
        }
    }

    fun delete(id: String) {
        conn.prepareStatement("DELETE FROM labs WHERE id = ?").use { ps ->
            ps.setString(1, id)
            ps.executeUpdate()
        }
    }

    /** Connection ids owned by labs, for the LAB badge join (PRV-CONN-4). */
    fun connectionIds(): Set<String> {
        val out = mutableSetOf<String>()
        conn.prepareStatement("SELECT connection_id FROM labs WHERE connection_id IS NOT NULL").use { ps ->
            ps.executeQuery().use { rs ->
                while (rs.next()) out += rs.getString(1)
            }
        }
        return out
    }

    /**
     * Null when the topology JSON is unreadable (a lab written by a future app version
     * with a new shape) — one such row must not take down the whole Provision view.
     */
    private fun parse(rs: java.sql.ResultSet): Lab? {
        val topology = rs.getString("topology").let {
            runCatching { json.decodeFromString<LabTopology>(it) }.getOrNull()
        } ?: return null
        return Lab(
            id = rs.getString("id"),
            name = rs.getString("name"),
            topology = topology,
            status = LabStatus.valueOf(rs.getString("status")),
            mongoTag = rs.getString("mongo_tag"),
            auth = rs.getInt("auth") != 0,
            portMap = rs.getString("port_map")?.let {
                runCatching { json.decodeFromString<Map<String, Int>>(it) }.getOrNull()
            } ?: emptyMap(),
            connectionId = rs.getString("connection_id"),
            dir = rs.getString("dir"),
            appMajor = rs.getInt("app_major"),
            error = rs.getString("error"),
            createdAt = rs.getLong("created_at"),
        )
    }
}

package io.mex.data

import com.github.f4b6a3.ulid.UlidCreator
import io.mex.crypto.Secrets

class ConnectionsRepo(private val store: Store) {
    private val conn get() = store.conn

    fun list(): List<ConnectionSummary> {
        val out = mutableListOf<ConnectionSummary>()
        conn.prepareStatement(
            """
            SELECT id, name, notes, created_at, updated_at, last_used_at
            FROM connections
            ORDER BY COALESCE(last_used_at, updated_at) DESC
            """.trimIndent(),
        ).use { ps ->
            ps.executeQuery().use { rs ->
                while (rs.next()) {
                    out += ConnectionSummary(
                        id = rs.getString("id"),
                        name = rs.getString("name"),
                        notes = rs.getString("notes"),
                        createdAt = rs.getLong("created_at"),
                        updatedAt = rs.getLong("updated_at"),
                        lastUsedAt = rs.getLong("last_used_at").takeIf { !rs.wasNull() },
                    )
                }
            }
        }
        return out
    }

    fun get(id: String): ConnectionRecord? {
        conn.prepareStatement("SELECT * FROM connections WHERE id = ?").use { ps ->
            ps.setString(1, id)
            ps.executeQuery().use { rs ->
                if (!rs.next()) return null
                val cipher = rs.getBytes("uri_cipher")
                return ConnectionRecord(
                    id = rs.getString("id"),
                    name = rs.getString("name"),
                    uri = Secrets.decrypt(cipher),
                    notes = rs.getString("notes"),
                    createdAt = rs.getLong("created_at"),
                    updatedAt = rs.getLong("updated_at"),
                    lastUsedAt = rs.getLong("last_used_at").takeIf { !rs.wasNull() },
                )
            }
        }
    }

    fun create(input: ConnectionInput): ConnectionSummary {
        val id = UlidCreator.getUlid().toString()
        val now = System.currentTimeMillis()
        val cipher = Secrets.encrypt(input.uri)
        conn.prepareStatement(
            """
            INSERT INTO connections (id, name, uri_cipher, notes, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """.trimIndent(),
        ).use { ps ->
            ps.setString(1, id)
            ps.setString(2, input.name)
            ps.setBytes(3, cipher)
            ps.setString(4, input.notes)
            ps.setLong(5, now)
            ps.setLong(6, now)
            ps.executeUpdate()
        }
        return ConnectionSummary(
            id = id,
            name = input.name,
            notes = input.notes,
            createdAt = now,
            updatedAt = now,
            lastUsedAt = null,
        )
    }

    fun update(id: String, input: ConnectionInput): ConnectionSummary? {
        if (get(id) == null) return null
        val now = System.currentTimeMillis()
        val cipher = Secrets.encrypt(input.uri)
        conn.prepareStatement(
            """
            UPDATE connections
            SET name = ?, uri_cipher = ?, notes = ?, updated_at = ?
            WHERE id = ?
            """.trimIndent(),
        ).use { ps ->
            ps.setString(1, input.name)
            ps.setBytes(2, cipher)
            ps.setString(3, input.notes)
            ps.setLong(4, now)
            ps.setString(5, id)
            ps.executeUpdate()
        }
        return list().firstOrNull { it.id == id }
    }

    fun delete(id: String): Boolean {
        conn.prepareStatement("DELETE FROM connections WHERE id = ?").use { ps ->
            ps.setString(1, id)
            return ps.executeUpdate() > 0
        }
    }

    fun touchLastUsed(id: String) {
        conn.prepareStatement("UPDATE connections SET last_used_at = ? WHERE id = ?").use { ps ->
            ps.setLong(1, System.currentTimeMillis())
            ps.setString(2, id)
            ps.executeUpdate()
        }
    }
}

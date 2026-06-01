package io.mex.data

import io.mex.crypto.Secrets

private const val MAX_HISTORY = 20

class UriHistoryRepo(private val store: Store) {
    private val conn get() = store.conn

    fun list(): List<UriHistoryEntry> {
        val out = mutableListOf<UriHistoryEntry>()
        conn.prepareStatement(
            "SELECT id, uri_cipher, preview, used_at FROM uri_history ORDER BY used_at DESC",
        ).use { ps ->
            ps.executeQuery().use { rs ->
                while (rs.next()) {
                    out += UriHistoryEntry(
                        id = rs.getLong("id"),
                        uri = Secrets.decrypt(rs.getBytes("uri_cipher")),
                        preview = rs.getString("preview"),
                        usedAt = rs.getLong("used_at"),
                    )
                }
            }
        }
        return out
    }

    fun add(uri: String) {
        val cipher = Secrets.encrypt(uri)
        val preview = redactPreview(uri)
        conn.prepareStatement(
            "INSERT INTO uri_history (uri_cipher, preview, used_at) VALUES (?, ?, ?)",
        ).use { ps ->
            ps.setBytes(1, cipher)
            ps.setString(2, preview)
            ps.setLong(3, System.currentTimeMillis())
            ps.executeUpdate()
        }
        trim()
    }

    fun delete(id: Long) {
        conn.prepareStatement("DELETE FROM uri_history WHERE id = ?").use { ps ->
            ps.setLong(1, id)
            ps.executeUpdate()
        }
    }

    private fun trim() {
        conn.prepareStatement(
            """
            DELETE FROM uri_history WHERE id NOT IN (
              SELECT id FROM uri_history ORDER BY used_at DESC LIMIT ?
            )
            """.trimIndent(),
        ).use { ps ->
            ps.setInt(1, MAX_HISTORY)
            ps.executeUpdate()
        }
    }

    /** Strip credentials before persisting the preview. */
    private fun redactPreview(uri: String): String =
        Regex("(mongodb(\\+srv)?://)([^@/]+@)").replace(uri, "$1***@")
}

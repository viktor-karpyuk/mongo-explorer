package io.mex.data

class PrefsRepo(private val store: Store) {
    private val conn get() = store.conn

    fun get(key: String): String? {
        conn.prepareStatement("SELECT value FROM prefs WHERE key = ?").use { ps ->
            ps.setString(1, key)
            ps.executeQuery().use { rs ->
                return if (rs.next()) rs.getString(1) else null
            }
        }
    }

    fun set(key: String, value: String) {
        conn.prepareStatement(
            """
            INSERT INTO prefs (key, value) VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """.trimIndent(),
        ).use { ps ->
            ps.setString(1, key)
            ps.setString(2, value)
            ps.executeUpdate()
        }
    }

    fun delete(key: String) {
        conn.prepareStatement("DELETE FROM prefs WHERE key = ?").use { ps ->
            ps.setString(1, key)
            ps.executeUpdate()
        }
    }
}

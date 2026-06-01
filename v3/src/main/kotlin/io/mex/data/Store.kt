package io.mex.data

import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement

class Store private constructor(val conn: Connection) : AutoCloseable {
    init {
        bootstrap()
        applyMigrations()
    }

    private fun bootstrap() {
        exec("PRAGMA journal_mode=WAL")
        exec("PRAGMA foreign_keys=ON")
        exec("PRAGMA synchronous=NORMAL")
        exec(
            """
            CREATE TABLE IF NOT EXISTS schema_version (
              version    INTEGER PRIMARY KEY,
              applied_at INTEGER NOT NULL
            )
            """.trimIndent(),
        )
    }

    private fun applyMigrations() {
        val current = currentVersion()
        val pending = MIGRATIONS.filter { it.version > current }
        if (pending.isEmpty()) return

        conn.autoCommit = false
        try {
            for (m in pending) {
                conn.createStatement().use { st ->
                    for (chunk in m.sql.split(";").map { it.trim() }.filter { it.isNotEmpty() }) {
                        st.executeUpdate(chunk)
                    }
                }
                conn.prepareStatement("INSERT INTO schema_version (version, applied_at) VALUES (?, ?)")
                    .use { ps ->
                        ps.setInt(1, m.version)
                        ps.setLong(2, System.currentTimeMillis())
                        ps.executeUpdate()
                    }
            }
            conn.commit()
        } catch (e: Exception) {
            conn.rollback()
            throw e
        } finally {
            conn.autoCommit = true
        }
    }

    private fun currentVersion(): Int =
        conn.createStatement().use { st ->
            st.executeQuery("SELECT COALESCE(MAX(version), 0) FROM schema_version").use { rs ->
                if (rs.next()) rs.getInt(1) else 0
            }
        }

    private fun exec(sql: String) {
        conn.createStatement().use { it.execute(sql) }
    }

    override fun close() = conn.close()

    companion object {
        fun open(path: Path): Store {
            Files.createDirectories(path.parent)
            // Class.forName not needed in modern JDBC, but be explicit for jpackage builds.
            Class.forName("org.sqlite.JDBC")
            val conn = DriverManager.getConnection("jdbc:sqlite:${path.toAbsolutePath()}")
            conn.createStatement().use(Statement::closeOnCompletion)
            return Store(conn)
        }
    }
}

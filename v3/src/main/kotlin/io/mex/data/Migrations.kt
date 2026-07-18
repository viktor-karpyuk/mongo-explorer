package io.mex.data

internal data class Migration(val version: Int, val sql: String)

internal val MIGRATIONS: List<Migration> = listOf(
    Migration(
        version = 1,
        sql = """
            CREATE TABLE connections (
              id            TEXT PRIMARY KEY,
              name          TEXT NOT NULL,
              uri_cipher    BLOB NOT NULL,
              notes         TEXT,
              created_at    INTEGER NOT NULL,
              updated_at    INTEGER NOT NULL,
              last_used_at  INTEGER
            );

            CREATE INDEX idx_connections_last_used
              ON connections(last_used_at DESC);

            CREATE TABLE uri_history (
              id            INTEGER PRIMARY KEY AUTOINCREMENT,
              uri_cipher    BLOB NOT NULL,
              preview       TEXT NOT NULL,
              used_at       INTEGER NOT NULL
            );

            CREATE INDEX idx_uri_history_used
              ON uri_history(used_at DESC);

            CREATE TABLE query_history (
              id            INTEGER PRIMARY KEY AUTOINCREMENT,
              connection_id TEXT NOT NULL REFERENCES connections(id) ON DELETE CASCADE,
              database_name TEXT NOT NULL,
              collection    TEXT NOT NULL,
              kind          TEXT NOT NULL CHECK (kind IN ('find','aggregate','command')),
              body          TEXT NOT NULL,
              duration_ms   INTEGER,
              row_count     INTEGER,
              error         TEXT,
              ran_at        INTEGER NOT NULL
            );

            CREATE INDEX idx_query_history_conn_ran
              ON query_history(connection_id, ran_at DESC);

            CREATE TABLE prefs (
              key   TEXT PRIMARY KEY,
              value TEXT NOT NULL
            );

            CREATE TABLE migration_jobs (
              id              TEXT PRIMARY KEY,
              source_conn_id  TEXT NOT NULL REFERENCES connections(id) ON DELETE CASCADE,
              target_conn_id  TEXT NOT NULL REFERENCES connections(id) ON DELETE CASCADE,
              spec            TEXT NOT NULL,
              status          TEXT NOT NULL CHECK (status IN
                                ('pending','running','paused','completed','failed')),
              heartbeat_at    INTEGER,
              started_at      INTEGER,
              finished_at     INTEGER,
              checkpoint      TEXT,
              error           TEXT
            );

            CREATE INDEX idx_migration_jobs_status ON migration_jobs(status);
        """.trimIndent(),
    ),
    // v3.1 migration hardening — persisted verification report (MIG-VERIFY-4).
    Migration(
        version = 2,
        sql = "ALTER TABLE migration_jobs ADD COLUMN report TEXT",
    ),
)

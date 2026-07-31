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
    // v3.2 — 'cancelled' becomes a first-class status so a user-initiated stop is no
    // longer indistinguishable from a genuine failure. SQLite cannot alter a CHECK
    // constraint in place, so the table is rebuilt.
    Migration(
        version = 3,
        sql = """
            CREATE TABLE migration_jobs_new (
              id              TEXT PRIMARY KEY,
              source_conn_id  TEXT NOT NULL REFERENCES connections(id) ON DELETE CASCADE,
              target_conn_id  TEXT NOT NULL REFERENCES connections(id) ON DELETE CASCADE,
              spec            TEXT NOT NULL,
              status          TEXT NOT NULL CHECK (status IN
                                ('pending','running','paused','completed','failed','cancelled')),
              heartbeat_at    INTEGER,
              started_at      INTEGER,
              finished_at     INTEGER,
              checkpoint      TEXT,
              error           TEXT,
              report          TEXT
            );

            INSERT INTO migration_jobs_new
              (id, source_conn_id, target_conn_id, spec, status, heartbeat_at,
               started_at, finished_at, checkpoint, error, report)
              SELECT id, source_conn_id, target_conn_id, spec, status, heartbeat_at,
                     started_at, finished_at, checkpoint, error, report
              FROM migration_jobs;

            DROP TABLE migration_jobs;

            ALTER TABLE migration_jobs_new RENAME TO migration_jobs;

            CREATE INDEX idx_migration_jobs_status ON migration_jobs(status)
        """.trimIndent(),
    ),
    // v3.3 — per-connection read-only mode (DBA-RO-1): a production connection can be
    // flagged so every mutating affordance in the UI is disabled for it.
    Migration(
        version = 4,
        sql = "ALTER TABLE connections ADD COLUMN read_only INTEGER NOT NULL DEFAULT 0",
    ),
    // v3.4 — backup catalog (DBA-BKP-1). connection_id is informational, not a FK:
    // a backup must outlive the connection it was taken from.
    Migration(
        version = 5,
        sql = """
            CREATE TABLE backups (
              id              TEXT PRIMARY KEY,
              connection_id   TEXT,
              connection_name TEXT NOT NULL,
              scope_db        TEXT,
              scope_coll      TEXT,
              gzip            INTEGER NOT NULL DEFAULT 1,
              path            TEXT NOT NULL,
              status          TEXT NOT NULL CHECK (status IN
                                ('running','completed','failed','cancelled')),
              error           TEXT,
              started_at      INTEGER NOT NULL,
              finished_at     INTEGER,
              size_bytes      INTEGER
            );

            CREATE INDEX idx_backups_started ON backups(started_at DESC)
        """.trimIndent(),
    ),
)

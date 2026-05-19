export interface Migration {
  readonly version: number;
  readonly sql: string;
}

export const migrations: readonly Migration[] = [
  {
    version: 1,
    sql: `
      CREATE TABLE connections (
        id            TEXT PRIMARY KEY,
        name          TEXT NOT NULL,
        uri_cipher    BLOB NOT NULL,
        ssh_host      TEXT,
        ssh_port      INTEGER,
        ssh_user      TEXT,
        ssh_key_path  TEXT,
        notes         TEXT,
        created_at    INTEGER NOT NULL,
        updated_at    INTEGER NOT NULL,
        last_used_at  INTEGER
      );

      CREATE INDEX idx_connections_last_used
        ON connections(last_used_at DESC);

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
        pid             INTEGER,
        heartbeat_at    INTEGER,
        started_at      INTEGER,
        finished_at     INTEGER,
        checkpoint      TEXT,
        error           TEXT
      );

      CREATE INDEX idx_migration_jobs_status ON migration_jobs(status);
    `,
  },
];

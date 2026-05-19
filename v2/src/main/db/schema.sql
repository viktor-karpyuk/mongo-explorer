-- Mongo Explorer v2 local store.
-- WAL mode + foreign keys are enabled at connection time, not via DDL.

CREATE TABLE IF NOT EXISTS schema_version (
  version INTEGER PRIMARY KEY,
  applied_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS connections (
  id            TEXT PRIMARY KEY,        -- ULID
  name          TEXT NOT NULL,
  uri_encrypted BLOB NOT NULL,           -- AES-256-GCM payload of full URI
  uri_iv        BLOB NOT NULL,
  uri_tag       BLOB NOT NULL,
  ssh_host      TEXT,
  ssh_port      INTEGER,
  ssh_user      TEXT,
  ssh_key_path  TEXT,
  notes         TEXT,
  created_at    INTEGER NOT NULL,
  updated_at    INTEGER NOT NULL,
  last_used_at  INTEGER
);

CREATE INDEX IF NOT EXISTS idx_connections_last_used
  ON connections(last_used_at DESC);

CREATE TABLE IF NOT EXISTS query_history (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  connection_id TEXT NOT NULL REFERENCES connections(id) ON DELETE CASCADE,
  database_name TEXT NOT NULL,
  collection    TEXT NOT NULL,
  kind          TEXT NOT NULL CHECK (kind IN ('find', 'aggregate', 'command')),
  body          TEXT NOT NULL,           -- JSON of the query
  duration_ms   INTEGER,
  row_count     INTEGER,
  error         TEXT,
  ran_at        INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_query_history_conn_ran
  ON query_history(connection_id, ran_at DESC);

CREATE TABLE IF NOT EXISTS prefs (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

-- Migration jobs (Phase M). Listed here so the rollback path can clean orphans
-- on app start even when the wizard hasn't loaded yet.
CREATE TABLE IF NOT EXISTS migration_jobs (
  id              TEXT PRIMARY KEY,
  source_conn_id  TEXT NOT NULL REFERENCES connections(id) ON DELETE CASCADE,
  target_conn_id  TEXT NOT NULL REFERENCES connections(id) ON DELETE CASCADE,
  spec            TEXT NOT NULL,         -- JSON spec
  status          TEXT NOT NULL CHECK (status IN
                    ('pending','running','paused','completed','failed')),
  pid             INTEGER,
  heartbeat_at    INTEGER,
  started_at      INTEGER,
  finished_at     INTEGER,
  checkpoint      TEXT,                  -- JSON checkpoint blob
  error           TEXT
);

CREATE INDEX IF NOT EXISTS idx_migration_jobs_status
  ON migration_jobs(status);

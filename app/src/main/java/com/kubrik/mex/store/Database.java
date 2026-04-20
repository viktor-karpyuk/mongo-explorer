package com.kubrik.mex.store;

import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class Database implements AutoCloseable {

    private final Connection connection;

    public Database() throws SQLException, IOException {
        Files.createDirectories(AppPaths.dataDir());
        String url = "jdbc:sqlite:" + AppPaths.databaseFile().toAbsolutePath();
        this.connection = DriverManager.getConnection(url);
        try (Statement st = connection.createStatement()) {
            st.execute("PRAGMA journal_mode=WAL");
            st.execute("PRAGMA foreign_keys=ON");
        }
        migrate();
    }

    public Connection connection() { return connection; }

    private void migrate() throws SQLException {
        try (Statement st = connection.createStatement()) {
            st.execute("""
                CREATE TABLE IF NOT EXISTS connections (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    uri TEXT NOT NULL DEFAULT '',
                    enc_password TEXT,
                    default_db TEXT,
                    tls INTEGER NOT NULL DEFAULT 0,
                    allow_invalid_certs INTEGER NOT NULL DEFAULT 0,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL
                )
                """);
            // Studio-3T-style fields. Each ALTER is wrapped because the table may
            // already have been created in an earlier migration.
            String[] alters = {
                    "ALTER TABLE connections ADD COLUMN mode TEXT NOT NULL DEFAULT 'FORM'",
                    "ALTER TABLE connections ADD COLUMN connection_type TEXT NOT NULL DEFAULT 'STANDALONE'",
                    "ALTER TABLE connections ADD COLUMN hosts TEXT NOT NULL DEFAULT 'localhost:27017'",
                    "ALTER TABLE connections ADD COLUMN srv_host TEXT",
                    "ALTER TABLE connections ADD COLUMN auth_mode TEXT NOT NULL DEFAULT 'NONE'",
                    "ALTER TABLE connections ADD COLUMN username TEXT",
                    "ALTER TABLE connections ADD COLUMN auth_db TEXT",
                    "ALTER TABLE connections ADD COLUMN gssapi_service_name TEXT",
                    "ALTER TABLE connections ADD COLUMN aws_session_token TEXT",
                    "ALTER TABLE connections ADD COLUMN tls_ca_file TEXT",
                    "ALTER TABLE connections ADD COLUMN tls_client_cert_file TEXT",
                    "ALTER TABLE connections ADD COLUMN enc_tls_client_cert_password TEXT",
                    "ALTER TABLE connections ADD COLUMN tls_allow_invalid_hostnames INTEGER NOT NULL DEFAULT 0",
                    "ALTER TABLE connections ADD COLUMN ssh_enabled INTEGER NOT NULL DEFAULT 0",
                    "ALTER TABLE connections ADD COLUMN ssh_host TEXT",
                    "ALTER TABLE connections ADD COLUMN ssh_port INTEGER NOT NULL DEFAULT 22",
                    "ALTER TABLE connections ADD COLUMN ssh_user TEXT",
                    "ALTER TABLE connections ADD COLUMN ssh_auth_mode TEXT NOT NULL DEFAULT 'PASSWORD'",
                    "ALTER TABLE connections ADD COLUMN enc_ssh_password TEXT",
                    "ALTER TABLE connections ADD COLUMN ssh_key_file TEXT",
                    "ALTER TABLE connections ADD COLUMN enc_ssh_key_passphrase TEXT",
                    "ALTER TABLE connections ADD COLUMN proxy_type TEXT NOT NULL DEFAULT 'NONE'",
                    "ALTER TABLE connections ADD COLUMN proxy_host TEXT",
                    "ALTER TABLE connections ADD COLUMN proxy_port INTEGER NOT NULL DEFAULT 1080",
                    "ALTER TABLE connections ADD COLUMN proxy_user TEXT",
                    "ALTER TABLE connections ADD COLUMN enc_proxy_password TEXT",
                    "ALTER TABLE connections ADD COLUMN replica_set_name TEXT",
                    "ALTER TABLE connections ADD COLUMN read_preference TEXT NOT NULL DEFAULT 'primary'",
                    "ALTER TABLE connections ADD COLUMN app_name TEXT",
                    "ALTER TABLE connections ADD COLUMN manual_uri_options TEXT"
            };
            for (String sql : alters) {
                try { st.execute(sql); } catch (SQLException ignored) {}
            }
            st.execute("""
                CREATE TABLE IF NOT EXISTS query_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    connection_id TEXT NOT NULL,
                    db_name TEXT,
                    coll_name TEXT,
                    kind TEXT NOT NULL,
                    body TEXT NOT NULL,
                    created_at INTEGER NOT NULL
                )
                """);
            st.execute("""
                CREATE TABLE IF NOT EXISTS app_settings (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
                """);

            // Migration feature (v1.1.0) — see docs/mvp-technical-spec.md §4.1
            st.execute("""
                CREATE TABLE IF NOT EXISTS migration_jobs (
                    id              TEXT PRIMARY KEY,
                    kind            TEXT NOT NULL,
                    source_conn_id  TEXT,
                    target_conn_id  TEXT NOT NULL,
                    spec_json       TEXT NOT NULL,
                    spec_hash       TEXT NOT NULL,
                    status          TEXT NOT NULL,
                    execution_mode  TEXT NOT NULL DEFAULT 'RUN',
                    started_at      INTEGER,
                    ended_at        INTEGER,
                    docs_copied     INTEGER NOT NULL DEFAULT 0,
                    bytes_copied    INTEGER NOT NULL DEFAULT 0,
                    errors          INTEGER NOT NULL DEFAULT 0,
                    error_message   TEXT,
                    resume_path     TEXT,
                    artifact_dir    TEXT,
                    created_at      INTEGER NOT NULL,
                    updated_at      INTEGER NOT NULL
                )
                """);
            st.execute("CREATE INDEX IF NOT EXISTS idx_migration_jobs_started_at ON migration_jobs(started_at DESC)");
            st.execute("CREATE INDEX IF NOT EXISTS idx_migration_jobs_status ON migration_jobs(status)");

            // v1.2.0 — owner-PID + heartbeat for startup reconciliation of orphaned RUNNING rows,
            // plus connection names (UX-11, preserved so deleted connections still render readably),
            // dry-run docs counter (OBS-5), and pause-aware active-time tracking (OBS-7).
            String[] migrationJobAlters = {
                    "ALTER TABLE migration_jobs ADD COLUMN owner_pid INTEGER",
                    "ALTER TABLE migration_jobs ADD COLUMN last_heartbeat_at INTEGER",
                    "ALTER TABLE migration_jobs ADD COLUMN source_connection_name TEXT",
                    "ALTER TABLE migration_jobs ADD COLUMN target_connection_name TEXT",
                    "ALTER TABLE migration_jobs ADD COLUMN docs_processed INTEGER NOT NULL DEFAULT 0",
                    "ALTER TABLE migration_jobs ADD COLUMN active_millis INTEGER NOT NULL DEFAULT 0",
                    // v2.0 VER-8 — environment tag on job rows. Null means "applies everywhere".
                    "ALTER TABLE migration_jobs ADD COLUMN environment TEXT"
            };
            for (String sql : migrationJobAlters) {
                try { st.execute(sql); } catch (SQLException ignored) {}
            }

            // v1.2.0 OBS-7 — per-collection start/end timings for the Job Details view.
            st.execute("""
                CREATE TABLE IF NOT EXISTS migration_collection_timings (
                    job_id     TEXT NOT NULL,
                    source_ns  TEXT NOT NULL,
                    started_at INTEGER NOT NULL,
                    ended_at   INTEGER,
                    PRIMARY KEY (job_id, source_ns),
                    FOREIGN KEY (job_id) REFERENCES migration_jobs(id) ON DELETE CASCADE
                )
                """);

            st.execute("""
                CREATE TABLE IF NOT EXISTS migration_profiles (
                    id          TEXT PRIMARY KEY,
                    name        TEXT NOT NULL,
                    kind        TEXT NOT NULL,
                    spec_json   TEXT NOT NULL,
                    created_at  INTEGER NOT NULL,
                    updated_at  INTEGER NOT NULL
                )
                """);
            st.execute("CREATE INDEX IF NOT EXISTS idx_migration_profiles_name ON migration_profiles(name)");

            // v2.0 VER-8 — environment tag on saved profiles.
            try { st.execute("ALTER TABLE migration_profiles ADD COLUMN environment TEXT"); }
            catch (SQLException ignored) {}

            // v2.0 UX-7 — local scheduler rows. The user picks a profile, writes a cron-
            // or interval-expression, and a background worker runs it on that schedule while
            // the app is open. No cross-instance coordination (single-box product).
            st.execute("""
                CREATE TABLE IF NOT EXISTS migration_schedules (
                    id           TEXT PRIMARY KEY,
                    profile_id   TEXT NOT NULL,
                    cron         TEXT NOT NULL,
                    enabled      INTEGER NOT NULL DEFAULT 1,
                    last_run_at  INTEGER,
                    next_run_at  INTEGER,
                    created_at   INTEGER NOT NULL,
                    FOREIGN KEY (profile_id) REFERENCES migration_profiles(id) ON DELETE CASCADE
                )
                """);
            st.execute("CREATE INDEX IF NOT EXISTS idx_migration_schedules_next_run " +
                    "ON migration_schedules(enabled, next_run_at)");

            // v2.1.0 Monitoring — see docs/v2/v2.1/milestone-v2.1.0.md §3.1.
            st.execute("""
                CREATE TABLE IF NOT EXISTS monitoring_profiles (
                    connection_id            TEXT PRIMARY KEY,
                    enabled                  INTEGER NOT NULL DEFAULT 1,
                    poll_interval_ms         INTEGER NOT NULL DEFAULT 1000,
                    storage_poll_ms          INTEGER NOT NULL DEFAULT 60000,
                    index_poll_ms            INTEGER NOT NULL DEFAULT 300000,
                    read_preference          TEXT    NOT NULL DEFAULT 'secondaryPreferred',
                    profiler_enabled         INTEGER NOT NULL DEFAULT 0,
                    profiler_slowms          INTEGER NOT NULL DEFAULT 100,
                    profiler_auto_disable_ms INTEGER NOT NULL DEFAULT 3600000,
                    topn_colls_per_db        INTEGER NOT NULL DEFAULT 50,
                    retention_raw_ms         INTEGER NOT NULL DEFAULT 86400000,
                    retention_s10_ms         INTEGER NOT NULL DEFAULT 604800000,
                    retention_m1_ms          INTEGER NOT NULL DEFAULT 7776000000,
                    retention_h1_ms          INTEGER NOT NULL DEFAULT 31536000000,
                    created_at               INTEGER NOT NULL,
                    updated_at               INTEGER NOT NULL
                )
                """);

            st.execute("""
                CREATE TABLE IF NOT EXISTS metric_samples_raw (
                    connection_id TEXT    NOT NULL,
                    metric        TEXT    NOT NULL,
                    labels_json   TEXT    NOT NULL,
                    ts            INTEGER NOT NULL,
                    value         REAL    NOT NULL,
                    PRIMARY KEY (connection_id, metric, labels_json, ts)
                )
                """);
            st.execute("CREATE INDEX IF NOT EXISTS idx_samples_raw_ts ON metric_samples_raw(connection_id, metric, ts)");

            for (String tier : new String[] { "10s", "1m", "1h" }) {
                st.execute("""
                    CREATE TABLE IF NOT EXISTS metric_samples_%s (
                        connection_id TEXT    NOT NULL,
                        metric        TEXT    NOT NULL,
                        labels_json   TEXT    NOT NULL,
                        ts            INTEGER NOT NULL,
                        min_v         REAL,
                        max_v         REAL,
                        avg_v         REAL,
                        p95_v         REAL,
                        p99_v         REAL,
                        cnt           INTEGER NOT NULL,
                        PRIMARY KEY (connection_id, metric, labels_json, ts)
                    )
                    """.formatted(tier));
                st.execute("CREATE INDEX IF NOT EXISTS idx_samples_%s_ts ON metric_samples_%s(connection_id, metric, ts)"
                        .formatted(tier, tier));
            }

            st.execute("""
                CREATE TABLE IF NOT EXISTS alert_rules (
                    id             TEXT PRIMARY KEY,
                    connection_id  TEXT,
                    metric         TEXT NOT NULL,
                    label_filter   TEXT,
                    comparator     TEXT NOT NULL,
                    warn_threshold REAL,
                    crit_threshold REAL,
                    for_seconds    INTEGER NOT NULL DEFAULT 60,
                    enabled        INTEGER NOT NULL DEFAULT 1,
                    source         TEXT,
                    created_at     INTEGER NOT NULL
                )
                """);

            st.execute("""
                CREATE TABLE IF NOT EXISTS alert_events (
                    id             TEXT PRIMARY KEY,
                    rule_id        TEXT NOT NULL,
                    connection_id  TEXT NOT NULL,
                    fired_at       INTEGER NOT NULL,
                    cleared_at     INTEGER,
                    severity       TEXT NOT NULL,
                    value_at_fire  REAL NOT NULL,
                    message        TEXT NOT NULL,
                    FOREIGN KEY (rule_id) REFERENCES alert_rules(id)
                )
                """);
            st.execute("CREATE INDEX IF NOT EXISTS idx_alert_events_fired_at ON alert_events(fired_at DESC)");

            st.execute("""
                CREATE TABLE IF NOT EXISTS profiler_samples (
                    connection_id   TEXT NOT NULL,
                    ts              INTEGER NOT NULL,
                    ns              TEXT NOT NULL,
                    op              TEXT NOT NULL,
                    millis          INTEGER NOT NULL,
                    plan_summary    TEXT,
                    docs_examined   INTEGER,
                    docs_returned   INTEGER,
                    keys_examined   INTEGER,
                    num_yield       INTEGER,
                    response_length INTEGER,
                    query_hash      TEXT,
                    plan_cache_key  TEXT,
                    command_json    TEXT NOT NULL,
                    PRIMARY KEY (connection_id, ts, ns, query_hash)
                )
                """);

            // v2.3.0 Monitoring Recording & Analysis — see docs/v2/v2.3/technical-spec.md §3.2.
            // Retention-exempt storage — the janitor never touches these tables.
            st.execute("""
                CREATE TABLE IF NOT EXISTS recordings (
                    id               TEXT PRIMARY KEY,
                    connection_id    TEXT NOT NULL,
                    name             TEXT NOT NULL,
                    note             TEXT,
                    tags_json        TEXT,
                    state            TEXT NOT NULL,      -- ACTIVE | PAUSED | STOPPED
                    stop_reason      TEXT,               -- MANUAL | AUTO_DURATION | AUTO_SIZE | CONNECTION_LOST | INTERRUPTED
                    started_at       INTEGER NOT NULL,
                    ended_at         INTEGER,
                    paused_millis    INTEGER NOT NULL DEFAULT 0,
                    max_duration_ms  INTEGER,
                    max_size_bytes   INTEGER,
                    capture_profiler INTEGER NOT NULL DEFAULT 0,
                    schema_version   INTEGER NOT NULL DEFAULT 1,
                    created_at       INTEGER NOT NULL
                )
                """);
            st.execute("CREATE INDEX IF NOT EXISTS idx_recordings_connection ON recordings(connection_id)");
            st.execute("CREATE INDEX IF NOT EXISTS idx_recordings_started    ON recordings(started_at DESC)");

            st.execute("""
                CREATE TABLE IF NOT EXISTS recording_samples (
                    recording_id  TEXT    NOT NULL,
                    connection_id TEXT    NOT NULL,
                    metric        TEXT    NOT NULL,
                    labels_json   TEXT    NOT NULL,
                    ts            INTEGER NOT NULL,
                    value         REAL    NOT NULL,
                    PRIMARY KEY (recording_id, metric, labels_json, ts),
                    FOREIGN KEY (recording_id) REFERENCES recordings(id) ON DELETE CASCADE
                )
                """);
            st.execute("CREATE INDEX IF NOT EXISTS idx_rec_samples_rec_ts        ON recording_samples(recording_id, ts)");
            st.execute("CREATE INDEX IF NOT EXISTS idx_rec_samples_rec_metric_ts ON recording_samples(recording_id, metric, ts)");

            st.execute("""
                CREATE TABLE IF NOT EXISTS recording_profile_samples (
                    recording_id    TEXT NOT NULL,
                    connection_id   TEXT NOT NULL,
                    ts              INTEGER NOT NULL,
                    ns              TEXT NOT NULL,
                    op              TEXT NOT NULL,
                    millis          INTEGER NOT NULL,
                    plan_summary    TEXT,
                    docs_examined   INTEGER,
                    docs_returned   INTEGER,
                    keys_examined   INTEGER,
                    num_yield       INTEGER,
                    response_length INTEGER,
                    query_hash      TEXT,
                    plan_cache_key  TEXT,
                    command_json    TEXT NOT NULL,
                    PRIMARY KEY (recording_id, ts, ns, query_hash),
                    FOREIGN KEY (recording_id) REFERENCES recordings(id) ON DELETE CASCADE
                )
                """);

            // v2.3.1 Recording Annotations — see docs/v2/v2.3/v2.3.1/technical-spec.md §3.1.
            st.execute("""
                CREATE TABLE IF NOT EXISTS recording_annotations (
                    id            TEXT PRIMARY KEY,
                    recording_id  TEXT    NOT NULL,
                    ts_ms         INTEGER NOT NULL,
                    label         TEXT    NOT NULL,
                    note          TEXT,
                    variant       TEXT    NOT NULL DEFAULT 'INFO',
                    created_at    INTEGER NOT NULL,
                    updated_at    INTEGER NOT NULL,
                    FOREIGN KEY (recording_id) REFERENCES recordings(id) ON DELETE CASCADE
                )
                """);
            st.execute("CREATE INDEX IF NOT EXISTS idx_ann_recording_ts ON recording_annotations(recording_id, ts_ms)");

            // v2.4.0 Cluster Operations — see docs/v2/v2.4/milestone-v2.4.0.md §3.1.
            st.execute("""
                CREATE TABLE IF NOT EXISTS topology_snapshots (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    connection_id   TEXT    NOT NULL,
                    captured_at     INTEGER NOT NULL,
                    cluster_kind    TEXT    NOT NULL,
                    snapshot_json   TEXT    NOT NULL,
                    sha256          TEXT    NOT NULL,
                    version_major   INTEGER NOT NULL,
                    version_minor   INTEGER NOT NULL,
                    member_count    INTEGER NOT NULL,
                    shard_count     INTEGER NOT NULL DEFAULT 0,
                    UNIQUE (connection_id, captured_at, sha256)
                )
                """);
            st.execute("CREATE INDEX IF NOT EXISTS idx_topology_conn_time " +
                    "ON topology_snapshots(connection_id, captured_at)");
            st.execute("CREATE INDEX IF NOT EXISTS idx_topology_sha256 " +
                    "ON topology_snapshots(sha256)");

            st.execute("""
                CREATE TABLE IF NOT EXISTS ops_audit (
                    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
                    connection_id         TEXT    NOT NULL,
                    db                    TEXT,
                    coll                  TEXT,
                    command_name          TEXT    NOT NULL,
                    command_json_redacted TEXT    NOT NULL,
                    preview_hash          TEXT    NOT NULL,
                    outcome               TEXT    NOT NULL,
                    server_message        TEXT,
                    role_used             TEXT,
                    started_at            INTEGER NOT NULL,
                    finished_at           INTEGER,
                    latency_ms            INTEGER,
                    caller_host           TEXT,
                    caller_user           TEXT,
                    ui_source             TEXT    NOT NULL,
                    paste                 INTEGER NOT NULL DEFAULT 0,
                    kill_switch           INTEGER NOT NULL DEFAULT 0
                )
                """);
            st.execute("CREATE INDEX IF NOT EXISTS idx_ops_audit_conn_time " +
                    "ON ops_audit(connection_id, started_at)");
            st.execute("CREATE INDEX IF NOT EXISTS idx_ops_audit_command " +
                    "ON ops_audit(command_name, started_at)");
            st.execute("CREATE INDEX IF NOT EXISTS idx_ops_audit_outcome " +
                    "ON ops_audit(outcome, started_at)");

            st.execute("""
                CREATE TABLE IF NOT EXISTS role_cache (
                    connection_id TEXT PRIMARY KEY,
                    roles_json    TEXT NOT NULL,
                    probed_at     INTEGER NOT NULL
                )
                """);
        }
    }

    @Override
    public void close() throws SQLException { connection.close(); }
}

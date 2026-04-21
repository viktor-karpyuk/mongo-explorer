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
        }
    }

    @Override
    public void close() throws SQLException { connection.close(); }
}

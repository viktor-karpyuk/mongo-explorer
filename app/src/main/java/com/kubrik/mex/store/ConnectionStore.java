package com.kubrik.mex.store;

import com.kubrik.mex.model.MongoConnection;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class ConnectionStore {

    private static final String COLS =
            "id,name,mode,uri,enc_password,default_db,tls,allow_invalid_certs," +
            "connection_type,hosts,srv_host," +
            "auth_mode,username,auth_db,gssapi_service_name,aws_session_token," +
            "tls_ca_file,tls_client_cert_file,enc_tls_client_cert_password,tls_allow_invalid_hostnames," +
            "ssh_enabled,ssh_host,ssh_port,ssh_user,ssh_auth_mode,enc_ssh_password,ssh_key_file,enc_ssh_key_passphrase," +
            "proxy_type,proxy_host,proxy_port,proxy_user,enc_proxy_password," +
            "replica_set_name,read_preference,app_name,manual_uri_options," +
            "created_at,updated_at";

    private final Database db;

    public ConnectionStore(Database db) { this.db = db; }

    public List<MongoConnection> list() {
        List<MongoConnection> out = new ArrayList<>();
        try (PreparedStatement ps = db.connection().prepareStatement(
                "SELECT " + COLS + " FROM connections ORDER BY name")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) out.add(map(rs));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return out;
    }

    public MongoConnection get(String id) {
        try (PreparedStatement ps = db.connection().prepareStatement(
                "SELECT " + COLS + " FROM connections WHERE id=?")) {
            ps.setString(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? map(rs) : null;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public MongoConnection upsert(MongoConnection c) {
        long now = System.currentTimeMillis();
        String id = c.id() != null ? c.id() : UUID.randomUUID().toString();
        long created = c.createdAt() == 0 ? now : c.createdAt();
        String sql = """
                INSERT INTO connections(""" + COLS + """
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(id) DO UPDATE SET
                    name=excluded.name, mode=excluded.mode, uri=excluded.uri,
                    enc_password=excluded.enc_password, default_db=excluded.default_db,
                    tls=excluded.tls, allow_invalid_certs=excluded.allow_invalid_certs,
                    connection_type=excluded.connection_type, hosts=excluded.hosts, srv_host=excluded.srv_host,
                    auth_mode=excluded.auth_mode, username=excluded.username, auth_db=excluded.auth_db,
                    gssapi_service_name=excluded.gssapi_service_name, aws_session_token=excluded.aws_session_token,
                    tls_ca_file=excluded.tls_ca_file, tls_client_cert_file=excluded.tls_client_cert_file,
                    enc_tls_client_cert_password=excluded.enc_tls_client_cert_password,
                    tls_allow_invalid_hostnames=excluded.tls_allow_invalid_hostnames,
                    ssh_enabled=excluded.ssh_enabled, ssh_host=excluded.ssh_host, ssh_port=excluded.ssh_port,
                    ssh_user=excluded.ssh_user, ssh_auth_mode=excluded.ssh_auth_mode,
                    enc_ssh_password=excluded.enc_ssh_password, ssh_key_file=excluded.ssh_key_file,
                    enc_ssh_key_passphrase=excluded.enc_ssh_key_passphrase,
                    proxy_type=excluded.proxy_type, proxy_host=excluded.proxy_host, proxy_port=excluded.proxy_port,
                    proxy_user=excluded.proxy_user, enc_proxy_password=excluded.enc_proxy_password,
                    replica_set_name=excluded.replica_set_name, read_preference=excluded.read_preference,
                    app_name=excluded.app_name, manual_uri_options=excluded.manual_uri_options,
                    updated_at=excluded.updated_at
                """;
        synchronized (db.writeLock()) {
        try (PreparedStatement ps = db.connection().prepareStatement(sql)) {
            int i = 1;
            ps.setString(i++, id);
            ps.setString(i++, c.name());
            ps.setString(i++, c.mode());
            ps.setString(i++, n(c.uri()));
            ps.setString(i++, c.encPassword());
            ps.setString(i++, c.defaultDb());
            ps.setInt(i++, c.tlsEnabled() ? 1 : 0);
            ps.setInt(i++, c.tlsAllowInvalidCertificates() ? 1 : 0);
            ps.setString(i++, c.connectionType());
            ps.setString(i++, c.hosts());
            ps.setString(i++, c.srvHost());
            ps.setString(i++, c.authMode());
            ps.setString(i++, c.username());
            ps.setString(i++, c.authDb());
            ps.setString(i++, c.gssapiServiceName());
            ps.setString(i++, c.awsSessionToken());
            ps.setString(i++, c.tlsCaFile());
            ps.setString(i++, c.tlsClientCertFile());
            ps.setString(i++, c.encTlsClientCertPassword());
            ps.setInt(i++, c.tlsAllowInvalidHostnames() ? 1 : 0);
            ps.setInt(i++, c.sshEnabled() ? 1 : 0);
            ps.setString(i++, c.sshHost());
            ps.setInt(i++, c.sshPort());
            ps.setString(i++, c.sshUser());
            ps.setString(i++, c.sshAuthMode());
            ps.setString(i++, c.encSshPassword());
            ps.setString(i++, c.sshKeyFile());
            ps.setString(i++, c.encSshKeyPassphrase());
            ps.setString(i++, c.proxyType());
            ps.setString(i++, c.proxyHost());
            ps.setInt(i++, c.proxyPort());
            ps.setString(i++, c.proxyUser());
            ps.setString(i++, c.encProxyPassword());
            ps.setString(i++, c.replicaSetName());
            ps.setString(i++, c.readPreference());
            ps.setString(i++, c.appName());
            ps.setString(i++, c.manualUriOptions());
            ps.setLong(i++, created);
            ps.setLong(i, now);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        }  // synchronized (db.writeLock())
        return get(id);
    }

    /**
     * v2.8.4 LAB-CONN-1 — Inserts a minimal Lab-origin connection
     * row and returns its id. Kept separate from {@link #upsert} so
     * the 35-column general path doesn't need to know about the
     * lab-specific fields.
     *
     * @param name   user-visible name ({@code display_name} in the
     *               lab_deployments row).
     * @param uri    {@code mongodb://localhost:<port>[/?replicaSet=X]}.
     * @param labDeploymentId FK back-pointer to lab_deployments.id.
     * @return the new connection's UUID.
     */
    public String insertLabOrigin(String name, String uri, long labDeploymentId) {
        long now = System.currentTimeMillis();
        String id = UUID.randomUUID().toString();
        // First insert the row via a minimal raw INSERT so we don't
        // depend on upsert's column-by-column ritual; then stamp the
        // two labs-only columns (origin, lab_deployment_id) that
        // upsert doesn't know about.
        String sql = """
                INSERT INTO connections(id, name, mode, uri, tls, allow_invalid_certs,
                    connection_type, hosts, auth_mode, ssh_auth_mode,
                    ssh_port, proxy_type, proxy_port, read_preference,
                    tls_allow_invalid_hostnames, ssh_enabled,
                    created_at, updated_at, origin, lab_deployment_id)
                VALUES (?, ?, 'FORM', ?, 0, 0, 'STANDALONE', '',
                    'NONE', 'PASSWORD', 22, 'NONE', 1080, 'primary',
                    0, 0, ?, ?, 'LAB', ?)
                """;
        synchronized (db.writeLock()) {
            try (PreparedStatement ps = db.connection().prepareStatement(sql)) {
                ps.setString(1, id);
                ps.setString(2, name);
                ps.setString(3, uri);
                ps.setLong(4, now);
                ps.setLong(5, now);
                ps.setLong(6, labDeploymentId);
                ps.executeUpdate();
                return id;
            } catch (SQLException e) {
                throw new RuntimeException("lab-origin connection insert failed", e);
            }
        }
    }

    /**
     * v2.8.1 Q2.8.1-B3 — Minimal {@code origin='K8S'} insert for the
     * "connect to existing" flow.
     *
     * <p>Mirrors {@link #insertLabOrigin} in style: skips the upsert
     * ritual and just writes the columns that matter. The URI
     * initially points at the in-cluster Service name; once
     * Q2.8.1-C ships the port-forward service the wiring rewrites it
     * to {@code mongodb://127.0.0.1:<localPort>} for the duration of
     * the session. Persisting the Service-name form keeps the row
     * recognizable and re-openable in future sessions.</p>
     */
    public String insertK8sOrigin(String name, String uri) {
        long now = System.currentTimeMillis();
        String id = UUID.randomUUID().toString();
        String sql = """
                INSERT INTO connections(id, name, mode, uri, tls, allow_invalid_certs,
                    connection_type, hosts, auth_mode, ssh_auth_mode,
                    ssh_port, proxy_type, proxy_port, read_preference,
                    tls_allow_invalid_hostnames, ssh_enabled,
                    created_at, updated_at, origin)
                VALUES (?, ?, 'FORM', ?, 0, 0, 'STANDALONE', '',
                    'NONE', 'PASSWORD', 22, 'NONE', 1080, 'primary',
                    0, 0, ?, ?, 'K8S')
                """;
        synchronized (db.writeLock()) {
            try (PreparedStatement ps = db.connection().prepareStatement(sql)) {
                ps.setString(1, id);
                ps.setString(2, name);
                ps.setString(3, uri);
                ps.setLong(4, now);
                ps.setLong(5, now);
                ps.executeUpdate();
                return id;
            } catch (SQLException e) {
                throw new RuntimeException("k8s-origin connection insert failed", e);
            }
        }
    }

    public void delete(String id) {
        // Cascade delete: SQLite's FK constraints are off by default, so we
        // perform the v2.4 cluster-table cleanup manually inside a single
        // transaction (spec §3.1 AUD-RET / TOPO / ROLE). Older tables that
        // reference the connection id (migrations, monitoring) are expected
        // to cope with stale rows pointing at a removed id — none of them
        // treats the id as a foreign key.
        //
        // The whole block is held under db.writeLock() because
        // setAutoCommit(false) mutates the SHARED JDBC connection; without
        // the lock, a concurrent monitoring-sampler write would be pulled
        // into this transaction (its commit would commit our deletes, or
        // its rollback would roll them back).
        synchronized (db.writeLock()) {
            java.sql.Connection c = db.connection();
            try {
                boolean prevAuto = c.getAutoCommit();
                c.setAutoCommit(false);
                try {
                    deleteFrom(c, "ops_audit", id);
                    deleteFrom(c, "topology_snapshots", id);
                    deleteFrom(c, "role_cache", id);
                    deleteFrom(c, "connections", id);
                    c.commit();
                } catch (SQLException inner) {
                    try { c.rollback(); } catch (SQLException ignored) {}
                    throw inner;
                } finally {
                    c.setAutoCommit(prevAuto);
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static void deleteFrom(java.sql.Connection c, String table, String id) throws SQLException {
        String sql = "DELETE FROM " + table + " WHERE "
                + ("connections".equals(table) ? "id" : "connection_id") + " = ?";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, id);
            ps.executeUpdate();
        }
    }

    private static String n(String s) { return s == null ? "" : s; }

    private MongoConnection map(ResultSet rs) throws SQLException {
        return new MongoConnection(
                rs.getString("id"),
                rs.getString("name"),
                rs.getString("mode"),
                rs.getString("uri"),
                rs.getString("connection_type"),
                rs.getString("hosts"),
                rs.getString("srv_host"),
                rs.getString("auth_mode"),
                rs.getString("username"),
                rs.getString("enc_password"),
                rs.getString("auth_db"),
                rs.getString("gssapi_service_name"),
                rs.getString("aws_session_token"),
                rs.getInt("tls") == 1,
                rs.getString("tls_ca_file"),
                rs.getString("tls_client_cert_file"),
                rs.getString("enc_tls_client_cert_password"),
                rs.getInt("tls_allow_invalid_hostnames") == 1,
                rs.getInt("allow_invalid_certs") == 1,
                rs.getInt("ssh_enabled") == 1,
                rs.getString("ssh_host"),
                rs.getInt("ssh_port"),
                rs.getString("ssh_user"),
                rs.getString("ssh_auth_mode"),
                rs.getString("enc_ssh_password"),
                rs.getString("ssh_key_file"),
                rs.getString("enc_ssh_key_passphrase"),
                rs.getString("proxy_type"),
                rs.getString("proxy_host"),
                rs.getInt("proxy_port"),
                rs.getString("proxy_user"),
                rs.getString("enc_proxy_password"),
                rs.getString("replica_set_name"),
                rs.getString("read_preference"),
                rs.getString("default_db"),
                rs.getString("app_name"),
                rs.getString("manual_uri_options"),
                rs.getLong("created_at"),
                rs.getLong("updated_at")
        );
    }
}

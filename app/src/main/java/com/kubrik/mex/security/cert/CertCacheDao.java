package com.kubrik.mex.security.cert;

import com.kubrik.mex.store.Database;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * v2.6 Q2.6-E1 — DAO for {@code sec_cert_cache}. Upsert semantics keyed
 * on the unique {@code (connection_id, host, fingerprint_sha256)} index
 * so reprobing an unchanged cert refreshes the {@code captured_at}
 * timestamp (for the Q2.6-E3 daily expiry check) without producing a
 * duplicate row.
 */
public final class CertCacheDao {

    private final Database db;

    public CertCacheDao(Database db) { this.db = db; }

    public record Row(
            long id,
            String connectionId,
            String host,
            String subjectCn,
            String issuerCn,
            String sansJson,
            Long notBefore,
            Long notAfter,
            String serialHex,
            String fingerprintSha256,
            long capturedAt
    ) {}

    /** Write-or-refresh an observed cert. Returns the row after the
     *  upsert. Duplicate-cert detection is driver-agnostic because SQLite
     *  supports {@code ON CONFLICT} resolution via the unique index. */
    public Row upsert(String connectionId, CertRecord cert, long capturedAt) {
        String sansJson = toJsonArray(cert.sans());
        synchronized (db.writeLock()) {
            try (PreparedStatement ps = db.connection().prepareStatement("""
                    INSERT INTO sec_cert_cache(connection_id, host, subject_cn, issuer_cn,
                        sans_json, not_before, not_after, serial_hex,
                        fingerprint_sha256, captured_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(connection_id, host, fingerprint_sha256)
                    DO UPDATE SET captured_at = excluded.captured_at
                    """, Statement.RETURN_GENERATED_KEYS)) {
                ps.setString(1, connectionId);
                ps.setString(2, cert.host());
                ps.setString(3, cert.subjectCn());
                ps.setString(4, cert.issuerCn());
                ps.setString(5, sansJson);
                ps.setLong(6, cert.notBefore());
                ps.setLong(7, cert.notAfter());
                ps.setString(8, cert.serialHex());
                ps.setString(9, cert.fingerprintSha256());
                ps.setLong(10, capturedAt);
                ps.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException("sec_cert_cache upsert failed", e);
            }
        }
        return byFingerprint(connectionId, cert.host(), cert.fingerprintSha256())
                .orElseThrow(() ->
                        new RuntimeException("cert not readable after upsert"));
    }

    public java.util.Optional<Row> byFingerprint(String connectionId, String host, String fingerprint) {
        try (PreparedStatement ps = db.connection().prepareStatement(
                "SELECT * FROM sec_cert_cache WHERE connection_id = ? " +
                        "AND host = ? AND fingerprint_sha256 = ?")) {
            ps.setString(1, connectionId);
            ps.setString(2, host);
            ps.setString(3, fingerprint);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? java.util.Optional.of(map(rs)) : java.util.Optional.empty();
            }
        } catch (SQLException e) {
            return java.util.Optional.empty();
        }
    }

    public List<Row> listForConnection(String connectionId) {
        try (PreparedStatement ps = db.connection().prepareStatement(
                "SELECT * FROM sec_cert_cache WHERE connection_id = ? " +
                        "ORDER BY not_after ASC, host ASC")) {
            ps.setString(1, connectionId);
            List<Row> out = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) out.add(map(rs));
            }
            return out;
        } catch (SQLException e) {
            return List.of();
        }
    }

    /** Certificates whose {@code not_after} is before {@code cutoffMs}.
     *  Drives the daily expiry check + Welcome-card badge. */
    public List<Row> listExpiringBefore(String connectionId, long cutoffMs) {
        try (PreparedStatement ps = db.connection().prepareStatement(
                "SELECT * FROM sec_cert_cache WHERE connection_id = ? " +
                        "AND not_after IS NOT NULL AND not_after < ? " +
                        "ORDER BY not_after ASC")) {
            ps.setString(1, connectionId);
            ps.setLong(2, cutoffMs);
            List<Row> out = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) out.add(map(rs));
            }
            return out;
        } catch (SQLException e) {
            return List.of();
        }
    }

    /* ============================== mapping =============================== */

    private static Row map(ResultSet rs) throws SQLException {
        Long nb = rs.getLong("not_before");
        if (rs.wasNull()) nb = null;
        Long na = rs.getLong("not_after");
        if (rs.wasNull()) na = null;
        return new Row(
                rs.getLong("id"),
                rs.getString("connection_id"),
                rs.getString("host"),
                rs.getString("subject_cn"),
                rs.getString("issuer_cn"),
                rs.getString("sans_json"),
                nb, na,
                rs.getString("serial_hex"),
                rs.getString("fingerprint_sha256"),
                rs.getLong("captured_at"));
    }

    private static String toJsonArray(List<String> sans) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < sans.size(); i++) {
            if (i > 0) sb.append(',');
            sb.append('"').append(sans.get(i).replace("\"", "\\\"")).append('"');
        }
        return sb.append(']').toString();
    }
}

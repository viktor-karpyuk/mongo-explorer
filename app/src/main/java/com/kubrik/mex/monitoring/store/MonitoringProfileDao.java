package com.kubrik.mex.monitoring.store;

import com.kubrik.mex.monitoring.MonitoringProfile;
import com.kubrik.mex.monitoring.model.RollupTier;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** CRUD over {@code monitoring_profiles}. */
public final class MonitoringProfileDao {

    private static final String UPSERT_SQL = """
            INSERT INTO monitoring_profiles
              (connection_id, enabled, poll_interval_ms, storage_poll_ms, index_poll_ms,
               read_preference, profiler_enabled, profiler_slowms, profiler_auto_disable_ms,
               topn_colls_per_db, retention_raw_ms, retention_s10_ms, retention_m1_ms, retention_h1_ms,
               created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(connection_id) DO UPDATE SET
              enabled                  = excluded.enabled,
              poll_interval_ms         = excluded.poll_interval_ms,
              storage_poll_ms          = excluded.storage_poll_ms,
              index_poll_ms            = excluded.index_poll_ms,
              read_preference          = excluded.read_preference,
              profiler_enabled         = excluded.profiler_enabled,
              profiler_slowms          = excluded.profiler_slowms,
              profiler_auto_disable_ms = excluded.profiler_auto_disable_ms,
              topn_colls_per_db        = excluded.topn_colls_per_db,
              retention_raw_ms         = excluded.retention_raw_ms,
              retention_s10_ms         = excluded.retention_s10_ms,
              retention_m1_ms          = excluded.retention_m1_ms,
              retention_h1_ms          = excluded.retention_h1_ms,
              updated_at               = excluded.updated_at
            """;

    private static final String SELECT_SQL = """
            SELECT connection_id, enabled, poll_interval_ms, storage_poll_ms, index_poll_ms,
                   read_preference, profiler_enabled, profiler_slowms, profiler_auto_disable_ms,
                   topn_colls_per_db, retention_raw_ms, retention_s10_ms, retention_m1_ms, retention_h1_ms,
                   created_at, updated_at
              FROM monitoring_profiles
             WHERE connection_id = ?
            """;

    private final Connection conn;

    public MonitoringProfileDao(Connection conn) { this.conn = conn; }

    public void upsert(MonitoringProfile p) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(UPSERT_SQL)) {
            ps.setString(1, p.connectionId());
            ps.setInt   (2, p.enabled() ? 1 : 0);
            ps.setLong  (3, p.instancePollInterval().toMillis());
            ps.setLong  (4, p.storagePollInterval().toMillis());
            ps.setLong  (5, p.indexUsagePollInterval().toMillis());
            ps.setString(6, p.readPreference());
            ps.setInt   (7, p.profilerEnabled() ? 1 : 0);
            ps.setInt   (8, p.profilerSlowMs());
            ps.setLong  (9, p.profilerAutoDisableAfter().toMillis());
            ps.setInt   (10, p.topNCollectionsPerDb());
            ps.setLong  (11, p.retention().get(RollupTier.RAW).toMillis());
            ps.setLong  (12, p.retention().get(RollupTier.S10).toMillis());
            ps.setLong  (13, p.retention().get(RollupTier.M1).toMillis());
            ps.setLong  (14, p.retention().get(RollupTier.H1).toMillis());
            ps.setLong  (15, p.createdAt().toEpochMilli());
            ps.setLong  (16, p.updatedAt().toEpochMilli());
            ps.executeUpdate();
        }
    }

    public Optional<MonitoringProfile> find(String connectionId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SELECT_SQL)) {
            ps.setString(1, connectionId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                Map<RollupTier, Duration> ret = new EnumMap<>(RollupTier.class);
                ret.put(RollupTier.RAW, Duration.ofMillis(rs.getLong("retention_raw_ms")));
                ret.put(RollupTier.S10, Duration.ofMillis(rs.getLong("retention_s10_ms")));
                ret.put(RollupTier.M1,  Duration.ofMillis(rs.getLong("retention_m1_ms")));
                ret.put(RollupTier.H1,  Duration.ofMillis(rs.getLong("retention_h1_ms")));
                return Optional.of(new MonitoringProfile(
                        rs.getString("connection_id"),
                        rs.getInt("enabled") != 0,
                        Duration.ofMillis(rs.getLong("poll_interval_ms")),
                        Duration.ofMillis(rs.getLong("storage_poll_ms")),
                        Duration.ofMillis(rs.getLong("index_poll_ms")),
                        rs.getString("read_preference"),
                        rs.getInt("profiler_enabled") != 0,
                        rs.getInt("profiler_slowms"),
                        Duration.ofMillis(rs.getLong("profiler_auto_disable_ms")),
                        rs.getInt("topn_colls_per_db"),
                        List.of(),
                        ret,
                        Instant.ofEpochMilli(rs.getLong("created_at")),
                        Instant.ofEpochMilli(rs.getLong("updated_at"))
                ));
            }
        }
    }

    public void delete(String connectionId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "DELETE FROM monitoring_profiles WHERE connection_id = ?")) {
            ps.setString(1, connectionId);
            ps.executeUpdate();
        }
    }
}

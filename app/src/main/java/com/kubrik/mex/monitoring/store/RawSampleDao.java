package com.kubrik.mex.monitoring.store;

import com.kubrik.mex.monitoring.model.LabelSet;
import com.kubrik.mex.monitoring.model.MetricId;
import com.kubrik.mex.monitoring.model.MetricSample;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Raw-tier ({@code metric_samples_raw}) DAO. Inserts use {@code INSERT OR REPLACE}
 * so retries on the same {@code (connection, metric, labels, ts)} key are idempotent.
 *
 * <p>This DAO has no internal state and can be shared across threads; concurrency
 * control lives in the single-writer {@link MetricStore} queue above it.
 */
public final class RawSampleDao {

    private static final String INSERT_SQL = """
            INSERT OR REPLACE INTO metric_samples_raw
                (connection_id, metric, labels_json, ts, value)
            VALUES (?, ?, ?, ?, ?)
            """;

    private static final String SELECT_RANGE_SQL = """
            SELECT connection_id, metric, labels_json, ts, value
              FROM metric_samples_raw
             WHERE connection_id = ?
               AND metric = ?
               AND ts >= ?
               AND ts <  ?
             ORDER BY ts ASC
            """;

    private final Connection conn;

    public RawSampleDao(Connection conn) { this.conn = conn; }

    /** Insert one batch transactionally. */
    public int insertBatch(List<MetricSample> samples) throws SQLException {
        if (samples.isEmpty()) return 0;
        boolean prevAuto = conn.getAutoCommit();
        conn.setAutoCommit(false);
        try (PreparedStatement ps = conn.prepareStatement(INSERT_SQL)) {
            for (MetricSample s : samples) {
                ps.setString(1, s.connectionId());
                ps.setString(2, s.metric().metricName());
                ps.setString(3, s.labels().toJson());
                ps.setLong  (4, s.tsMs());
                ps.setDouble(5, s.value());
                ps.addBatch();
            }
            int[] rc = ps.executeBatch();
            conn.commit();
            int total = 0;
            for (int r : rc) if (r > 0) total += r;
            return total;
        } catch (SQLException e) {
            try { conn.rollback(); } catch (SQLException ignored) {}
            throw e;
        } finally {
            conn.setAutoCommit(prevAuto);
        }
    }

    /** Load all raw samples for a metric+connection in {@code [fromMsInclusive, toMsExclusive)}. */
    public List<MetricSample> loadRange(String connectionId, MetricId metric,
                                        long fromMsInclusive, long toMsExclusive) throws SQLException {
        List<MetricSample> out = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(SELECT_RANGE_SQL)) {
            ps.setString(1, connectionId);
            ps.setString(2, metric.metricName());
            ps.setLong(3, fromMsInclusive);
            ps.setLong(4, toMsExclusive);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String labelsJson = rs.getString("labels_json");
                    LabelSet labels = LabelSetJson.parse(labelsJson);
                    out.add(new MetricSample(
                            rs.getString("connection_id"),
                            metric,
                            labels,
                            rs.getLong("ts"),
                            rs.getDouble("value")
                    ));
                }
            }
        }
        return out;
    }

    public long countForConnection(String connectionId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT COUNT(*) FROM metric_samples_raw WHERE connection_id = ?")) {
            ps.setString(1, connectionId);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }
}

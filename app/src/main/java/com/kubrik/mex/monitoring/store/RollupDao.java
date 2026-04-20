package com.kubrik.mex.monitoring.store;

import com.kubrik.mex.monitoring.model.RollupTier;
import com.kubrik.mex.monitoring.util.Percentile;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Aggregates a finer-grained tier into a coarser one. See technical-spec §5.3.
 * {@code min / max / avg / p95 / p99} are computed in-process from the grouped
 * raw values (SQLite lacks {@code percentile_cont}).
 */
public final class RollupDao {

    /** One aggregated row ready to insert into {@code metric_samples_*}. */
    public record Aggregate(
            String connectionId,
            String metric,
            String labelsJson,
            long windowTsMs,
            double min,
            double max,
            double avg,
            double p95,
            double p99,
            int count
    ) {}

    private final Connection conn;

    public RollupDao(Connection conn) { this.conn = conn; }

    /**
     * Aggregate raw samples whose {@code ts} falls in {@code [fromMs, toMs)} into
     * windows of {@code tier.windowSize()}. Returns the aggregates ready to be
     * upserted via {@link #upsert(RollupTier, List)}.
     */
    public List<Aggregate> rollupFromRaw(RollupTier tier, long fromMs, long toMs) throws SQLException {
        return rollupFrom(RollupTier.RAW, tier, fromMs, toMs, /* fromIsRaw */ true);
    }

    /** Roll finer-tier buckets into the coarser tier. */
    public List<Aggregate> rollupFromTier(RollupTier from, RollupTier to, long fromMs, long toMs) throws SQLException {
        if (from == RollupTier.RAW) return rollupFromRaw(to, fromMs, toMs);
        return rollupFrom(from, to, fromMs, toMs, /* fromIsRaw */ false);
    }

    public void upsert(RollupTier tier, List<Aggregate> aggs) throws SQLException {
        if (aggs.isEmpty()) return;
        String sql = "INSERT OR REPLACE INTO " + tier.tableName() + """
                 (connection_id, metric, labels_json, ts, min_v, max_v, avg_v, p95_v, p99_v, cnt)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;
        boolean prevAuto = conn.getAutoCommit();
        conn.setAutoCommit(false);
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (Aggregate a : aggs) {
                ps.setString(1, a.connectionId);
                ps.setString(2, a.metric);
                ps.setString(3, a.labelsJson);
                ps.setLong  (4, a.windowTsMs);
                ps.setDouble(5, a.min);
                ps.setDouble(6, a.max);
                ps.setDouble(7, a.avg);
                ps.setDouble(8, a.p95);
                ps.setDouble(9, a.p99);
                ps.setInt   (10, a.count);
                ps.addBatch();
            }
            ps.executeBatch();
            conn.commit();
        } catch (SQLException e) {
            try { conn.rollback(); } catch (SQLException ignored) {}
            throw e;
        } finally {
            conn.setAutoCommit(prevAuto);
        }
    }

    /** Delete rows older than {@code cutoffMs}. Returns the number of rows removed. */
    public int deleteOlderThan(RollupTier tier, long cutoffMs) throws SQLException {
        String sql = "DELETE FROM " + tier.tableName() + " WHERE ts < ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, cutoffMs);
            return ps.executeUpdate();
        }
    }

    /** Count rows in a tier — used for cap enforcement. */
    public long count(RollupTier tier) throws SQLException {
        String sql = "SELECT COUNT(*) FROM " + tier.tableName();
        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private List<Aggregate> rollupFrom(RollupTier from, RollupTier to,
                                       long fromMs, long toMs, boolean fromIsRaw) throws SQLException {
        long windowMs = to.windowSize().toMillis();
        String sql = fromIsRaw ? """
                SELECT connection_id, metric, labels_json, ts, value
                  FROM metric_samples_raw
                 WHERE ts >= ? AND ts < ?
                """ : """
                SELECT connection_id, metric, labels_json, ts, avg_v, min_v, max_v, cnt
                  FROM %s
                 WHERE ts >= ? AND ts < ?
                """.formatted(from.tableName());

        Map<GroupKey, GroupState> groups = new HashMap<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, fromMs);
            ps.setLong(2, toMs);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String cid = rs.getString(1);
                    String metric = rs.getString(2);
                    String labels = rs.getString(3);
                    long ts = rs.getLong(4);
                    long windowTs = (ts / windowMs) * windowMs;
                    GroupKey k = new GroupKey(cid, metric, labels, windowTs);
                    GroupState st = groups.computeIfAbsent(k, x -> new GroupState());
                    if (fromIsRaw) {
                        st.add(rs.getDouble(5));
                    } else {
                        // When rolling from a tier, use the existing min/max/avg as
                        // representative values and weight by count. We feed (avg)
                        // into the percentile pool — good enough at larger windows.
                        double avg = rs.getDouble(5);
                        double mn  = rs.getDouble(6);
                        double mx  = rs.getDouble(7);
                        int cnt    = rs.getInt(8);
                        st.addWeighted(avg, mn, mx, cnt);
                    }
                }
            }
        }
        List<Aggregate> out = new ArrayList<>(groups.size());
        for (Map.Entry<GroupKey, GroupState> e : groups.entrySet()) {
            GroupKey k = e.getKey();
            GroupState s = e.getValue();
            out.add(s.toAggregate(k));
        }
        return out;
    }

    private record GroupKey(String connectionId, String metric, String labelsJson, long windowTsMs) {}

    private static final class GroupState {
        final java.util.ArrayList<Double> values = new java.util.ArrayList<>(8);
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        double sum;
        int count;

        void add(double v) {
            values.add(v);
            if (v < min) min = v;
            if (v > max) max = v;
            sum += v;
            count++;
        }

        void addWeighted(double avg, double mn, double mx, int n) {
            if (n <= 0) return;
            values.add(avg);
            if (mn < min) min = mn;
            if (mx > max) max = mx;
            sum += avg * n;
            count += n;
        }

        Aggregate toAggregate(GroupKey k) {
            double[] arr = new double[values.size()];
            for (int i = 0; i < arr.length; i++) arr[i] = values.get(i);
            double p95 = Percentile.of(arr.clone(), 0.95);
            double p99 = Percentile.of(arr.clone(), 0.99);
            double avg = count == 0 ? Double.NaN : sum / count;
            return new Aggregate(k.connectionId, k.metric, k.labelsJson, k.windowTsMs,
                    min, max, avg, p95, p99, count);
        }
    }
}

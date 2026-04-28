package com.kubrik.mex.monitoring.recording.store;

import com.kubrik.mex.monitoring.recording.RecordedSample;
import com.kubrik.mex.monitoring.recording.RecordingWriteTask;
import com.kubrik.mex.monitoring.recording.Series;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Batched DAO over {@code recording_samples}. Writes use {@code INSERT OR REPLACE}
 * so retries on the same {@code (recording, metric, labels, ts)} key are idempotent
 * — same invariant as {@link com.kubrik.mex.monitoring.store.RawSampleDao}.
 *
 * <p>Concurrency is handled outside this class by the recording capture writer
 * thread (see technical-spec §4.2).
 */
public final class RecordingSampleDao {

    private static final String INSERT_SQL = """
            INSERT OR REPLACE INTO recording_samples
                (recording_id, connection_id, metric, labels_json, ts, value)
            VALUES (?, ?, ?, ?, ?, ?)
            """;

    /** Keeps {@code recordings.bytes_approx} in sync inside the same transaction as the
     *  insert batch, so {@code RecordingService.onTick} can enforce {@code maxSizeBytes}
     *  via a point read instead of a full-scan SUM(LENGTH) (P2). */
    private static final String BUMP_BYTES_SQL =
            "UPDATE recordings SET bytes_approx = bytes_approx + ? WHERE id = ?";

    private static final String LOAD_RANGE_SQL = """
            SELECT ts, labels_json, value
              FROM recording_samples
             WHERE recording_id = ?
               AND metric = ?
               AND ts >= ?
               AND ts <  ?
             ORDER BY ts ASC
            """;

    private static final String LOAD_RANGE_LABELS_SQL = """
            SELECT ts, labels_json, value
              FROM recording_samples
             WHERE recording_id = ?
               AND metric = ?
               AND labels_json = ?
               AND ts >= ?
               AND ts <  ?
             ORDER BY ts ASC
            """;

    private static final String COUNT_SQL =
            "SELECT COUNT(*) FROM recording_samples WHERE recording_id = ?";

    private static final String COUNT_BY_METRIC_SQL =
            "SELECT COUNT(*) FROM recording_samples WHERE recording_id = ? AND metric = ?";

    private static final String LIST_SERIES_SQL = """
            SELECT metric, labels_json, COUNT(*) AS cnt
              FROM recording_samples
             WHERE recording_id = ?
             GROUP BY metric, labels_json
             ORDER BY metric ASC, labels_json ASC
            """;

    /** Approximation of stored bytes — matches technical-spec §6 (LENGTH + 28 * COUNT). */
    private static final String ESTIMATE_BYTES_SQL = """
            SELECT COALESCE(SUM(LENGTH(labels_json)) + 28 * COUNT(*), 0) AS bytes
              FROM recording_samples
            """;

    private static final String ESTIMATE_BYTES_FOR_RECORDING_SQL = """
            SELECT COALESCE(SUM(LENGTH(labels_json)) + 28 * COUNT(*), 0) AS bytes
              FROM recording_samples
             WHERE recording_id = ?
            """;

    private final Connection conn;

    public RecordingSampleDao(Connection conn) { this.conn = conn; }

    public int insertBatch(List<RecordingWriteTask> batch) throws SQLException {
        if (batch.isEmpty()) return 0;
        // Pre-compute per-recording byte additions to UPDATE in the same transaction.
        java.util.Map<String, Long> bytesPerRecording = new java.util.LinkedHashMap<>();
        for (RecordingWriteTask t : batch) {
            long bytes = (t.labelsJson() == null ? 0 : t.labelsJson().length()) + 28L;
            bytesPerRecording.merge(t.recordingId(), bytes, Long::sum);
        }
        boolean prevAuto = conn.getAutoCommit();
        conn.setAutoCommit(false);
        try {
            int total;
            try (PreparedStatement ps = conn.prepareStatement(INSERT_SQL)) {
                for (RecordingWriteTask t : batch) {
                    ps.setString(1, t.recordingId());
                    ps.setString(2, t.connectionId());
                    ps.setString(3, t.metric());
                    ps.setString(4, t.labelsJson());
                    ps.setLong  (5, t.tsMs());
                    ps.setDouble(6, t.value());
                    ps.addBatch();
                }
                int[] rc = ps.executeBatch();
                total = 0;
                for (int r : rc) if (r > 0) total += r;
            }
            try (PreparedStatement bump = conn.prepareStatement(BUMP_BYTES_SQL)) {
                for (java.util.Map.Entry<String, Long> e : bytesPerRecording.entrySet()) {
                    bump.setLong  (1, e.getValue());
                    bump.setString(2, e.getKey());
                    bump.addBatch();
                }
                bump.executeBatch();
            }
            conn.commit();
            return total;
        } catch (SQLException e) {
            try { conn.rollback(); } catch (SQLException ignored) {}
            throw e;
        } finally {
            conn.setAutoCommit(prevAuto);
        }
    }

    public List<RecordedSample> loadSamples(String recordingId, String metric,
                                            long fromMsInclusive, long toMsExclusive) throws SQLException {
        List<RecordedSample> out = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(LOAD_RANGE_SQL)) {
            ps.setString(1, recordingId);
            ps.setString(2, metric);
            ps.setLong(3, fromMsInclusive);
            ps.setLong(4, toMsExclusive);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new RecordedSample(
                            rs.getLong("ts"),
                            rs.getString("labels_json"),
                            rs.getDouble("value")));
                }
            }
        }
        return out;
    }

    public List<RecordedSample> loadSamples(String recordingId, String metric, String labelsJson,
                                            long fromMsInclusive, long toMsExclusive) throws SQLException {
        List<RecordedSample> out = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(LOAD_RANGE_LABELS_SQL)) {
            ps.setString(1, recordingId);
            ps.setString(2, metric);
            ps.setString(3, labelsJson);
            ps.setLong(4, fromMsInclusive);
            ps.setLong(5, toMsExclusive);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new RecordedSample(
                            rs.getLong("ts"),
                            rs.getString("labels_json"),
                            rs.getDouble("value")));
                }
            }
        }
        return out;
    }

    public long sampleCount(String recordingId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(COUNT_SQL)) {
            ps.setString(1, recordingId);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    public long sampleCount(String recordingId, String metric) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(COUNT_BY_METRIC_SQL)) {
            ps.setString(1, recordingId);
            ps.setString(2, metric);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    public List<Series> listSeries(String recordingId) throws SQLException {
        List<Series> out = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(LIST_SERIES_SQL)) {
            ps.setString(1, recordingId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new Series(
                            rs.getString("metric"),
                            rs.getString("labels_json"),
                            rs.getLong("cnt")));
                }
            }
        }
        return out;
    }

    public long estimateBytes() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(ESTIMATE_BYTES_SQL);
             ResultSet rs = ps.executeQuery()) {
            rs.next();
            return rs.getLong(1);
        }
    }

    public long estimateBytes(String recordingId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(ESTIMATE_BYTES_FOR_RECORDING_SQL)) {
            ps.setString(1, recordingId);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }
}

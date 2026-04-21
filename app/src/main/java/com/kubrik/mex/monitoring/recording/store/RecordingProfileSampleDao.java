package com.kubrik.mex.monitoring.recording.store;

import com.kubrik.mex.monitoring.store.ProfileSampleRecord;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Batched DAO over {@code recording_profile_samples}. Mirrors the shape of
 * {@link com.kubrik.mex.monitoring.store.ProfileSampleDao} but scoped to a
 * recording id — populated only when {@code captureProfilerSamples} is on.
 */
public final class RecordingProfileSampleDao {

    private static final String INSERT_SQL = """
            INSERT OR REPLACE INTO recording_profile_samples
                (recording_id, connection_id, ts, ns, op, millis, plan_summary,
                 docs_examined, docs_returned, keys_examined, num_yield,
                 response_length, query_hash, plan_cache_key, command_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

    private static final String LOAD_RANGE_SQL = """
            SELECT connection_id, ts, ns, op, millis, plan_summary,
                   docs_examined, docs_returned, keys_examined, num_yield,
                   response_length, query_hash, plan_cache_key, command_json
              FROM recording_profile_samples
             WHERE recording_id = ?
               AND ts >= ?
               AND ts <  ?
             ORDER BY ts ASC
            """;

    private static final String COUNT_SQL =
            "SELECT COUNT(*) FROM recording_profile_samples WHERE recording_id = ?";

    private final Connection conn;

    public RecordingProfileSampleDao(Connection conn) { this.conn = conn; }

    public int insertBatch(String recordingId, List<ProfileSampleRecord> samples) throws SQLException {
        if (samples.isEmpty()) return 0;
        boolean prevAuto = conn.getAutoCommit();
        conn.setAutoCommit(false);
        try (PreparedStatement ps = conn.prepareStatement(INSERT_SQL)) {
            for (ProfileSampleRecord s : samples) {
                ps.setString(1, recordingId);
                ps.setString(2, s.connectionId());
                ps.setLong  (3, s.tsMs());
                ps.setString(4, s.ns());
                ps.setString(5, s.op());
                ps.setLong  (6, s.millis());
                ps.setString(7, s.planSummary());
                setNullableLong(ps, 8, s.docsExamined());
                setNullableLong(ps, 9, s.docsReturned());
                setNullableLong(ps, 10, s.keysExamined());
                setNullableLong(ps, 11, s.numYield());
                setNullableLong(ps, 12, s.responseLength());
                ps.setString(13, s.queryHash());
                ps.setString(14, s.planCacheKey());
                ps.setString(15, s.commandJson());
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

    public List<ProfileSampleRecord> loadRange(String recordingId,
                                               long fromMsInclusive, long toMsExclusive) throws SQLException {
        List<ProfileSampleRecord> out = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(LOAD_RANGE_SQL)) {
            ps.setString(1, recordingId);
            ps.setLong(2, fromMsInclusive);
            ps.setLong(3, toMsExclusive);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new ProfileSampleRecord(
                            rs.getString("connection_id"),
                            rs.getLong("ts"),
                            rs.getString("ns"),
                            rs.getString("op"),
                            rs.getLong("millis"),
                            rs.getString("plan_summary"),
                            nullableLong(rs, "docs_examined"),
                            nullableLong(rs, "docs_returned"),
                            nullableLong(rs, "keys_examined"),
                            nullableLong(rs, "num_yield"),
                            nullableLong(rs, "response_length"),
                            rs.getString("query_hash"),
                            rs.getString("plan_cache_key"),
                            rs.getString("command_json")));
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

    private static void setNullableLong(PreparedStatement ps, int idx, Long v) throws SQLException {
        if (v == null) ps.setNull(idx, java.sql.Types.INTEGER);
        else ps.setLong(idx, v);
    }

    private static Long nullableLong(ResultSet rs, String col) throws SQLException {
        long v = rs.getLong(col);
        return rs.wasNull() ? null : v;
    }
}

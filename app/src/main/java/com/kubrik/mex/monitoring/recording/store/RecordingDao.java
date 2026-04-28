package com.kubrik.mex.monitoring.recording.store;

import com.kubrik.mex.monitoring.recording.Recording;
import com.kubrik.mex.monitoring.recording.RecordingState;
import com.kubrik.mex.monitoring.recording.StopReason;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * CRUD for the {@code recordings} metadata row plus the startup crash-recovery
 * sweep required by technical-spec §4.5.
 *
 * <p>The DAO is stateless aside from holding the shared {@link Connection}; all
 * concurrency control is handled by the SQLite single-writer convention at the
 * transaction boundary.
 */
public final class RecordingDao {

    private static final String INSERT_SQL = """
            INSERT INTO recordings
                (id, connection_id, name, note, tags_json, state, stop_reason,
                 started_at, ended_at, paused_millis, max_duration_ms, max_size_bytes,
                 capture_profiler, schema_version, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

    private static final String UPDATE_SQL = """
            UPDATE recordings
               SET name = ?, note = ?, tags_json = ?, state = ?, stop_reason = ?,
                   ended_at = ?, paused_millis = ?, max_duration_ms = ?,
                   max_size_bytes = ?, capture_profiler = ?
             WHERE id = ?
            """;

    private static final String SELECT_BY_ID_SQL = """
            SELECT id, connection_id, name, note, tags_json, state, stop_reason,
                   started_at, ended_at, paused_millis, max_duration_ms, max_size_bytes,
                   capture_profiler, schema_version, created_at
              FROM recordings
             WHERE id = ?
            """;

    private static final String SELECT_ALL_SQL = """
            SELECT id, connection_id, name, note, tags_json, state, stop_reason,
                   started_at, ended_at, paused_millis, max_duration_ms, max_size_bytes,
                   capture_profiler, schema_version, created_at
              FROM recordings
             ORDER BY started_at DESC
            """;

    private static final String SELECT_ACTIVE_FOR_CONNECTION_SQL = """
            SELECT id, connection_id, name, note, tags_json, state, stop_reason,
                   started_at, ended_at, paused_millis, max_duration_ms, max_size_bytes,
                   capture_profiler, schema_version, created_at
              FROM recordings
             WHERE connection_id = ?
               AND state IN ('ACTIVE','PAUSED')
            """;

    private static final String SELECT_ACTIVE_AND_PAUSED_SQL = """
            SELECT id, connection_id, name, note, tags_json, state, stop_reason,
                   started_at, ended_at, paused_millis, max_duration_ms, max_size_bytes,
                   capture_profiler, schema_version, created_at
              FROM recordings
             WHERE state IN ('ACTIVE','PAUSED')
            """;

    private static final String DELETE_SQL = "DELETE FROM recordings WHERE id = ?";

    private static final String SELECT_BYTES_SQL =
            "SELECT bytes_approx FROM recordings WHERE id = ?";

    /** Crash-recovery sweep — marks any ACTIVE/PAUSED row as INTERRUPTED. See §4.5. */
    private static final String CRASH_SWEEP_SQL = """
            UPDATE recordings
               SET state       = 'STOPPED',
                   stop_reason = 'INTERRUPTED',
                   ended_at    = COALESCE(
                       (SELECT MAX(ts) FROM recording_samples WHERE recording_id = recordings.id),
                       started_at
                   )
             WHERE state IN ('ACTIVE','PAUSED')
            """;

    private final Connection conn;

    public RecordingDao(Connection conn) { this.conn = conn; }

    public void insert(Recording r) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(INSERT_SQL)) {
            ps.setString(1, r.id());
            ps.setString(2, r.connectionId());
            ps.setString(3, r.name());
            ps.setString(4, r.note());
            ps.setString(5, encodeTags(r.tags()));
            ps.setString(6, r.state().name());
            ps.setString(7, r.stopReason() == null ? null : r.stopReason().name());
            ps.setLong  (8, r.startedAtMs());
            if (r.endedAtMs() == null) ps.setNull(9, java.sql.Types.INTEGER); else ps.setLong(9, r.endedAtMs());
            ps.setLong  (10, r.pausedMillis());
            if (r.maxDurationMs() == null) ps.setNull(11, java.sql.Types.INTEGER); else ps.setLong(11, r.maxDurationMs());
            if (r.maxSizeBytes() == null) ps.setNull(12, java.sql.Types.INTEGER); else ps.setLong(12, r.maxSizeBytes());
            ps.setInt   (13, r.captureProfilerSamples() ? 1 : 0);
            ps.setInt   (14, r.schemaVersion());
            ps.setLong  (15, r.createdAtMs());
            ps.executeUpdate();
        }
    }

    public void update(Recording r) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(UPDATE_SQL)) {
            ps.setString(1, r.name());
            ps.setString(2, r.note());
            ps.setString(3, encodeTags(r.tags()));
            ps.setString(4, r.state().name());
            ps.setString(5, r.stopReason() == null ? null : r.stopReason().name());
            if (r.endedAtMs() == null) ps.setNull(6, java.sql.Types.INTEGER); else ps.setLong(6, r.endedAtMs());
            ps.setLong  (7, r.pausedMillis());
            if (r.maxDurationMs() == null) ps.setNull(8, java.sql.Types.INTEGER); else ps.setLong(8, r.maxDurationMs());
            if (r.maxSizeBytes() == null) ps.setNull(9, java.sql.Types.INTEGER); else ps.setLong(9, r.maxSizeBytes());
            ps.setInt   (10, r.captureProfilerSamples() ? 1 : 0);
            ps.setString(11, r.id());
            ps.executeUpdate();
        }
    }

    public Optional<Recording> findById(String id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SELECT_BY_ID_SQL)) {
            ps.setString(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(map(rs)) : Optional.empty();
            }
        }
    }

    public List<Recording> listAll() throws SQLException {
        List<Recording> out = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(SELECT_ALL_SQL);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) out.add(map(rs));
        }
        return out;
    }

    public List<Recording> findActiveForConnection(String connectionId) throws SQLException {
        List<Recording> out = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(SELECT_ACTIVE_FOR_CONNECTION_SQL)) {
            ps.setString(1, connectionId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) out.add(map(rs));
            }
        }
        return out;
    }

    /** Only ACTIVE/PAUSED rows — consumed by {@code RecordingService.onTick} / {@code close()}
     *  so the 5 s tick doesn't scan the full table when thousands of STOPPED recordings are
     *  kept (tech-spec §6). */
    public List<Recording> findActiveAndPaused() throws SQLException {
        List<Recording> out = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(SELECT_ACTIVE_AND_PAUSED_SQL);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) out.add(map(rs));
        }
        return out;
    }

    /** Cascade via FK removes samples + profiler rows + annotations in one transaction. */
    public void delete(String id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(DELETE_SQL)) {
            ps.setString(1, id);
            ps.executeUpdate();
        }
    }

    /** Read the running byte counter maintained inside {@link com.kubrik.mex.monitoring.recording.store.RecordingSampleDao#insertBatch}
     *  (P2). Returns 0 for recordings that pre-date the counter column or haven't
     *  seen samples yet. Caller must hold {@code Database.writeLock()}. */
    public long bytesApprox(String recordingId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SELECT_BYTES_SQL)) {
            ps.setString(1, recordingId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : 0L;
            }
        }
    }

    /**
     * Run the one-shot crash-recovery sweep on startup. Marks every ACTIVE/PAUSED
     * row as STOPPED with {@link StopReason#INTERRUPTED}, backfilling {@code ended_at}
     * from the last captured sample (or {@code started_at} if nothing was written).
     *
     * <p>Returns the number of rows swept so callers can log it. Idempotent — running
     * twice is a no-op because no ACTIVE/PAUSED rows remain.
     */
    public int sweepInterrupted() throws SQLException {
        try (Statement st = conn.createStatement()) {
            return st.executeUpdate(CRASH_SWEEP_SQL);
        }
    }

    private Recording map(ResultSet rs) throws SQLException {
        long endedRaw = rs.getLong("ended_at");
        Long ended = rs.wasNull() ? null : endedRaw;
        long maxDurRaw = rs.getLong("max_duration_ms");
        Long maxDur = rs.wasNull() ? null : maxDurRaw;
        long maxSzRaw = rs.getLong("max_size_bytes");
        Long maxSz = rs.wasNull() ? null : maxSzRaw;
        String stopRaw = rs.getString("stop_reason");
        StopReason stop = stopRaw == null ? null : StopReason.valueOf(stopRaw);
        return new Recording(
                rs.getString("id"),
                rs.getString("connection_id"),
                rs.getString("name"),
                rs.getString("note"),
                decodeTags(rs.getString("tags_json")),
                RecordingState.valueOf(rs.getString("state")),
                stop,
                rs.getLong("started_at"),
                ended,
                rs.getLong("paused_millis"),
                maxDur,
                maxSz,
                rs.getInt("capture_profiler") != 0,
                rs.getLong("created_at"),
                rs.getInt("schema_version"));
    }

    /** Minimal JSON array encoder for the tag list. Tag values are validated alphanumeric + hyphen. */
    static String encodeTags(List<String> tags) {
        if (tags == null || tags.isEmpty()) return "[]";
        StringBuilder sb = new StringBuilder(tags.size() * 10);
        sb.append('[');
        boolean first = true;
        for (String t : tags) {
            if (!first) sb.append(',');
            first = false;
            sb.append('"').append(t).append('"');
        }
        sb.append(']');
        return sb.toString();
    }

    static List<String> decodeTags(String json) {
        if (json == null || json.isBlank() || "[]".equals(json)) return List.of();
        String inner = json.trim();
        if (!inner.startsWith("[") || !inner.endsWith("]")) return List.of();
        inner = inner.substring(1, inner.length() - 1).trim();
        if (inner.isEmpty()) return List.of();
        List<String> out = new ArrayList<>();
        for (String raw : inner.split(",")) {
            String s = raw.trim();
            if (s.startsWith("\"") && s.endsWith("\"") && s.length() >= 2) {
                out.add(s.substring(1, s.length() - 1));
            }
        }
        return out;
    }
}

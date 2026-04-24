package com.kubrik.mex.monitoring.store;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/** CRUD for the {@code profiler_samples} table. */
public final class ProfileSampleDao {

    private static final String INSERT_SQL = """
            INSERT OR REPLACE INTO profiler_samples
              (connection_id, ts, ns, op, millis, plan_summary,
               docs_examined, docs_returned, keys_examined, num_yield, response_length,
               query_hash, plan_cache_key, command_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

    private static final String SELECT_RANGE = """
            SELECT connection_id, ts, ns, op, millis, plan_summary,
                   docs_examined, docs_returned, keys_examined, num_yield, response_length,
                   query_hash, plan_cache_key, command_json
              FROM profiler_samples
             WHERE connection_id = ? AND ts >= ? AND ts < ?
             ORDER BY ts ASC
            """;

    private final Connection conn;
    private final Object writeLock;

    /**
     * @deprecated prefer the two-arg form; the single-arg falls back
     *   to a private monitor that doesn't prevent concurrent writers
     *   on other DAOs from racing our setAutoCommit(false) toggle.
     */
    @Deprecated
    public ProfileSampleDao(Connection conn) { this(conn, new Object()); }

    public ProfileSampleDao(Connection conn, Object writeLock) {
        this.conn = conn;
        this.writeLock = writeLock;
    }

    public void insert(ProfileSampleRecord r) throws SQLException {
        synchronized (writeLock) {
            try (PreparedStatement ps = conn.prepareStatement(INSERT_SQL)) {
                bind(ps, r);
                ps.executeUpdate();
            }
        }
    }

    public void insertBatch(List<ProfileSampleRecord> records) throws SQLException {
        if (records.isEmpty()) return;
        synchronized (writeLock) {
            boolean prevAuto = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(INSERT_SQL)) {
                for (ProfileSampleRecord r : records) {
                    bind(ps, r);
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
    }

    public List<ProfileSampleRecord> loadRange(String connectionId, long fromMs, long toMs) throws SQLException {
        List<ProfileSampleRecord> out = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(SELECT_RANGE)) {
            ps.setString(1, connectionId);
            ps.setLong(2, fromMs);
            ps.setLong(3, toMs);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new ProfileSampleRecord(
                            rs.getString("connection_id"),
                            rs.getLong("ts"),
                            rs.getString("ns"),
                            rs.getString("op"),
                            rs.getLong("millis"),
                            rs.getString("plan_summary"),
                            (Long) rs.getObject("docs_examined"),
                            (Long) rs.getObject("docs_returned"),
                            (Long) rs.getObject("keys_examined"),
                            (Long) rs.getObject("num_yield"),
                            (Long) rs.getObject("response_length"),
                            rs.getString("query_hash"),
                            rs.getString("plan_cache_key"),
                            rs.getString("command_json")));
                }
            }
        }
        return out;
    }

    private void bind(PreparedStatement ps, ProfileSampleRecord r) throws SQLException {
        ps.setString(1, r.connectionId());
        ps.setLong  (2, r.tsMs());
        ps.setString(3, r.ns());
        ps.setString(4, r.op());
        ps.setLong  (5, r.millis());
        if (r.planSummary() != null) ps.setString(6, r.planSummary()); else ps.setNull(6, Types.VARCHAR);
        setNullableLong(ps, 7,  r.docsExamined());
        setNullableLong(ps, 8,  r.docsReturned());
        setNullableLong(ps, 9,  r.keysExamined());
        setNullableLong(ps, 10, r.numYield());
        setNullableLong(ps, 11, r.responseLength());
        ps.setString(12, r.queryHash() != null ? r.queryHash() : "");
        if (r.planCacheKey() != null) ps.setString(13, r.planCacheKey()); else ps.setNull(13, Types.VARCHAR);
        ps.setString(14, r.commandJson());
    }

    private static void setNullableLong(PreparedStatement ps, int idx, Long v) throws SQLException {
        if (v == null) ps.setNull(idx, Types.INTEGER);
        else ps.setLong(idx, v);
    }
}

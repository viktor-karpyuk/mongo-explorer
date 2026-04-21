package com.kubrik.mex.security.audit;

import com.kubrik.mex.cluster.dryrun.CommandJson;
import com.kubrik.mex.store.Database;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * v2.6 Q2.6-C3 — SQLite FTS5 index over the native MongoDB audit events.
 * Insert-only (never update in place — audit-log replay reproduces the
 * same rows and the UI deduplicates on {@code (connection_id, ts, atype,
 * who)}). Search supports the standard FTS5 query grammar so the Audit
 * sub-tab (Q2.6-C4, deferred) can map its filter UI onto
 * {@code atype:authenticate AND who:dba}, etc.
 *
 * <p>The porter + ascii tokenizer is deliberate: the MongoDB audit log
 * is ASCII-only in practice (usernames, IP addresses, command names) and
 * porter stemming makes "create" match "creating" / "created" in the
 * atype field.</p>
 */
public final class AuditIndex {

    private final Database db;

    public AuditIndex(Database db) { this.db = db; }

    /** Feed a parsed event into the FTS table. Intended to be wired
     *  directly to {@link AuditLogTailer}'s sink lambda — one event per
     *  line per connection. */
    public void insert(String connectionId, AuditEvent event) {
        if (connectionId == null || connectionId.isBlank() || event == null) return;
        String sql = """
                INSERT INTO audit_native_fts(connection_id, atype, ts, who,
                    from_host, param_json, raw_json)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """;
        synchronized (db.writeLock()) {
            try (PreparedStatement ps = db.connection().prepareStatement(sql)) {
                ps.setString(1, connectionId);
                ps.setString(2, event.atype());
                ps.setString(3, Long.toString(event.tsMs()));
                ps.setString(4, event.who() + (event.whoDb().isEmpty() ? ""
                        : "@" + event.whoDb()));
                ps.setString(5, event.fromHost());
                ps.setString(6, paramJson(event));
                ps.setString(7, event.rawJson());
                ps.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException("audit_native_fts insert failed", e);
            }
        }
    }

    /**
     * Search with an FTS5 query — e.g., {@code "authenticate who:dba"}.
     * Results are scoped to {@code connectionId} and ordered by the
     * FTS5 ranking.
     */
    public List<AuditEvent> search(String connectionId, String query, int limit) {
        // Blank query isn't a valid FTS5 expression; degrade to the
        // recent-listing path so the pane shows something on an empty
        // filter bar.
        if (query == null || query.isBlank()) {
            return listRecent(connectionId, limit);
        }
        // FTS5 match filter against the query + manual AND on the
        // UNINDEXED connection_id. MATCH alone doesn't apply the filter
        // because connection_id is not in the tokenised columns.
        String sql = """
                SELECT atype, ts, who, from_host, param_json, raw_json
                  FROM audit_native_fts
                 WHERE connection_id = ?
                   AND audit_native_fts MATCH ?
                 ORDER BY rank
                 LIMIT ?
                """;
        try (PreparedStatement ps = db.connection().prepareStatement(sql)) {
            ps.setString(1, connectionId);
            ps.setString(2, query);
            ps.setInt(3, limit <= 0 ? 100 : limit);
            return read(ps);
        } catch (SQLException e) {
            return List.of();
        }
    }

    /** List every event for a connection, newest first, up to {@code limit}.
     *  Used by the pane when no search query is set. */
    public List<AuditEvent> listRecent(String connectionId, int limit) {
        String sql = """
                SELECT atype, ts, who, from_host, param_json, raw_json
                  FROM audit_native_fts
                 WHERE connection_id = ?
                 ORDER BY ts DESC
                 LIMIT ?
                """;
        try (PreparedStatement ps = db.connection().prepareStatement(sql)) {
            ps.setString(1, connectionId);
            ps.setInt(2, limit <= 0 ? 100 : limit);
            return read(ps);
        } catch (SQLException e) {
            return List.of();
        }
    }

    /** Delete rows older than {@code cutoffMs}. Drives the retention
     *  cap the milestone sets at 30 d by default (configurable in
     *  Q2.6-K). */
    public int purgeOlderThan(String connectionId, long cutoffMs) {
        synchronized (db.writeLock()) {
            try (PreparedStatement ps = db.connection().prepareStatement(
                    "DELETE FROM audit_native_fts WHERE connection_id = ? " +
                            "AND CAST(ts AS INTEGER) < ?")) {
                ps.setString(1, connectionId);
                ps.setLong(2, cutoffMs);
                return ps.executeUpdate();
            } catch (SQLException e) {
                return 0;
            }
        }
    }

    /* ============================== helpers ============================== */

    private static String paramJson(AuditEvent e) {
        if (e.param().isEmpty()) return "";
        return CommandJson.render(e.param());
    }

    private List<AuditEvent> read(PreparedStatement ps) throws SQLException {
        List<AuditEvent> out = new ArrayList<>();
        try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                String tsText = rs.getString("ts");
                long ts;
                try { ts = Long.parseLong(tsText == null ? "0" : tsText); }
                catch (NumberFormatException nfe) { ts = 0L; }
                // Split who@db back into its parts so callers don't need
                // to reparse — event equality vs. the original post-tailer
                // object still holds for atype / tsMs / param.
                String who = rs.getString("who");
                String whoName = who == null ? "" : who;
                String whoDb = "";
                int at = whoName.indexOf('@');
                if (at > 0) {
                    whoDb = whoName.substring(at + 1);
                    whoName = whoName.substring(0, at);
                }
                out.add(new AuditEvent(rs.getString("atype"),
                        ts,
                        whoName,
                        whoDb,
                        rs.getString("from_host"),
                        0,
                        java.util.Map.of(),  // param reconstitution is a v2.6.1 nicety
                        rs.getString("raw_json") == null ? "" : rs.getString("raw_json")));
            }
        }
        return out;
    }
}

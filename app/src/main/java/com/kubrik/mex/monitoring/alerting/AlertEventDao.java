package com.kubrik.mex.monitoring.alerting;

import com.kubrik.mex.monitoring.model.Severity;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class AlertEventDao {

    private static final String INSERT = """
            INSERT OR REPLACE INTO alert_events
              (id, rule_id, connection_id, fired_at, cleared_at,
               severity, value_at_fire, message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """;

    private static final String MARK_CLEARED = """
            UPDATE alert_events SET cleared_at = ? WHERE rule_id = ? AND connection_id = ? AND cleared_at IS NULL
            """;

    private static final String LOAD_RECENT = """
            SELECT id, rule_id, connection_id, fired_at, cleared_at, severity,
                   value_at_fire, message
              FROM alert_events
             WHERE fired_at >= ?
             ORDER BY fired_at DESC
             LIMIT ?
            """;

    private final Connection conn;

    public AlertEventDao(Connection conn) { this.conn = conn; }

    public void insert(AlertEvent e) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(INSERT)) {
            ps.setString(1, e.id());
            ps.setString(2, e.ruleId());
            ps.setString(3, e.connectionId());
            ps.setLong  (4, e.firedAtMs());
            if (e.clearedAtMs() != null) ps.setLong(5, e.clearedAtMs()); else ps.setNull(5, Types.INTEGER);
            ps.setString(6, e.severity().name());
            ps.setDouble(7, e.valueAtFire());
            ps.setString(8, e.message());
            ps.executeUpdate();
        }
    }

    public void markCleared(String ruleId, String connectionId, long clearedAtMs) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(MARK_CLEARED)) {
            ps.setLong(1, clearedAtMs);
            ps.setString(2, ruleId);
            ps.setString(3, connectionId);
            ps.executeUpdate();
        }
    }

    public List<AlertEvent> loadRecent(long sinceMs, int limit) throws SQLException {
        List<AlertEvent> out = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(LOAD_RECENT)) {
            ps.setLong(1, sinceMs);
            ps.setInt(2, limit);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new AlertEvent(
                            rs.getString("id"),
                            rs.getString("rule_id"),
                            rs.getString("connection_id"),
                            Severity.valueOf(rs.getString("severity")),
                            rs.getLong("fired_at"),
                            (Long) rs.getObject("cleared_at"),
                            rs.getDouble("value_at_fire"),
                            Map.of(),
                            rs.getString("message")));
                }
            }
        }
        return out;
    }
}

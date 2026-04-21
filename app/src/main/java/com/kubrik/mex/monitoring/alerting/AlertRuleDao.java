package com.kubrik.mex.monitoring.alerting;

import com.kubrik.mex.monitoring.model.MetricId;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Persists alert rules. Label-filter JSON uses the same flat-object shape as {@code labels_json}. */
public final class AlertRuleDao {

    private static final String UPSERT = """
            INSERT INTO alert_rules
              (id, connection_id, metric, label_filter, comparator,
               warn_threshold, crit_threshold, for_seconds, enabled, source, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
              connection_id  = excluded.connection_id,
              metric         = excluded.metric,
              label_filter   = excluded.label_filter,
              comparator     = excluded.comparator,
              warn_threshold = excluded.warn_threshold,
              crit_threshold = excluded.crit_threshold,
              for_seconds    = excluded.for_seconds,
              enabled        = excluded.enabled,
              source         = excluded.source
            """;

    private static final String LOAD_ALL = """
            SELECT id, connection_id, metric, label_filter, comparator,
                   warn_threshold, crit_threshold, for_seconds, enabled, source
              FROM alert_rules
            """;

    private static final String COUNT_DEFAULTS = """
            SELECT COUNT(*) FROM alert_rules WHERE source = ?
            """;

    private final Connection conn;

    public AlertRuleDao(Connection conn) { this.conn = conn; }

    public void upsert(AlertRule r) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(UPSERT)) {
            ps.setString(1, r.id());
            if (r.connectionId() != null) ps.setString(2, r.connectionId()); else ps.setNull(2, Types.VARCHAR);
            ps.setString(3, r.metric().metricName());
            ps.setString(4, encodeLabels(r.labelFilter()));
            ps.setString(5, r.comparator().name());
            if (r.warnThreshold() != null) ps.setDouble(6, r.warnThreshold()); else ps.setNull(6, Types.REAL);
            if (r.critThreshold() != null) ps.setDouble(7, r.critThreshold()); else ps.setNull(7, Types.REAL);
            ps.setLong  (8, r.sustain().toSeconds());
            ps.setInt   (9, r.enabled() ? 1 : 0);
            if (r.source() != null) ps.setString(10, r.source()); else ps.setNull(10, Types.VARCHAR);
            ps.setLong  (11, System.currentTimeMillis());
            ps.executeUpdate();
        }
    }

    public long countBySource(String source) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(COUNT_DEFAULTS)) {
            ps.setString(1, source);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    public List<AlertRule> loadAll() throws SQLException {
        List<AlertRule> out = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(LOAD_ALL);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                String metricName = rs.getString("metric");
                var metricOpt = MetricId.byMetricName(metricName);
                if (metricOpt.isEmpty()) continue; // defensively skip unknown metrics (version skew)
                out.add(new AlertRule(
                        rs.getString("id"),
                        rs.getString("connection_id"),
                        metricOpt.get(),
                        decodeLabels(rs.getString("label_filter")),
                        Comparator.valueOf(rs.getString("comparator")),
                        (Double) rs.getObject("warn_threshold"),
                        (Double) rs.getObject("crit_threshold"),
                        Duration.ofSeconds(rs.getLong("for_seconds")),
                        rs.getInt("enabled") != 0,
                        rs.getString("source")));
            }
        }
        return out;
    }

    /** Install default rules if none are present with that source tag. */
    public int installDefaultsIfMissing(List<AlertRule> defaults, String source) throws SQLException {
        if (countBySource(source) > 0) return 0;
        int n = 0;
        for (AlertRule r : defaults) { upsert(r); n++; }
        return n;
    }

    private static String encodeLabels(Map<String, String> labels) {
        if (labels == null || labels.isEmpty()) return null;
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        boolean first = true;
        for (var e : labels.entrySet()) {
            if (!first) sb.append(',');
            first = false;
            sb.append('"').append(e.getKey()).append("\":\"").append(e.getValue()).append('"');
        }
        sb.append('}');
        return sb.toString();
    }

    private static Map<String, String> decodeLabels(String s) {
        if (s == null || s.isBlank() || s.equals("{}")) return Map.of();
        Map<String, String> out = new LinkedHashMap<>();
        int i = 1; int end = s.length() - 1;
        while (i < end) {
            if (s.charAt(i) == ',') { i++; continue; }
            if (s.charAt(i) != '"') break;
            int kStart = ++i;
            while (i < end && s.charAt(i) != '"') i++;
            String k = s.substring(kStart, i);
            i += 2; // skip closing " and :
            if (s.charAt(i) != '"') break;
            int vStart = ++i;
            while (i < end && s.charAt(i) != '"') i++;
            String v = s.substring(vStart, i);
            i++;
            out.put(k, v);
        }
        return out;
    }
}

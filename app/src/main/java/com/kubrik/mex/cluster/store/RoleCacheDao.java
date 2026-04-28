package com.kubrik.mex.cluster.store;

import com.kubrik.mex.cluster.safety.RoleSet;
import com.kubrik.mex.cluster.util.CanonicalJson;
import com.kubrik.mex.store.Database;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

/**
 * v2.4 ROLE-1..4 — persists the parsed role set + last-probe timestamp per
 * connection. Only the role names are stored (no privilege arrays) since the
 * synchronous gate only needs to answer "does the user hold role X?".
 */
public final class RoleCacheDao {

    private final Database db;

    public RoleCacheDao(Database db) { this.db = db; }

    public void upsert(String connectionId, RoleSet roles, long probedAtMs) {
        String json = encode(roles);
        String sql = "INSERT INTO role_cache(connection_id, roles_json, probed_at) " +
                "VALUES(?,?,?) ON CONFLICT(connection_id) DO UPDATE SET " +
                "roles_json = excluded.roles_json, probed_at = excluded.probed_at";
        try (PreparedStatement ps = db.connection().prepareStatement(sql)) {
            ps.setString(1, connectionId);
            ps.setString(2, json);
            ps.setLong(3, probedAtMs);
            ps.executeUpdate();
        } catch (SQLException ignored) {}
    }

    public Cached load(String connectionId) {
        String sql = "SELECT roles_json, probed_at FROM role_cache WHERE connection_id = ?";
        try (PreparedStatement ps = db.connection().prepareStatement(sql)) {
            ps.setString(1, connectionId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return new Cached(decode(rs.getString(1)), rs.getLong(2));
                }
            }
        } catch (SQLException ignored) {}
        return null;
    }

    public void invalidate(String connectionId) {
        try (PreparedStatement ps = db.connection().prepareStatement(
                "DELETE FROM role_cache WHERE connection_id = ?")) {
            ps.setString(1, connectionId);
            ps.executeUpdate();
        } catch (SQLException ignored) {}
    }

    public record Cached(RoleSet roles, long probedAtMs) {}

    /* ----------------------------- encoding ----------------------------- */

    /** Canonical JSON array of role names, sorted. */
    static String encode(RoleSet roles) {
        TreeSet<String> sorted = new TreeSet<>(roles.roles());
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        boolean first = true;
        for (String r : sorted) {
            if (!first) sb.append(',');
            first = false;
            sb.append(CanonicalJson.quote(r));
        }
        sb.append(']');
        return sb.toString();
    }

    /** Simple decoder matching {@link #encode}. */
    static RoleSet decode(String json) {
        if (json == null || json.isBlank()) return RoleSet.EMPTY;
        Set<String> out = new HashSet<>();
        int i = json.indexOf('[');
        int end = json.lastIndexOf(']');
        if (i < 0 || end <= i) return RoleSet.EMPTY;
        String inner = json.substring(i + 1, end).trim();
        if (inner.isEmpty()) return RoleSet.EMPTY;
        int p = 0;
        while (p < inner.length()) {
            while (p < inner.length() && inner.charAt(p) != '"') p++;
            if (p >= inner.length()) break;
            StringBuilder sb = new StringBuilder();
            p++; // skip opening quote
            while (p < inner.length()) {
                char c = inner.charAt(p);
                if (c == '\\' && p + 1 < inner.length()) {
                    char n = inner.charAt(p + 1);
                    sb.append(switch (n) {
                        case '"' -> '"';
                        case '\\' -> '\\';
                        case 'n' -> '\n';
                        case 't' -> '\t';
                        default -> n;
                    });
                    p += 2;
                } else if (c == '"') {
                    p++;
                    break;
                } else {
                    sb.append(c);
                    p++;
                }
            }
            out.add(sb.toString());
        }
        return new RoleSet(out);
    }
}

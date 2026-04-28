package com.kubrik.mex.maint.param;

import com.kubrik.mex.maint.model.ParamProposal;
import com.kubrik.mex.store.Database;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * v2.7 Q2.7-F — DAO for {@code param_tuning_proposals}. Persists the
 * recommender's output so the pane shows history across sessions
 * and the wizard can re-open a prior proposal.
 */
public final class ParamProposalDao {

    public enum Status { OPEN, ACCEPTED, REJECTED, SUPERSEDED }

    public record Row(
            long id,
            String connectionId,
            String host,
            ParamProposal proposal,
            long createdAt,
            Status status
    ) {}

    private final Database db;

    public ParamProposalDao(Database db) { this.db = db; }

    public long insert(String connectionId, String host,
                       ParamProposal proposal, long now) {
        String sql = """
                INSERT INTO param_tuning_proposals(connection_id, host, param,
                    current_value, proposed_value, rationale, severity,
                    created_at, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'OPEN')
                """;
        synchronized (db.writeLock()) {
            try (PreparedStatement ps = db.connection().prepareStatement(sql,
                    Statement.RETURN_GENERATED_KEYS)) {
                ps.setString(1, connectionId);
                ps.setString(2, host);
                ps.setString(3, proposal.param());
                ps.setString(4, proposal.currentValue());
                ps.setString(5, proposal.proposedValue());
                ps.setString(6, proposal.rationale());
                ps.setString(7, proposal.severity().name());
                ps.setLong(8, now);
                ps.executeUpdate();
                try (ResultSet keys = ps.getGeneratedKeys()) {
                    return keys.next() ? keys.getLong(1) : -1L;
                }
            } catch (SQLException e) {
                throw new RuntimeException("param_tuning_proposals insert failed", e);
            }
        }
    }

    public List<Row> listOpenForConnection(String connectionId) {
        try (PreparedStatement ps = db.connection().prepareStatement(
                "SELECT * FROM param_tuning_proposals " +
                "WHERE connection_id = ? AND status = 'OPEN' " +
                "ORDER BY created_at DESC")) {
            ps.setString(1, connectionId);
            return read(ps);
        } catch (SQLException e) {
            return List.of();
        }
    }

    public boolean transition(long id, Status newStatus) {
        synchronized (db.writeLock()) {
            try (PreparedStatement ps = db.connection().prepareStatement(
                    "UPDATE param_tuning_proposals SET status = ? WHERE id = ?")) {
                ps.setString(1, newStatus.name());
                ps.setLong(2, id);
                return ps.executeUpdate() > 0;
            } catch (SQLException e) {
                return false;
            }
        }
    }

    /* ============================ internals ============================ */

    private List<Row> read(PreparedStatement ps) throws SQLException {
        List<Row> out = new ArrayList<>();
        try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) out.add(map(rs));
        }
        return out;
    }

    private static Row map(ResultSet rs) throws SQLException {
        ParamProposal prop = new ParamProposal(
                rs.getString("param"),
                rs.getString("current_value"),
                rs.getString("proposed_value"),
                ParamProposal.Severity.valueOf(rs.getString("severity")),
                rs.getString("rationale"));
        return new Row(rs.getLong("id"), rs.getString("connection_id"),
                rs.getString("host"), prop, rs.getLong("created_at"),
                Status.valueOf(rs.getString("status")));
    }
}

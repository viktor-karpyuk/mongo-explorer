package com.kubrik.mex.k8s.rollout;

import com.kubrik.mex.store.Database;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * v2.8.1 Q2.8.1-H — Persistence for {@code rollout_events}.
 *
 * <p>Events are append-only; cascade-deleted with their parent
 * {@code provisioning_records} row. {@link #listForProvisioning}
 * drives the rollout viewer's event log.</p>
 */
public final class RolloutEventDao {

    private final Database database;

    public RolloutEventDao(Database database) {
        this.database = Objects.requireNonNull(database, "database");
    }

    public long insert(RolloutEvent e) throws SQLException {
        synchronized (database.writeLock()) {
            String sql = "INSERT INTO rollout_events("
                    + "provisioning_id, at, source, severity, raw_reason, raw_message, diagnosis_hint) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?)";
            try (PreparedStatement ps = database.connection()
                    .prepareStatement(sql, java.sql.Statement.RETURN_GENERATED_KEYS)) {
                ps.setLong(1, e.provisioningId());
                ps.setLong(2, e.at());
                ps.setString(3, e.source().name());
                ps.setString(4, e.severity().name());
                if (e.reason().isPresent()) ps.setString(5, e.reason().get());
                else ps.setNull(5, java.sql.Types.VARCHAR);
                if (e.message().isPresent()) ps.setString(6, e.message().get());
                else ps.setNull(6, java.sql.Types.VARCHAR);
                if (e.diagnosisHint().isPresent()) ps.setString(7, e.diagnosisHint().get());
                else ps.setNull(7, java.sql.Types.VARCHAR);
                ps.executeUpdate();
                try (ResultSet keys = ps.getGeneratedKeys()) {
                    return keys.next() ? keys.getLong(1) : -1L;
                }
            }
        }
    }

    public List<RolloutEvent> listForProvisioning(long provisioningId) throws SQLException {
        List<RolloutEvent> out = new ArrayList<>();
        try (PreparedStatement ps = database.connection().prepareStatement(
                "SELECT * FROM rollout_events WHERE provisioning_id = ? ORDER BY at ASC")) {
            ps.setLong(1, provisioningId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new RolloutEvent(
                            rs.getLong("provisioning_id"),
                            rs.getLong("at"),
                            RolloutEvent.Source.valueOf(rs.getString("source")),
                            RolloutEvent.Severity.valueOf(rs.getString("severity")),
                            Optional.ofNullable(rs.getString("raw_reason")),
                            Optional.ofNullable(rs.getString("raw_message")),
                            Optional.ofNullable(rs.getString("diagnosis_hint"))));
                }
            }
        }
        return out;
    }
}

package com.kubrik.mex.security.drift;

import com.kubrik.mex.store.Database;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.6 Q2.6-D3 — ack + mute semantics + DAO round-trip. ACK hides the
 * finding only for the exact baseline it was recorded against; MUTE
 * hides the path across every future diff until the user un-mutes.
 *
 * <p>DAO tests also guard the FK cascade — deleting a baseline row
 * should cascade-delete the ack rows that pointed at it. The schema
 * migration uses {@code ON DELETE CASCADE}; without {@code PRAGMA
 * foreign_keys=ON} the cascade would be silently skipped, so this also
 * indirectly verifies the pragma is set during Database construction.</p>
 */
class DriftAckTest {

    @TempDir Path home;
    private Database db;
    private DriftAckDao dao;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", home.toString());
        db = new Database();
        dao = new DriftAckDao(db);
        seedBaseline(42L);
        seedBaseline(43L);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (db != null) db.close();
    }

    @Test
    void ack_hides_finding_for_same_baseline_only() {
        DriftAck ack = dao.insert(new DriftAck(-1, "cx-a", 42L,
                "users.dba@admin.roleBindings[0].role",
                1_000L, "dba", DriftAck.Mode.ACK, ""));
        assertTrue(ack.id() > 0);

        List<DriftFinding> findings = List.of(new DriftFinding(
                "users.dba@admin.roleBindings[0].role",
                DriftFinding.Kind.CHANGED, "root", "clusterAdmin", "users"));

        // For the baseline the ack was written against: hidden.
        List<DriftFinding> filteredSame = DriftAck.hideAcked(findings, 42L,
                dao.listForConnection("cx-a"));
        assertTrue(filteredSame.isEmpty());

        // For a later baseline: re-surfaces.
        List<DriftFinding> filteredLater = DriftAck.hideAcked(findings, 43L,
                dao.listForConnection("cx-a"));
        assertEquals(1, filteredLater.size());
    }

    @Test
    void mute_hides_finding_across_every_baseline() {
        dao.insert(new DriftAck(-1, "cx-a", 42L,
                "users.metrics@admin.authenticationRestrictions[0].clientSource",
                1_000L, "dba", DriftAck.Mode.MUTE,
                "deliberately-fluctuating login audit"));

        List<DriftFinding> findings = List.of(new DriftFinding(
                "users.metrics@admin.authenticationRestrictions[0].clientSource",
                DriftFinding.Kind.CHANGED, "10.0.0.1", "10.0.0.2", "users"));

        // Any baseline id: MUTE still hides it.
        assertTrue(DriftAck.hideAcked(findings, 42L,
                dao.listForConnection("cx-a")).isEmpty());
        assertTrue(DriftAck.hideAcked(findings, 43L,
                dao.listForConnection("cx-a")).isEmpty());
        assertTrue(DriftAck.hideAcked(findings, 999L,
                dao.listForConnection("cx-a")).isEmpty());
    }

    @Test
    void delete_removes_the_ack_and_refilters() {
        DriftAck ack = dao.insert(new DriftAck(-1, "cx-a", 42L,
                "users.dba@admin.roleBindings[0].role",
                1_000L, "dba", DriftAck.Mode.ACK, ""));

        List<DriftFinding> findings = List.of(new DriftFinding(
                "users.dba@admin.roleBindings[0].role",
                DriftFinding.Kind.CHANGED, "x", "y", "users"));

        assertTrue(DriftAck.hideAcked(findings, 42L,
                dao.listForConnection("cx-a")).isEmpty());

        assertTrue(dao.delete(ack.id()));
        assertEquals(1, DriftAck.hideAcked(findings, 42L,
                dao.listForConnection("cx-a")).size());
    }

    @Test
    void cascade_delete_removes_acks_when_baseline_is_dropped() throws Exception {
        dao.insert(new DriftAck(-1, "cx-a", 42L, "some.path",
                1_000L, "dba", DriftAck.Mode.ACK, ""));
        assertEquals(1, dao.listForConnection("cx-a").size());

        try (java.sql.PreparedStatement ps = db.connection().prepareStatement(
                "DELETE FROM sec_baselines WHERE id = ?")) {
            ps.setLong(1, 42L);
            ps.executeUpdate();
        }

        // FK cascade should have dropped the ack row as well.
        assertEquals(0, dao.listForConnection("cx-a").size(),
                "ack row must cascade-delete with its parent sec_baselines row");
    }

    @Test
    void hide_acked_with_no_acks_returns_the_original_findings() {
        List<DriftFinding> findings = List.of(new DriftFinding(
                "users.x", DriftFinding.Kind.ADDED, null, "{}", "users"));
        assertEquals(findings, DriftAck.hideAcked(findings, 42L, List.of()));
    }

    /** Seed a sec_baselines row with a fixed id so FK-cascaded drift acks
     *  have a real parent to anchor to. We write minimal JSON / hash
     *  values — these tests don't exercise the baseline payload. */
    private void seedBaseline(long id) throws Exception {
        try (java.sql.PreparedStatement ps = db.connection().prepareStatement(
                "INSERT INTO sec_baselines(id, connection_id, captured_at, " +
                        "captured_by, notes, snapshot_json, sha256) " +
                        "VALUES (?, 'cx-a', ?, 'dba', '', '{}', ?)")) {
            ps.setLong(1, id);
            ps.setLong(2, id);
            ps.setString(3, "h".repeat(64));
            ps.executeUpdate();
        }
    }
}

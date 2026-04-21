package com.kubrik.mex.security;

import com.kubrik.mex.security.access.RoleBinding;
import com.kubrik.mex.security.access.UserRecord;
import com.kubrik.mex.security.access.UsersRolesFetcher;
import com.kubrik.mex.security.baseline.SecurityBaselineCaptureService;
import com.kubrik.mex.security.baseline.SecurityBaselineDao;
import com.kubrik.mex.security.cert.CertCacheDao;
import com.kubrik.mex.security.cert.CertRecord;
import com.kubrik.mex.security.drift.DriftAck;
import com.kubrik.mex.security.drift.DriftAckDao;
import com.kubrik.mex.store.Database;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Clock;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.6 Q2.6-D4 + Q2.6-E4 — welcome-card summary math.
 */
class SecuritySignalsTest {

    @TempDir Path home;
    private Database db;
    private SecurityBaselineDao baselineDao;
    private DriftAckDao ackDao;
    private CertCacheDao certCacheDao;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", home.toString());
        db = new Database();
        baselineDao = new SecurityBaselineDao(db);
        ackDao = new DriftAckDao(db);
        certCacheDao = new CertCacheDao(db);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (db != null) db.close();
    }

    @Test
    void clean_summary_on_empty_state() {
        SecuritySignals.Summary s = SecuritySignals.compute("cx-a",
                baselineDao, ackDao, certCacheDao, 1_000L);
        assertTrue(s.clean());
        assertFalse(s.hasBaseline());
        assertEquals(0, s.unackedDrifts());
        assertEquals(0, s.expiredCerts());
        assertEquals(0, s.expiringSoonCerts());
    }

    @Test
    void expired_and_expiring_cert_counts_are_bucketed_by_30d_window() {
        long now = 1_000_000_000_000L;
        certCacheDao.upsert("cx-a",
                cert("h-expired:27017", now - 86_400_000L, "a".repeat(64)), now);
        certCacheDao.upsert("cx-a",
                cert("h-amber:27017",   now + 10L * 86_400_000L, "b".repeat(64)), now);
        certCacheDao.upsert("cx-a",
                cert("h-green:27017",   now + 400L * 86_400_000L, "c".repeat(64)), now);

        SecuritySignals.Summary s = SecuritySignals.compute("cx-a",
                baselineDao, ackDao, certCacheDao, now);
        assertEquals(1, s.expiredCerts());
        assertEquals(1, s.expiringSoonCerts());
    }

    @Test
    void drift_count_reflects_unacked_paths_between_two_most_recent_baselines() {
        UsersRolesFetcher.Snapshot before = snap(new UserRecord("admin", "dba",
                List.of(new RoleBinding("root", "admin")), List.of(), List.of()));
        UsersRolesFetcher.Snapshot after = snap(new UserRecord("admin", "dba",
                List.of(new RoleBinding("clusterAdmin", "admin")), List.of(), List.of()));

        SecurityBaselineCaptureService capture =
                new SecurityBaselineCaptureService(new UsersRolesFetcher(),
                        baselineDao, Clock.systemUTC());
        capture.persist("cx-a", "dba", "before", before);
        var afterResult = capture.persist("cx-a", "dba", "after", after);

        SecuritySignals.Summary s = SecuritySignals.compute("cx-a",
                baselineDao, ackDao, certCacheDao, 1_000L);
        assertTrue(s.hasBaseline());
        assertEquals(1, s.unackedDrifts(),
                "role binding flipped root → clusterAdmin is one drift");
    }

    @Test
    void ack_reduces_the_visible_drift_count() {
        SecurityBaselineCaptureService capture =
                new SecurityBaselineCaptureService(new UsersRolesFetcher(),
                        baselineDao, Clock.systemUTC());
        capture.persist("cx-a", "dba", "before",
                snap(new UserRecord("admin", "dba",
                        List.of(new RoleBinding("root", "admin")),
                        List.of(), List.of())));
        var afterResult = capture.persist("cx-a", "dba", "after",
                snap(new UserRecord("admin", "dba",
                        List.of(new RoleBinding("clusterAdmin", "admin")),
                        List.of(), List.of())));

        ackDao.insert(new DriftAck(-1, "cx-a", afterResult.baselineId(),
                "users.dba@admin.roleBindings[0].role",
                1_000L, "dba", DriftAck.Mode.ACK, "expected"));

        SecuritySignals.Summary s = SecuritySignals.compute("cx-a",
                baselineDao, ackDao, certCacheDao, 1_000L);
        assertEquals(0, s.unackedDrifts());
        assertTrue(s.clean());
    }

    /* ============================== fixtures ============================== */

    private static UsersRolesFetcher.Snapshot snap(UserRecord... users) {
        return new UsersRolesFetcher.Snapshot(List.of(users), List.of());
    }

    private static CertRecord cert(String host, long notAfter, String fingerprint) {
        return new CertRecord(host, "CN=" + host, "CN=ca", List.of(),
                notAfter - 86_400_000L, notAfter, "aa", fingerprint);
    }
}

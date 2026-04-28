package com.kubrik.mex.security.cis;

import com.kubrik.mex.store.Database;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.6 Q2.6-H3 — DAO round-trip plus listActive's TTL filter which is
 * what the runner consumes on every scan.
 */
class CisSuppressionsDaoTest {

    @TempDir Path home;
    private Database db;
    private CisSuppressionsDao dao;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", home.toString());
        db = new Database();
        dao = new CisSuppressionsDao(db);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (db != null) db.close();
    }

    @Test
    void insert_then_listActive_returns_the_row() {
        dao.insert(new CisSuppression(-1, "cx-a", "CIS-2.1", "CLUSTER",
                "auditor-approved through end of Q2",
                1_000L, "dba", null));

        List<CisSuppression> active = dao.listActive("cx-a", 2_000L);
        assertEquals(1, active.size());
        assertEquals("CIS-2.1", active.get(0).ruleId());
    }

    @Test
    void listActive_filters_out_expired_rows() {
        dao.insert(new CisSuppression(-1, "cx-a", "CIS-2.1", "CLUSTER",
                "short-lived waiver", 1_000L, "dba", 500L));
        dao.insert(new CisSuppression(-1, "cx-a", "CIS-2.5", "CLUSTER",
                "still active", 1_000L, "dba", 10_000L));

        List<CisSuppression> active = dao.listActive("cx-a", 2_000L);
        assertEquals(1, active.size());
        assertEquals("CIS-2.5", active.get(0).ruleId());
    }

    @Test
    void listActive_excludes_other_connections() {
        dao.insert(new CisSuppression(-1, "cx-a", "CIS-2.1", "CLUSTER",
                "a", 1_000L, "dba", null));
        dao.insert(new CisSuppression(-1, "cx-b", "CIS-2.1", "CLUSTER",
                "b", 1_000L, "dba", null));
        assertEquals(1, dao.listActive("cx-a", 2_000L).size());
        assertEquals(1, dao.listActive("cx-b", 2_000L).size());
    }

    @Test
    void delete_removes_the_row_and_listActive_reflects_it() {
        CisSuppression ins = dao.insert(new CisSuppression(-1, "cx-a",
                "CIS-2.1", "CLUSTER", "tmp", 1_000L, "dba", null));
        assertTrue(dao.delete(ins.id()));
        assertTrue(dao.listActive("cx-a", 2_000L).isEmpty());
    }

    @Test
    void runner_integration_with_persisted_suppression() {
        // End-to-end: persist → load → hand to runner. CIS-2.1 fails a
        // no-encryption fixture by default; the suppression flips the
        // finding to suppressed.
        dao.insert(new CisSuppression(-1, "cx-a", "CIS-2.1", "CLUSTER",
                "audit-approved delay", 1_000L, "dba", null));

        CisRunner runner = new CisRunner(List.of(
                new com.kubrik.mex.security.cis.rules.RequireEncryptionAtRest()));

        ComplianceContext ctx = new ComplianceContext("cx-a",
                new com.kubrik.mex.security.access.UsersRolesFetcher.Snapshot(List.of(), List.of()),
                new com.kubrik.mex.security.authn.AuthBackendProbe.Snapshot(List.of(), 0L),
                List.of(new com.kubrik.mex.security.encryption.EncryptionStatus(
                        "h1", false, "", com.kubrik.mex.security.encryption.EncryptionStatus.Keystore.NONE,
                        null, "", "serverStatus")),
                List.of());

        CisReport report = runner.run(ctx, dao.listActive("cx-a", 2_000L), 2_000L);
        assertEquals(0, report.fail());
        assertEquals(1, report.suppressed());
    }
}

package com.kubrik.mex.maint.param;

import com.kubrik.mex.maint.model.ParamProposal;
import com.kubrik.mex.store.Database;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ParamProposalDaoTest {

    @TempDir Path dataDir;

    private Database db;
    private ParamProposalDao dao;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", dataDir.toString());
        db = new Database();
        dao = new ParamProposalDao(db);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (db != null) db.close();
    }

    @Test
    void round_trips_an_open_proposal() {
        ParamProposal p = new ParamProposal(
                "wiredTigerConcurrentReadTransactions",
                "128", "256", ParamProposal.Severity.ACT,
                "Hot-OLTP box warrants 256 readers.");
        long id = dao.insert("cx-1", "h1:27017", p, 1_700_000_000_000L);
        assertTrue(id > 0);

        List<ParamProposalDao.Row> open = dao.listOpenForConnection("cx-1");
        assertEquals(1, open.size());
        assertEquals("256", open.get(0).proposal().proposedValue());
        assertEquals(ParamProposalDao.Status.OPEN, open.get(0).status());
    }

    @Test
    void accepting_removes_from_open_listing() {
        ParamProposal p = new ParamProposal("ttlMonitorSleepSecs",
                "60", "30", ParamProposal.Severity.CONSIDER, "…");
        long id = dao.insert("cx-1", "h1:27017", p, 1_700_000_000_000L);
        assertTrue(dao.transition(id, ParamProposalDao.Status.ACCEPTED));
        assertTrue(dao.listOpenForConnection("cx-1").isEmpty());
    }

    @Test
    void proposals_are_scoped_per_connection() {
        ParamProposal p = new ParamProposal("notablescan",
                "false", "true", ParamProposal.Severity.ACT, "…");
        dao.insert("cx-1", "h1", p, 1L);
        dao.insert("cx-2", "h9", p, 2L);
        assertEquals(1, dao.listOpenForConnection("cx-1").size());
        assertEquals(1, dao.listOpenForConnection("cx-2").size());
    }
}

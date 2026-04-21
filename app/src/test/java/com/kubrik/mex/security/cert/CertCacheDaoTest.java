package com.kubrik.mex.security.cert;

import com.kubrik.mex.store.Database;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.6 Q2.6-E1 — DAO round-trip + the ON CONFLICT semantics that let
 * the Q2.6-E3 daily expiry check reprobe without producing duplicate
 * rows.
 */
class CertCacheDaoTest {

    @TempDir Path home;
    private Database db;
    private CertCacheDao dao;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", home.toString());
        db = new Database();
        dao = new CertCacheDao(db);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (db != null) db.close();
    }

    @Test
    void upsert_inserts_a_new_row_and_reads_back_identically() {
        CertRecord c = sampleCert("h1:27017", 2_000, 3_000, "0".repeat(64));
        CertCacheDao.Row row = dao.upsert("cx-a", c, 1_500);

        assertTrue(row.id() > 0);
        assertEquals("h1:27017", row.host());
        assertEquals("CN=example", row.subjectCn());
        assertEquals(1_500L, row.capturedAt());
        assertEquals("[\"DNS:example.com\",\"IP:10.0.0.1\"]", row.sansJson());
    }

    @Test
    void upsert_same_fingerprint_refreshes_captured_at_not_duplicate_row() {
        CertRecord c = sampleCert("h1:27017", 2_000, 3_000, "0".repeat(64));
        CertCacheDao.Row first = dao.upsert("cx-a", c, 1_500);
        CertCacheDao.Row second = dao.upsert("cx-a", c, 2_999);

        // Same id (upsert path) + refreshed captured_at. This matters for
        // the E3 daily check that reprobes every morning — we want last-
        // seen time to move forward without producing one row per day.
        assertEquals(first.id(), second.id());
        assertEquals(2_999L, second.capturedAt());

        assertEquals(1, dao.listForConnection("cx-a").size(),
                "a single logical cert must produce exactly one cache row");
    }

    @Test
    void listForConnection_orders_soonest_expiry_first() {
        long now = 1_000_000L;
        dao.upsert("cx-a", sampleCert("h1:27017", now, now + 100_000L, "a".repeat(64)), now);
        dao.upsert("cx-a", sampleCert("h2:27017", now, now + 10_000L, "b".repeat(64)), now);
        dao.upsert("cx-a", sampleCert("h3:27017", now, now + 50_000L, "c".repeat(64)), now);

        List<CertCacheDao.Row> rows = dao.listForConnection("cx-a");
        assertEquals("h2:27017", rows.get(0).host());
        assertEquals("h3:27017", rows.get(1).host());
        assertEquals("h1:27017", rows.get(2).host());
    }

    @Test
    void listExpiringBefore_filters_by_notAfter_cutoff() {
        long now = 1_000_000L;
        dao.upsert("cx-a", sampleCert("soon:27017", now, now + 10_000L, "a".repeat(64)), now);
        dao.upsert("cx-a", sampleCert("later:27017", now, now + 100_000L, "b".repeat(64)), now);

        List<CertCacheDao.Row> expiring = dao.listExpiringBefore("cx-a", now + 50_000L);
        assertEquals(1, expiring.size());
        assertEquals("soon:27017", expiring.get(0).host());
    }

    @Test
    void other_connection_certs_do_not_leak_through_the_connection_filter() {
        CertRecord c = sampleCert("h1:27017", 1, 2, "0".repeat(64));
        dao.upsert("cx-a", c, 1);
        dao.upsert("cx-b", c, 1);

        assertEquals(1, dao.listForConnection("cx-a").size());
        assertEquals(1, dao.listForConnection("cx-b").size());
    }

    private static CertRecord sampleCert(String host, long notBefore, long notAfter,
                                          String fingerprint) {
        return new CertRecord(host, "CN=example", "CN=issuer",
                List.of("DNS:example.com", "IP:10.0.0.1"),
                notBefore, notAfter, "deadbeef", fingerprint);
    }
}

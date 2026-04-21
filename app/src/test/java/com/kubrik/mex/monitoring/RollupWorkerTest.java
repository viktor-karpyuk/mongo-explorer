package com.kubrik.mex.monitoring;

import com.kubrik.mex.monitoring.model.LabelSet;
import com.kubrik.mex.monitoring.model.MetricId;
import com.kubrik.mex.monitoring.model.MetricSample;
import com.kubrik.mex.monitoring.model.RollupTier;
import com.kubrik.mex.monitoring.store.MetricStore;
import com.kubrik.mex.monitoring.store.RollupDao;
import com.kubrik.mex.monitoring.store.RollupWorker;
import com.kubrik.mex.store.Database;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RollupWorkerTest {

    @TempDir Path tmp;
    private Database db;
    private MetricStore store;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", tmp.toString());
        db = new Database();
        store = new MetricStore(db.connection());
    }

    @AfterEach
    void tearDown() throws Exception {
        store.close();
        db.close();
    }

    @Test
    void s10RollupAggregatesRawSamplesIntoTenSecondBuckets() throws Exception {
        List<MetricSample> samples = new ArrayList<>();
        long base = 1_700_000_000_000L; // aligned on 10 s: ends with 000
        // Bucket 0: 5 samples in [base, base+10s)
        for (int i = 0; i < 5; i++) samples.add(new MetricSample(
                "c1", MetricId.WT_3, LabelSet.EMPTY, base + i * 1_000L, 0.5 + i * 0.1));
        // Bucket 1: 3 samples in [base+10s, base+20s)
        for (int i = 0; i < 3; i++) samples.add(new MetricSample(
                "c1", MetricId.WT_3, LabelSet.EMPTY, base + 10_000 + i * 1_000L, 0.9));
        store.persistAsync(samples);
        store.flush();

        // Freeze "now" just after bucket 1 ends.
        Clock fixed = Clock.fixed(Instant.ofEpochMilli(base + 22_000), ZoneOffset.UTC);
        RollupDao dao = new RollupDao(db.connection());
        RollupWorker worker = new RollupWorker(dao, fixed);
        worker.runOnce(RollupTier.S10);

        long rowCount = rowCountIn("metric_samples_10s");
        assertEquals(2, rowCount, "expected 2 ten-second buckets");
    }

    private long rowCountIn(String table) throws Exception {
        try (PreparedStatement ps = db.connection().prepareStatement("SELECT COUNT(*) FROM " + table);
             ResultSet rs = ps.executeQuery()) {
            rs.next();
            return rs.getLong(1);
        }
    }
}

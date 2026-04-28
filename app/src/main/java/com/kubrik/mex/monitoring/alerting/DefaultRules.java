package com.kubrik.mex.monitoring.alerting;

import com.kubrik.mex.monitoring.model.MetricId;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/** The 18 ALERT-DEF-* rules shipped in v2.1.0. */
public final class DefaultRules {

    public static final String SOURCE_TAG = "default-v2.1.0";

    private DefaultRules() {}

    /** Installed as globals on first startup (connectionId = null). */
    public static List<AlertRule> all() {
        return List.of(
                g("ALERT-DEF-1",  MetricId.INST_CONN_5, Comparator.GT, 0.70, 0.90, 60),
                g("ALERT-DEF-2",  MetricId.WT_3,        Comparator.GT, 0.90, 0.97, 120),
                g("ALERT-DEF-3",  MetricId.WT_5,        Comparator.GT, 0.15, 0.20, 120),
                g("ALERT-DEF-4",  MetricId.WT_TKT_4,    Comparator.GT, 0.80, 0.95, 30),
                g("ALERT-DEF-5",  MetricId.WT_TKT_8,    Comparator.GT, 0.80, 0.95, 30),
                g("ALERT-DEF-6",  MetricId.LOCK_3,      Comparator.GT, 50.0, 200.0, 30),
                g("ALERT-DEF-7",  MetricId.LAT_5,       Comparator.GT, 100_000.0, 500_000.0, 60),
                g("ALERT-DEF-8",  MetricId.LAT_6,       Comparator.GT, 100_000.0, 500_000.0, 60),
                g("ALERT-DEF-9",  MetricId.REPL_NODE_5, Comparator.GT, 10.0, 60.0, 60),
                g("ALERT-DEF-10", MetricId.REPL_OPLOG_3, Comparator.LT, 48.0, 24.0, 300),
                // ALERT-DEF-11 member state (enum); encoded-state EQ on DOWN=8 for CRIT.
                g("ALERT-DEF-11", MetricId.REPL_NODE_1, Comparator.EQ, null, 8.0, 30),
                g("ALERT-DEF-12", MetricId.SHARD_BAL_1, Comparator.EQ, 0.0, null, 900),
                g("ALERT-DEF-13", MetricId.SHARD_CHK_3, Comparator.GT, 0.0, 10.0, 300),
                g("ALERT-DEF-14", MetricId.DBSTAT_9,    Comparator.GT, 0.30, 0.50, 3600),
                g("ALERT-DEF-15", MetricId.CUR_4,       Comparator.GT, 1.0, 10.0, 120),
                g("ALERT-DEF-16", MetricId.TXN_6,       Comparator.GT, 0.05, 0.20, 120),
                g("ALERT-DEF-17", MetricId.ASRT_4,      Comparator.GT, 1.0, 10.0, 60),
                g("ALERT-DEF-18", MetricId.WT_10,       Comparator.GT, 0.0, 10.0, 60)
        );
    }

    private static AlertRule g(String id, MetricId m, Comparator c,
                               Double warn, Double crit, long sustainSeconds) {
        return new AlertRule(id, null, m, Map.of(), c, warn, crit,
                Duration.ofSeconds(sustainSeconds), true, SOURCE_TAG);
    }
}

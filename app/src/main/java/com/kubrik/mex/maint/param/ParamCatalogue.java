package com.kubrik.mex.maint.param;

import com.kubrik.mex.maint.model.ClusterShape;

import java.util.List;
import java.util.Optional;

/**
 * v2.7 PARAM-2 — Curated catalogue of high-impact
 * {@code setParameter} knobs the wizard reasons about. A raw
 * escape-hatch ("run any setParameter") is explicitly NOT wired
 * here — that path lives behind three gates per the open-question
 * resolution in milestone §9.2.
 *
 * <p>Each entry carries a human rationale, an applicability check,
 * and a recommender function that produces the proposed value given
 * the {@link ClusterShape}. Pure functions; no driver calls; easy to
 * unit-test.</p>
 */
public final class ParamCatalogue {

    public record Entry(
            String name,
            String description,
            String rationale,
            java.util.function.Predicate<ClusterShape> appliesTo,
            java.util.function.Function<ClusterShape, Optional<Object>> recommend,
            Optional<Range> range
    ) {
        public Entry {
            java.util.Objects.requireNonNull(name);
            java.util.Objects.requireNonNull(description);
            java.util.Objects.requireNonNull(rationale);
            java.util.Objects.requireNonNull(appliesTo);
            java.util.Objects.requireNonNull(recommend);
            range = range == null ? Optional.empty() : range;
        }
    }

    /** Inclusive numeric range — an optional guardrail so the UI can
     *  refuse an out-of-bounds override. */
    public record Range(long min, long max) {
        public boolean contains(long v) { return v >= min && v <= max; }
    }

    public static List<Entry> all() {
        return java.util.List.of(
                WT_READ_TXNS,
                WT_WRITE_TXNS,
                TTL_MONITOR_SLEEP,
                NOTABLESCAN,
                QUERY_PLAN_EVAL_MAX
        );
    }

    public static Optional<Entry> byName(String name) {
        return all().stream().filter(e -> e.name().equals(name)).findFirst();
    }

    /* ============================== entries ============================== */

    /** Concurrent reader limit in WT. Defaults to 128; bump to 256 on
     *  high-core OLTP boxes, back off to 64 on memory-constrained
     *  hosts. */
    private static final Entry WT_READ_TXNS = new Entry(
            "wiredTigerConcurrentReadTransactions",
            "Concurrent reader slots in the WiredTiger storage engine.",
            "128 is the server default; high-core OLTP boxes benefit "
            + "from 256; memory-constrained or OLAP workloads should "
            + "stay at the default to avoid cache-churn under long "
            + "scans.",
            shape -> "wiredTiger".equals(shape.storageEngine()),
            shape -> {
                if (shape.cpuCores() >= 32
                        && shape.ramBytes() >= (64L * 1024L * 1024L * 1024L)
                        && shape.workload() == ClusterShape.Workload.OLTP) {
                    return Optional.of(256);
                }
                if (shape.ramBytes() < (8L * 1024L * 1024L * 1024L)) {
                    return Optional.of(64);
                }
                return Optional.of(128);
            },
            Optional.of(new Range(16, 512))
    );

    /** Concurrent writer limit in WT; same shape as the reader limit
     *  but typically half because writes are more expensive. */
    private static final Entry WT_WRITE_TXNS = new Entry(
            "wiredTigerConcurrentWriteTransactions",
            "Concurrent writer slots in the WiredTiger storage engine.",
            "128 is the default; OLTP hot-write workloads benefit "
            + "from 256. For analytics-heavy clusters, 64 keeps "
            + "writer contention low so long analytical reads don't "
            + "starve.",
            shape -> "wiredTiger".equals(shape.storageEngine()),
            shape -> {
                if (shape.cpuCores() >= 32
                        && shape.workload() == ClusterShape.Workload.OLTP) {
                    return Optional.of(256);
                }
                if (shape.workload() == ClusterShape.Workload.OLAP) {
                    return Optional.of(64);
                }
                return Optional.of(128);
            },
            Optional.of(new Range(16, 512))
    );

    /** TTL monitor sleep. Default 60 s; lower on high-churn TTL
     *  collections, higher on archival clusters where TTL is
     *  secondary. */
    private static final Entry TTL_MONITOR_SLEEP = new Entry(
            "ttlMonitorSleepSecs",
            "Seconds between TTL-expiry sweep passes.",
            "Default 60 s. Drop to 30 s when TTL deletions are a hot "
            + "path and staleness matters; raise to 300 s on "
            + "archive clusters where the TTL sweep contention "
            + "outweighs the deletion lag.",
            shape -> true,
            shape -> {
                if (shape.workload() == ClusterShape.Workload.OLTP
                        && shape.docCountApprox() > 1_000_000_000L) {
                    return Optional.of(30);
                }
                if (shape.workload() == ClusterShape.Workload.OLAP) {
                    return Optional.of(300);
                }
                return Optional.of(60);
            },
            Optional.of(new Range(1, 86_400))
    );

    /** notablescan — forbids full-collection scans when set. Default
     *  false (scans allowed). Recommend true for strict-OLTP
     *  production to catch missing indexes at query time. */
    private static final Entry NOTABLESCAN = new Entry(
            "notablescan",
            "When true, unindexed queries fail with an error.",
            "Enable on OLTP production clusters so a missing-index "
            + "regression fails loudly instead of dragging latency. "
            + "Leave off on analytics clusters where scans are "
            + "legitimate workloads.",
            shape -> true,
            shape -> shape.workload() == ClusterShape.Workload.OLTP
                    ? Optional.of(true)
                    : Optional.of(false),
            Optional.empty()
    );

    /** How many candidate plans the query planner evaluates.
     *  Default 101. Rarely needs changing; the rationale entry
     *  exists mostly as a signal that the knob exists. */
    private static final Entry QUERY_PLAN_EVAL_MAX = new Entry(
            "internalQueryPlanEvaluationMaxResults",
            "Max results considered per candidate plan during plan "
            + "evaluation.",
            "Default 101. Lower to 50 on large-collection clusters "
            + "if plan evaluation itself is eating noticeable time; "
            + "otherwise leave alone — the default is well-tuned.",
            shape -> true,
            shape -> shape.docCountApprox() > 10_000_000_000L
                    ? Optional.of(50) : Optional.of(101),
            Optional.of(new Range(10, 10_000))
    );

    private ParamCatalogue() {}
}

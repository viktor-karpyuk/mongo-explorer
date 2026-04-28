package com.kubrik.mex.monitoring.model;

import java.util.Map;
import java.util.Optional;

/**
 * Stable, enumerated identifier for every metric the Monitoring subsystem can emit.
 * The enum constant name matches the requirement ID in {@code docs/v2/v2.1/requirements.md}
 * §2–§6 exactly (dashes replaced with underscores). The {@link #metricName()} is the
 * stable wire / storage / export name.
 *
 * <p>Only Workstream A (instance-level) metrics are enumerated in P-1. The remaining
 * workstreams (REPL, SHARD, DBSTAT, COLLSTAT, IDX, TOP, OP, QPERF) extend this enum
 * in their own phases.
 */
public enum MetricId {
    // 2.1 Throughput
    INST_OP_1("mongo.ops.insert",          Unit.OPS_PER_SECOND),
    INST_OP_2("mongo.ops.query",           Unit.OPS_PER_SECOND),
    INST_OP_3("mongo.ops.update",          Unit.OPS_PER_SECOND),
    INST_OP_4("mongo.ops.delete",          Unit.OPS_PER_SECOND),
    INST_OP_5("mongo.ops.getmore",         Unit.OPS_PER_SECOND),
    INST_OP_6("mongo.ops.command",         Unit.OPS_PER_SECOND),
    INST_OP_7("mongo.ops.repl.insert",     Unit.OPS_PER_SECOND),
    INST_OP_8("mongo.ops.repl.update",     Unit.OPS_PER_SECOND),
    INST_OP_9("mongo.ops.repl.delete",     Unit.OPS_PER_SECOND),

    // 2.2 Latency
    LAT_1("mongo.latency.reads.avg_us",        Unit.MICROSECONDS),
    LAT_2("mongo.latency.writes.avg_us",       Unit.MICROSECONDS),
    LAT_3("mongo.latency.commands.avg_us",     Unit.MICROSECONDS),
    LAT_4("mongo.latency.transactions.avg_us", Unit.MICROSECONDS),
    LAT_5("mongo.latency.reads.p99_us",        Unit.MICROSECONDS),
    LAT_6("mongo.latency.writes.p99_us",       Unit.MICROSECONDS),
    LAT_7("mongo.latency.commands.p99_us",     Unit.MICROSECONDS),

    // 2.3 Connections
    INST_CONN_1("mongo.connections.current",       Unit.COUNT),
    INST_CONN_2("mongo.connections.available",     Unit.COUNT),
    INST_CONN_3("mongo.connections.active",        Unit.COUNT),
    INST_CONN_4("mongo.connections.totalCreated",  Unit.OPS_PER_SECOND),
    INST_CONN_5("mongo.connections.saturation",    Unit.RATIO),
    INST_CONN_6("mongo.connections.threaded",      Unit.COUNT),
    INST_CONN_7("mongo.connections.loadBalanced",  Unit.COUNT),

    // 2.4 Memory
    INST_MEM_1("mongo.mem.resident",  Unit.BYTES),
    INST_MEM_2("mongo.mem.virtual",   Unit.BYTES),
    INST_MEM_3("mongo.mem.mapped",    Unit.BYTES),
    INST_MEM_4("mongo.mem.supported", Unit.BOOL),

    // 2.5 WiredTiger cache
    WT_1 ("wt.cache.bytes_in_cache",           Unit.BYTES),
    WT_2 ("wt.cache.max_bytes",                Unit.BYTES),
    WT_3 ("wt.cache.fill_ratio",               Unit.RATIO),
    WT_4 ("wt.cache.dirty_bytes",              Unit.BYTES),
    WT_5 ("wt.cache.dirty_ratio",              Unit.RATIO),
    WT_6 ("wt.cache.pages_read",               Unit.OPS_PER_SECOND),
    WT_7 ("wt.cache.pages_written",            Unit.OPS_PER_SECOND),
    WT_8 ("wt.cache.pages_evicted",            Unit.OPS_PER_SECOND),
    WT_9 ("wt.cache.eviction.worker",          Unit.OPS_PER_SECOND),
    WT_10("wt.cache.eviction.app_thread",      Unit.OPS_PER_SECOND),
    WT_11("wt.cache.hit_ratio",                Unit.RATIO),
    WT_12("wt.cache.unmodified_pages_evicted", Unit.OPS_PER_SECOND),
    WT_13("wt.cache.modified_pages_evicted",   Unit.OPS_PER_SECOND),

    // 2.6 WiredTiger tickets
    WT_TKT_1("wt.tickets.read.out",         Unit.COUNT),
    WT_TKT_2("wt.tickets.read.available",   Unit.COUNT),
    WT_TKT_3("wt.tickets.read.total",       Unit.COUNT),
    WT_TKT_4("wt.tickets.read.saturation",  Unit.RATIO),
    WT_TKT_5("wt.tickets.write.out",        Unit.COUNT),
    WT_TKT_6("wt.tickets.write.available",  Unit.COUNT),
    WT_TKT_7("wt.tickets.write.total",      Unit.COUNT),
    WT_TKT_8("wt.tickets.write.saturation", Unit.RATIO),

    // 2.7 WiredTiger checkpoints
    WT_CKP_1("wt.checkpoint.last_duration_ms", Unit.MILLISECONDS),
    WT_CKP_2("wt.checkpoint.avg_duration_ms",  Unit.MILLISECONDS),
    WT_CKP_3("wt.checkpoint.max_duration_ms",  Unit.MILLISECONDS),
    WT_CKP_4("wt.checkpoint.scrub_ms",         Unit.MILLISECONDS),

    // 2.8 Global lock
    LOCK_1("mongo.globalLock.queue.readers",         Unit.COUNT),
    LOCK_2("mongo.globalLock.queue.writers",         Unit.COUNT),
    LOCK_3("mongo.globalLock.queue.total",           Unit.COUNT),
    LOCK_4("mongo.globalLock.activeClients.readers", Unit.COUNT),
    LOCK_5("mongo.globalLock.activeClients.writers", Unit.COUNT),

    // 2.9 Network
    NET_1("mongo.network.bytesIn",                    Unit.BYTES_PER_SECOND),
    NET_2("mongo.network.bytesOut",                   Unit.BYTES_PER_SECOND),
    NET_3("mongo.network.requests",                   Unit.OPS_PER_SECOND),
    NET_4("mongo.network.serviceExecutorTaskQueue",   Unit.COUNT),

    // 2.10 Cursors
    CUR_1("mongo.cursors.open.total",     Unit.COUNT),
    CUR_2("mongo.cursors.open.noTimeout", Unit.COUNT),
    CUR_3("mongo.cursors.open.pinned",    Unit.COUNT),
    CUR_4("mongo.cursors.timedOut",       Unit.OPS_PER_SECOND),

    // 2.11 Transactions
    TXN_1("mongo.txn.currentActive",   Unit.COUNT),
    TXN_2("mongo.txn.currentInactive", Unit.COUNT),
    TXN_3("mongo.txn.currentOpen",     Unit.COUNT),
    TXN_4("mongo.txn.committed",       Unit.OPS_PER_SECOND),
    TXN_5("mongo.txn.aborted",         Unit.OPS_PER_SECOND),
    TXN_6("mongo.txn.abortRate",       Unit.RATIO),
    TXN_7("mongo.txn.prepared",        Unit.OPS_PER_SECOND),
    TXN_8("mongo.txn.retried",         Unit.OPS_PER_SECOND),

    // 2.12 Asserts
    ASRT_1("mongo.asserts.regular",   Unit.OPS_PER_SECOND),
    ASRT_2("mongo.asserts.warning",   Unit.OPS_PER_SECOND),
    ASRT_3("mongo.asserts.msg",       Unit.OPS_PER_SECOND),
    ASRT_4("mongo.asserts.user",      Unit.OPS_PER_SECOND),
    ASRT_5("mongo.asserts.rollovers", Unit.OPS_PER_SECOND),

    // 3.1 Replication — per-node
    REPL_NODE_1("repl.member.state",             Unit.COUNT),
    REPL_NODE_2("repl.member.health",            Unit.BOOL),
    REPL_NODE_3("repl.member.uptime",            Unit.COUNT),
    REPL_NODE_4("repl.member.pingMs",            Unit.MILLISECONDS),
    REPL_NODE_5("repl.member.lagSeconds",        Unit.COUNT),
    REPL_NODE_6("repl.member.lastHeartbeatRecv", Unit.COUNT),
    REPL_NODE_7("repl.member.configVersion",     Unit.COUNT),
    REPL_NODE_8("repl.member.syncSourceId",      Unit.COUNT),

    // 3.2 Oplog
    REPL_OPLOG_1("repl.oplog.sizeBytes",   Unit.BYTES),
    REPL_OPLOG_2("repl.oplog.usedBytes",   Unit.BYTES),
    REPL_OPLOG_3("repl.oplog.windowHours", Unit.COUNT),
    REPL_OPLOG_4("repl.oplog.insertRate",  Unit.OPS_PER_SECOND),
    REPL_OPLOG_5("repl.oplog.insertBytes", Unit.BYTES_PER_SECOND),

    // 3.3 Elections
    REPL_ELEC_1("repl.election.lastAt",            Unit.TIMESTAMP_MS),
    REPL_ELEC_2("repl.election.count",             Unit.COUNT),
    REPL_ELEC_3("repl.election.priorityTakeovers", Unit.COUNT),
    REPL_ELEC_4("repl.election.stepDowns",         Unit.COUNT),

    // 4.1 Sharding balancer
    SHARD_BAL_1("shard.balancer.enabled",      Unit.BOOL),
    SHARD_BAL_2("shard.balancer.running",      Unit.BOOL),
    SHARD_BAL_3("shard.balancer.activeWindow", Unit.BOOL),

    // 4.2 Sharding chunks
    SHARD_CHK_1("shard.chunks.count",     Unit.COUNT),
    SHARD_CHK_2("shard.chunks.imbalance", Unit.COUNT),
    SHARD_CHK_3("shard.chunks.jumbo",     Unit.COUNT),
    SHARD_CHK_4("shard.chunks.nsJumbo",   Unit.COUNT),

    // 4.3 Sharding migrations
    SHARD_MIG_1("shard.migrations.success24h",   Unit.COUNT),
    SHARD_MIG_2("shard.migrations.failed24h",    Unit.COUNT),
    SHARD_MIG_3("shard.migrations.lastCompleted", Unit.TIMESTAMP_MS),

    // 4.4 Mongos / config servers
    SHARD_MGS_1("shard.mongos.count",         Unit.COUNT),
    SHARD_MGS_2("shard.configServers.state",  Unit.COUNT),

    // 5.1 Database stats
    DBSTAT_1 ("db.collections",        Unit.COUNT),
    DBSTAT_2 ("db.views",              Unit.COUNT),
    DBSTAT_3 ("db.objects",            Unit.COUNT),
    DBSTAT_4 ("db.dataSize",           Unit.BYTES),
    DBSTAT_5 ("db.storageSize",        Unit.BYTES),
    DBSTAT_6 ("db.indexSize",          Unit.BYTES),
    DBSTAT_7 ("db.totalSize",          Unit.BYTES),
    DBSTAT_8 ("db.freeStorageSize",    Unit.BYTES),
    DBSTAT_9 ("db.fragmentationRatio", Unit.RATIO),
    DBSTAT_10("db.avgObjSize",         Unit.BYTES),
    DBSTAT_11("db.indexes",            Unit.COUNT),
    DBSTAT_12("db.dailyGrowthBytes",   Unit.BYTES),

    // 5.2 Collection stats
    COLLSTAT_1 ("coll.count",                 Unit.COUNT),
    COLLSTAT_2 ("coll.size",                  Unit.BYTES),
    COLLSTAT_3 ("coll.avgObjSize",            Unit.BYTES),
    COLLSTAT_4 ("coll.storageSize",           Unit.BYTES),
    COLLSTAT_5 ("coll.freeStorageSize",       Unit.BYTES),
    COLLSTAT_6 ("coll.totalIndexSize",        Unit.BYTES),
    COLLSTAT_7 ("coll.nindexes",              Unit.COUNT),
    COLLSTAT_8 ("coll.capped",                Unit.BOOL),
    COLLSTAT_9 ("coll.wt.cacheBytesRead",     Unit.BYTES_PER_SECOND),
    COLLSTAT_10("coll.wt.blockBytesRead",     Unit.BYTES_PER_SECOND),
    COLLSTAT_11("coll.wt.blockBytesWritten",  Unit.BYTES_PER_SECOND),
    COLLSTAT_12("coll.wt.btreeEntries",       Unit.COUNT),

    // 5.3 Index footprint
    IDX_FOOT_1("idx.size",         Unit.BYTES),
    IDX_FOOT_2("idx.keyPattern",   Unit.COUNT),
    IDX_FOOT_3("idx.unique",       Unit.BOOL),
    IDX_FOOT_4("idx.sparse",       Unit.BOOL),
    IDX_FOOT_5("idx.ttlSeconds",   Unit.COUNT),
    IDX_FOOT_6("idx.partial",      Unit.BOOL),
    IDX_FOOT_7("idx.hidden",       Unit.BOOL),

    // 5.4 Index utilisation
    IDX_USE_1("idx.accesses.ops",    Unit.COUNT),
    IDX_USE_2("idx.accesses.since",  Unit.TIMESTAMP_MS),
    IDX_USE_3("idx.opsPerSec",       Unit.OPS_PER_SECOND),
    IDX_USE_4("idx.unusedCandidate", Unit.BOOL),

    // 6.1 Per-namespace top
    TOP_1("top.ns.readLockMs",        Unit.MILLISECONDS),
    TOP_2("top.ns.writeLockMs",       Unit.MILLISECONDS),
    TOP_3("top.ns.totalMs",           Unit.MILLISECONDS),
    TOP_4("top.ns.readCount",         Unit.OPS_PER_SECOND),
    TOP_5("top.ns.writeCount",        Unit.OPS_PER_SECOND),
    TOP_6("top.ns.avgReadLatencyMs",  Unit.MILLISECONDS),
    TOP_7("top.ns.avgWriteLatencyMs", Unit.MILLISECONDS),

    // 6.2 Current ops
    OP_1("op.active.count",            Unit.COUNT),
    OP_2("op.longestRunningSeconds",   Unit.COUNT),
    OP_3("op.waitingForLock.count",    Unit.COUNT),
    OP_4("op.prepareConflict.count",   Unit.COUNT),
    OP_5("op.byOpType",                Unit.COUNT);

    private final String metricName;
    private final Unit unit;

    MetricId(String metricName, Unit unit) {
        this.metricName = metricName;
        this.unit = unit;
    }

    public String metricName() { return metricName; }
    public Unit unit() { return unit; }

    private static final Map<String, MetricId> BY_NAME;
    static {
        java.util.Map<String, MetricId> m = new java.util.HashMap<>(values().length * 2);
        for (MetricId id : values()) m.put(id.metricName, id);
        BY_NAME = Map.copyOf(m);
    }

    public static Optional<MetricId> byMetricName(String name) {
        return Optional.ofNullable(BY_NAME.get(name));
    }
}

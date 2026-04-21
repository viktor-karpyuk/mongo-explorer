package com.kubrik.mex.ui.monitoring;

import com.kubrik.mex.monitoring.model.MetricId;

import java.util.EnumMap;
import java.util.Map;

/**
 * Human-readable MongoDB shell snippets that produce the same value as each
 * {@link MetricId}. Displayed in the expand modal's "Diagnostic command" block so
 * the user can reproduce a metric on the console without digging through server
 * docs. Falls back to a generic hint when no specific command is registered.
 */
public final class DiagnosticCommands {

    private static final Map<MetricId, String> COMMANDS = new EnumMap<>(MetricId.class);

    static {
        // --- Instance-level: serverStatus projections -----------------------
        String ss = "db.adminCommand({ serverStatus: 1 })";
        register(MetricId.INST_OP_1, ss + ".opcounters.insert  // counter → rate/s");
        register(MetricId.INST_OP_2, ss + ".opcounters.query");
        register(MetricId.INST_OP_3, ss + ".opcounters.update");
        register(MetricId.INST_OP_4, ss + ".opcounters.delete");
        register(MetricId.INST_OP_5, ss + ".opcounters.getmore");
        register(MetricId.INST_OP_6, ss + ".opcounters.command");
        register(MetricId.INST_CONN_1, ss + ".connections.current");
        register(MetricId.INST_CONN_2, ss + ".connections.available");
        register(MetricId.INST_CONN_3, ss + ".connections.active");
        register(MetricId.INST_CONN_5,
                "let s = " + ss + "; s.connections.current / (s.connections.current + s.connections.available)");

        // --- Latency buckets ------------------------------------------------
        register(MetricId.LAT_1, ss + ".opLatencies.reads     // totalLatency / ops = avg µs");
        register(MetricId.LAT_2, ss + ".opLatencies.writes");
        register(MetricId.LAT_3, ss + ".opLatencies.commands");

        // --- WiredTiger cache ----------------------------------------------
        register(MetricId.WT_1, ss + ".wiredTiger.cache['bytes currently in the cache']");
        register(MetricId.WT_2, ss + ".wiredTiger.cache['maximum bytes configured']");
        register(MetricId.WT_3, "let c = " + ss + ".wiredTiger.cache;"
                + " c['bytes currently in the cache'] / c['maximum bytes configured']");
        register(MetricId.WT_5, "let c = " + ss + ".wiredTiger.cache;"
                + " c['tracked dirty bytes in the cache'] / c['maximum bytes configured']");
        register(MetricId.WT_10, ss + ".wiredTiger.cache['application threads page read from disk to cache count']");

        // --- WT tickets -----------------------------------------------------
        register(MetricId.WT_TKT_4,
                "let t = " + ss + ".wiredTiger.concurrentTransactions.read; t.out / (t.out + t.available)");
        register(MetricId.WT_TKT_8,
                "let t = " + ss + ".wiredTiger.concurrentTransactions.write; t.out / (t.out + t.available)");

        // --- Global lock ----------------------------------------------------
        register(MetricId.LOCK_3, ss + ".globalLock.currentQueue.total");

        // --- Network --------------------------------------------------------
        register(MetricId.NET_1, ss + ".network.bytesIn   // counter → bytes/s");
        register(MetricId.NET_2, ss + ".network.bytesOut  // counter → bytes/s");

        // --- Cursors / Txn -------------------------------------------------
        register(MetricId.CUR_4, ss + ".metrics.cursor.timedOut // counter → rate/s");
        register(MetricId.TXN_6,
                "let t = " + ss + ".transactions; t.totalAborted / Math.max(1, t.totalCommitted + t.totalAborted)");

        // --- Replication ---------------------------------------------------
        register(MetricId.REPL_NODE_1, "rs.status().members  // member.state (1 primary, 2 secondary, ...)");
        register(MetricId.REPL_NODE_5, "rs.status().members  // member.optimeDate vs primary → lagSeconds");
        register(MetricId.REPL_OPLOG_1, "db.getSiblingDB('local').oplog.rs.stats().size");
        register(MetricId.REPL_OPLOG_3,
                "let c = db.getSiblingDB('local').oplog.rs;"
              + " let a = c.find().sort({ts:1}).limit(1).next().ts.t;"
              + " let b = c.find().sort({ts:-1}).limit(1).next().ts.t;"
              + " (b - a) / 3600");

        // --- Sharding ------------------------------------------------------
        register(MetricId.SHARD_BAL_1, "db.getSiblingDB('config').settings.findOne({_id:'balancer'})  // stopped flag");
        register(MetricId.SHARD_CHK_1,
                "db.getSiblingDB('config').chunks.aggregate([{$group:{_id:'$shard',n:{$sum:1}}}])");
        register(MetricId.SHARD_CHK_3, "db.getSiblingDB('config').chunks.countDocuments({ jumbo: true })");
        register(MetricId.SHARD_MIG_1,
                "db.getSiblingDB('config').changelog.countDocuments({what:'moveChunk.commit',"
                        + " time:{ $gt: new Date(Date.now() - 86400000) }})");
        register(MetricId.SHARD_MIG_2,
                "db.getSiblingDB('config').changelog.countDocuments({what:'moveChunk.error',"
                        + " time:{ $gt: new Date(Date.now() - 86400000) }})");
        register(MetricId.SHARD_MGS_1,
                "db.getSiblingDB('config').mongos.countDocuments({ ping: { $gt: new Date(Date.now()-60000) } })");

        // --- DB / collection / index -------------------------------------
        register(MetricId.DBSTAT_1, "db.getSiblingDB('<db>').stats().collections");
        register(MetricId.DBSTAT_4, "db.getSiblingDB('<db>').stats().dataSize");
        register(MetricId.DBSTAT_5, "db.getSiblingDB('<db>').stats().storageSize");
        register(MetricId.DBSTAT_6, "db.getSiblingDB('<db>').stats().indexSize");
        register(MetricId.COLLSTAT_1, "db.getSiblingDB('<db>').<coll>.estimatedDocumentCount()");
        register(MetricId.COLLSTAT_4, "db.getSiblingDB('<db>').<coll>.stats().storageSize");
        register(MetricId.COLLSTAT_6, "db.getSiblingDB('<db>').<coll>.stats().totalIndexSize");
        register(MetricId.IDX_FOOT_1, "db.getSiblingDB('<db>').<coll>.stats().indexSizes['<index>']");
        register(MetricId.IDX_USE_3,
                "db.getSiblingDB('<db>').<coll>.aggregate([{$indexStats:{}}])  // ops over interval");

        // --- Workload / top -----------------------------------------------
        register(MetricId.OP_1, "db.currentOp({active:true}).inprog.length");
        register(MetricId.OP_2, "db.currentOp({active:true}).inprog  // max secs_running");
        register(MetricId.OP_3, "db.currentOp({active:true,waitingForLock:true}).inprog.length");
        register(MetricId.TOP_1, "db.adminCommand({top:1}).totals['<db>.<coll>'].readLock.time");
        register(MetricId.TOP_3, "db.adminCommand({top:1}).totals['<db>.<coll>'].total.time");
    }

    private static void register(MetricId id, String cmd) { COMMANDS.put(id, cmd); }

    private DiagnosticCommands() {}

    /** Shell snippet that produces {@code metric}'s underlying value. Falls back to a generic hint. */
    public static String forMetric(MetricId metric) {
        String specific = COMMANDS.get(metric);
        if (specific != null) return specific;
        return "// No diagnostic snippet registered for " + metric.name() + "\n"
                + "// Metric wire name: " + metric.metricName() + "\n"
                + "// Derive from: db.adminCommand({serverStatus:1})";
    }
}

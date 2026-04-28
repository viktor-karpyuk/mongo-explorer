package com.kubrik.mex.monitoring.sampler;

import com.kubrik.mex.core.MongoService;
import com.kubrik.mex.monitoring.model.LabelSet;
import com.kubrik.mex.monitoring.model.MetricId;
import com.kubrik.mex.monitoring.model.MetricSample;
import com.kubrik.mex.monitoring.util.DocUtil;
import com.mongodb.MongoCommandException;
import org.bson.Document;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * REPL-NODE-* + REPL-ELEC-* via {@code replSetGetStatus}. Skipped silently when
 * the server is a standalone.
 */
public final class ReplStatusSampler implements Sampler {

    private final String connectionId;
    private final MongoService svc;

    public ReplStatusSampler(String connectionId, MongoService svc) {
        this.connectionId = connectionId;
        this.svc = svc;
    }

    @Override public SamplerKind kind() { return SamplerKind.REPL_STATUS; }
    @Override public String connectionId() { return connectionId; }

    @Override
    public List<MetricSample> sample(Instant now) {
        Document rs;
        try {
            rs = svc.database("admin").runCommand(new Document("replSetGetStatus", 1));
        } catch (MongoCommandException mce) {
            // NotYetInitialized / NoReplicationEnabled / etc. — standalone.
            return List.of();
        } catch (Throwable t) {
            return List.of();
        }
        long ts = now.toEpochMilli();
        List<MetricSample> out = new ArrayList<>();

        long primaryOptime = 0L;
        List<Document> members = rs.getList("members", Document.class, List.of());
        for (Document m : members) {
            if ("PRIMARY".equals(m.getString("stateStr"))) {
                Date d = (Date) m.get("optimeDate");
                if (d != null) primaryOptime = d.getTime();
                break;
            }
        }
        for (Document m : members) {
            String name = m.getString("name");
            LabelSet lbl = LabelSet.of("member", name == null ? "unknown" : name);
            emit(out, MetricId.REPL_NODE_1, lbl, ts, encodeState(m.getString("stateStr")));
            emit(out, MetricId.REPL_NODE_2, lbl, ts, DocUtil.longVal(m, "health", 0));
            emit(out, MetricId.REPL_NODE_3, lbl, ts, DocUtil.longVal(m, "uptime", 0));
            if (m.containsKey("pingMs")) emit(out, MetricId.REPL_NODE_4, lbl, ts, DocUtil.longVal(m, "pingMs", 0));
            if (!"PRIMARY".equals(m.getString("stateStr"))) {
                Date d = (Date) m.get("optimeDate");
                if (d != null && primaryOptime > 0) {
                    long lagMs = Math.max(0, primaryOptime - d.getTime());
                    emit(out, MetricId.REPL_NODE_5, lbl, ts, lagMs / 1000.0);
                }
            }
            Date hb = (Date) m.get("lastHeartbeatRecv");
            if (hb != null) emit(out, MetricId.REPL_NODE_6, lbl, ts, (ts - hb.getTime()) / 1000.0);
            emit(out, MetricId.REPL_NODE_7, lbl, ts, DocUtil.longVal(m, "configVersion", 0));
            if (m.containsKey("syncSourceId")) emit(out, MetricId.REPL_NODE_8, lbl, ts,
                    DocUtil.longVal(m, "syncSourceId", 0));
        }

        Document ecm = DocUtil.sub(rs, "electionCandidateMetrics");
        if (!ecm.isEmpty()) {
            Date led = (Date) ecm.get("lastElectionDate");
            if (led != null) emit(out, MetricId.REPL_ELEC_1, LabelSet.EMPTY, ts, led.getTime());
            emit(out, MetricId.REPL_ELEC_3, LabelSet.EMPTY, ts, DocUtil.longVal(ecm, "priorityTakeover", 0));
        }
        Document epm = DocUtil.sub(rs, "electionParticipantMetrics");
        if (!epm.isEmpty()) {
            emit(out, MetricId.REPL_ELEC_4, LabelSet.EMPTY, ts,
                    DocUtil.longVal(epm, "stepDownsCausedByHigherTerm", 0));
        }
        return out;
    }

    private void emit(List<MetricSample> out, MetricId id, LabelSet lbl, long ts, double v) {
        out.add(new MetricSample(connectionId, id, lbl, ts, v));
    }

    /** Stable numeric encoding for the enum field. */
    static double encodeState(String s) {
        return switch (s == null ? "" : s) {
            case "PRIMARY"    -> 1;
            case "SECONDARY"  -> 2;
            case "RECOVERING" -> 3;
            case "STARTUP"    -> 4;
            case "STARTUP2"   -> 5;
            case "ARBITER"    -> 7;
            case "DOWN"       -> 8;
            case "ROLLBACK"   -> 9;
            case "REMOVED"    -> 10;
            default           -> 0; // UNKNOWN
        };
    }
}

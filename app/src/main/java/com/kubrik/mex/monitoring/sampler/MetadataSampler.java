package com.kubrik.mex.monitoring.sampler;

import com.kubrik.mex.core.MongoService;
import com.kubrik.mex.monitoring.model.MetricSample;
import org.bson.Document;

import java.time.Instant;
import java.util.List;

/**
 * Produces the META-* labels (host, version, topology, etc.) once per hour.
 * The result is not a time-series metric — it flows through a separate channel
 * (see {@link #latestSnapshot()}) and the UI pulls the latest snapshot to label
 * the header.
 */
public final class MetadataSampler implements Sampler {

    public record Snapshot(
            String host,
            String version,
            String gitVersion,
            String storageEngine,
            String topology,
            long uptime,
            int pid,
            int cpuCores,
            long memSizeMB
    ) {}

    private final String connectionId;
    private final MongoService svc;
    private volatile Snapshot latest;

    public MetadataSampler(String connectionId, MongoService svc) {
        this.connectionId = connectionId;
        this.svc = svc;
    }

    public Snapshot latestSnapshot() { return latest; }

    @Override public SamplerKind kind() { return SamplerKind.METADATA; }
    @Override public String connectionId() { return connectionId; }

    @Override
    public List<MetricSample> sample(Instant now) {
        Document hi;
        Document bi;
        Document ss;
        try {
            hi = svc.database("admin").runCommand(new Document("hostInfo", 1));
            bi = svc.database("admin").runCommand(new Document("buildInfo", 1));
            ss = svc.database("admin").runCommand(new Document("serverStatus", 1));
        } catch (Throwable t) {
            return List.of();
        }
        Document sys = (Document) hi.getOrDefault("system", new Document());
        Document hello = svc.hello();
        String topology;
        if (hello.containsKey("setName")) topology = "replicaSet";
        else if (Boolean.TRUE.equals(hello.getBoolean("isdbgrid"))) topology = "sharded";
        else topology = "standalone";

        latest = new Snapshot(
                sys.getString("hostname"),
                bi.getString("version"),
                bi.getString("gitVersion"),
                ((Document) ss.getOrDefault("storageEngine", new Document())).getString("name"),
                topology,
                ss.get("uptime") instanceof Number n ? n.longValue() : 0L,
                ss.get("pid") instanceof Number p ? p.intValue() : 0,
                sys.get("numCores") instanceof Number c ? c.intValue() : 0,
                sys.get("memSizeMB") instanceof Number m ? m.longValue() : 0L
        );
        return List.of(); // not a time-series metric
    }
}

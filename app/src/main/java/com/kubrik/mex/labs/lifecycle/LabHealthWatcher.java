package com.kubrik.mex.labs.lifecycle;

import com.kubrik.mex.labs.model.LabDeployment;
import com.kubrik.mex.labs.model.LabTemplate;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;

/**
 * v2.8.4 — Polls the Lab's published mongod port until
 * {@code db.adminCommand("ping").ok} returns 1. Used by the apply
 * flow to decide when to call the seeder + connection writer.
 *
 * <p>The wait is bounded by the template's {@code est_startup_seconds}
 * plus a 2x slop factor (cold-pull from Docker Hub can blow the
 * nominal budget on a slow link). Returns {@code false} on timeout
 * rather than throwing — the caller flips status to FAILED + runs
 * cleanup.</p>
 */
public final class LabHealthWatcher {

    private static final Logger log = LoggerFactory.getLogger(LabHealthWatcher.class);

    /** How long after template's advertised startup budget before we
     *  give up. 2x matches spec §1.4 "healthy or wait 2 minutes". */
    public static final double SLOP_FACTOR = 2.0;

    private final Duration pollInterval;

    public LabHealthWatcher() { this(Duration.ofSeconds(2)); }

    LabHealthWatcher(Duration pollInterval) {
        this.pollInterval = pollInterval;
    }

    /**
     * Block until the Lab's designated "ready" port answers ping,
     * or the deadline expires. Returns the fraction of wall time
     * consumed (for metrics / event payload).
     */
    public boolean awaitHealthy(LabDeployment lab, LabTemplate template) {
        String readyContainer = chooseReadyContainer(template);
        int port = lab.portMap().portFor(readyContainer);
        String uri = "mongodb://127.0.0.1:" + port + "/?directConnection=true";

        long budgetMs = (long) (template.estStartupSeconds() * SLOP_FACTOR * 1000);
        Instant deadline = Instant.now().plusMillis(budgetMs);

        log.info("watching {} on port {} (budget {} ms)",
                readyContainer, port, budgetMs);

        while (Instant.now().isBefore(deadline)) {
            if (pingOnce(uri)) return true;
            try { Thread.sleep(pollInterval.toMillis()); }
            catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return false;
    }

    private static boolean pingOnce(String uri) {
        try (MongoClient client = MongoClients.create(uri)) {
            Document reply = client.getDatabase("admin")
                    .runCommand(new Document("ping", 1));
            double ok = reply.get("ok") instanceof Number n ? n.doubleValue() : 0.0;
            return ok >= 1.0;
        } catch (Exception e) {
            return false;
        }
    }

    /** Pick the container whose "ready" answers healthy for the Lab:
     *  for sharded → mongos (the entry point), for replsets → the
     *  first member seed, for standalone → the lone mongod. Public
     *  because SeedRunner + LabAutoConnectionWriter both call it. */
    public static String chooseReadyContainer(LabTemplate template) {
        var containers = template.containerNames();
        if (containers.contains("mongos")) return "mongos";
        if (containers.contains("rs1a")) return "rs1a";
        if (containers.contains("mongo")) return "mongo";
        // Fallback: first non-init container.
        for (String c : containers) {
            if (!"init".equalsIgnoreCase(c)) return c;
        }
        throw new IllegalStateException(
                "template " + template.id() + " has no non-init container");
    }
}

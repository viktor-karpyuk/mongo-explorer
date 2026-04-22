package com.kubrik.mex.maint.reconfig;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * v2.7 Q2.7-D — Polls {@code replSetGetStatus} after a reconfig and
 * confirms the cluster has caught up to the new config version on a
 * majority of reachable members (RCFG-6).
 *
 * <p>Separate from the runner so the wait loop can be tested with a
 * fake command executor; the driver-facing implementation just passes
 * through to {@code admin.runCommand}.</p>
 */
public final class PostChangeVerifier {

    private static final Logger log = LoggerFactory.getLogger(PostChangeVerifier.class);

    /** RCFG-6 — up to 120 s for majority catch-up. */
    public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(120);

    @FunctionalInterface
    public interface StatusFetcher {
        Document fetch() throws Exception;
    }

    public record Verdict(
            boolean converged,
            int reachableMembers,
            int caughtUpMembers,
            List<String> lagging,
            Duration elapsed) {}

    private final Duration timeout;
    private final Duration pollInterval;
    private final java.util.function.LongSupplier clock;

    public PostChangeVerifier() {
        this(DEFAULT_TIMEOUT, Duration.ofMillis(500), System::currentTimeMillis);
    }

    public PostChangeVerifier(Duration timeout, Duration pollInterval,
                              java.util.function.LongSupplier clock) {
        this.timeout = Objects.requireNonNull(timeout);
        this.pollInterval = Objects.requireNonNull(pollInterval);
        this.clock = Objects.requireNonNull(clock);
    }

    public Verdict awaitConvergence(MongoClient client, int expectedVersion) {
        MongoDatabase admin = client.getDatabase("admin");
        return awaitConvergence(() -> admin.runCommand(
                new Document("replSetGetStatus", 1)), expectedVersion);
    }

    /** Test-visible form. The fetcher hands back a parsed
     *  {@code replSetGetStatus} reply. */
    public Verdict awaitConvergence(StatusFetcher fetcher, int expectedVersion) {
        long start = clock.getAsLong();
        long deadline = start + timeout.toMillis();
        Verdict last = new Verdict(false, 0, 0, List.of(), Duration.ZERO);
        while (clock.getAsLong() < deadline) {
            Verdict v = probe(fetcher, expectedVersion, start);
            last = v;
            if (v.converged()) return v;
            sleep(pollInterval);
        }
        return last;
    }

    @SuppressWarnings("unchecked")
    private Verdict probe(StatusFetcher fetcher, int expectedVersion, long start) {
        Document reply;
        try {
            reply = fetcher.fetch();
        } catch (Exception e) {
            log.debug("replSetGetStatus failed during post-change verify: {}",
                    e.getMessage());
            return new Verdict(false, 0, 0, List.of(),
                    Duration.ofMillis(clock.getAsLong() - start));
        }
        List<Document> members = (List<Document>) reply.get("members", List.class);
        if (members == null) {
            return new Verdict(false, 0, 0, List.of(),
                    Duration.ofMillis(clock.getAsLong() - start));
        }

        int reachable = 0;
        int caughtUp = 0;
        List<String> lagging = new ArrayList<>();
        for (Document m : members) {
            Integer health = m.getInteger("health");
            if (health == null || health == 0) continue;  // unreachable
            reachable++;
            // Pre-6.0 emits the new configVersion in `configVersion`;
            // post-6.0 also stamps configTerm. Both carry the same
            // monotonic bump we care about.
            Integer cfgVersion = m.getInteger("configVersion");
            if (cfgVersion != null && cfgVersion >= expectedVersion) {
                caughtUp++;
            } else {
                lagging.add(m.getString("name"));
            }
        }
        // Majority = more than half the reachable set (strict >
        // avoids the 2-of-4 tie).
        boolean converged = reachable > 0 && caughtUp * 2 > reachable;
        return new Verdict(converged, reachable, caughtUp,
                List.copyOf(lagging),
                Duration.ofMillis(clock.getAsLong() - start));
    }

    private static void sleep(Duration d) {
        try { Thread.sleep(d.toMillis()); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}

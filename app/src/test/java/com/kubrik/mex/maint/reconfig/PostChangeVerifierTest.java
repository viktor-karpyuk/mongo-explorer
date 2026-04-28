package com.kubrik.mex.maint.reconfig;

import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class PostChangeVerifierTest {

    @Test
    void converges_when_majority_catches_up() {
        AtomicInteger probe = new AtomicInteger();
        PostChangeVerifier v = new PostChangeVerifier(
                Duration.ofSeconds(5), Duration.ofMillis(1),
                new AtomicLong(0L)::incrementAndGet);

        // First probe: 1 of 3 caught up. Second: 3 of 3.
        PostChangeVerifier.StatusFetcher fetcher = () -> probe.incrementAndGet() == 1
                ? statusReply(List.of(8, 7, 7))
                : statusReply(List.of(8, 8, 8));

        PostChangeVerifier.Verdict verdict = v.awaitConvergence(fetcher, 8);
        assertTrue(verdict.converged());
        assertEquals(3, verdict.reachableMembers());
        assertEquals(3, verdict.caughtUpMembers());
    }

    @Test
    void reports_lagging_members_when_timeout_hits() {
        PostChangeVerifier v = new PostChangeVerifier(
                Duration.ofMillis(5), Duration.ofMillis(1),
                System::currentTimeMillis);
        PostChangeVerifier.StatusFetcher fetcher = () ->
                statusReply(List.of(8, 7, 7));  // 1/3, never caught up
        PostChangeVerifier.Verdict verdict = v.awaitConvergence(fetcher, 8);
        assertFalse(verdict.converged());
        assertEquals(2, verdict.lagging().size());
    }

    @Test
    void unreachable_member_does_not_count_toward_reachable() {
        AtomicLong clock = new AtomicLong(0);
        PostChangeVerifier v = new PostChangeVerifier(
                Duration.ofSeconds(5), Duration.ofMillis(1),
                clock::incrementAndGet);
        PostChangeVerifier.StatusFetcher fetcher = () -> {
            Document reply = new Document();
            reply.put("members", List.of(
                    new Document("health", 1)
                            .append("configVersion", 8)
                            .append("name", "h1"),
                    new Document("health", 0).append("name", "h2"),  // down
                    new Document("health", 1)
                            .append("configVersion", 8)
                            .append("name", "h3")));
            return reply;
        };
        PostChangeVerifier.Verdict verdict = v.awaitConvergence(fetcher, 8);
        assertTrue(verdict.converged());
        assertEquals(2, verdict.reachableMembers(),
                "only health=1 members count toward reachable");
    }

    private static Document statusReply(List<Integer> versions) {
        Document reply = new Document();
        reply.put("members", versions.stream()
                .map(v -> new Document("health", 1)
                        .append("configVersion", v)
                        .append("name", "h" + v))
                .toList());
        return reply;
    }
}

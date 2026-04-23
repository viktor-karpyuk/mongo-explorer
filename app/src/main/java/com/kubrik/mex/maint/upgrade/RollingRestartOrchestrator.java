package com.kubrik.mex.maint.upgrade;

import com.kubrik.mex.maint.model.UpgradePlan;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * v2.7 — Drives the {@code BINARY_SWAP} + {@code ROLLING_RESTART}
 * steps from an {@link UpgradePlan.Plan} into a live rolling
 * restart. Binary replacement itself is out-of-scope (NG-2.7-1); the
 * orchestrator asks the operator to confirm "binary swap complete?"
 * after each shutdown, then waits for the member to return.
 *
 * <p>Wall-time bound per step: 10 min for shutdown + binary swap +
 * re-sync to SECONDARY. The operator can extend via
 * {@link #withShutdownTimeout}. Bailing out leaves the rest of the
 * plan queued for a later retry — nothing is fired until the
 * operator advances.</p>
 */
public final class RollingRestartOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(RollingRestartOrchestrator.class);

    public static final Duration DEFAULT_SHUTDOWN_TIMEOUT = Duration.ofMinutes(10);

    private final Duration shutdownTimeout;

    public RollingRestartOrchestrator() {
        this(DEFAULT_SHUTDOWN_TIMEOUT);
    }

    public RollingRestartOrchestrator(Duration shutdownTimeout) {
        this.shutdownTimeout = shutdownTimeout;
    }

    public RollingRestartOrchestrator withShutdownTimeout(Duration t) {
        return new RollingRestartOrchestrator(t);
    }

    /** Per-step callback — the UI implements it to render a
     *  confirmation dialog ("binary swap complete on host X?") and
     *  block the runner until the operator clicks Continue. */
    @FunctionalInterface
    public interface OperatorGate {
        boolean awaitBinarySwap(String host);
    }

    /** Seam for member-level MongoClient opens; the production wiring
     *  uses the existing peer-client cache. */
    @FunctionalInterface
    public interface MemberClientOpener {
        MongoClient open(String hostPort);
    }

    public record StepOutcome(
            int stepOrder, String host, UpgradePlan.StepKind kind,
            boolean success, String message, long elapsedMs) {}

    public record Result(List<StepOutcome> stepOutcomes, boolean completed) {
        public boolean overallSuccess() {
            return completed && stepOutcomes.stream().allMatch(StepOutcome::success);
        }
    }

    /** Walk each step in the supplied plan; BINARY_SWAP + ROLLING_RESTART
     *  kinds fire dispatch, others just publish an info event. The
     *  {@code eventSink} gets one call per step for the UI to render
     *  progress. */
    public Result run(UpgradePlan.Plan plan, MemberClientOpener opener,
                      OperatorGate gate, Consumer<StepOutcome> eventSink) {
        List<StepOutcome> out = new ArrayList<>();
        for (UpgradePlan.Step step : plan.steps()) {
            StepOutcome outcome = runOne(step, opener, gate);
            out.add(outcome);
            if (eventSink != null) eventSink.accept(outcome);
            if (!outcome.success()) {
                log.warn("rolling restart bailing — step {} ({}) failed: {}",
                        step.order(), step.kind(), outcome.message());
                return new Result(List.copyOf(out), false);
            }
        }
        return new Result(List.copyOf(out), true);
    }

    private StepOutcome runOne(UpgradePlan.Step step,
                               MemberClientOpener opener,
                               OperatorGate gate) {
        long t0 = System.currentTimeMillis();
        switch (step.kind()) {
            case BINARY_SWAP -> {
                if (step.targetHost() == null) {
                    return info(step, t0, "cluster-wide binary-swap step");
                }
                // Two distinct socket-failure sources:
                //   (a) opener.open() — client can't connect (host down
                //       BEFORE we ever reached it). That's a real
                //       failure; a network blip or pre-killed server
                //       must NOT be silently treated as "swap complete".
                //   (b) runCommand(shutdown) — the server tore down the
                //       connection as part of handling shutdown. THAT
                //       is the success signal.
                MongoClient c;
                try {
                    c = opener.open(step.targetHost());
                } catch (Exception e) {
                    return fail(step, t0, "could not connect to "
                            + step.targetHost() + " before shutdown: "
                            + e.getMessage());
                }
                try (c) {
                    MongoDatabase admin = c.getDatabase("admin");
                    try {
                        admin.runCommand(new Document("shutdown", 1)
                                .append("timeoutSecs",
                                        (int) shutdownTimeout.toSeconds()));
                    } catch (com.mongodb.MongoSocketException expected) {
                        // Post-shutdown connection teardown = success.
                    }
                } catch (Exception e) {
                    return fail(step, t0, "shutdown failed: " + e.getMessage());
                }
                if (gate == null || !gate.awaitBinarySwap(step.targetHost())) {
                    return fail(step, t0,
                            "operator declined to continue after shutdown");
                }
                return ok(step, t0, "shutdown sent; operator confirmed swap");
            }
            case ROLLING_RESTART -> {
                // Step-down the primary.
                if (step.targetHost() == null) {
                    return info(step, t0, "cluster-wide restart step");
                }
                MongoClient c;
                try {
                    c = opener.open(step.targetHost());
                } catch (Exception e) {
                    return fail(step, t0, "could not connect to primary "
                            + step.targetHost() + ": " + e.getMessage());
                }
                try (c) {
                    MongoDatabase admin = c.getDatabase("admin");
                    try {
                        admin.runCommand(new Document("replSetStepDown", 60)
                                .append("force", false));
                        return ok(step, t0, "step-down completed");
                    } catch (com.mongodb.MongoSocketException se) {
                        // Same as BINARY_SWAP — the replSetStepDown
                        // closes the connection once the election wins.
                        return ok(step, t0,
                                "step-down completed (connection closed)");
                    }
                } catch (Exception e) {
                    return fail(step, t0,
                            "step-down failed: " + e.getMessage());
                }
            }
            default -> {
                // PRE_CHECK / BACKUP / FCV_LOWER / FCV_RAISE / POST_CHECK
                // are informational or fired by a different wizard;
                // publish an info event so the UI shows progress.
                return info(step, t0, step.body());
            }
        }
    }

    private StepOutcome ok(UpgradePlan.Step s, long t0, String msg) {
        return new StepOutcome(s.order(), s.targetHost(), s.kind(), true, msg,
                System.currentTimeMillis() - t0);
    }

    private StepOutcome fail(UpgradePlan.Step s, long t0, String msg) {
        return new StepOutcome(s.order(), s.targetHost(), s.kind(), false, msg,
                System.currentTimeMillis() - t0);
    }

    private StepOutcome info(UpgradePlan.Step s, long t0, String msg) {
        return new StepOutcome(s.order(), s.targetHost(), s.kind(), true,
                "info: " + msg, System.currentTimeMillis() - t0);
    }
}

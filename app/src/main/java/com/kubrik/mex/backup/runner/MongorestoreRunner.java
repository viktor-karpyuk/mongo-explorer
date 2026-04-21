package com.kubrik.mex.backup.runner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * v2.5 Q2.5-E — {@code mongorestore} subprocess wrapper, mirroring
 * {@link MongodumpRunner}'s contract. Spawns the binary, streams lines
 * through a {@link RunLog}, parses progress via
 * {@link RestoreProgressLine}, and escalates SIGTERM → SIGKILL on
 * {@link #cancel()} after {@link #SIGKILL_ESCALATION}.
 *
 * <p>Blocks on {@link #run()}; callers should invoke on a background thread.
 * {@code DumpOutcome} is reused as the result type — the name is historical
 * but the shape (exit code + killed flag + duration + stderr tail) fits
 * both runners.</p>
 */
public final class MongorestoreRunner {

    private static final Logger log = LoggerFactory.getLogger(MongorestoreRunner.class);

    static final Duration SIGKILL_ESCALATION = Duration.ofSeconds(10);
    static final int STDERR_TAIL_LINES = 100;

    private final String binary;
    private final MongorestoreOptions options;
    private final Consumer<RestoreProgressLine> onProgress;

    private volatile Process process;
    private final AtomicBoolean cancelled = new AtomicBoolean(false);

    public MongorestoreRunner(String binary, MongorestoreOptions options,
                              Consumer<RestoreProgressLine> onProgress) {
        this.binary = binary == null || binary.isBlank() ? "mongorestore" : binary;
        this.options = options;
        this.onProgress = onProgress == null ? p -> { } : onProgress;
    }

    public DumpOutcome run() throws IOException {
        List<String> argv = MongorestoreCommandBuilder.build(binary, options);
        log.info("mongorestore argv: {}", MongorestoreCommandBuilder.redactUri(argv));
        ProcessBuilder pb = new ProcessBuilder(argv);
        pb.redirectErrorStream(true);
        long started = System.nanoTime();
        process = pb.start();

        RunLog runLog = new RunLog();
        readAllLines(process, runLog);

        int exit;
        try {
            exit = process.waitFor();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return killAndReport(runLog, started, true);
        }
        long elapsed = (System.nanoTime() - started) / 1_000_000L;
        return new DumpOutcome(exit, cancelled.get(), elapsed,
                runLog.tail(STDERR_TAIL_LINES));
    }

    public void cancel() {
        if (!cancelled.compareAndSet(false, true)) return;
        Process p = this.process;
        if (p == null || !p.isAlive()) return;
        p.destroy();
        try {
            if (!p.waitFor(SIGKILL_ESCALATION.toMillis(), TimeUnit.MILLISECONDS)) {
                p.destroyForcibly();
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            p.destroyForcibly();
        }
    }

    /* ============================= internals ============================= */

    private void readAllLines(Process p, RunLog runLog) {
        Thread t = Thread.ofVirtual().name("mongorestore-reader").unstarted(() -> {
            try (BufferedReader r = new BufferedReader(
                    new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = r.readLine()) != null) {
                    runLog.append(line);
                    RestoreProgressLine.parse(line).ifPresent(onProgress);
                }
            } catch (IOException e) {
                log.debug("mongorestore reader ended: {}", e.getMessage());
            }
        });
        t.start();
    }

    private DumpOutcome killAndReport(RunLog runLog, long startedNanos, boolean killed) {
        Process p = this.process;
        if (p != null && p.isAlive()) p.destroyForcibly();
        long elapsed = (System.nanoTime() - startedNanos) / 1_000_000L;
        return new DumpOutcome(-1, killed, elapsed, runLog.tail(STDERR_TAIL_LINES));
    }
}

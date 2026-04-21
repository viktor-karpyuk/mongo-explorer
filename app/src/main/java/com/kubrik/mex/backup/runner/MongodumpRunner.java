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
 * v2.5 BKP-RUN-1..8 — spawns {@code mongodump} as a subprocess, reads
 * stdout + stderr through a {@link RunLog}, parses progress lines via
 * {@link ProgressLine}, and enforces the SIGTERM → SIGKILL watchdog.
 *
 * <p>Side effects are scoped to {@link #run}: it blocks until the process
 * terminates (or the watchdog escalates), so callers should invoke it on a
 * background thread. {@link #cancel()} is thread-safe and fires SIGTERM;
 * if the process doesn't exit within {@link #SIGKILL_ESCALATION} the
 * runner destroys it forcibly.</p>
 */
public final class MongodumpRunner {

    private static final Logger log = LoggerFactory.getLogger(MongodumpRunner.class);

    /** Time between SIGTERM and SIGKILL when {@link #cancel()} is called. */
    static final Duration SIGKILL_ESCALATION = Duration.ofSeconds(10);
    /** Lines captured from stderr for the notes field on failure. */
    static final int STDERR_TAIL_LINES = 100;

    private final String binary;
    private final MongodumpOptions options;
    private final Consumer<ProgressLine> onProgress;

    private volatile Process process;
    private final AtomicBoolean cancelled = new AtomicBoolean(false);

    public MongodumpRunner(String binary, MongodumpOptions options,
                           Consumer<ProgressLine> onProgress) {
        this.binary = binary == null || binary.isBlank() ? "mongodump" : binary;
        this.options = options;
        this.onProgress = onProgress == null ? p -> { } : onProgress;
    }

    /** Runs the subprocess to completion. Blocks until exit / kill. */
    public DumpOutcome run() throws IOException {
        List<String> argv = MongodumpCommandBuilder.build(binary, options);
        log.info("mongodump argv: {}", MongodumpCommandBuilder.redactUri(argv));
        ProcessBuilder pb = new ProcessBuilder(argv);
        pb.redirectErrorStream(true);  // stderr merges into stdout
        java.nio.file.Files.createDirectories(options.outDir());
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
        String tail = runLog.tail(STDERR_TAIL_LINES);
        return new DumpOutcome(exit, cancelled.get(), elapsed, tail);
    }

    /**
     * Cancels the running subprocess. Sends SIGTERM; if the process doesn't
     * exit within {@link #SIGKILL_ESCALATION} the runner escalates to
     * SIGKILL. Safe to call from any thread; subsequent calls are no-ops.
     */
    public void cancel() {
        if (!cancelled.compareAndSet(false, true)) return;
        Process p = this.process;
        if (p == null || !p.isAlive()) return;
        p.destroy();   // SIGTERM
        try {
            if (!p.waitFor(SIGKILL_ESCALATION.toMillis(), TimeUnit.MILLISECONDS)) {
                p.destroyForcibly();  // SIGKILL
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            p.destroyForcibly();
        }
    }

    /* ============================= internals ============================= */

    private void readAllLines(Process p, RunLog runLog) {
        Thread t = Thread.ofVirtual().name("mongodump-reader").unstarted(() -> {
            try (BufferedReader r = new BufferedReader(
                    new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = r.readLine()) != null) {
                    runLog.append(line);
                    ProgressLine.parse(line).ifPresent(onProgress);
                }
            } catch (IOException e) {
                log.debug("mongodump reader ended: {}", e.getMessage());
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

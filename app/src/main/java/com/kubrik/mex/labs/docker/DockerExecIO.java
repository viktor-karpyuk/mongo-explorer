package com.kubrik.mex.labs.docker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * v2.8.4 — Bounded-wall process runner for {@code docker} CLI
 * invocations. Captures stdout + stderr to per-Lab log files so an
 * apply that failed overnight is still inspectable next session.
 *
 * <p>Uses virtual threads for the stream-drainers — no thread pool
 * to tune, no leaks on a process that printed a lot before exiting.</p>
 *
 * <p>The docker CLI itself is assumed to be on {@code PATH}; no
 * attempt at shell scripting — we invoke the binary directly with
 * an array of args so quoting is the caller's problem, not a
 * shell-escape hazard.</p>
 */
public final class DockerExecIO {

    private static final Logger log = LoggerFactory.getLogger(DockerExecIO.class);

    /** Keep the last-N lines of each stream in memory for quick
     *  inline surfacing — the full log is on disk for deep dives. */
    private static final int TAIL_LINES = 40;

    /**
     * Run {@code args} with the given wall timeout. Stream both
     * outputs to the supplied log files AND keep the tail for
     * inline rendering. Kills the process tree on timeout.
     */
    public static ExecResult run(List<String> args, Path stdoutLog,
                                  Path stderrLog, Duration wallTimeout) throws IOException {
        Files.createDirectories(stdoutLog.getParent());
        Files.createDirectories(stderrLog.getParent());

        ProcessBuilder pb = new ProcessBuilder(args);
        pb.redirectErrorStream(false);
        log.debug("exec: {}", String.join(" ", args));
        Instant t0 = Instant.now();

        Process proc = pb.start();

        ExecutorService drainers = Executors.newVirtualThreadPerTaskExecutor();
        Future<String> stdoutTail = drainers.submit(() ->
                drainAndTail(proc.getInputStream(), stdoutLog));
        Future<String> stderrTail = drainers.submit(() ->
                drainAndTail(proc.getErrorStream(), stderrLog));

        boolean finished;
        try {
            finished = proc.waitFor(wallTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            proc.destroyForcibly();
            drainers.shutdownNow();
            throw new IOException("interrupted waiting for docker", ie);
        }

        if (!finished) {
            // Exceeded wall — kill the process + every descendant.
            proc.descendants().forEach(ProcessHandle::destroyForcibly);
            proc.destroyForcibly();
            try { proc.waitFor(5, TimeUnit.SECONDS); }
            catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
            drainers.shutdownNow();
            throw new IOException("docker exceeded wall timeout of "
                    + wallTimeout.toSeconds() + "s: " + String.join(" ", args));
        }

        String soTail, seTail;
        try {
            soTail = stdoutTail.get(5, TimeUnit.SECONDS);
            seTail = stderrTail.get(5, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            soTail = "(tail unavailable: " + e.getClass().getSimpleName() + ")";
            seTail = soTail;
        } finally {
            drainers.shutdown();
        }

        return new ExecResult(proc.exitValue(),
                Duration.between(t0, Instant.now()),
                stdoutLog, stderrLog, soTail, seTail);
    }

    /** Convenience: default log paths under {@code /tmp/docker-<pid>} — used
     *  for one-shot probes where per-Lab logs aren't meaningful. */
    public static ExecResult runOneShot(List<String> args,
                                         Duration wallTimeout) throws IOException {
        Path tmp = Files.createTempDirectory("mex-docker-probe-");
        return run(args, tmp.resolve("stdout.log"),
                tmp.resolve("stderr.log"), wallTimeout);
    }

    /** Drain an input stream into a file AND keep the tail in memory. */
    private static String drainAndTail(InputStream in, Path file) throws IOException {
        ArrayList<String> tail = new ArrayList<>(TAIL_LINES);
        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(in, StandardCharsets.UTF_8))) {
            String line;
            while ((line = br.readLine()) != null) {
                Files.writeString(file, line + "\n",
                        StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.APPEND);
                tail.add(line);
                if (tail.size() > TAIL_LINES) tail.remove(0);
            }
        }
        return String.join("\n", tail);
    }

    /** Convenience factory for the command line used in a single log line. */
    public static String cmdline(String... parts) { return String.join(" ", Arrays.asList(parts)); }

    private DockerExecIO() {}
}

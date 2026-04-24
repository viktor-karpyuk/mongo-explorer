package com.kubrik.mex.labs.k8s.distro;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * v2.8.1 Q2.8-N2 — Minimal CLI process runner for minikube / k3d.
 *
 * <p>Scoped narrower than {@code labs.docker.DockerExecIO} — we
 * capture stdout/stderr in-memory (no per-run log files), return
 * them verbatim, and bound the wall budget. Distro CLI calls are
 * short-lived (few seconds) so streaming to disk is overkill.</p>
 *
 * <p>Virtual threads for the drainers so a slow process can't leak
 * platform-thread workers.</p>
 */
public final class CliRunner {

    private static final Logger log = LoggerFactory.getLogger(CliRunner.class);

    private CliRunner() {}

    public static CliResult run(List<String> args, Duration wallTimeout) throws IOException {
        ProcessBuilder pb = new ProcessBuilder(args);
        pb.redirectErrorStream(false);
        log.debug("cli: {}", String.join(" ", args));

        Process proc = pb.start();
        ExecutorService drainers = Executors.newVirtualThreadPerTaskExecutor();
        Future<String> stdoutF = drainers.submit(() -> drain(proc.getInputStream()));
        Future<String> stderrF = drainers.submit(() -> drain(proc.getErrorStream()));

        boolean finished;
        try {
            finished = proc.waitFor(wallTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            proc.destroyForcibly();
            drainers.shutdownNow();
            throw new IOException("interrupted waiting for CLI", ie);
        }

        if (!finished) {
            proc.descendants().forEach(ProcessHandle::destroyForcibly);
            proc.destroyForcibly();
            try { proc.waitFor(2, TimeUnit.SECONDS); }
            catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
            drainers.shutdownNow();
            throw new IOException("CLI exceeded wall timeout of "
                    + wallTimeout.toSeconds() + "s: " + String.join(" ", args));
        }

        String stdout, stderr;
        try {
            stdout = stdoutF.get(2, TimeUnit.SECONDS);
            stderr = stderrF.get(2, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            stdout = "";
            stderr = "(drain failed: " + e.getClass().getSimpleName() + ")";
        } finally {
            drainers.shutdown();
        }
        return new CliResult(proc.exitValue(), stdout, stderr);
    }

    /**
     * Check whether a binary is on {@code PATH} by running {@code <name> version}
     * or {@code <name> --version}. Returns empty when the binary isn't found or
     * fails within the budget.
     */
    public static java.util.Optional<String> probeVersion(String bin, Duration budget) {
        // Try `version` then `--version`. Some CLIs accept only one.
        for (List<String> args : List.of(
                List.of(bin, "version"),
                List.of(bin, "--version"))) {
            try {
                CliResult r = run(args, budget);
                if (r.ok()) return java.util.Optional.of(r.stdout().trim());
            } catch (IOException ignored) {
                // Binary not on PATH — fall through to next attempt.
            }
        }
        return java.util.Optional.empty();
    }

    private static String drain(InputStream in) throws IOException {
        StringBuilder sb = new StringBuilder();
        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(in, StandardCharsets.UTF_8))) {
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line).append('\n');
            }
        }
        return sb.toString();
    }

    public record CliResult(int exitCode, String stdout, String stderr) {
        public boolean ok() { return exitCode == 0; }

        /** First N lines of stderr — useful for error surfacing. */
        public String stderrTail(int maxLines) {
            ArrayList<String> tail = new ArrayList<>();
            for (String line : stderr.split("\n")) {
                tail.add(line);
                if (tail.size() > maxLines) tail.remove(0);
            }
            return String.join("\n", tail);
        }
    }
}

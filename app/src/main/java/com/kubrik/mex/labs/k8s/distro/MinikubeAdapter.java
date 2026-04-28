package com.kubrik.mex.labs.k8s.distro;

import com.kubrik.mex.labs.k8s.model.LabK8sDistro;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * v2.8.1 Q2.8-N2 — Adapter for the {@code minikube} CLI.
 *
 * <p>Uses {@code --profile <identifier>} as the cluster addressing
 * convention; minikube's default single-instance flow is fine for
 * quick-start but conflicts when multiple Labs run concurrently.
 * Profiles sidestep that.</p>
 */
public final class MinikubeAdapter implements DistroAdapter {

    public static final String BIN = "minikube";
    private static final Duration VERSION_BUDGET = Duration.ofSeconds(5);

    @Override public LabK8sDistro distro() { return LabK8sDistro.MINIKUBE; }

    @Override
    public Optional<String> detectVersion() {
        // `minikube version --short` produces just the version string.
        try {
            CliRunner.CliResult r = CliRunner.run(
                    List.of(BIN, "version", "--short"), VERSION_BUDGET);
            if (r.ok()) return Optional.of(r.stdout().trim());
        } catch (IOException ignored) {}
        return CliRunner.probeVersion(BIN, VERSION_BUDGET);
    }

    @Override
    public CliRunner.CliResult create(String identifier, int wallSeconds) throws IOException {
        return CliRunner.run(
                List.of(BIN, "start", "--profile", identifier,
                        "--interactive=false"),
                Duration.ofSeconds(wallSeconds));
    }

    @Override
    public CliRunner.CliResult start(String identifier, int wallSeconds) throws IOException {
        // `minikube start` on an existing profile is idempotent —
        // same command as create.
        return create(identifier, wallSeconds);
    }

    @Override
    public CliRunner.CliResult stop(String identifier, int wallSeconds) throws IOException {
        return CliRunner.run(
                List.of(BIN, "stop", "--profile", identifier),
                Duration.ofSeconds(wallSeconds));
    }

    @Override
    public CliRunner.CliResult delete(String identifier, int wallSeconds) throws IOException {
        return CliRunner.run(
                List.of(BIN, "delete", "--profile", identifier),
                Duration.ofSeconds(wallSeconds));
    }

    @Override
    public CliRunner.CliResult status(String identifier, int wallSeconds) throws IOException {
        return CliRunner.run(
                List.of(BIN, "status", "--profile", identifier,
                        "--format=json"),
                Duration.ofSeconds(wallSeconds));
    }

    @Override
    public boolean isRunning(CliRunner.CliResult r) {
        if (!r.ok()) return false;
        // JSON output: {"Host": "Running", "Kubelet": "Running",
        //               "APIServer": "Running", ...}
        // A full Running cluster has Host / Kubelet / APIServer all
        // Running. We don't need a full JSON parse — substring on
        // the APIServer key is precise enough and tolerant of
        // minikube version drift.
        String s = r.stdout();
        return s.contains("\"APIServer\": \"Running\"")
                || s.contains("\"APIServer\":\"Running\"");
    }
}

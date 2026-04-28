package com.kubrik.mex.labs.k8s.distro;

import com.kubrik.mex.labs.k8s.model.LabK8sDistro;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * v2.8.1 Q2.8-N2 — Adapter for the {@code k3d} CLI (k3s-in-docker).
 *
 * <p>Depends on Docker being on the machine — k3d is a Docker client
 * that wraps k3s container images. We don't double-check Docker
 * presence here because k3d itself errors if Docker isn't reachable,
 * which {@link #isRunning} surfaces to the user.</p>
 *
 * <p>Context name convention: k3d writes {@code k3d-<identifier>}
 * into {@code ~/.kube/config}, so the {@link DistroAdapter#contextFor}
 * default on {@link LabK8sDistro#K3D} already produces the right
 * string.</p>
 */
public final class K3dAdapter implements DistroAdapter {

    public static final String BIN = "k3d";
    private static final Duration VERSION_BUDGET = Duration.ofSeconds(5);

    @Override public LabK8sDistro distro() { return LabK8sDistro.K3D; }

    @Override
    public Optional<String> detectVersion() {
        return CliRunner.probeVersion(BIN, VERSION_BUDGET);
    }

    @Override
    public CliRunner.CliResult create(String identifier, int wallSeconds) throws IOException {
        // `k3d cluster create <name>` — defaults are fine for a Lab
        // (one server, one agent, auto-exposed LoadBalancer port). If
        // a Lab needs custom topology it's express that via template
        // extras in a later minor.
        return CliRunner.run(
                List.of(BIN, "cluster", "create", identifier),
                Duration.ofSeconds(wallSeconds));
    }

    @Override
    public CliRunner.CliResult start(String identifier, int wallSeconds) throws IOException {
        return CliRunner.run(
                List.of(BIN, "cluster", "start", identifier),
                Duration.ofSeconds(wallSeconds));
    }

    @Override
    public CliRunner.CliResult stop(String identifier, int wallSeconds) throws IOException {
        return CliRunner.run(
                List.of(BIN, "cluster", "stop", identifier),
                Duration.ofSeconds(wallSeconds));
    }

    @Override
    public CliRunner.CliResult delete(String identifier, int wallSeconds) throws IOException {
        return CliRunner.run(
                List.of(BIN, "cluster", "delete", identifier),
                Duration.ofSeconds(wallSeconds));
    }

    @Override
    public CliRunner.CliResult status(String identifier, int wallSeconds) throws IOException {
        // `k3d cluster list -o json` returns an array; the desired
        // cluster's `serversRunning` + `agentsRunning` counts tell
        // us if it's up.
        return CliRunner.run(
                List.of(BIN, "cluster", "list", "-o", "json"),
                Duration.ofSeconds(wallSeconds));
    }

    @Override
    public boolean isRunning(CliRunner.CliResult r) {
        if (!r.ok()) return false;
        // Generic substring check — any running server is enough
        // to declare the cluster up for our purposes. A real JSON
        // parse would be better but the strings are stable across
        // k3d 5.x versions and this avoids a Jackson dependency at
        // the distro layer.
        String s = r.stdout();
        return s.contains("\"serversRunning\": 1")
                || s.contains("\"serversRunning\":1")
                || s.contains("\"nodesRunning\":")
                    && !s.contains("\"nodesRunning\": 0")
                    && !s.contains("\"nodesRunning\":0");
    }
}

package com.kubrik.mex.labs.k8s.distro;

import com.kubrik.mex.labs.k8s.model.LabK8sDistro;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

/**
 * v2.8.1 Q2.8-N2 — Per-distro CLI wrapper.
 *
 * <p>Scope is minimal: presence check, version readout, four
 * lifecycle operations ({@code create} / {@code start} / {@code stop}
 * / {@code delete}), and a kubeconfig-path accessor. Anything
 * operator-specific (CRD install, registry mirroring) is out of
 * scope — Decision 11 says the v2.8.1 production pipeline handles
 * everything past "cluster is reachable".</p>
 */
public interface DistroAdapter {

    /** Identity used for log prefixes + task dispatch. */
    LabK8sDistro distro();

    /** CLI binary presence + version string. Empty when missing. */
    Optional<String> detectVersion();

    /**
     * Create the cluster. Blocking up to {@code wallSeconds} — typical
     * first-run pulls container images and can take a couple of
     * minutes on a cold box.
     */
    CliRunner.CliResult create(String identifier, int wallSeconds) throws IOException;

    /** Start an existing cluster that's stopped. */
    CliRunner.CliResult start(String identifier, int wallSeconds) throws IOException;

    /** Stop a running cluster — preserves state (like docker compose stop). */
    CliRunner.CliResult stop(String identifier, int wallSeconds) throws IOException;

    /** Tear the cluster down — removes all state. */
    CliRunner.CliResult delete(String identifier, int wallSeconds) throws IOException;

    /** Read the cluster's status string (distro-specific — parse via {@link #isRunning}). */
    CliRunner.CliResult status(String identifier, int wallSeconds) throws IOException;

    /** Best-effort parse of {@link #status}'s output — true when the cluster is up. */
    boolean isRunning(CliRunner.CliResult statusResult);

    /**
     * Return the kubeconfig path the CLI writes into. Both distros
     * default to {@code ~/.kube/config} and merge contexts; we echo
     * that back so the v2.8.1 foundation (KubeConfigLoader) picks
     * the right file.
     */
    default Path kubeconfigPath() {
        return Path.of(System.getProperty("user.home", "."), ".kube", "config");
    }

    /** Kubeconfig context name the distro writes for {@code identifier}. */
    default String contextFor(String identifier) {
        return distro().contextFor(identifier);
    }
}

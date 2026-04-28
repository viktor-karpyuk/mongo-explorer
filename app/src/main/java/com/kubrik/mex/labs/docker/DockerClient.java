package com.kubrik.mex.labs.docker;

import com.kubrik.mex.labs.model.EngineStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

/**
 * v2.8.4 LAB-DOCKER-1..4 — Thin adapter around the {@code docker}
 * CLI. We shell out (milestone decision 1) rather than link a Docker
 * SDK / testcontainers runtime — a tiny surface, zero new deps, and
 * compatible with any Docker-ish CLI on PATH (OrbStack, colima,
 * podman-docker).
 *
 * <p>All compose commands target a named project, never the current
 * working directory — that way two Labs of the same template run
 * side-by-side without racing on {@code docker-compose.yml}.</p>
 */
public final class DockerClient {

    private static final Logger log = LoggerFactory.getLogger(DockerClient.class);

    /** How long a {@code compose up} is allowed to take before we
     *  kill the process tree. 2 min covers image pulls on a fresh
     *  install; the caller's health-watch has its own 90s budget on
     *  top for replset initiation. */
    public static final Duration COMPOSE_UP_TIMEOUT = Duration.ofMinutes(4);
    public static final Duration COMPOSE_DOWN_TIMEOUT = Duration.ofMinutes(2);
    public static final Duration COMPOSE_START_STOP_TIMEOUT = Duration.ofSeconds(60);
    public static final Duration PROBE_TIMEOUT = Duration.ofSeconds(8);
    public static final Duration PS_TIMEOUT = Duration.ofSeconds(15);

    private final String dockerBinary;
    private volatile DockerVersion cachedVersion;

    public DockerClient() { this("docker"); }

    /** Test seam — pass an alternate binary (e.g., a fake script). */
    public DockerClient(String dockerBinary) {
        this.dockerBinary = dockerBinary;
    }

    /** Probe the CLI + daemon. Results are not cached — call from
     *  the empty-state renderer each time so a user who starts
     *  Docker mid-session sees the state flip. */
    public EngineStatus status() {
        try {
            ExecResult vr = DockerExecIO.runOneShot(
                    List.of(dockerBinary, "version", "--format",
                            "{{.Client.Version}}"),
                    PROBE_TIMEOUT);
            if (!vr.ok()) {
                // CLI present but version failed — usually means the
                // daemon isn't running (client can't talk to it).
                String err = vr.combinedTail().toLowerCase();
                if (err.contains("cannot connect") || err.contains("daemon")
                        || err.contains("is the docker daemon running")) {
                    return EngineStatus.DAEMON_DOWN;
                }
                return EngineStatus.UNKNOWN;
            }
            DockerVersion v = DockerVersion.parse(vr.tailStdout());
            if (v == null) return EngineStatus.UNKNOWN;
            cachedVersion = v;
            if (!v.atLeast(DockerVersion.MIN_SUPPORTED)) return EngineStatus.VERSION_LOW;

            // Daemon liveness via a lightweight `docker info` —
            // `version` talks to the daemon already, so if we got a
            // non-zero client version back the daemon is reachable.
            return EngineStatus.READY;
        } catch (IOException ioe) {
            String msg = ioe.getMessage() == null ? "" : ioe.getMessage().toLowerCase();
            if (msg.contains("no such file") || msg.contains("cannot run program")
                    || msg.contains("not found")) {
                return EngineStatus.CLI_MISSING;
            }
            log.debug("docker status probe failed: {}", ioe.getMessage());
            return EngineStatus.UNKNOWN;
        }
    }

    /** The Docker CLI version from the last successful {@link #status}
     *  probe. Null if we haven't probed yet or the probe failed. */
    public DockerVersion version() { return cachedVersion; }

    public ExecResult composeUp(Path composeFile, String projectName,
                                 Path stdoutLog, Path stderrLog) throws IOException {
        return DockerExecIO.run(
                List.of(dockerBinary, "compose",
                        "--project-name", projectName,
                        "--file", composeFile.toAbsolutePath().toString(),
                        "up", "-d", "--wait"),
                stdoutLog, stderrLog, COMPOSE_UP_TIMEOUT);
    }

    public ExecResult composeDown(String projectName, boolean removeVolumes,
                                   Path stdoutLog, Path stderrLog) throws IOException {
        var args = new java.util.ArrayList<String>();
        args.add(dockerBinary); args.add("compose");
        args.add("--project-name"); args.add(projectName);
        args.add("down");
        if (removeVolumes) args.add("-v");
        return DockerExecIO.run(args, stdoutLog, stderrLog, COMPOSE_DOWN_TIMEOUT);
    }

    public ExecResult composeStart(String projectName, Path stdoutLog,
                                    Path stderrLog) throws IOException {
        return DockerExecIO.run(
                List.of(dockerBinary, "compose",
                        "--project-name", projectName, "start"),
                stdoutLog, stderrLog, COMPOSE_START_STOP_TIMEOUT);
    }

    public ExecResult composeStop(String projectName, Path stdoutLog,
                                   Path stderrLog) throws IOException {
        return DockerExecIO.run(
                List.of(dockerBinary, "compose",
                        "--project-name", projectName, "stop"),
                stdoutLog, stderrLog, COMPOSE_START_STOP_TIMEOUT);
    }

    /** JSON-formatted `compose ls` so the reconciler can scan for
     *  {@code mex-lab-*} projects at startup. */
    public ExecResult composeLs(Path stdoutLog, Path stderrLog) throws IOException {
        return DockerExecIO.run(
                List.of(dockerBinary, "compose", "ls", "--all",
                        "--format", "json"),
                stdoutLog, stderrLog, PS_TIMEOUT);
    }

    public ExecResult composePs(String projectName, Path stdoutLog,
                                 Path stderrLog) throws IOException {
        return DockerExecIO.run(
                List.of(dockerBinary, "compose",
                        "--project-name", projectName,
                        "ps", "--all", "--format", "json"),
                stdoutLog, stderrLog, PS_TIMEOUT);
    }
}

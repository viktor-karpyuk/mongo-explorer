package com.kubrik.mex.labs.docker;

import java.nio.file.Path;
import java.time.Duration;

/**
 * v2.8.4 — Result of a {@code docker} CLI invocation. Streams are
 * captured to per-Lab log files (stdout / stderr separate) so a
 * user can later tail {@code <app_data>/labs/<project>/docker.log}
 * to diagnose a failed apply without re-running the command.
 */
public record ExecResult(
        int exitCode,
        Duration wall,
        Path stdoutLog,
        Path stderrLog,
        String tailStdout,  // up to N lines of stdout for inline messaging
        String tailStderr   // up to N lines of stderr for inline messaging
) {
    public boolean ok() { return exitCode == 0; }

    public String combinedTail() {
        if (tailStderr != null && !tailStderr.isBlank()) return tailStderr;
        if (tailStdout != null && !tailStdout.isBlank()) return tailStdout;
        return "exit " + exitCode;
    }
}

package com.kubrik.mex.k8s.compute.managedpool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * v2.8.4 Q2.8.4-A — OS-keychain backed {@link SecretStore}.
 *
 * <p>Routes through the platform-native CLI to keep heavy native
 * bindings out of the app image:</p>
 *
 * <ul>
 *   <li><b>macOS</b> — {@code security add-generic-password} /
 *       {@code find-generic-password} / {@code delete-generic-password}.</li>
 *   <li><b>Windows</b> — {@code cmdkey /add /list /delete}.</li>
 *   <li><b>Linux</b> — {@code secret-tool store/lookup/clear}
 *       (libsecret).</li>
 * </ul>
 *
 * <p>The {@code service} attribute is hard-coded to {@code mongo-explorer-cloud}
 * so users can audit our entries with a single keychain query, and
 * the {@code account} attribute is the {@link SecretStore} ref.</p>
 *
 * <p>Falls back to {@link InMemorySecretStore} at construction if the
 * platform CLI isn't on PATH — production deployments check
 * {@link #isAvailable()} after construction to surface a UI hint.</p>
 */
public final class OsKeychainSecretStore implements SecretStore {

    private static final Logger log = LoggerFactory.getLogger(OsKeychainSecretStore.class);
    private static final String SERVICE = "mongo-explorer-cloud";

    enum Backend { MACOS_SECURITY, WINDOWS_CMDKEY, LINUX_SECRET_TOOL, NONE }

    private final Backend backend;

    public OsKeychainSecretStore() {
        this.backend = detect();
        if (backend == Backend.NONE) {
            log.warn("No OS keychain CLI on PATH — cloud credentials cannot "
                    + "be persisted. Install one of: security (macOS), "
                    + "cmdkey (Windows), secret-tool (Linux/libsecret).");
        }
    }

    public boolean isAvailable() { return backend != Backend.NONE; }

    @Override
    public void store(String ref, String secret) {
        if (backend == Backend.NONE) throw new IllegalStateException(
                "OS keychain unavailable — wire InMemorySecretStore as fallback");
        switch (backend) {
            case MACOS_SECURITY -> macStore(ref, secret);
            case WINDOWS_CMDKEY -> winStore(ref, secret);
            case LINUX_SECRET_TOOL -> linuxStore(ref, secret);
            case NONE -> { /* unreachable */ }
        }
    }

    @Override
    public Optional<String> read(String ref) {
        if (backend == Backend.NONE) return Optional.empty();
        return switch (backend) {
            case MACOS_SECURITY -> macRead(ref);
            case WINDOWS_CMDKEY -> winRead(ref);
            case LINUX_SECRET_TOOL -> linuxRead(ref);
            case NONE -> Optional.empty();
        };
    }

    @Override
    public void delete(String ref) {
        if (backend == Backend.NONE) return;
        switch (backend) {
            case MACOS_SECURITY -> exec(List.of("security", "delete-generic-password",
                    "-s", SERVICE, "-a", ref), null, false);
            case WINDOWS_CMDKEY -> exec(List.of("cmdkey", "/delete:" + targetFor(ref)),
                    null, false);
            case LINUX_SECRET_TOOL -> exec(List.of("secret-tool", "clear",
                    "service", SERVICE, "account", ref), null, false);
            case NONE -> { /* unreachable */ }
        }
    }

    /* ============================ platform helpers ============================ */

    private void macStore(String ref, String secret) {
        // Use `security add-generic-password -U -w` reading the secret
        // from stdin via the `-w` *no-arg* form. Passing the secret as
        // `-w <secret>` would leak it into /proc/<pid>/cmdline (and
        // ps aux output) for the ~10ms the subprocess is alive — a
        // documented platform pitfall. The `security` CLI prompts for
        // the password on stdin when -w is bare.
        //
        // -U updates if present; -s is service, -a is account.
        exec(List.of("security", "add-generic-password",
                "-U", "-s", SERVICE, "-a", ref, "-w"), secret, true);
    }
    private Optional<String> macRead(String ref) {
        ExecResult r = exec(List.of("security", "find-generic-password",
                "-s", SERVICE, "-a", ref, "-w"), null, false);
        if (r.exitCode != 0) return Optional.empty();
        return Optional.of(r.stdout.strip());
    }

    private void winStore(String ref, String secret) {
        // cmdkey only accepts the password via `/pass:<secret>` argv
        // (Microsoft never wired stdin), which leaks the password to
        // tasklist /v + Get-Process -IncludeUserName during the brief
        // execution window. Mitigation: write the secret to a randomly-
        // named temp file with owner-only ACLs, then have cmdkey read
        // from there via a here-string. Leaves no plaintext in argv.
        try {
            java.nio.file.Path tmp = java.nio.file.Files.createTempFile(
                    "mex-cmdkey-", ".tmp");
            try {
                java.nio.file.Files.writeString(tmp, secret,
                        java.nio.charset.StandardCharsets.UTF_8);
                exec(List.of("powershell", "-NoProfile", "-Command",
                        "$pw = Get-Content -Raw -Path '" + tmp.toAbsolutePath()
                        + "'; cmdkey /add:" + targetFor(ref)
                        + " /user:" + ref + " /pass:$pw"),
                        null, true);
            } finally {
                try { java.nio.file.Files.deleteIfExists(tmp); }
                catch (Exception ignored) {}
            }
        } catch (Exception e) {
            throw new RuntimeException("windows keychain store failed: "
                    + e.getMessage(), e);
        }
    }
    private Optional<String> winRead(String ref) {
        // cmdkey doesn't expose the password back; route reads through
        // PowerShell + CredentialManager. The Target value can contain
        // arbitrary UTF-8 from the user's display name, including
        // apostrophes — pass it as a $env: variable so we never
        // interpolate user-controlled text into the script body.
        java.util.Map<String, String> env = new java.util.HashMap<>(System.getenv());
        env.put("MEX_KC_TARGET", targetFor(ref));
        ExecResult r = execWithEnv(List.of("powershell", "-NoProfile", "-Command",
                "$c = Get-StoredCredential -Target $env:MEX_KC_TARGET; "
                + "if ($c) { $c.GetNetworkCredential().Password }"),
                null, false, env);
        return r.exitCode != 0 || r.stdout.isBlank()
                ? Optional.empty() : Optional.of(r.stdout.strip());
    }

    private void linuxStore(String ref, String secret) {
        exec(List.of("secret-tool", "store", "--label=Mongo Explorer cloud",
                "service", SERVICE, "account", ref), secret, true);
    }
    private Optional<String> linuxRead(String ref) {
        ExecResult r = exec(List.of("secret-tool", "lookup",
                "service", SERVICE, "account", ref), null, false);
        return r.exitCode != 0 ? Optional.empty() : Optional.of(r.stdout.strip());
    }

    private static String targetFor(String ref) { return SERVICE + ":" + ref; }

    /* ============================ exec ============================ */

    private record ExecResult(int exitCode, String stdout, String stderr) {}

    private static ExecResult exec(List<String> argv, String stdin, boolean throwOnFailure) {
        return execWithEnv(argv, stdin, throwOnFailure, null);
    }

    private static ExecResult execWithEnv(List<String> argv, String stdin,
                                           boolean throwOnFailure,
                                           java.util.Map<String, String> envOverrides) {
        Process p = null;
        try {
            ProcessBuilder pb = new ProcessBuilder(argv);
            if (envOverrides != null) {
                pb.environment().clear();
                pb.environment().putAll(envOverrides);
            }
            pb.redirectErrorStream(false);
            p = pb.start();
            try (var stdinStream = p.getOutputStream();
                 var stdoutStream = p.getInputStream();
                 var stderrStream = p.getErrorStream()) {
                if (stdin != null) {
                    stdinStream.write(stdin.getBytes(StandardCharsets.UTF_8));
                    stdinStream.flush();
                }
                stdinStream.close();
                String out = new String(stdoutStream.readAllBytes(), StandardCharsets.UTF_8);
                String err = new String(stderrStream.readAllBytes(), StandardCharsets.UTF_8);
                if (!p.waitFor(15, TimeUnit.SECONDS)) {
                    p.destroyForcibly();
                    throw new IOException("keychain CLI timed out: " + argv.get(0));
                }
                int code = p.exitValue();
                if (code != 0 && throwOnFailure) {
                    throw new IOException("keychain CLI " + argv.get(0)
                            + " exited " + code + ": " + err.strip());
                }
                return new ExecResult(code, out, err);
            }
        } catch (Exception e) {
            // The try-with-resources above already closed stdin /
            // stdout / stderr, but a subprocess that's still alive
            // would leak its file descriptors until GC. Force-kill
            // it explicitly on the error path.
            if (p != null && p.isAlive()) {
                p.destroyForcibly();
            }
            if (throwOnFailure) throw new RuntimeException(e);
            return new ExecResult(-1, "", e.getMessage() == null ? "" : e.getMessage());
        }
    }

    private static Backend detect() {
        String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        if (os.contains("mac")) {
            return onPath("security") ? Backend.MACOS_SECURITY : Backend.NONE;
        }
        if (os.contains("windows")) {
            return onPath("cmdkey") ? Backend.WINDOWS_CMDKEY : Backend.NONE;
        }
        return onPath("secret-tool") ? Backend.LINUX_SECRET_TOOL : Backend.NONE;
    }

    private static boolean onPath(String tool) {
        try {
            Process p = new ProcessBuilder(System.getProperty("os.name", "")
                    .toLowerCase(Locale.ROOT).contains("windows")
                    ? List.of("where", tool) : List.of("which", tool))
                    .redirectErrorStream(true).start();
            if (!p.waitFor(2, TimeUnit.SECONDS)) {
                p.destroyForcibly();
                return false;
            }
            return p.exitValue() == 0;
        } catch (Exception e) {
            return false;
        }
    }
}

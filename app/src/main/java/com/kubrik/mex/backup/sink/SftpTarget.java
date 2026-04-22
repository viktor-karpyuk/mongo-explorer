package com.kubrik.mex.backup.sink;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Vector;

/**
 * v2.6.1 Q2.6.1-C — SFTP backup sink. Replaces the v2.5 stub using
 * the maintained JSch fork ({@code com.github.mwiede:jsch}).
 *
 * <p>URI: {@code sftp://<user>@<host>[:port]/<absolute-path>}. Host
 * port defaults to 22; path defaults to {@code "."} (the user's
 * SFTP home).</p>
 *
 * <p>Credentials JSON — one of:</p>
 * <pre>
 * { "password": "…" }
 * { "privateKey": "-----BEGIN …", "passphrase": "…" }
 * </pre>
 *
 * <p>Host-key verification is deliberately permissive for the backup
 * use case — operators ship backups to their own SFTP endpoints and
 * rely on network-layer isolation. If stricter verification is
 * needed, set {@code StrictHostKeyChecking=yes} via the {@code
 * extras_json} column and add a known-hosts file path (v2.6.2
 * follow-up).</p>
 *
 * <p>Each operation opens + closes its own SFTP session — JSch
 * sessions drop aggressively on idle and reconnect-on-each-op beats a
 * long-lived pool for the sparse-write backup workload. Each session
 * is wrapped in try-with-resources via the {@link #withSession}
 * helper so a failed PUT can never leak an open session.</p>
 */
public final class SftpTarget implements StorageTarget {

    private static final Logger log = LoggerFactory.getLogger(SftpTarget.class);

    private final String name;
    private final String uri;
    private final String user;
    private final String host;
    private final int port;
    private final String rootPath;
    private final String password;
    private final byte[] privateKey;
    private final String passphrase;

    public SftpTarget(String name, String uri) { this(name, uri, null); }

    public SftpTarget(String name, String uri, String credentialsJson) {
        this.name = Objects.requireNonNull(name, "name");
        this.uri = Objects.requireNonNull(uri, "uri");
        Parsed p = parseUri(uri);
        this.user = p.user();
        this.host = p.host();
        this.port = p.port();
        this.rootPath = p.rootPath();
        Creds c = parseCredentials(credentialsJson);
        this.password = c.password();
        this.privateKey = c.privateKey();
        this.passphrase = c.passphrase();
    }

    public String host() { return host; }
    public int port() { return port; }
    public String rootPath() { return rootPath; }

    /* ============================= probe ============================= */

    @Override
    public Probe testWrite() {
        long t0 = System.currentTimeMillis();
        String marker = resolveAbsolute(".mex-testwrite-" + java.util.UUID.randomUUID());
        byte[] payload = new byte[1024];
        try {
            withSession(channel -> {
                ensureParent(channel, marker);
                channel.put(new ByteArrayInputStream(payload), marker);
                try (InputStream in = channel.get(marker)) { in.readAllBytes(); }
                channel.rm(marker);
                return null;
            });
            return new Probe(true, System.currentTimeMillis() - t0, Optional.empty());
        } catch (Exception e) {
            return new Probe(false, System.currentTimeMillis() - t0,
                    Optional.of(e.getClass().getSimpleName() + ": " + e.getMessage()));
        }
    }

    /* ============================= writes ============================= */

    @Override
    public OutputStream put(String relPath) {
        String abs = resolveAbsolute(relPath);
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        return new OutputStream() {
            private volatile boolean closed = false;

            @Override public void write(int b) { buffer.write(b); }
            @Override public void write(byte[] b, int off, int len) { buffer.write(b, off, len); }

            @Override
            public void close() throws IOException {
                if (closed) return;
                closed = true;
                try {
                    withSession(channel -> {
                        ensureParent(channel, abs);
                        channel.put(new ByteArrayInputStream(buffer.toByteArray()), abs);
                        return null;
                    });
                } catch (Exception e) {
                    throw new IOException("SFTP PUT " + abs + " failed: "
                            + e.getMessage(), e);
                }
            }
        };
    }

    /* ============================= reads ============================= */

    @Override
    public InputStream get(String relPath) throws IOException {
        String abs = resolveAbsolute(relPath);
        try {
            return withSession(channel -> {
                try (InputStream in = channel.get(abs)) {
                    return new ByteArrayInputStream(in.readAllBytes());
                }
            });
        } catch (Exception e) {
            throw new IOException("SFTP GET " + abs + " failed: "
                    + e.getMessage(), e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<Entry> list(String relPath) throws IOException {
        String abs = resolveAbsolute(relPath);
        try {
            return withSession(channel -> {
                List<Entry> out = new ArrayList<>();
                // channel.ls throws on missing directory — propagate so
                // callers see an IOException with LIST semantics,
                // matching S3 / GCS / Azure. Previously a missing
                // directory returned an empty list silently, which
                // could mask a backup-sink misconfiguration.
                Vector<ChannelSftp.LsEntry> entries = channel.ls(abs);
                for (ChannelSftp.LsEntry e : entries) {
                    String fn = e.getFilename();
                    if (".".equals(fn) || "..".equals(fn)) continue;
                    SftpATTRS a = e.getAttrs();
                    String base = relPath == null ? "" : relPath;
                    // Strip a trailing slash without engaging the regex
                    // engine — regex on a user-supplied path is a
                    // correctness smell (bracket chars, etc.).
                    if (base.endsWith("/")) base = base.substring(0, base.length() - 1);
                    String rel = base.isEmpty() ? fn : base + "/" + fn;
                    out.add(new Entry(rel, a.getSize(), a.getMTime() * 1000L));
                }
                return out;
            });
        } catch (Exception e) {
            throw new IOException("SFTP LIST " + abs + " failed: "
                    + e.getMessage(), e);
        }
    }

    @Override
    public Entry stat(String relPath) throws IOException {
        String abs = resolveAbsolute(relPath);
        try {
            return withSession(channel -> {
                SftpATTRS a = channel.stat(abs);
                return new Entry(relPath == null ? "" : relPath,
                        a.getSize(), a.getMTime() * 1000L);
            });
        } catch (Exception e) {
            throw new IOException("SFTP STAT " + abs + " failed: "
                    + e.getMessage(), e);
        }
    }

    @Override
    public void delete(String relPath) throws IOException {
        String abs = resolveAbsolute(relPath);
        try {
            withSession(channel -> { channel.rm(abs); return null; });
        } catch (Exception e) {
            throw new IOException("SFTP DELETE " + abs + " failed: "
                    + e.getMessage(), e);
        }
    }

    @Override public String canonicalRoot() { return uri; }
    @Override public boolean supportsServerSideHash() { return false; }

    /* ============================ helpers ============================ */

    private String resolveAbsolute(String relPath) {
        String rel = relPath == null ? "" : relPath;
        while (rel.startsWith("/")) rel = rel.substring(1);
        if (rootPath.isEmpty() || ".".equals(rootPath)) return rel.isEmpty() ? "." : rel;
        return rel.isEmpty() ? rootPath : rootPath + "/" + rel;
    }

    /**
     * Opens an SSH + SFTP session, hands {@code op} the ready channel,
     * and guarantees both are closed on exit. Errors propagate wrapped
     * in the op's exception type.
     */
    private <T> T withSession(SftpOp<T> op) throws Exception {
        JSch jsch = new JSch();
        if (privateKey != null && privateKey.length > 0) {
            // Clone the key bytes and passphrase bytes before handing
            // them to JSch. addIdentity retains the array internally
            // and, under concurrent probes / operations, a second
            // caller's arrival could race with the first's identity
            // parser; defensive copies make the per-call state fully
            // independent.
            jsch.addIdentity(name, privateKey.clone(),
                    /*pubkey=*/null,
                    passphrase == null ? null
                            : passphrase.getBytes(StandardCharsets.UTF_8));
        }
        Session session = jsch.getSession(user, host, port);
        if (password != null && !password.isEmpty()) session.setPassword(password);
        Properties props = new Properties();
        // The backup use case is operator-to-operator-owned endpoint;
        // a known-hosts verification pass lands with a v2.6.2 polish.
        props.put("StrictHostKeyChecking", "no");
        session.setConfig(props);
        session.setTimeout(15_000);
        session.connect(15_000);
        try {
            ChannelSftp channel = (ChannelSftp) session.openChannel("sftp");
            channel.connect(15_000);
            try {
                return op.run(channel);
            } finally {
                try { channel.disconnect(); } catch (Exception ignored) {}
            }
        } finally {
            try { session.disconnect(); } catch (Exception ignored) {}
        }
    }

    /** Recursive mkdir for the directory containing {@code absPath}.
     *  Silent when the directory already exists. */
    private static void ensureParent(ChannelSftp channel, String absPath) throws SftpException {
        int slash = absPath.lastIndexOf('/');
        if (slash <= 0) return;
        String dir = absPath.substring(0, slash);
        try {
            channel.stat(dir);
            return;
        } catch (SftpException ignored) {}
        // mkdir -p equivalent — walk the path segment by segment.
        String[] parts = dir.split("/");
        StringBuilder sb = new StringBuilder();
        for (String part : parts) {
            if (part.isEmpty()) { sb.append('/'); continue; }
            if (sb.length() > 1 || (sb.length() == 1 && sb.charAt(0) != '/')) sb.append('/');
            sb.append(part);
            try { channel.stat(sb.toString()); }
            catch (SftpException ex) {
                try { channel.mkdir(sb.toString()); }
                catch (SftpException mkdirEx) {
                    if (mkdirEx.id != ChannelSftp.SSH_FX_FAILURE) throw mkdirEx;
                }
            }
        }
    }

    @FunctionalInterface
    private interface SftpOp<T> { T run(ChannelSftp channel) throws Exception; }

    /* =========================== URI + creds =========================== */

    /**
     * Parses {@code sftp://user@host[:port]/absolute/path}. Port
     * defaults to 22; path defaults to {@code "."} when absent.
     * Public so UI save-time validation can reject malformed URIs
     * without opening an SSH session.
     */
    public static Parsed parseUri(String uri) {
        if (uri == null || uri.isBlank())
            throw new IllegalArgumentException("uri is blank");
        // Strip ?query / #fragment — uncommon for sftp:// but a user
        // who pastes a file manager's bookmark URL might have them.
        String u = com.kubrik.mex.backup.sink.S3Target.stripQueryAndFragment(uri.trim());
        if (!u.regionMatches(true, 0, "sftp://", 0, 7))
            throw new IllegalArgumentException("uri must start with sftp://");
        String tail = u.substring(7);
        int at = tail.indexOf('@');
        if (at <= 0) throw new IllegalArgumentException("sftp:// missing user@");
        String user = tail.substring(0, at);
        // Reject userinfo with an embedded password — the form gives
        // us a dedicated credentials field, and letting user:pass@host
        // through would both break JSch (it expects a bare username)
        // AND put the password at risk of leaking into log / status
        // strings that include the URI.
        if (user.indexOf(':') >= 0) {
            throw new IllegalArgumentException(
                    "sftp:// must not embed a password; use the credentials "
                    + "form for password or private-key auth");
        }
        String hostPart = tail.substring(at + 1);
        int slash = hostPart.indexOf('/');
        String hostPortPart = slash < 0 ? hostPart : hostPart.substring(0, slash);
        String path = slash < 0 ? "." : hostPart.substring(slash + 1);
        if (path.isEmpty()) path = ".";
        String host;
        int port = 22;
        int colon = hostPortPart.lastIndexOf(':');
        if (colon < 0) {
            host = hostPortPart;
        } else {
            host = hostPortPart.substring(0, colon);
            try { port = Integer.parseInt(hostPortPart.substring(colon + 1)); }
            catch (NumberFormatException nfe) {
                throw new IllegalArgumentException("invalid port in sftp uri");
            }
        }
        if (host == null || host.isBlank())
            throw new IllegalArgumentException("sftp uri missing host");
        return new Parsed(user, host, port, path);
    }

    public record Parsed(String user, String host, int port, String rootPath) {}

    private record Creds(String password, byte[] privateKey, String passphrase) {}

    private static Creds parseCredentials(String json) {
        if (json == null || json.isBlank()) return new Creds(null, null, null);
        try {
            Document d = Document.parse(json);
            String pw = d.getString("password");
            String pk = d.getString("privateKey");
            String pass = d.getString("passphrase");
            return new Creds(pw,
                    pk == null ? null : pk.getBytes(StandardCharsets.UTF_8), pass);
        } catch (Exception e) {
            log.warn("SFTP credentials JSON parse failed: {}", e.getMessage());
            return new Creds(null, null, null);
        }
    }

    public void putBytes(String relPath, byte[] payload) throws IOException {
        try (OutputStream out = put(relPath)) {
            out.write(payload == null ? new byte[0] : payload);
        }
    }

    /** Credential shape classifier for tests + SinkEditor form
     *  rendering. */
    public enum AuthKind { PASSWORD, PRIVATE_KEY, UNAUTHENTICATED, INVALID }

    public static AuthKind classifyCredentials(String credentialsJson) {
        if (credentialsJson == null || credentialsJson.isBlank())
            return AuthKind.UNAUTHENTICATED;
        try {
            Document d = Document.parse(credentialsJson);
            String pw = d.getString("password");
            String pk = d.getString("privateKey");
            if (pk != null && !pk.isBlank()) return AuthKind.PRIVATE_KEY;
            if (pw != null && !pw.isBlank()) return AuthKind.PASSWORD;
            return AuthKind.INVALID;
        } catch (Exception e) {
            return AuthKind.INVALID;
        }
    }

    /** Exposed for the JSchException catch-site in callers that want
     *  to classify a connection failure without depending on JSch. */
    public static boolean isSftpException(Throwable t) {
        return t instanceof JSchException || t instanceof SftpException;
    }
}

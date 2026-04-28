package com.kubrik.mex.backup.sink;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.common.StorageSharedKeyCredential;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * v2.6.1 Q2.6.1-B — Azure Blob Storage backup sink. Replaces the v2.5
 * stub; covers SAS-token and account-key auth. AAD / managed-identity
 * is out of scope for v2.6.1 (needs MSAL and is uncommon for backup
 * endpoints).
 *
 * <p>URI accepts two shapes:
 * <ul>
 *   <li><b>Full HTTPS</b> — {@code https://<account>.blob.core.windows.net/<container>[/<prefix>]}</li>
 *   <li><b>Short scheme</b> — {@code azblob://<account>/<container>[/<prefix>]}</li>
 * </ul>
 *
 * <p>Credentials JSON shape (whichever applies):</p>
 * <pre>
 * { "sasToken": "?sv=...&amp;sig=..." }
 * { "accountName": "…", "accountKey": "…" }
 * </pre>
 *
 * <p>Blank credentials → anonymous (public container) access. The
 * probe surfaces authorisation failures as {@link Probe#error()}.</p>
 */
public final class AzureBlobTarget implements StorageTarget {

    private static final Logger log = LoggerFactory.getLogger(AzureBlobTarget.class);

    private final String name;
    private final String uri;
    private final String account;
    private final String container;
    private final String keyPrefix;
    private final BlobContainerClient client;

    public AzureBlobTarget(String name, String uri) { this(name, uri, null); }

    public AzureBlobTarget(String name, String uri, String credentialsJson) {
        this.name = Objects.requireNonNull(name, "name");
        this.uri = Objects.requireNonNull(uri, "uri");
        Parsed parsed = parseUri(uri);
        this.account = parsed.account();
        this.container = parsed.container();
        this.keyPrefix = parsed.keyPrefix();
        this.client = buildClient(parsed, credentialsJson);
    }

    public String name() { return name; }
    public String account() { return account; }
    public String container() { return container; }
    public String keyPrefix() { return keyPrefix; }

    /* ============================= probe ============================= */

    @Override
    public Probe testWrite() {
        long t0 = System.currentTimeMillis();
        String key = keyPrefix + ".mex-testwrite-" + java.util.UUID.randomUUID();
        byte[] payload = new byte[1024];
        try {
            BlobClient blob = client.getBlobClient(key);
            blob.upload(BinaryData.fromBytes(payload), /*overwrite=*/true);
            blob.downloadContent().toBytes();
            blob.delete();
            return new Probe(true, System.currentTimeMillis() - t0, Optional.empty());
        } catch (Exception e) {
            return new Probe(false, System.currentTimeMillis() - t0,
                    Optional.of(e.getClass().getSimpleName() + ": " + e.getMessage()));
        }
    }

    /* ============================= writes ============================= */

    @Override
    public OutputStream put(String relPath) {
        String key = resolveKey(relPath);
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
                    client.getBlobClient(key).upload(
                            BinaryData.fromBytes(buffer.toByteArray()),
                            /*overwrite=*/true);
                } catch (BlobStorageException e) {
                    throw new IOException("Azure PUT " + key + " failed: "
                            + e.getMessage(), e);
                }
            }
        };
    }

    /* ============================== reads ============================== */

    @Override
    public InputStream get(String relPath) throws IOException {
        String key = resolveKey(relPath);
        try {
            return new ByteArrayInputStream(
                    client.getBlobClient(key).downloadContent().toBytes());
        } catch (BlobStorageException e) {
            throw new IOException("Azure GET " + key + " failed: "
                    + e.getMessage(), e);
        }
    }

    @Override
    public List<Entry> list(String relPath) throws IOException {
        String prefix = resolveKey(relPath);
        if (!prefix.isEmpty() && !prefix.endsWith("/")) prefix = prefix + "/";
        try {
            List<Entry> out = new ArrayList<>();
            for (BlobItem item : client.listBlobs(
                    new ListBlobsOptions().setPrefix(prefix), null)) {
                String rel = item.getName();
                if (!keyPrefix.isEmpty() && rel.startsWith(keyPrefix)) {
                    rel = rel.substring(keyPrefix.length());
                }
                long size = item.getProperties() == null
                        || item.getProperties().getContentLength() == null
                        ? 0L : item.getProperties().getContentLength();
                OffsetDateTime mod = item.getProperties() == null ? null
                        : item.getProperties().getLastModified();
                long mtime = mod == null ? 0L : mod.toInstant().toEpochMilli();
                out.add(new Entry(rel, size, mtime));
            }
            return out;
        } catch (BlobStorageException e) {
            throw new IOException("Azure LIST " + prefix + " failed: "
                    + e.getMessage(), e);
        }
    }

    @Override
    public Entry stat(String relPath) throws IOException {
        String key = resolveKey(relPath);
        try {
            var props = client.getBlobClient(key).getProperties();
            String rel = key;
            if (!keyPrefix.isEmpty() && rel.startsWith(keyPrefix)) {
                rel = rel.substring(keyPrefix.length());
            }
            long size = props.getBlobSize();
            long mtime = props.getLastModified() == null ? 0L
                    : props.getLastModified().toInstant().toEpochMilli();
            return new Entry(rel, size, mtime);
        } catch (BlobStorageException e) {
            throw new IOException("Azure HEAD " + key + " failed: "
                    + e.getMessage(), e);
        }
    }

    @Override
    public void delete(String relPath) throws IOException {
        String key = resolveKey(relPath);
        try {
            client.getBlobClient(key).delete();
        } catch (BlobStorageException e) {
            throw new IOException("Azure DELETE " + key + " failed: "
                    + e.getMessage(), e);
        }
    }

    @Override public String canonicalRoot() { return uri; }
    @Override public boolean supportsServerSideHash() { return true; }

    /* ============================ helpers ============================ */

    private String resolveKey(String relPath) {
        String rel = relPath == null ? "" : relPath;
        while (rel.startsWith("/")) rel = rel.substring(1);
        return keyPrefix + rel;
    }

    /**
     * Visible for tests — parses either
     * {@code azblob://account/container/prefix} or
     * {@code https://account.blob.core.windows.net/container/prefix}.
     * Returns {@code (account, container, keyPrefix)} where keyPrefix
     * ends with {@code /} (or is empty).
     */
    /** Default Azure Blob host suffix — commercial cloud. Gov Cloud
     *  and China Cloud use different suffixes, which the full
     *  {@code https://} URI carries explicitly; the {@code azblob://}
     *  short-form assumes commercial. */
    public static final String DEFAULT_HOST_SUFFIX = "blob.core.windows.net";

    public static Parsed parseUri(String uri) {
        if (uri == null || uri.isBlank())
            throw new IllegalArgumentException("uri is blank");
        // Strip ?query / #fragment tails (e.g., SAS tokens pasted
        // directly into the URI instead of the credentials field —
        // an easy mistake from the Azure portal).
        String u = com.kubrik.mex.backup.sink.S3Target.stripQueryAndFragment(uri.trim());
        String account;
        String hostSuffix;
        String tail;
        if (u.regionMatches(true, 0, "azblob://", 0, 9)) {
            // Short-form: assumes commercial cloud. Gov Cloud users
            // must paste the full https:// URL so parseUri sees the
            // correct host suffix.
            tail = u.substring(9);
            int slash = tail.indexOf('/');
            if (slash < 0) throw new IllegalArgumentException("azblob:// missing container");
            account = tail.substring(0, slash);
            hostSuffix = DEFAULT_HOST_SUFFIX;
            tail = tail.substring(slash + 1);
        } else if (u.regionMatches(true, 0, "https://", 0, 8)) {
            tail = u.substring(8);
            int slash = tail.indexOf('/');
            if (slash < 0) throw new IllegalArgumentException("https URL missing container");
            String host = tail.substring(0, slash);
            int dot = host.indexOf('.');
            if (dot < 0) throw new IllegalArgumentException("https host missing account");
            account = host.substring(0, dot);
            hostSuffix = host.substring(dot + 1);  // preserves Gov Cloud / China Cloud suffix
            tail = tail.substring(slash + 1);
        } else {
            throw new IllegalArgumentException("uri must start with azblob:// or https://");
        }
        if (account == null || account.isBlank())
            throw new IllegalArgumentException("account is blank");
        while (tail.startsWith("/")) tail = tail.substring(1);
        if (tail.isEmpty()) throw new IllegalArgumentException("container is missing");
        int slash = tail.indexOf('/');
        String container = slash < 0 ? tail : tail.substring(0, slash);
        String prefix = slash < 0 ? "" : tail.substring(slash + 1);
        while (prefix.startsWith("/")) prefix = prefix.substring(1);
        if (!prefix.isEmpty() && !prefix.endsWith("/")) prefix = prefix + "/";
        return new Parsed(account, container, prefix, hostSuffix);
    }

    public record Parsed(String account, String container, String keyPrefix, String hostSuffix) {
        public Parsed {
            if (hostSuffix == null || hostSuffix.isBlank()) hostSuffix = DEFAULT_HOST_SUFFIX;
        }
    }

    private static BlobContainerClient buildClient(Parsed parsed, String credentialsJson) {
        String endpoint = "https://" + parsed.account() + "."
                + parsed.hostSuffix() + "/" + parsed.container();
        BlobContainerClientBuilder b = new BlobContainerClientBuilder()
                .endpoint(endpoint);
        if (credentialsJson != null && !credentialsJson.isBlank()) {
            // Explicit credentials must parse. A silent anonymous
            // fallback on malformed JSON would mask a bad paste (e.g.,
            // the operator put the SAS in accountKey) and then every
            // write would fail with an opaque 403 later. Aligns with
            // GcsTarget's behavior on bad service-account JSON.
            Document d;
            try {
                d = Document.parse(credentialsJson);
            } catch (Exception e) {
                throw new IllegalArgumentException(
                        "Azure credentials JSON could not be parsed. Expected "
                        + "{\"sasToken\":\"…\"} or "
                        + "{\"accountName\":\"…\",\"accountKey\":\"…\"}, or "
                        + "leave blank for anonymous access.", e);
            }
            String sas = d.getString("sasToken");
            String accountKey = d.getString("accountKey");
            if (sas != null && !sas.isBlank()) {
                b.sasToken(sas.startsWith("?") ? sas.substring(1) : sas);
            } else if (accountKey != null && !accountKey.isBlank()) {
                String n = d.getString("accountName");
                if (n == null || n.isBlank()) n = parsed.account();
                b.credential(new StorageSharedKeyCredential(n, accountKey));
            } else {
                throw new IllegalArgumentException(
                        "Azure credentials JSON must contain either sasToken "
                        + "or accountKey; neither was found.");
            }
        }
        return b.buildClient();
    }

    public void putBytes(String relPath, byte[] payload) throws IOException {
        try (OutputStream out = put(relPath)) {
            out.write(payload == null ? new byte[0] : payload);
        }
    }

    /** Classifier for tests + the SinkEditor form — tells callers
     *  which credential shape was parsed. */
    public enum AuthKind { SAS, ACCOUNT_KEY, ANONYMOUS, INVALID }

    public static AuthKind classifyCredentials(String credentialsJson) {
        if (credentialsJson == null || credentialsJson.isBlank()) return AuthKind.ANONYMOUS;
        try {
            Document d = Document.parse(credentialsJson);
            String sas = d.getString("sasToken");
            String accountKey = d.getString("accountKey");
            if (sas != null && !sas.isBlank()) return AuthKind.SAS;
            if (accountKey != null && !accountKey.isBlank()) return AuthKind.ACCOUNT_KEY;
            return AuthKind.INVALID;
        } catch (Exception e) {
            return AuthKind.INVALID;
        }
    }
}

package com.kubrik.mex.backup.sink;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
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

/**
 * v2.6.1 Q2.6.1-A — Google Cloud Storage backup sink. Real google-cloud-
 * storage impl replacing the v2.5 stub.
 *
 * <p>URI: {@code gs://<bucket>/<prefix>}. Credentials: a service-account
 * JSON key's contents in {@link SinkRecord#credentialsJson}. Blank
 * credentials fall back to Application Default Credentials so GKE
 * workload-identity, Cloud Run metadata, and {@code gcloud auth
 * application-default login} all work.</p>
 *
 * <p>Ships with the HTTP/JSON transport; the gRPC Netty-shaded client
 * is deliberately excluded at the Gradle level to keep the app image
 * lean. Backup upload is not a hot path so the small per-request
 * overhead is acceptable.</p>
 */
public final class GcsTarget implements StorageTarget {

    private static final Logger log = LoggerFactory.getLogger(GcsTarget.class);

    private final String name;
    private final String bucketUri;
    private final String bucket;
    private final String keyPrefix;
    private final Storage storage;

    public GcsTarget(String name, String bucketUri) {
        this(name, bucketUri, /*credentialsJson=*/null);
    }

    public GcsTarget(String name, String bucketUri, String credentialsJson) {
        this.name = Objects.requireNonNull(name, "name");
        this.bucketUri = Objects.requireNonNull(bucketUri, "bucketUri");
        Parsed parsed = parseBucketUri(bucketUri);
        this.bucket = parsed.bucket();
        this.keyPrefix = parsed.keyPrefix();
        this.storage = buildClient(credentialsJson);
    }

    public String name() { return name; }
    public String bucket() { return bucket; }
    public String keyPrefix() { return keyPrefix; }

    /* ============================ probe ============================ */

    @Override
    public Probe testWrite() {
        long t0 = System.currentTimeMillis();
        String key = keyPrefix + ".mex-testwrite-" + java.util.UUID.randomUUID();
        byte[] payload = new byte[1024];
        try {
            BlobId id = BlobId.of(bucket, key);
            storage.create(BlobInfo.newBuilder(id).build(), payload);
            Blob read = storage.get(id);
            if (read == null) {
                return new Probe(false, System.currentTimeMillis() - t0,
                        Optional.of("probe blob vanished immediately after write"));
            }
            read.getContent();
            storage.delete(id);
            return new Probe(true, System.currentTimeMillis() - t0, Optional.empty());
        } catch (Exception e) {
            return new Probe(false, System.currentTimeMillis() - t0,
                    Optional.of(e.getClass().getSimpleName() + ": " + e.getMessage()));
        }
    }

    /* ============================ writes ============================ */

    /**
     * Returns a buffered {@link OutputStream}; the PUT fires on
     * {@link OutputStream#close()}. Callers must close the stream or
     * use {@link #putBytes} — a leaked stream silently fails.
     */
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
                    storage.create(
                            BlobInfo.newBuilder(BlobId.of(bucket, key)).build(),
                            buffer.toByteArray());
                } catch (StorageException e) {
                    throw new IOException("GCS PUT " + key + " failed: "
                            + e.getMessage(), e);
                }
            }
        };
    }

    /* ============================ reads ============================ */

    @Override
    public InputStream get(String relPath) throws IOException {
        String key = resolveKey(relPath);
        try {
            Blob blob = storage.get(BlobId.of(bucket, key));
            if (blob == null) {
                throw new IOException("GCS GET " + key + " not found");
            }
            return new ByteArrayInputStream(blob.getContent());
        } catch (StorageException e) {
            throw new IOException("GCS GET " + key + " failed: "
                    + e.getMessage(), e);
        }
    }

    @Override
    public List<Entry> list(String relPath) throws IOException {
        String prefix = resolveKey(relPath);
        if (!prefix.isEmpty() && !prefix.endsWith("/")) prefix = prefix + "/";
        try {
            List<Entry> out = new ArrayList<>();
            var page = storage.list(bucket, BlobListOption.prefix(prefix));
            for (Blob b : page.iterateAll()) {
                String rel = b.getName();
                if (!keyPrefix.isEmpty() && rel.startsWith(keyPrefix)) {
                    rel = rel.substring(keyPrefix.length());
                }
                long size = b.getSize() == null ? 0L : b.getSize();
                long mtime = b.getUpdateTimeOffsetDateTime() == null ? 0L
                        : b.getUpdateTimeOffsetDateTime().toInstant().toEpochMilli();
                out.add(new Entry(rel, size, mtime));
            }
            return out;
        } catch (StorageException e) {
            throw new IOException("GCS LIST " + prefix + " failed: "
                    + e.getMessage(), e);
        }
    }

    @Override
    public Entry stat(String relPath) throws IOException {
        String key = resolveKey(relPath);
        try {
            Blob blob = storage.get(BlobId.of(bucket, key));
            if (blob == null) {
                throw new IOException("GCS HEAD " + key + " not found");
            }
            String rel = key;
            if (!keyPrefix.isEmpty() && rel.startsWith(keyPrefix)) {
                rel = rel.substring(keyPrefix.length());
            }
            long size = blob.getSize() == null ? 0L : blob.getSize();
            long mtime = blob.getUpdateTimeOffsetDateTime() == null ? 0L
                    : blob.getUpdateTimeOffsetDateTime().toInstant().toEpochMilli();
            return new Entry(rel, size, mtime);
        } catch (StorageException e) {
            throw new IOException("GCS HEAD " + key + " failed: "
                    + e.getMessage(), e);
        }
    }

    @Override
    public void delete(String relPath) throws IOException {
        String key = resolveKey(relPath);
        try {
            storage.delete(BlobId.of(bucket, key));
        } catch (StorageException e) {
            throw new IOException("GCS DELETE " + key + " failed: "
                    + e.getMessage(), e);
        }
    }

    @Override public String canonicalRoot() { return bucketUri; }
    @Override public boolean supportsServerSideHash() { return true; }

    /* ============================ helpers ============================ */

    private String resolveKey(String relPath) {
        String rel = relPath == null ? "" : relPath;
        while (rel.startsWith("/")) rel = rel.substring(1);
        return keyPrefix + rel;
    }

    /**
     * Parses {@code gs://bucket/optional/prefix} into
     * {@code (bucket, keyPrefix)} — exposed for unit testing.
     */
    static Parsed parseBucketUri(String uri) {
        if (uri == null || uri.isBlank())
            throw new IllegalArgumentException("bucketUri is blank");
        String s = uri.trim();
        if (!s.regionMatches(true, 0, "gs://", 0, 5))
            throw new IllegalArgumentException("bucketUri must start with gs://");
        s = s.substring(5);
        while (s.startsWith("/")) s = s.substring(1);
        if (s.isEmpty()) throw new IllegalArgumentException("bucketUri missing bucket name");
        int slash = s.indexOf('/');
        String bucket = slash < 0 ? s : s.substring(0, slash);
        String prefix = slash < 0 ? "" : s.substring(slash + 1);
        while (prefix.startsWith("/")) prefix = prefix.substring(1);
        if (!prefix.isEmpty() && !prefix.endsWith("/")) prefix = prefix + "/";
        return new Parsed(bucket, prefix);
    }

    record Parsed(String bucket, String keyPrefix) {}

    private static Storage buildClient(String credentialsJson) {
        StorageOptions.Builder b = StorageOptions.newBuilder();
        if (credentialsJson != null && !credentialsJson.isBlank()) {
            try {
                GoogleCredentials creds = ServiceAccountCredentials.fromStream(
                        new ByteArrayInputStream(
                                credentialsJson.getBytes(StandardCharsets.UTF_8)));
                b.setCredentials(creds);
            } catch (Exception e) {
                log.warn("GCS credentials JSON parse failed, falling back to ADC: {}",
                        e.getMessage());
            }
        }
        return b.build().getService();
    }

    /** Helper for callers that have the raw bytes in hand. */
    public void putBytes(String relPath, byte[] payload) throws IOException {
        try (OutputStream out = put(relPath)) {
            out.write(payload == null ? new byte[0] : payload);
        }
    }
}

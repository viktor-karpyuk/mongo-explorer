package com.kubrik.mex.backup.sink;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

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
 * v2.6 Q2.6-L1 — S3 backup sink. Real AWS SDK v2 impl replacing the
 * v2.5 stub that threw {@link CloudSinkUnavailableException}.
 *
 * <p>URL-connection HTTP transport (not Netty) keeps the app image
 * small — S3 upload is not a hot path, so connection-per-request
 * overhead is acceptable. Credentials come from the {@link SinkRecord}
 * {@code credentialsJson} (already AES-decrypted by {@code SinkDao});
 * a blank credentials JSON falls back to the default provider chain so
 * IAM-roles-on-EC2 and AWS SSO continue to work.</p>
 *
 * <p>The {@link #put(String)} return value wraps a
 * {@link ByteArrayOutputStream} — the caller writes, and the PUT fires
 * on close. Backup artefacts are written once per file (catalog
 * manifest, bson files) and re-read via checksum later; buffering is
 * bounded by the individual artefact size.</p>
 */
public final class S3Target implements StorageTarget {

    private static final Logger log = LoggerFactory.getLogger(S3Target.class);

    private final String name;
    private final String bucketUri;
    private final String region;
    private final String bucket;
    private final String keyPrefix;
    private final S3Client client;

    public S3Target(String name, String bucketUri, String region) {
        this(name, bucketUri, region, /*credentialsJson=*/null);
    }

    /**
     * @param credentialsJson JSON of the shape
     *                        {@code {"accessKeyId":"…","secretAccessKey":"…","sessionToken":"…"}}.
     *                        Pass {@code null} / blank to fall back to AWS's
     *                        default provider chain (env vars, IMDS, SSO, …).
     */
    public S3Target(String name, String bucketUri, String region, String credentialsJson) {
        this.name = Objects.requireNonNull(name, "name");
        this.bucketUri = Objects.requireNonNull(bucketUri, "bucketUri");
        this.region = region == null ? Region.US_EAST_1.id() : region;
        Parsed parsed = parseBucketUri(bucketUri);
        this.bucket = parsed.bucket();
        this.keyPrefix = parsed.keyPrefix();
        this.client = buildClient(credentialsJson, this.region);
    }

    public String name() { return name; }
    public String region() { return region; }
    public String bucket() { return bucket; }
    public String keyPrefix() { return keyPrefix; }

    /* ============================ probe ============================ */

    @Override
    public Probe testWrite() {
        long t0 = System.currentTimeMillis();
        // UUID (not System.nanoTime) so two concurrent probes against
        // the same bucket + prefix can't collide on the marker key and
        // falsely pass by reading each other's bytes.
        String marker = keyPrefix + ".mex-testwrite-"
                + java.util.UUID.randomUUID();
        byte[] payload = new byte[1024];
        try {
            client.putObject(
                    PutObjectRequest.builder().bucket(bucket).key(marker).build(),
                    RequestBody.fromBytes(payload));
            try (ResponseInputStream<?> stream = client.getObject(
                    GetObjectRequest.builder().bucket(bucket).key(marker).build(),
                    ResponseTransformer.toInputStream())) {
                stream.readAllBytes();
            }
            client.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(marker).build());
            return new Probe(true, System.currentTimeMillis() - t0, Optional.empty());
        } catch (Exception e) {
            return new Probe(false, System.currentTimeMillis() - t0,
                    Optional.of(e.getClass().getSimpleName() + ": " + e.getMessage()));
        }
    }

    /* ============================ writes ============================ */

    /**
     * Returns a buffered {@link OutputStream}; the PUT fires on
     * {@link OutputStream#close()}. <b>Callers MUST close the stream</b>
     * (try-with-resources is the safe idiom) — a leaked stream never
     * uploads and the upload failure is silent. Use {@link #putBytes}
     * when the payload is already in memory.
     *
     * <p>The buffer is heap-resident, so this method is appropriate for
     * backup manifest files + small-to-medium bson dumps. Larger
     * artefacts (&gt; ~100 MB) risk GC pressure; streaming upload with
     * S3 multipart is a v2.6.1+ item.</p>
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
                byte[] payload = buffer.toByteArray();
                try {
                    client.putObject(
                            PutObjectRequest.builder().bucket(bucket).key(key).build(),
                            RequestBody.fromBytes(payload));
                } catch (S3Exception e) {
                    throw new IOException("S3 PUT " + key + " failed: "
                            + e.awsErrorDetails().errorMessage(), e);
                }
            }
        };
    }

    /* ============================ reads ============================ */

    /**
     * Buffers the full object into memory before returning an
     * {@link InputStream}. This matches the existing {@link
     * StorageTarget} contract (callers consume the stream without
     * positional seek) but means a multi-GB artefact will allocate
     * multi-GB of heap. The {@link CatalogVerifier} streams individual
     * backup files (&lt; 100 MB each in practice); multi-GB single-file
     * restores are a v2.6.1 concern if they arrive.
     */
    @Override
    public InputStream get(String relPath) throws IOException {
        String key = resolveKey(relPath);
        try {
            ResponseInputStream<?> stream = client.getObject(
                    GetObjectRequest.builder().bucket(bucket).key(key).build(),
                    ResponseTransformer.toInputStream());
            return new ByteArrayInputStream(stream.readAllBytes());
        } catch (S3Exception e) {
            throw new IOException("S3 GET " + key + " failed: "
                    + e.awsErrorDetails().errorMessage(), e);
        }
    }

    @Override
    public List<Entry> list(String relPath) throws IOException {
        String prefix = resolveKey(relPath);
        if (!prefix.isEmpty() && !prefix.endsWith("/")) prefix = prefix + "/";
        try {
            List<Entry> out = new ArrayList<>();
            String continuation = null;
            do {
                ListObjectsV2Request.Builder b = ListObjectsV2Request.builder()
                        .bucket(bucket).prefix(prefix);
                if (continuation != null) b.continuationToken(continuation);
                ListObjectsV2Response resp = client.listObjectsV2(b.build());
                for (S3Object obj : resp.contents()) {
                    String relative = obj.key();
                    if (!keyPrefix.isEmpty() && relative.startsWith(keyPrefix)) {
                        relative = relative.substring(keyPrefix.length());
                    }
                    out.add(new Entry(relative, obj.size() == null ? 0L : obj.size(),
                            obj.lastModified() == null ? 0L : obj.lastModified().toEpochMilli()));
                }
                continuation = Boolean.TRUE.equals(resp.isTruncated())
                        ? resp.nextContinuationToken() : null;
            } while (continuation != null);
            return out;
        } catch (S3Exception e) {
            throw new IOException("S3 LIST " + prefix + " failed: "
                    + e.awsErrorDetails().errorMessage(), e);
        }
    }

    @Override
    public Entry stat(String relPath) throws IOException {
        String key = resolveKey(relPath);
        try {
            HeadObjectResponse h = client.headObject(
                    HeadObjectRequest.builder().bucket(bucket).key(key).build());
            String rel = key;
            if (!keyPrefix.isEmpty() && rel.startsWith(keyPrefix)) {
                rel = rel.substring(keyPrefix.length());
            }
            return new Entry(rel, h.contentLength() == null ? 0L : h.contentLength(),
                    h.lastModified() == null ? 0L : h.lastModified().toEpochMilli());
        } catch (S3Exception e) {
            throw new IOException("S3 HEAD " + key + " failed: "
                    + e.awsErrorDetails().errorMessage(), e);
        }
    }

    @Override
    public void delete(String relPath) throws IOException {
        String key = resolveKey(relPath);
        try {
            client.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(key).build());
        } catch (S3Exception e) {
            throw new IOException("S3 DELETE " + key + " failed: "
                    + e.awsErrorDetails().errorMessage(), e);
        }
    }

    @Override public String canonicalRoot() { return bucketUri; }
    @Override public boolean supportsServerSideHash() { return true; }

    @Override
    public void close() {
        try { client.close(); } catch (Exception ignored) {}
    }

    /* ============================= helpers ============================= */

    private String resolveKey(String relPath) {
        String rel = relPath == null ? "" : relPath;
        while (rel.startsWith("/")) rel = rel.substring(1);
        return keyPrefix + rel;
    }

    /**
     * Parses {@code s3://bucket/optional/prefix} into
     * {@code (bucket, keyPrefix)} where keyPrefix ends with {@code /}
     * (or is empty). Throws {@link IllegalArgumentException} on any
     * other scheme or a missing bucket name. Public so UI save-time
     * validation can classify a pasted URI without constructing a
     * real SDK client (and its cost-per-request network round-trip).
     */
    public static Parsed parseBucketUri(String uri) {
        if (uri == null || uri.isBlank())
            throw new IllegalArgumentException("bucketUri is blank");
        String s = uri.trim();
        if (!s.regionMatches(true, 0, "s3://", 0, 5))
            throw new IllegalArgumentException("bucketUri must start with s3://");
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

    public record Parsed(String bucket, String keyPrefix) {}

    private static S3Client buildClient(String credentialsJson, String region) {
        S3ClientBuilder b = S3Client.builder()
                .httpClient(UrlConnectionHttpClient.create())
                .region(Region.of(region));
        if (credentialsJson != null && !credentialsJson.isBlank()) {
            try {
                Document d = Document.parse(credentialsJson);
                String ak = d.getString("accessKeyId");
                String sk = d.getString("secretAccessKey");
                String st = d.getString("sessionToken");
                if (ak != null && sk != null) {
                    if (st != null && !st.isBlank()) {
                        b.credentialsProvider(StaticCredentialsProvider.create(
                                AwsSessionCredentials.create(ak, sk, st)));
                    } else {
                        b.credentialsProvider(StaticCredentialsProvider.create(
                                AwsBasicCredentials.create(ak, sk)));
                    }
                }
            } catch (Exception e) {
                log.warn("S3 credentials JSON parse failed, falling back to default chain: {}",
                        e.getMessage());
            }
        }
        // Default chain picks up env vars / IMDS / AWS SSO.
        if (credentialsJson == null || credentialsJson.isBlank()) {
            b.credentialsProvider(DefaultCredentialsProvider.create());
        }
        return b.build();
    }

    /** Helper for the raw byte form when callers can't use the streaming
     *  OutputStream — keeps the byte round-trip at one place. */
    public void putBytes(String relPath, byte[] payload) throws IOException {
        try (OutputStream out = put(relPath)) {
            out.write(payload == null ? new byte[0] : payload);
        }
    }

    /** Convenience for probe smoke-tests outside the interface. */
    public byte[] getBytes(String relPath) throws IOException {
        try (InputStream in = get(relPath)) {
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            byte[] chunk = new byte[4096];
            int n;
            while ((n = in.read(chunk)) > 0) buf.write(chunk, 0, n);
            return buf.toByteArray();
        }
    }

    /** Makes the UTF-8 round-trip convenience of {@link #putBytes} /
     *  {@link #getBytes} discoverable via {@code new String(getBytes,
     *  UTF_8)} without importing StandardCharsets at each call-site. */
    public String getString(String relPath) throws IOException {
        return new String(getBytes(relPath), StandardCharsets.UTF_8);
    }
}

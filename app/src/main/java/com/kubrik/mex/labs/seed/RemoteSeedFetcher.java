package com.kubrik.mex.labs.seed;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.time.Duration;

/**
 * v2.8.4 LAB-SEED-3 — HTTP fetcher for large sample datasets
 * (sample_mflix is ~55 MiB, exceeds the NFR-LAB-5 bundled cap).
 *
 * <p>Cache layout:
 * {@code <app_data>/labs/cache/<sha256-or-filename>.archive}.
 * If a SHA-256 is specified in the SeedSpec, the cached file is
 * verified before use; a mismatch re-fetches.</p>
 *
 * <p>Fetch is resumable only at the HTTP-client's level (GET with
 * Range headers isn't done here — mongorestore's input is the
 * whole archive, and a partial download fails the SHA check).</p>
 */
public final class RemoteSeedFetcher {

    private static final Logger log = LoggerFactory.getLogger(RemoteSeedFetcher.class);

    public static final Duration FETCH_TIMEOUT = Duration.ofMinutes(10);

    private final Path cacheDir;
    private final HttpClient http;

    public RemoteSeedFetcher(Path cacheDir) {
        this.cacheDir = cacheDir;
        this.http = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NORMAL)
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    /**
     * Fetch (or reuse cached) archive at {@code url}. Verifies the
     * SHA-256 if one was supplied; returns the local path.
     */
    public Path fetch(String url, String expectedSha256) throws IOException {
        Files.createDirectories(cacheDir);
        String cacheKey = expectedSha256 == null || expectedSha256.isBlank()
                ? filenameFromUrl(url) : expectedSha256;
        Path cached = cacheDir.resolve(cacheKey + ".archive");
        if (Files.exists(cached)
                && (expectedSha256 == null || expectedSha256.isBlank()
                    || sha256Hex(cached).equalsIgnoreCase(expectedSha256))) {
            log.info("seed cache hit: {}", cached);
            return cached;
        }

        log.info("seed cache miss — fetching {}", url);
        Path tmp = Files.createTempFile(cacheDir, "seed-", ".download");
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(FETCH_TIMEOUT)
                    .GET()
                    .build();
            HttpResponse<Path> resp = http.send(req,
                    HttpResponse.BodyHandlers.ofFile(tmp,
                            java.nio.file.StandardOpenOption.WRITE,
                            java.nio.file.StandardOpenOption.TRUNCATE_EXISTING));
            if (resp.statusCode() / 100 != 2) {
                throw new IOException("HTTP " + resp.statusCode()
                        + " fetching " + url);
            }
            if (expectedSha256 != null && !expectedSha256.isBlank()) {
                String got = sha256Hex(tmp);
                if (!got.equalsIgnoreCase(expectedSha256)) {
                    throw new IOException("SHA-256 mismatch for " + url
                            + "; expected " + expectedSha256 + " got " + got);
                }
            }
            Files.move(tmp, cached, StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
            return cached;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted fetching " + url, ie);
        } finally {
            Files.deleteIfExists(tmp);
        }
    }

    static String sha256Hex(Path p) throws IOException {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            try (var in = Files.newInputStream(p)) {
                byte[] buf = new byte[8192];
                int n;
                while ((n = in.read(buf)) > 0) md.update(buf, 0, n);
            }
            byte[] d = md.digest();
            StringBuilder sb = new StringBuilder(d.length * 2);
            for (byte b : d) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (java.security.NoSuchAlgorithmException nsa) {
            throw new IOException("sha-256 unavailable", nsa);
        }
    }

    static String filenameFromUrl(String url) {
        String tail = url;
        int slash = tail.lastIndexOf('/');
        if (slash >= 0 && slash < tail.length() - 1) tail = tail.substring(slash + 1);
        int q = tail.indexOf('?');
        if (q >= 0) tail = tail.substring(0, q);
        return tail.replaceAll("[^a-zA-Z0-9_.-]", "_");
    }
}

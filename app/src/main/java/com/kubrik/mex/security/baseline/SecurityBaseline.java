package com.kubrik.mex.security.baseline;

import com.kubrik.mex.cluster.dryrun.CommandJson;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * v2.6 Q2.6-A3 — immutable snapshot of the security-relevant state of a
 * connection at a point in time.
 *
 * <p>Kept deliberately loose: {@link #payload()} is a nested
 * {@code Map<String, Object>} tree that the capture pass populates with
 * users, roles, parameters, and the security-relevant slice of
 * {@code getCmdLineOpts}. Using a generic map means Q2.6-B can evolve
 * the payload shape without changing this record's public API.</p>
 *
 * <p>Canonical JSON sorts keys recursively, strips whitespace, and
 * normalises numbers (see {@link CommandJson}). Two baselines built
 * from structurally-equal payloads therefore produce the same
 * {@link #sha256()} — this is what the drift diff in Q2.6-D relies on
 * to detect change cheaply before descending into the tree.</p>
 */
public record SecurityBaseline(
        String version,
        String connectionId,
        long capturedAtMs,
        String capturedBy,
        String notes,
        Map<String, Object> payload
) {

    public static final String CURRENT_VERSION = "1";

    public SecurityBaseline {
        if (version == null || version.isBlank()) version = CURRENT_VERSION;
        if (connectionId == null || connectionId.isBlank())
            throw new IllegalArgumentException("connectionId");
        if (capturedBy == null) capturedBy = "";
        if (notes == null) notes = "";
        payload = payload == null ? Map.of() : Map.copyOf(payload);
    }

    /** Canonical JSON of {@link #payload()} with the record's envelope
     *  fields wrapping it. Used for both persistence and the integrity
     *  hash. Key order inside the envelope is sorted (via the renderer)
     *  so capturedAt → connectionId → notes → payload → version. */
    public String canonicalJson() {
        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("version", version);
        envelope.put("connectionId", connectionId);
        envelope.put("capturedAtMs", capturedAtMs);
        envelope.put("capturedBy", capturedBy);
        envelope.put("notes", notes);
        envelope.put("payload", payload);
        return CommandJson.render(envelope);
    }

    /** Lower-case hex SHA-256 over {@link #canonicalJson()}. */
    public String sha256() {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(canonicalJson().getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(digest.length * 2);
            for (byte b : digest) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }
}

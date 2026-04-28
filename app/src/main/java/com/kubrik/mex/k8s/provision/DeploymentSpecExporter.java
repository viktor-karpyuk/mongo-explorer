package com.kubrik.mex.k8s.provision;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Optional;

/**
 * v2.8.1 Q2.8.1-J — Writes a {@link DeploymentSpec} as a JSON file.
 *
 * <p>Password stripping is explicit: even if the source
 * {@link ProvisionModel} has {@code passwordMode = PROVIDE} with a
 * real password in memory, the exported file carries
 * {@code providedPassword = empty}. The re-import prompts the user
 * for a fresh password — spec export is never a credential
 * propagation vector (milestone §2.10).</p>
 *
 * <p>Evidence signing (v2.6 key) is optional — callers pass a
 * signer; the exporter stamps the signature into
 * {@link DeploymentSpec#evidenceSig} so auditors can verify the
 * file wasn't tampered with after export.</p>
 */
public final class DeploymentSpecExporter {

    private static final ObjectMapper JSON = jacksonMapper();

    private DeploymentSpecExporter() {}

    public static DeploymentSpec build(ProvisionModel source, String sourceDeploymentLabel) {
        ProvisionModel sanitised = stripPassword(source);
        return new DeploymentSpec(
                DeploymentSpec.CURRENT_SCHEMA,
                Instant.now().toString(),
                Optional.ofNullable(sourceDeploymentLabel),
                Optional.empty(),
                sanitised);
    }

    /** Sign + serialise. {@code signer} may be null → no signature. */
    public static byte[] toJson(DeploymentSpec spec, EvidenceSigner signer) {
        try {
            DeploymentSpec signed = signer == null
                    ? spec
                    : new DeploymentSpec(spec.schemaVersion(), spec.exportedAt(),
                            spec.sourceDeployment(),
                            Optional.of(signer.sign(unsignedBytes(spec))),
                            spec.model());
            return JSON.writeValueAsBytes(signed);
        } catch (IOException ioe) {
            throw new IllegalStateException("export failed", ioe);
        }
    }

    /** Convenience — export directly to disk. */
    public static Path writeToFile(DeploymentSpec spec, EvidenceSigner signer, Path target)
            throws IOException {
        byte[] bytes = toJson(spec, signer);
        Files.write(target, bytes);
        return target;
    }

    /**
     * Round-trip-stable input to the evidence signer — JSON without
     * the {@code evidenceSig} field. Two exports of the same spec
     * produce the same signing input, so a verifier can re-compute
     * + compare.
     */
    public static byte[] unsignedBytes(DeploymentSpec spec) {
        DeploymentSpec bare = new DeploymentSpec(spec.schemaVersion(), spec.exportedAt(),
                spec.sourceDeployment(), Optional.empty(), spec.model());
        try {
            return JSON.writeValueAsBytes(bare);
        } catch (IOException ioe) {
            throw new IllegalStateException(ioe);
        }
    }

    static ProvisionModel stripPassword(ProvisionModel m) {
        // Replace PROVIDE-with-password with GENERATE so the file
        // doesn't leak secrets. PROVIDE-without-password passes
        // through unchanged.
        if (m.auth().passwordMode() == AuthSpec.PasswordMode.PROVIDE
                && m.auth().providedPassword().isPresent()) {
            return m.withAuth(new AuthSpec(
                    m.auth().rootUsername(),
                    AuthSpec.PasswordMode.GENERATE,
                    Optional.empty()));
        }
        return m;
    }

    private static ObjectMapper jacksonMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new Jdk8Module());
        mapper.registerModule(computeStrategyModule());
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        // Record accessors for derived helpers (isProdAcceptable, coordinates,
        // replicasPerReplset, …) show up as properties. The mapper writes them
        // anyway; the importer tolerates them via FAIL_ON_UNKNOWN_PROPERTIES=false.
        return mapper;
    }

    /** v2.8.4 — route ComputeStrategy through the canonical
     *  {@link com.kubrik.mex.k8s.compute.ComputeStrategyJson} codec
     *  so the sealed type round-trips inside the spec file. Jackson
     *  can't reflect over a sealed interface with optional records;
     *  delegating to the explicit codec keeps the wire format
     *  identical to the {@code provisioning_records.compute_strategy_json}
     *  column.
     *
     *  <p>Only the serializer is registered here — the importer owns
     *  the deserialiser (this exporter never reads back a JSON it
     *  produced). The serialiser routes through {@code writeTree} on
     *  a Jackson-parsed copy of the codec's output so any embedded
     *  control characters, escapes, or non-canonical encoding are
     *  re-canonicalised by the parent ObjectMapper rather than being
     *  blindly written via {@code writeRawValue}.</p> */
    private static com.fasterxml.jackson.databind.module.SimpleModule computeStrategyModule() {
        com.fasterxml.jackson.databind.module.SimpleModule m =
                new com.fasterxml.jackson.databind.module.SimpleModule("mex-compute-strategy");
        m.addSerializer(com.kubrik.mex.k8s.compute.ComputeStrategy.class,
                new com.fasterxml.jackson.databind.JsonSerializer<>() {
                    @Override public void serialize(
                            com.kubrik.mex.k8s.compute.ComputeStrategy value,
                            com.fasterxml.jackson.core.JsonGenerator gen,
                            com.fasterxml.jackson.databind.SerializerProvider provs)
                            throws IOException {
                        String json = com.kubrik.mex.k8s.compute.ComputeStrategyJson.toJson(value);
                        if (json == null) {
                            gen.writeNull();
                            return;
                        }
                        // Re-parse so the parent generator emits a
                        // canonical embedding — never trust raw bytes
                        // from a third-party codec to be quote-safe
                        // inside our document.
                        com.fasterxml.jackson.databind.JsonNode node = JSON.readTree(json);
                        gen.writeTree(node);
                    }
                });
        return m;
    }

    /** Minimal signer seam — implementers return a base64 signature. */
    public interface EvidenceSigner {
        String sign(byte[] body);
    }

    /** No-op signer handy for tests and pre-v2.6 installs. */
    public static final EvidenceSigner NULL_SIGNER = body -> "";
}

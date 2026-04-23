package com.kubrik.mex.k8s.provision;

import java.util.Objects;
import java.util.Optional;

/**
 * v2.8.1 Q2.8.1-D1 — TLS material for the deployment.
 *
 * <p>Four modes; {@link ProfileEnforcer} locks Prod to a non-OFF
 * value:</p>
 * <ul>
 *   <li>{@link Mode#OFF} — no TLS. Dev/Test only.</li>
 *   <li>{@link Mode#OPERATOR_GENERATED} — operator issues a self-
 *       signed cert. Dev/Test only.</li>
 *   <li>{@link Mode#CERT_MANAGER} — cert-manager Issuer (named in
 *       {@code certManagerIssuer}). Prod preferred.</li>
 *   <li>{@link Mode#BYO_SECRET} — user-provided Secret name. Prod
 *       alternative for air-gapped / existing-CA environments.</li>
 * </ul>
 */
public record TlsSpec(
        Mode mode,
        Optional<String> certManagerIssuer,
        Optional<String> byoSecretName) {

    public enum Mode { OFF, OPERATOR_GENERATED, CERT_MANAGER, BYO_SECRET }

    public TlsSpec {
        Objects.requireNonNull(mode, "mode");
        Objects.requireNonNull(certManagerIssuer, "certManagerIssuer");
        Objects.requireNonNull(byoSecretName, "byoSecretName");
    }

    public static TlsSpec off() {
        return new TlsSpec(Mode.OFF, Optional.empty(), Optional.empty());
    }

    public static TlsSpec operatorGenerated() {
        return new TlsSpec(Mode.OPERATOR_GENERATED, Optional.empty(), Optional.empty());
    }

    public static TlsSpec certManager(String issuerName) {
        return new TlsSpec(Mode.CERT_MANAGER, Optional.of(issuerName), Optional.empty());
    }

    public static TlsSpec byoSecret(String secretName) {
        return new TlsSpec(Mode.BYO_SECRET, Optional.empty(), Optional.of(secretName));
    }

    /** True when the mode is acceptable for the Prod profile. */
    public boolean isProdAcceptable() {
        return mode == Mode.CERT_MANAGER || mode == Mode.BYO_SECRET;
    }
}

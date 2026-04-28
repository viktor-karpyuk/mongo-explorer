package com.kubrik.mex.security.authn;

import java.util.Map;
import java.util.Objects;

/**
 * v2.6 Q2.6-F1 — one authentication backend as reported by the server.
 * Fields are limited to what {@code getParameter authenticationMechanisms}
 * + the {@code security} section of {@code getCmdLineOpts} expose —
 * never a secret (no bind passwords, no keytab paths, no TLS keys).
 *
 * @param mechanism  one of {@link Mechanism}.
 * @param enabled    true when the mechanism appears in
 *                   {@code authenticationMechanisms}.
 * @param details    backend-specific config fields (e.g. LDAP {@code
 *                   servers}, {@code userToDNMapping} template, Kerberos
 *                   {@code serviceName}). Values are plain strings; the
 *                   probe scrubs anything that looks secret.
 */
public record AuthBackend(Mechanism mechanism, boolean enabled, Map<String, String> details) {

    public AuthBackend {
        Objects.requireNonNull(mechanism, "mechanism");
        details = details == null ? Map.of() : Map.copyOf(details);
    }

    /** Mechanism names as MongoDB emits them; the enum constant matches
     *  the wire-format string so {@link #fromWire} is a trivial lookup. */
    public enum Mechanism {
        SCRAM_SHA_256("SCRAM-SHA-256"),
        SCRAM_SHA_1("SCRAM-SHA-1"),
        MONGODB_X509("MONGODB-X509"),
        /** Enterprise LDAP: MongoDB uses SASL PLAIN to carry the LDAP bind. */
        PLAIN_LDAP("PLAIN"),
        /** Enterprise Kerberos. */
        GSSAPI("GSSAPI");

        private final String wire;
        Mechanism(String wire) { this.wire = wire; }
        public String wire() { return wire; }

        public static Mechanism fromWire(String s) {
            if (s == null) return null;
            for (Mechanism m : values()) if (m.wire.equalsIgnoreCase(s)) return m;
            return null;
        }
    }
}

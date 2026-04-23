package com.kubrik.mex.k8s.provision;

import java.util.Objects;
import java.util.Optional;

/**
 * v2.8.1 Q2.8.1-D1 — Root user credentials.
 *
 * <p>SCRAM-SHA-256 + keyfile internal auth are operator defaults;
 * this record only captures the root username + how the password
 * is supplied. A blank password in GENERATE mode means "operator
 * generates", in PROVIDE mode means "ask the user next step."</p>
 */
public record AuthSpec(
        String rootUsername,
        PasswordMode passwordMode,
        Optional<String> providedPassword) {

    public enum PasswordMode {
        /** Operator generates; Mongo Explorer never sees the secret. */
        GENERATE,
        /** User supplies; wizard writes a Secret referencing this value. */
        PROVIDE
    }

    public AuthSpec {
        Objects.requireNonNull(rootUsername, "rootUsername");
        Objects.requireNonNull(passwordMode, "passwordMode");
        Objects.requireNonNull(providedPassword, "providedPassword");
    }

    public static AuthSpec defaults() {
        return new AuthSpec("mongoAdmin", PasswordMode.GENERATE, Optional.empty());
    }
}

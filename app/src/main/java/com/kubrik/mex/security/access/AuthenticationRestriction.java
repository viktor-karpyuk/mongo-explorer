package com.kubrik.mex.security.access;

import java.util.List;
import java.util.Objects;

/**
 * v2.6 Q2.6-B1 — one entry in a user's {@code authenticationRestrictions}
 * array. Encodes the allow-lists MongoDB enforces at authentication time:
 * the client source IP ranges it expects the connection to originate
 * from, and the server address IP ranges the connection can target.
 *
 * <p>No secrets — this is a policy declaration, not a credential.</p>
 */
public record AuthenticationRestriction(
        List<String> clientSource,
        List<String> serverAddress
) {
    public AuthenticationRestriction {
        Objects.requireNonNull(clientSource, "clientSource");
        Objects.requireNonNull(serverAddress, "serverAddress");
        clientSource = List.copyOf(clientSource);
        serverAddress = List.copyOf(serverAddress);
    }

    public static AuthenticationRestriction empty() {
        return new AuthenticationRestriction(List.of(), List.of());
    }
}

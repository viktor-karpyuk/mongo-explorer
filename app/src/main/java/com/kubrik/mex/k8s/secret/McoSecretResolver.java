package com.kubrik.mex.k8s.secret;

import com.kubrik.mex.k8s.model.DiscoveredMongo;
import com.kubrik.mex.k8s.model.MongoCredentials;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.models.V1Secret;

import java.util.Optional;

/**
 * v2.8.1 Q2.8.1-B2 — MCO-aware Secret resolver.
 *
 * <p>MongoDB Community Operator does not ship a single "root Secret"
 * — credentials are embedded in per-user Secrets referenced from
 * {@code spec.users[].passwordSecretRef.name}. For Alpha we resolve
 * the <em>first</em> user Secret that matches the conventional name
 * pattern {@code <cr-name>-admin}, which is what the operator
 * documentation uses in its examples and what every community-
 * maintained chart emits. If the user has named their Secret
 * differently, they'll pick it manually in the B3 UI.</p>
 *
 * <p>TLS material: when the CR sets {@code spec.security.tls.enabled}
 * the operator generates a CA Secret named {@code <cr-name>-ca}. We
 * pick that up and take a SHA-256 fingerprint of {@code ca.crt} so
 * the connection row has a stable ID to track across cert rotations
 * without persisting the CA bytes themselves.</p>
 */
public final class McoSecretResolver {

    /** Convention: {@code <cr-name>-admin-user} — matches MCO's quickstart + most charts. */
    static final String ADMIN_USER_SECRET_SUFFIX = "-admin-user";

    /** Convention: {@code <cr-name>-ca} for the TLS CA bundle when operator-generated. */
    static final String CA_SECRET_SUFFIX = "-ca";

    public MongoCredentials resolve(SecretReader reader, DiscoveredMongo m) throws ApiException {
        if (m.origin() != DiscoveredMongo.Origin.MCO) {
            return MongoCredentials.empty();
        }
        String ns = m.namespace();
        String crName = m.name();

        // Credentials — convention: username=<crName>-admin, password
        // under `password` key. Real deployments sometimes rename;
        // the UI fallback lets the user pick the Secret themselves.
        Optional<V1Secret> adminSecret = reader.read(ns, crName + ADMIN_USER_SECRET_SUFFIX);
        Optional<String> username = adminSecret
                .flatMap(s -> SecretReader.stringValue(s, "username"))
                .or(() -> Optional.of(crName + "-admin"));
        Optional<String> password = adminSecret
                .flatMap(s -> SecretReader.stringValue(s, "password"));

        // TLS CA — operator writes to `ca.crt` when tls.enabled=true.
        // We fingerprint the bytes for audit but keep the PEM in-
        // memory only for the caller to wire into the client.
        Optional<V1Secret> caSecret = reader.read(ns, crName + CA_SECRET_SUFFIX);
        Optional<byte[]> caPem = caSecret.flatMap(s -> SecretReader.byteValue(s, "ca.crt"));
        Optional<String> caFingerprint = caPem.flatMap(SecretReader::fingerprint);

        return new MongoCredentials(
                username,
                password,
                Optional.of("admin"),
                caPem,
                caFingerprint,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }
}

package com.kubrik.mex.k8s.secret;

import com.kubrik.mex.k8s.model.DiscoveredMongo;
import com.kubrik.mex.k8s.model.MongoCredentials;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.models.V1Secret;

import java.util.Optional;

/**
 * v2.8.1 Q2.8.1-B2 — PSMDB-aware Secret resolver.
 *
 * <p>The Percona operator creates a fixed-name Secret set per CR
 * unless the user overrides it via {@code spec.secrets}. The
 * canonical names are:</p>
 * <ul>
 *   <li>{@code <cr-name>-secrets} — the users Secret
 *       ({@code MONGODB_USER_ADMIN_USER} / {@code _PASSWORD},
 *       {@code MONGODB_CLUSTER_ADMIN_USER} / {@code _PASSWORD},
 *       and friends).</li>
 *   <li>{@code <cr-name>-ssl} — external TLS (server + CA).</li>
 *   <li>{@code <cr-name>-ssl-internal} — internal TLS (operator-
 *       only; not useful to clients).</li>
 * </ul>
 *
 * <p>For Alpha we surface <em>userAdmin</em> creds by default — the
 * admin path into the cluster. The wizard's connect flow lets the
 * user switch to the clusterAdmin or monitoring roles when that
 * role is what they need.</p>
 */
public final class PsmdbSecretResolver {

    static final String USERS_SECRET_SUFFIX = "-secrets";
    static final String SSL_SECRET_SUFFIX = "-ssl";

    public static final String USER_KEY = "MONGODB_USER_ADMIN_USER";
    public static final String PASS_KEY = "MONGODB_USER_ADMIN_PASSWORD";

    public MongoCredentials resolve(SecretReader reader, DiscoveredMongo m) throws ApiException {
        if (m.origin() != DiscoveredMongo.Origin.PSMDB) {
            return MongoCredentials.empty();
        }
        String ns = m.namespace();
        String crName = m.name();

        Optional<V1Secret> users = reader.read(ns, crName + USERS_SECRET_SUFFIX);
        Optional<String> user = users.flatMap(s -> SecretReader.stringValue(s, USER_KEY))
                .or(() -> Optional.of("userAdmin"));
        Optional<String> pass = users.flatMap(s -> SecretReader.stringValue(s, PASS_KEY));

        // TLS CA bundle — key is `ca.crt` inside the ssl Secret, same
        // convention as MCO. External (not internal) TLS is what
        // client connections consume.
        Optional<V1Secret> ssl = reader.read(ns, crName + SSL_SECRET_SUFFIX);
        Optional<byte[]> caPem = ssl.flatMap(s -> SecretReader.byteValue(s, "ca.crt"));
        Optional<String> caFingerprint = caPem.flatMap(SecretReader::fingerprint);

        // Client cert path — PSMDB surfaces it in the same ssl Secret
        // as tls.crt / tls.key when the user supplies a BYO-CA. We
        // pick it up opportunistically.
        Optional<byte[]> clientCert = ssl.flatMap(s -> SecretReader.byteValue(s, "tls.crt"));
        Optional<byte[]> clientKey = ssl.flatMap(s -> SecretReader.byteValue(s, "tls.key"));

        return new MongoCredentials(
                user,
                pass,
                Optional.of("admin"),
                caPem,
                caFingerprint,
                clientCert,
                clientKey,
                Optional.empty());
    }
}

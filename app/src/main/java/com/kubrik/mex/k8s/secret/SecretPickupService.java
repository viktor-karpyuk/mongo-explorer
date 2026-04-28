package com.kubrik.mex.k8s.secret;

import com.kubrik.mex.k8s.client.KubeClientFactory;
import com.kubrik.mex.k8s.model.DiscoveredMongo;
import com.kubrik.mex.k8s.model.K8sClusterRef;
import com.kubrik.mex.k8s.model.MongoCredentials;
import io.kubernetes.client.openapi.ApiException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

/**
 * v2.8.1 Q2.8.1-B2 — Resolves credentials + TLS material for a
 * {@link DiscoveredMongo}, dispatching to the operator-aware
 * resolver.
 *
 * <p>No persistence: the returned {@link MongoCredentials} lives
 * entirely in memory. Callers who write a Mongo Explorer connection
 * row from the result must strip everything except the TLS
 * fingerprint before touching the store (enforced in the B3 connect
 * flow).</p>
 *
 * <p>Plain-STS discoveries always yield {@link MongoCredentials#empty()}
 * — we have no way to know which Secret holds credentials, so the
 * B3 UI offers a "pick a Secret" step.</p>
 */
public final class SecretPickupService {

    private static final Logger log = LoggerFactory.getLogger(SecretPickupService.class);

    private final KubeClientFactory clientFactory;
    private final McoSecretResolver mco;
    private final PsmdbSecretResolver psmdb;

    public SecretPickupService(KubeClientFactory clientFactory) {
        this(clientFactory, new McoSecretResolver(), new PsmdbSecretResolver());
    }

    SecretPickupService(KubeClientFactory clientFactory,
                         McoSecretResolver mco, PsmdbSecretResolver psmdb) {
        this.clientFactory = clientFactory;
        this.mco = mco;
        this.psmdb = psmdb;
    }

    public MongoCredentials resolve(K8sClusterRef ref, DiscoveredMongo m) {
        Objects.requireNonNull(ref, "ref");
        Objects.requireNonNull(m, "m");
        SecretReader reader;
        try {
            reader = new SecretReader(clientFactory.get(ref));
        } catch (IOException ioe) {
            log.debug("client build failed for {}: {}", ref.coordinates(), ioe.toString());
            return MongoCredentials.empty();
        }
        try {
            return switch (m.origin()) {
                case MCO -> mco.resolve(reader, m);
                case PSMDB -> psmdb.resolve(reader, m);
                case PLAIN_STS -> MongoCredentials.empty();
            };
        } catch (ApiException ae) {
            // 403 in the caller's namespace is common — surface
            // empty rather than failing the whole operation, and let
            // the UI render the "pick a Secret" fallback.
            log.debug("secret pickup for {}: HTTP {}", m.coordinates(), ae.getCode());
            return MongoCredentials.empty();
        } catch (Exception e) {
            log.debug("secret pickup for {}: {}", m.coordinates(), e.toString());
            return MongoCredentials.empty();
        }
    }
}

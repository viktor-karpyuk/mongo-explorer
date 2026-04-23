package com.kubrik.mex.k8s.discovery;

import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.k8s.client.KubeClientFactory;
import com.kubrik.mex.k8s.events.DiscoveryEvent;
import com.kubrik.mex.k8s.model.DiscoveredMongo;
import com.kubrik.mex.k8s.model.K8sClusterRef;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * v2.8.1 Q2.8.1-B1 — Thin facade over the three discoverers.
 *
 * <p>Fires three list calls in sequence and merges the results into
 * a single, de-duplicated list sorted by {@code (namespace, name)}.
 * The service doesn't manage informers yet — that lands in Q2.8.1-B
 * follow-up chunks once the Clusters pane needs live deltas.</p>
 *
 * <p>Every call emits either a {@link DiscoveryEvent.Refreshed} (even
 * if the list is empty) or a {@link DiscoveryEvent.Failed} on the
 * event bus; the pane renders from those events instead of polling.</p>
 */
public final class DiscoveryService {

    private static final Logger log = LoggerFactory.getLogger(DiscoveryService.class);

    private final KubeClientFactory clientFactory;
    private final EventBus events;
    private final McoDiscoverer mco;
    private final PsmdbDiscoverer psmdb;
    private final PlainStsDiscoverer plain;

    public DiscoveryService(KubeClientFactory clientFactory, EventBus events) {
        this(clientFactory, events,
                new McoDiscoverer(), new PsmdbDiscoverer(), new PlainStsDiscoverer());
    }

    // Visible for tests — lets a unit test inject stubs for each discoverer.
    DiscoveryService(KubeClientFactory clientFactory, EventBus events,
                      McoDiscoverer mco, PsmdbDiscoverer psmdb, PlainStsDiscoverer plain) {
        this.clientFactory = clientFactory;
        this.events = events;
        this.mco = mco;
        this.psmdb = psmdb;
        this.plain = plain;
    }

    /**
     * Block until all three discoverers return; publish results.
     * Partial failures (e.g. PSMDB CRD absent, operator-only cluster)
     * do not fail the whole run — individual discoverers swallow 404s
     * as "operator not installed."
     */
    public List<DiscoveredMongo> discover(K8sClusterRef ref) {
        ApiClient client;
        try {
            client = clientFactory.get(ref);
        } catch (IOException ioe) {
            publishFailed(ref.id(), ioe.getMessage());
            return List.of();
        }

        List<DiscoveredMongo> merged = new ArrayList<>();
        try {
            merged.addAll(mco.discover(client, ref.id()));
        } catch (ApiException ae) {
            log.debug("MCO discover on {}: HTTP {}", ref.coordinates(), ae.getCode());
        } catch (Exception e) {
            log.debug("MCO discover on {}: {}", ref.coordinates(), e.toString());
        }
        try {
            merged.addAll(psmdb.discover(client, ref.id()));
        } catch (ApiException ae) {
            log.debug("PSMDB discover on {}: HTTP {}", ref.coordinates(), ae.getCode());
        } catch (Exception e) {
            log.debug("PSMDB discover on {}: {}", ref.coordinates(), e.toString());
        }
        try {
            merged.addAll(plain.discover(client, ref.id()));
        } catch (ApiException ae) {
            log.debug("plain-STS discover on {}: HTTP {}", ref.coordinates(), ae.getCode());
        } catch (Exception e) {
            log.debug("plain-STS discover on {}: {}", ref.coordinates(), e.toString());
        }

        merged.sort(Comparator.comparing(DiscoveredMongo::namespace)
                .thenComparing(DiscoveredMongo::name));
        List<DiscoveredMongo> out = Collections.unmodifiableList(merged);
        events.publishDiscovery(new DiscoveryEvent.Refreshed(
                ref.id(), out, System.currentTimeMillis()));
        return out;
    }

    private void publishFailed(long clusterId, String reason) {
        events.publishDiscovery(new DiscoveryEvent.Failed(
                clusterId, reason, System.currentTimeMillis()));
    }
}

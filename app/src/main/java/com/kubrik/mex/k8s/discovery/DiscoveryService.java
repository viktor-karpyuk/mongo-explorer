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
        int hardFailures = 0;
        String lastHardReason = null;
        Object[][] probes = {
                {"MCO",     (DiscovererCall) () -> mco.discover(client, ref.id())},
                {"PSMDB",   (DiscovererCall) () -> psmdb.discover(client, ref.id())},
                {"plain-STS", (DiscovererCall) () -> plain.discover(client, ref.id())},
        };
        for (Object[] probe : probes) {
            String label = (String) probe[0];
            DiscovererCall call = (DiscovererCall) probe[1];
            try {
                merged.addAll(call.run());
            } catch (ApiException ae) {
                log.debug("{} discover on {}: HTTP {}", label, ref.coordinates(), ae.getCode());
                // 401 / 403 / 5xx are hard failures: the caller can't
                // reach the API at all. A 404 from the CustomObjectsApi
                // is swallowed by the discoverer itself as "operator not
                // installed" and never reaches this catch.
                if (ae.getCode() == 401 || ae.getCode() == 403 || ae.getCode() >= 500) {
                    hardFailures++;
                    lastHardReason = label + ": HTTP " + ae.getCode()
                            + (ae.getMessage() == null ? "" : " " + ae.getMessage());
                }
            } catch (Exception e) {
                log.debug("{} discover on {}: {}", label, ref.coordinates(), e.toString());
                hardFailures++;
                lastHardReason = label + ": " + e.getMessage();
            }
        }

        // If EVERY probe hard-failed and we found nothing, the user's
        // cluster is probably unreachable / auth-expired rather than
        // "empty of Mongo workloads." Tell the UI so it doesn't render
        // a misleading "Found 0" result.
        if (hardFailures == probes.length && merged.isEmpty()) {
            publishFailed(ref.id(), "all discoverers failed; last reason: " + lastHardReason);
            return List.of();
        }

        merged.sort(Comparator.comparing(DiscoveredMongo::namespace)
                .thenComparing(DiscoveredMongo::name));
        List<DiscoveredMongo> out = Collections.unmodifiableList(merged);
        events.publishDiscovery(new DiscoveryEvent.Refreshed(
                ref.id(), out, System.currentTimeMillis()));
        return out;
    }

    @FunctionalInterface
    private interface DiscovererCall {
        List<DiscoveredMongo> run() throws ApiException, Exception;
    }

    private void publishFailed(long clusterId, String reason) {
        events.publishDiscovery(new DiscoveryEvent.Failed(
                clusterId, reason, System.currentTimeMillis()));
    }
}

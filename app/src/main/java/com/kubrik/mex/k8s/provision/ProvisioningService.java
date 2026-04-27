package com.kubrik.mex.k8s.provision;

import com.kubrik.mex.k8s.apply.ApplyOrchestrator;
import com.kubrik.mex.k8s.apply.ProvisioningRecordDao;
import com.kubrik.mex.k8s.model.K8sClusterRef;
import com.kubrik.mex.k8s.operator.OperatorAdapter;
import com.kubrik.mex.k8s.operator.mco.McoAdapter;
import com.kubrik.mex.k8s.operator.psmdb.PsmdbAdapter;
import com.kubrik.mex.k8s.preflight.PreflightEngine;
import com.kubrik.mex.k8s.preflight.PreflightSummary;
import com.kubrik.mex.k8s.rollout.PostReadyConnector;
import com.kubrik.mex.k8s.rollout.RolloutWatcher;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * v2.8.1 Q2.8.1-L — Facade that bundles the full provisioning flow.
 *
 * <p>The UI (ProvisionWizard / ProvisionDialog) only wires one
 * service — this — rather than 5 separate collaborators. Internally
 * the facade owns:</p>
 * <ul>
 *   <li>{@link PreflightEngine} for the pre-Apply gate.</li>
 *   <li>A map of {@link OperatorAdapter}s keyed on {@link OperatorId}.</li>
 *   <li>{@link ApplyOrchestrator}, {@link RolloutWatcher}, and
 *       {@link PostReadyConnector} for the Apply → Watch →
 *       auto-connect pipeline.</li>
 * </ul>
 *
 * <p>Two entry points:</p>
 * <ol>
 *   <li>{@link #preflight} — pure-read pre-Apply gate. UI disables
 *       Apply while any {@link PreflightSummary#hasAnyFail} is true.</li>
 *   <li>{@link #provision} — full Apply → Watch → auto-connect.</li>
 * </ol>
 */
public final class ProvisioningService {

    private final PreflightEngine preflight;
    private final ApplyOrchestrator orchestrator;
    private final RolloutWatcher watcher;
    private final PostReadyConnector connector;
    private final Map<OperatorId, OperatorAdapter> adapters;

    public ProvisioningService(PreflightEngine preflight,
                                 ApplyOrchestrator orchestrator,
                                 RolloutWatcher watcher,
                                 PostReadyConnector connector) {
        this(preflight, orchestrator, watcher, connector,
                Map.of(
                        OperatorId.MCO, new McoAdapter(),
                        OperatorId.PSMDB, new PsmdbAdapter()));
    }

    public ProvisioningService(PreflightEngine preflight,
                                 ApplyOrchestrator orchestrator,
                                 RolloutWatcher watcher,
                                 PostReadyConnector connector,
                                 Map<OperatorId, OperatorAdapter> adapters) {
        this.preflight = Objects.requireNonNull(preflight);
        this.orchestrator = Objects.requireNonNull(orchestrator);
        this.watcher = Objects.requireNonNull(watcher);
        this.connector = Objects.requireNonNull(connector);
        this.adapters = Map.copyOf(adapters);
    }

    /** Run pre-flight against the cluster + model without mutating anything. */
    public PreflightSummary preflight(K8sClusterRef ref, ProvisionModel model) {
        return preflight.run(ref, model);
    }

    /**
     * Run Apply → Watch → auto-connect. Caller should have gated
     * on {@link #preflight} (no FAILs, warnings acknowledged).
     */
    public ApplyOrchestrator.ApplyResult provision(K8sClusterRef ref,
                                                      ProvisionModel model) throws IOException {
        OperatorAdapter adapter = adapters.get(model.operator());
        if (adapter == null) {
            throw new IllegalArgumentException("no adapter for operator " + model.operator());
        }
        return orchestrator.applyAndWatch(ref, adapter, model, watcher, connector);
    }

    /** Same as {@link #provision} but skips the auto-connect hook. */
    public ApplyOrchestrator.ApplyResult provisionNoAutoConnect(K8sClusterRef ref,
                                                                   ProvisionModel model) throws IOException {
        OperatorAdapter adapter = adapters.get(model.operator());
        if (adapter == null) {
            throw new IllegalArgumentException("no adapter for operator " + model.operator());
        }
        return orchestrator.applyAndWatch(ref, adapter, model, watcher);
    }

    /**
     * Factory helper that wires the standard production pipeline from
     * the minimum set of collaborators. MainView calls this to keep
     * the service graph explicit.
     */
    public static ProvisioningService wire(com.kubrik.mex.k8s.client.KubeClientFactory clientFactory,
                                              ProvisioningRecordDao recordDao,
                                              com.kubrik.mex.k8s.rollout.RolloutEventDao eventDao,
                                              com.kubrik.mex.events.EventBus events,
                                              com.kubrik.mex.k8s.portforward.PortForwardService portForwardService,
                                              com.kubrik.mex.store.ConnectionStore connectionStore) {
        return wire(clientFactory, recordDao, eventDao, events,
                portForwardService, connectionStore, null);
    }

    /** v2.8.4 — overload that takes a managed-pool phase service so
     *  the orchestrator runs the cloud phase before the K8s phase. */
    public static ProvisioningService wire(com.kubrik.mex.k8s.client.KubeClientFactory clientFactory,
                                              ProvisioningRecordDao recordDao,
                                              com.kubrik.mex.k8s.rollout.RolloutEventDao eventDao,
                                              com.kubrik.mex.events.EventBus events,
                                              com.kubrik.mex.k8s.portforward.PortForwardService portForwardService,
                                              com.kubrik.mex.store.ConnectionStore connectionStore,
                                              com.kubrik.mex.k8s.compute.managedpool.ManagedPoolPhaseService managedPoolPhase) {
        PreflightEngine preflight = new PreflightEngine(clientFactory);
        ApplyOrchestrator orchestrator =
                new ApplyOrchestrator(clientFactory, recordDao, eventDao, events);
        if (managedPoolPhase != null) orchestrator.setManagedPoolPhase(managedPoolPhase);
        RolloutWatcher watcher = new RolloutWatcher(clientFactory);
        PostReadyConnector connector =
                new PostReadyConnector(portForwardService, connectionStore, recordDao);
        return new ProvisioningService(preflight, orchestrator, watcher, connector);
    }
}

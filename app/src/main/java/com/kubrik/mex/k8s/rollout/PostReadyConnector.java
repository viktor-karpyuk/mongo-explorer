package com.kubrik.mex.k8s.rollout;

import com.kubrik.mex.k8s.apply.ProvisioningRecordDao;
import com.kubrik.mex.k8s.model.K8sClusterRef;
import com.kubrik.mex.k8s.model.PortForwardSession;
import com.kubrik.mex.k8s.model.PortForwardTarget;
import com.kubrik.mex.k8s.portforward.PortForwardService;
import com.kubrik.mex.k8s.provision.OperatorId;
import com.kubrik.mex.k8s.provision.ProvisionModel;
import com.kubrik.mex.k8s.provision.Topology;
import com.kubrik.mex.store.ConnectionStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Optional;

/**
 * v2.8.1 Q2.8.1-L — Auto-connect hook run after a provision lands
 * on READY.
 *
 * <p>Three steps, each best-effort:</p>
 * <ol>
 *   <li>Pick the right Service name for the deployment's topology
 *       + operator (mongos for PSMDB sharded; rs0 for everything
 *       else).</li>
 *   <li>Open a port-forward via {@link PortForwardService}. The
 *       session's local port becomes part of the Mongo Explorer
 *       connection URI.</li>
 *   <li>Write a {@code origin='K8S'} row via {@link
 *       ConnectionStore#insertK8sOrigin} and back-link it to the
 *       provisioning row via {@link
 *       ProvisioningRecordDao#attachConnection}.</li>
 * </ol>
 *
 * <p>A failure in any step logs + returns {@link ConnectOutcome}
 * with the reason — the caller doesn't re-mark the row as FAILED
 * because a ready deployment without an auto-connection is still
 * usable manually.</p>
 */
public final class PostReadyConnector {

    private static final Logger log = LoggerFactory.getLogger(PostReadyConnector.class);

    private final PortForwardService portForwardService;
    private final ConnectionStore connectionStore;
    private final ProvisioningRecordDao recordDao;

    public PostReadyConnector(PortForwardService portForwardService,
                                ConnectionStore connectionStore,
                                ProvisioningRecordDao recordDao) {
        this.portForwardService = Objects.requireNonNull(portForwardService);
        this.connectionStore = Objects.requireNonNull(connectionStore);
        this.recordDao = Objects.requireNonNull(recordDao);
    }

    public ConnectOutcome connect(K8sClusterRef clusterRef,
                                    long provisioningId,
                                    ProvisionModel model) {
        String serviceName = serviceNameFor(model);
        int servicePort = 27017;
        String connectionName = labelFor(model);

        PortForwardSession session;
        try {
            session = portForwardService.open(clusterRef, connectionName,
                    PortForwardTarget.forService(model.namespace(), serviceName, servicePort));
        } catch (IOException ioe) {
            log.debug("post-ready port-forward failed for {}: {}",
                    model.deploymentName(), ioe.toString());
            return ConnectOutcome.failed("port-forward: " + ioe.getMessage());
        }

        String uri = "mongodb://127.0.0.1:" + session.localPort() + "/?directConnection="
                + (model.topology() == Topology.STANDALONE ? "true" : "false");
        String connectionId;
        try {
            connectionId = connectionStore.insertK8sOrigin(connectionName, uri);
        } catch (RuntimeException re) {
            log.debug("post-ready connection insert failed for {}: {}",
                    model.deploymentName(), re.toString());
            portForwardService.close(session.auditRowId(), "CONNECT_INSERT_FAILED");
            return ConnectOutcome.failed("connection insert: " + re.getMessage());
        }

        try {
            recordDao.attachConnection(provisioningId, connectionId);
        } catch (SQLException sqle) {
            // Connection row + forward stay in place even if the back-
            // link fails — the connection is usable; the row just isn't
            // linked. Log but don't tear anything down.
            log.warn("attachConnection {} → {}: {}",
                    provisioningId, connectionId, sqle.toString());
        }
        return ConnectOutcome.ok(connectionId, session);
    }

    /* ============================ helpers ============================ */

    /**
     * Pick the canonical Service name the operator emits. For both
     * operators the rule is "mongos for sharded, rs0 for replica".
     */
    static String serviceNameFor(ProvisionModel m) {
        return switch (m.operator()) {
            case MCO -> m.deploymentName() + "-svc";
            case PSMDB -> m.topology() == Topology.SHARDED
                    ? m.deploymentName() + "-mongos"
                    : m.deploymentName() + "-rs0";
        };
    }

    static String labelFor(ProvisionModel m) {
        return (m.operator() == OperatorId.MCO ? "mco:" : "psmdb:")
                + m.namespace() + "/" + m.deploymentName();
    }

    public record ConnectOutcome(
            boolean ok,
            Optional<String> connectionId,
            Optional<PortForwardSession> session,
            Optional<String> failureReason) {

        public static ConnectOutcome ok(String connectionId, PortForwardSession session) {
            return new ConnectOutcome(true,
                    Optional.of(connectionId), Optional.of(session), Optional.empty());
        }

        public static ConnectOutcome failed(String reason) {
            return new ConnectOutcome(false,
                    Optional.empty(), Optional.empty(), Optional.of(reason));
        }
    }
}

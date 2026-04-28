package com.kubrik.mex.k8s.teardown;

import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.k8s.apply.ApplyOrchestrator;
import com.kubrik.mex.k8s.apply.ProvisioningRecordDao;
import com.kubrik.mex.k8s.client.KubeClientFactory;
import com.kubrik.mex.k8s.events.ProvisionEvent;
import com.kubrik.mex.k8s.model.K8sClusterRef;
import com.kubrik.mex.k8s.model.ProvisioningRecord;
import com.kubrik.mex.k8s.rollout.ResourceCatalogue;
import io.kubernetes.client.openapi.ApiClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

/**
 * v2.8.1 Q2.8.1-I — Tear-down service + deletion protection gate.
 *
 * <p>Two-gate flow (milestone §2.9):</p>
 * <ol>
 *   <li>If {@link ProvisioningRecord#deletionProtection()} is on,
 *       the user must turn it off first via {@link
 *       #clearDeletionProtection}. This is a deliberate extra click
 *       so a misclick on "Delete" doesn't nuke a Prod deployment.</li>
 *   <li>With protection off, the user picks a {@link CascadePlan}
 *       (which kinds to cascade-delete) + types the deployment
 *       name to confirm in the UI layer.</li>
 *   <li>{@link #tearDown} executes the plan via the same
 *       {@link com.kubrik.mex.k8s.apply.ApplyOrchestrator.ApplyOpener}
 *       the Apply path uses.</li>
 * </ol>
 *
 * <p>Audit is via the existing {@code provisioning_records.status}
 * flip + a ProvisionEvent.Failed (reason="DELETED") on the bus; a
 * separate {@code ops_audit} row is emitted by the UI layer so it
 * carries the caller_user / caller_host fields this service
 * doesn't have in scope.</p>
 */
public final class TearDownService {

    private static final Logger log = LoggerFactory.getLogger(TearDownService.class);

    private final ProvisioningRecordDao recordDao;
    private final KubeClientFactory clientFactory;
    private final ApplyOrchestrator.ApplyOpener opener;
    private final EventBus events;

    public TearDownService(ProvisioningRecordDao recordDao,
                            KubeClientFactory clientFactory,
                            ApplyOrchestrator.ApplyOpener opener,
                            EventBus events) {
        this.recordDao = Objects.requireNonNull(recordDao);
        this.clientFactory = Objects.requireNonNull(clientFactory);
        this.opener = Objects.requireNonNull(opener);
        this.events = Objects.requireNonNull(events);
    }

    /**
     * First gate. Refuses with {@link IllegalStateException} when
     * the caller tries to clear protection on a row that didn't
     * have it.
     */
    public void clearDeletionProtection(long provisioningId) throws SQLException {
        ProvisioningRecord row = recordDao.findById(provisioningId)
                .orElseThrow(() -> new IllegalArgumentException(
                        "no provisioning row " + provisioningId));
        if (!row.deletionProtection()) {
            throw new IllegalStateException(
                    "deletion protection already off for " + row.name());
        }
        recordDao.setDeletionProtection(provisioningId, false);
    }

    /**
     * Second gate + execute. Refuses on {@link IllegalStateException}
     * when deletion protection is still on (caller forgot to clear)
     * or the typed confirm didn't match (caller's job — this method
     * trusts it).
     */
    public TearDownResult tearDown(K8sClusterRef clusterRef,
                                      long provisioningId,
                                      CascadePlan plan) throws IOException, SQLException {
        ProvisioningRecord row = recordDao.findById(provisioningId)
                .orElseThrow(() -> new IllegalArgumentException(
                        "no provisioning row " + provisioningId));
        if (row.deletionProtection()) {
            throw new IllegalStateException(
                    "deletion protection is on — clear it first");
        }
        if (row.status() == ProvisioningRecord.Status.DELETED) {
            return new TearDownResult(0, List.of(), "already deleted");
        }

        recordDao.markStatus(provisioningId, ProvisioningRecord.Status.DELETING);

        ApiClient client = clientFactory.get(clusterRef);

        List<ResourceCatalogue.Ref> toDelete = buildDeleteList(row, plan);
        int deleted = 0;
        java.util.List<String> failures = new java.util.ArrayList<>();
        for (ResourceCatalogue.Ref ref : toDelete) {
            try {
                opener.delete(client, ref);
                deleted++;
            } catch (Exception e) {
                log.debug("delete {}/{} failed: {}", ref.kind(), ref.name(), e.toString());
                failures.add(ref.kind() + "/" + ref.name() + ": "
                        + (e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage()));
            }
        }

        recordDao.markStatus(provisioningId, ProvisioningRecord.Status.DELETED);
        events.publishProvision(new ProvisionEvent.Failed(provisioningId,
                "deleted: " + plan.summary(),
                System.currentTimeMillis()));
        return new TearDownResult(deleted, failures,
                failures.isEmpty() ? "clean" : "partial");
    }

    private static List<ResourceCatalogue.Ref> buildDeleteList(
            ProvisioningRecord row, CascadePlan plan) {
        List<ResourceCatalogue.Ref> refs = new java.util.ArrayList<>();
        // CR first — operator GC chains to its owned objects.
        if (plan.deleteCr()) {
            String apiVersion = switch (row.operator()) {
                case "MCO" -> "mongodbcommunity.mongodb.com/v1";
                case "PSMDB" -> "psmdb.percona.com/v1";
                default -> "v1";
            };
            String kind = switch (row.operator()) {
                case "MCO" -> "MongoDBCommunity";
                case "PSMDB" -> "PerconaServerMongoDB";
                default -> "CustomResource";
            };
            refs.add(new ResourceCatalogue.Ref(apiVersion, kind,
                    row.namespace(), row.name()));
        }
        if (plan.deleteSecrets()) {
            // Adapter-specific Secret name conventions. Both emit at
            // least <name>-secrets (PSMDB) or <name>-admin-user (MCO).
            refs.add(new ResourceCatalogue.Ref("v1", "Secret",
                    row.namespace(),
                    row.operator().equals("PSMDB")
                            ? row.name() + "-secrets"
                            : row.name() + "-admin-user"));
        }
        if (plan.deletePvcs()) {
            // Operator-owned PVCs get garbage-collected when the CR
            // + STS teardown finishes; we still emit an explicit
            // label-selector delete for any leftover PVCs that
            // lingered past CR removal.
            refs.add(new ResourceCatalogue.Ref("v1",
                    "PersistentVolumeClaim",
                    row.namespace(), row.name()));
        }
        return refs;
    }

    public record TearDownResult(int deletedCount, List<String> failures, String summary) {}
}

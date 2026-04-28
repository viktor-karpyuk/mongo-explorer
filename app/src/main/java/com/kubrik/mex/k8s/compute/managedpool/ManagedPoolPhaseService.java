package com.kubrik.mex.k8s.compute.managedpool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * v2.8.4 Q2.8.4-D — Pre-CR cloud phase. Routes a {@link ManagedPoolSpec}
 * through the right {@link ManagedPoolAdapter}, polls until the pool
 * is Ready, and stamps every cloud call into {@code managed_pool_operations}.
 *
 * <p>Returns a {@link Result} that {@link com.kubrik.mex.k8s.apply.ApplyOrchestrator}
 * uses to gate the rest of Apply: {@link Result#ok()} false means the
 * orchestrator should fail the row before touching the Mongo CR.</p>
 */
public final class ManagedPoolPhaseService {

    private static final Logger log = LoggerFactory.getLogger(ManagedPoolPhaseService.class);
    private static final Duration POLL_INTERVAL = Duration.ofSeconds(15);
    private static final Duration READY_TIMEOUT = Duration.ofMinutes(10);

    private final ManagedPoolAdapterRegistry registry;
    private final CloudCredentialDao credDao;
    private final ManagedPoolOperationDao opDao;

    public ManagedPoolPhaseService(ManagedPoolAdapterRegistry registry,
                                     CloudCredentialDao credDao,
                                     ManagedPoolOperationDao opDao) {
        this.registry = Objects.requireNonNull(registry, "registry");
        this.credDao = Objects.requireNonNull(credDao, "credDao");
        this.opDao = Objects.requireNonNull(opDao, "opDao");
    }

    public record Result(boolean ok, Optional<String> errorMessage,
                          Optional<String> cloudCallId) {
        public static Result success(String cloudCallId) {
            return new Result(true, Optional.empty(), Optional.ofNullable(cloudCallId));
        }
        public static Result failure(String message) {
            return new Result(false, Optional.ofNullable(message), Optional.empty());
        }
    }

    /** Run the create-pool → wait-Ready → done sequence for a Mongo
     *  provisioning row. Returns once the pool is Ready or after the
     *  {@link #READY_TIMEOUT} budget. */
    public Result createAndAwaitReady(long provisioningRecordId, ManagedPoolSpec spec) {
        ManagedPoolAdapter adapter = registry.lookup(spec.provider()).orElse(null);
        if (adapter == null) {
            return Result.failure("no adapter for cloud provider " + spec.provider());
        }
        Optional<CloudCredential> credOpt;
        try { credOpt = credDao.findById(spec.credentialId()); }
        catch (SQLException sqle) {
            return Result.failure("credential lookup failed: " + sqle.getMessage());
        }
        if (credOpt.isEmpty()) {
            return Result.failure("credential id " + spec.credentialId() + " not found");
        }
        CloudCredential cred = credOpt.get();

        long createOpId = startOp(provisioningRecordId, cred,
                ManagedPoolOperationDao.Action.POOL_CREATE, spec);
        var createResult = adapter.createPool(cred, spec);
        finishOp(createOpId, createResult.cloudCallId(),
                createResult.status() == ManagedPoolAdapter.PoolOperationResult.Status.ACCEPTED
                        ? ManagedPoolOperationDao.Status.ACCEPTED
                        : ManagedPoolOperationDao.Status.REJECTED,
                createResult.errorMessage());
        if (createResult.status() != ManagedPoolAdapter.PoolOperationResult.Status.ACCEPTED) {
            return Result.failure("createPool rejected: "
                    + createResult.errorMessage().orElse("?"));
        }

        long deadline = System.currentTimeMillis() + READY_TIMEOUT.toMillis();
        // Adapter.describe folds "pool genuinely deleted" and
        // "transient API error" into the same Optional.empty(). Track
        // consecutive empties so a real deletion exits fast instead of
        // burning the entire 10-minute budget — a flapping API can
        // recover within a minute or two before we give up.
        int consecutiveEmpty = 0;
        final int EMPTY_BAIL_THRESHOLD = 6;
        while (System.currentTimeMillis() < deadline) {
            long describeOpId = startOp(provisioningRecordId, cred,
                    ManagedPoolOperationDao.Action.POOL_DESCRIBE, spec);
            Optional<ManagedPoolAdapter.PoolDescription> desc =
                    adapter.describe(cred, spec.region(), spec.poolName());
            finishOp(describeOpId, Optional.empty(),
                    desc.isPresent() ? ManagedPoolOperationDao.Status.OK
                            : ManagedPoolOperationDao.Status.FAILED,
                    Optional.empty());
            if (desc.isPresent()) {
                consecutiveEmpty = 0;
                if (desc.get().phase() == ManagedPoolAdapter.PoolPhase.READY) {
                    return Result.success(createResult.cloudCallId().orElse(null));
                }
                if (desc.get().phase() == ManagedPoolAdapter.PoolPhase.FAILED) {
                    return Result.failure("pool entered FAILED state after create");
                }
            } else {
                consecutiveEmpty++;
                if (consecutiveEmpty >= EMPTY_BAIL_THRESHOLD) {
                    return Result.failure("pool absent for " + EMPTY_BAIL_THRESHOLD
                            + " consecutive describe calls — assumed deleted "
                            + "out-of-band or never created");
                }
            }
            try { Thread.sleep(POLL_INTERVAL); }
            catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return Result.failure("interrupted while waiting for pool Ready");
            }
        }
        return Result.failure("pool did not become Ready within "
                + READY_TIMEOUT.toMinutes() + " minutes");
    }

    /** Counterpart for tear-down: deletes the pool the create phase
     *  rolled out. Best-effort — caller decides whether to surface a
     *  failure to the user. */
    public Result delete(long provisioningRecordId, ManagedPoolSpec spec) {
        ManagedPoolAdapter adapter = registry.lookup(spec.provider()).orElse(null);
        if (adapter == null) {
            return Result.failure("no adapter for cloud provider " + spec.provider());
        }
        Optional<CloudCredential> credOpt;
        try { credOpt = credDao.findById(spec.credentialId()); }
        catch (SQLException sqle) {
            return Result.failure("credential lookup failed: " + sqle.getMessage());
        }
        if (credOpt.isEmpty()) {
            return Result.failure("credential id " + spec.credentialId() + " not found");
        }
        CloudCredential cred = credOpt.get();
        long opId = startOp(provisioningRecordId, cred,
                ManagedPoolOperationDao.Action.POOL_DELETE, spec);
        var del = adapter.deletePool(cred, spec.region(), spec.poolName());
        finishOp(opId, del.cloudCallId(),
                del.status() == ManagedPoolAdapter.PoolOperationResult.Status.ACCEPTED
                        ? ManagedPoolOperationDao.Status.ACCEPTED
                        : ManagedPoolOperationDao.Status.REJECTED,
                del.errorMessage());
        return del.status() == ManagedPoolAdapter.PoolOperationResult.Status.ACCEPTED
                ? Result.success(del.cloudCallId().orElse(null))
                : Result.failure(del.errorMessage().orElse("delete rejected"));
    }

    /* ============================ internals ============================ */

    private long startOp(long provisioningRecordId, CloudCredential cred,
                          ManagedPoolOperationDao.Action action, ManagedPoolSpec spec) {
        try {
            return opDao.start(Optional.of(provisioningRecordId),
                    cred.provider(), action,
                    Optional.of(spec.region()),
                    accountFor(cred),
                    Optional.of(spec.poolName()));
        } catch (SQLException sqle) {
            log.warn("audit start {} failed: {}", action, sqle.toString());
            return -1L;
        }
    }

    private void finishOp(long opId, Optional<String> cloudCallId,
                           ManagedPoolOperationDao.Status status,
                           Optional<String> errorMessage) {
        if (opId <= 0) return;
        try { opDao.finish(opId, status, cloudCallId, errorMessage); }
        catch (SQLException sqle) {
            log.warn("audit finish {} failed: {}", opId, sqle.toString());
        }
    }

    private static Optional<String> accountFor(CloudCredential cred) {
        return switch (cred.provider()) {
            case AWS -> cred.awsAccountId();
            case GCP -> cred.gcpProject();
            case AZURE -> cred.azureSubscription();
        };
    }
}

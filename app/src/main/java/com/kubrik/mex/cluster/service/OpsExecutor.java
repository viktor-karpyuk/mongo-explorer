package com.kubrik.mex.cluster.service;

import com.kubrik.mex.cluster.audit.OpsAuditRecord;
import com.kubrik.mex.cluster.audit.Outcome;
import com.kubrik.mex.cluster.safety.Command;
import com.kubrik.mex.cluster.safety.DryRunResult;
import com.kubrik.mex.cluster.safety.KillSwitch;
import com.kubrik.mex.cluster.safety.PreviewHashChecker;
import com.kubrik.mex.cluster.safety.RoleSet;
import com.kubrik.mex.cluster.store.OpsAuditDao;
import com.kubrik.mex.core.ConnectionManager;
import com.kubrik.mex.core.MongoService;
import com.kubrik.mex.events.EventBus;
import com.mongodb.MongoCommandException;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.Objects;

/**
 * v2.4 SAFE-OPS-* / AUD-* — central dispatcher for destructive {@link Command}s.
 * Enforces the three safety gates in order:
 *
 * <ol>
 *   <li>{@link KillSwitch#requireDisengaged} — aborts with {@code CANCELLED}
 *       when the switch is engaged, still writing an audit row.</li>
 *   <li>Role gate via {@link RoleProbeService#currentOrProbe} — aborts with
 *       {@code FAIL} when the user's role set does not include any of the
 *       command's {@link Command#requiredRoles()}.</li>
 *   <li>{@link PreviewHashChecker} — re-renders the command and checks the
 *       supplied preview hash is byte-equal. Aborts with {@code FAIL} on
 *       mismatch (guards against mid-dialog tampering).</li>
 * </ol>
 *
 * <p>All outcomes — including cancellations — produce an {@code ops_audit}
 * row and an {@link EventBus#publishOpsAudit} event. The caller receives a
 * {@link Result} describing what happened so the UI can render a toast or
 * inline error without re-parsing the audit row.</p>
 */
public final class OpsExecutor {

    private static final Logger log = LoggerFactory.getLogger(OpsExecutor.class);

    private final ConnectionManager connManager;
    private final OpsAuditDao audit;
    private final EventBus bus;
    private final KillSwitch killSwitch;
    private final RoleProbeService roleProbe;
    private final Clock clock;

    public OpsExecutor(ConnectionManager connManager, OpsAuditDao audit, EventBus bus,
                       KillSwitch killSwitch, RoleProbeService roleProbe, Clock clock) {
        this.connManager = Objects.requireNonNull(connManager);
        this.audit = Objects.requireNonNull(audit);
        this.bus = Objects.requireNonNull(bus);
        this.killSwitch = Objects.requireNonNull(killSwitch);
        this.roleProbe = Objects.requireNonNull(roleProbe);
        this.clock = clock == null ? Clock.systemUTC() : clock;
    }

    public Result execute(String connectionId, Command cmd, DryRunResult preview,
                          boolean paste, String callerUser, String callerHost) {
        Objects.requireNonNull(connectionId);
        Objects.requireNonNull(cmd);
        Objects.requireNonNull(preview);
        long startedAt = clock.millis();

        // Gate 1: kill-switch
        if (killSwitch.isEngaged()) {
            return recordAndReturn(connectionId, cmd, preview, paste, callerUser, callerHost,
                    Outcome.CANCELLED, "kill_switch_engaged", null, startedAt, startedAt, true);
        }

        // Gate 2: role
        RoleSet roles = roleProbe.currentOrProbe(connectionId);
        if (!roles.allows(cmd)) {
            return recordAndReturn(connectionId, cmd, preview, paste, callerUser, callerHost,
                    Outcome.FAIL, "role_denied: needed any of " + cmd.requiredRoles(),
                    null, startedAt, clock.millis(), false);
        }

        // Gate 3: preview hash integrity. Recompute over the canonical-JSON the
        // dispatcher is about to send (via PreviewHashChecker) and confirm it
        // matches what the user saw on the approval screen.
        try {
            PreviewHashChecker.requireMatch(preview.commandBson(), preview.previewHash());
        } catch (PreviewHashChecker.PreviewTamperedException e) {
            return recordAndReturn(connectionId, cmd, preview, paste, callerUser, callerHost,
                    Outcome.FAIL, "preview_tampered", null, startedAt, clock.millis(), false);
        }

        MongoService svc = connManager.service(connectionId);
        if (svc == null) {
            return recordAndReturn(connectionId, cmd, preview, paste, callerUser, callerHost,
                    Outcome.FAIL, "not_connected", null, startedAt, clock.millis(), false);
        }

        // Dispatch.
        try {
            Document reply = dispatch(svc, cmd, preview);
            String msg = reply == null ? "ok" : reply.toJson();
            String roleUsed = pickRoleUsed(roles, cmd);
            return recordAndReturn(connectionId, cmd, preview, paste, callerUser, callerHost,
                    Outcome.OK, msg, roleUsed, startedAt, clock.millis(), false);
        } catch (MongoCommandException mce) {
            if (mentionsRole(mce.getMessage())) {
                roleProbe.invalidate(connectionId);
            }
            return recordAndReturn(connectionId, cmd, preview, paste, callerUser, callerHost,
                    Outcome.FAIL, mce.getErrorMessage(), null, startedAt, clock.millis(), false);
        } catch (Exception e) {
            log.warn("dispatch failed for {} against {}", cmd.name(), connectionId, e);
            return recordAndReturn(connectionId, cmd, preview, paste, callerUser, callerHost,
                    Outcome.FAIL, String.valueOf(e.getMessage()), null, startedAt, clock.millis(), false);
        }
    }

    /** Subset of {@link Command} currently wired through to MongoDB. Tag-range
     *  variants land with Q2.4-G part 3 once the zones pane is in place. */
    private static Document dispatch(MongoService svc, Command cmd, DryRunResult preview) {
        return switch (cmd) {
            case Command.KillOp k        -> svc.database("admin").runCommand(preview.commandBson());
            case Command.Freeze f        -> svc.database("admin").runCommand(preview.commandBson());
            case Command.StepDown s      -> svc.database("admin").runCommand(preview.commandBson());
            case Command.BalancerStart b -> svc.database("admin").runCommand(preview.commandBson());
            case Command.BalancerStop  b -> svc.database("admin").runCommand(preview.commandBson());
            case Command.BalancerWindow w -> dispatchBalancerWindow(svc, w);
            case Command.MoveChunk m     -> svc.database("admin").runCommand(preview.commandBson());
            case Command.AddTagRange a   -> svc.database("admin").runCommand(preview.commandBson());
            case Command.RemoveTagRange r -> svc.database("admin").runCommand(preview.commandBson());
            default -> throw new UnsupportedOperationException(
                    "Dispatch for " + cmd.name() + " lands with a later Q2.4 phase.");
        };
    }

    /** Balancer window is an upsert on {@code config.settings}, not an admin
     *  command — {@code sh.updateBalancerWindow()} under the hood. The return
     *  document mirrors the driver's UpdateResult so the audit trail carries
     *  the modified / upserted counts instead of a terse "ok". */
    private static Document dispatchBalancerWindow(MongoService svc, Command.BalancerWindow w) {
        Document filter = new Document("_id", "balancer");
        Document update = new Document("$set", new Document("activeWindow",
                new Document("start", w.startHhmm()).append("stop", w.stopHhmm())));
        com.mongodb.client.result.UpdateResult res = svc.database("config")
                .getCollection("settings").updateOne(filter, update,
                        new com.mongodb.client.model.UpdateOptions().upsert(true));
        return new Document("matched", res.getMatchedCount())
                .append("modified", res.getModifiedCount())
                .append("upsertedId", res.getUpsertedId());
    }

    private Result recordAndReturn(String connectionId, Command cmd, DryRunResult preview,
                                   boolean paste, String callerUser, String callerHost,
                                   Outcome outcome, String serverMessage, String roleUsed,
                                   long startedAt, long finishedAt, boolean killSwitchEngaged) {
        OpsAuditRecord row = new OpsAuditRecord(
                -1L, connectionId, null, null,
                cmd.name(), preview.commandJson(), preview.previewHash(),
                outcome, serverMessage, roleUsed,
                startedAt, finishedAt, Math.max(0, finishedAt - startedAt),
                callerHost, callerUser, cmd.uiSource(), paste, killSwitchEngaged);
        OpsAuditRecord saved = audit.insert(row);
        bus.publishOpsAudit(saved);
        return new Result(outcome, serverMessage, saved);
    }

    private static String pickRoleUsed(RoleSet roles, Command cmd) {
        for (String r : cmd.requiredRoles()) if (roles.hasRole(r)) return r;
        return null;
    }

    private static boolean mentionsRole(String msg) {
        if (msg == null) return false;
        String low = msg.toLowerCase();
        return low.contains("unauthorized") || low.contains("not authorized")
                || low.contains("role");
    }

    public record Result(Outcome outcome, String serverMessage, OpsAuditRecord audit) {
        public boolean ok() { return outcome == Outcome.OK; }
    }
}

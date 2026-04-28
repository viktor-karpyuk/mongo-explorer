package com.kubrik.mex.cluster.dryrun;

import com.kubrik.mex.cluster.safety.Command;
import com.kubrik.mex.cluster.safety.DryRunResult;
import org.bson.Document;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * v2.4 SAFE-OPS-1..4 — central dispatcher that turns a {@link Command} into a
 * preview-safe {@link DryRunResult}. Rendering is pure: no I/O, no clock, no
 * random. Adding a new command kind means (a) adding a new
 * {@link Command} variant and (b) extending the {@code switch} below — the
 * compiler flags any missing case since {@link Command} is sealed.
 */
public final class DryRunRenderer {

    private DryRunRenderer() {}

    public static DryRunResult render(Command cmd) {
        return switch (cmd) {
            case Command.StepDown s        -> renderStepDown(s);
            case Command.Freeze f          -> renderFreeze(f);
            case Command.KillOp k          -> renderKillOp(k);
            case Command.MoveChunk m       -> renderMoveChunk(m);
            case Command.BalancerStart b   -> renderBalancer(b.clusterName(), true);
            case Command.BalancerStop b    -> renderBalancer(b.clusterName(), false);
            case Command.BalancerWindow w  -> renderBalancerWindow(w);
            case Command.AddTagRange a     -> renderTagRange(a.ns(), a.min(), a.max(), a.zone());
            case Command.RemoveTagRange r  -> renderRemoveTagRange(r.ns(), r.min(), r.max());
            case Command.KillSwitchToggle k-> renderKillSwitch(k.engaging());
        };
    }

    /* ================== renderers ================== */

    private static DryRunResult renderStepDown(Command.StepDown s) {
        Map<String, Object> body = ordered();
        body.put("replSetStepDown", s.stepDownSecs());
        body.put("secondaryCatchUpPeriodSecs", s.catchUpSecs());
        String summary = String.format(
                "Step down primary %s for %d seconds.", s.host(), s.stepDownSecs());
        String predicted = String.format(
                "Primary %s will step down for %d seconds. A secondary is expected to take over within 5–15 seconds.",
                s.host(), s.stepDownSecs());
        return finalise(s.name(), body, summary, predicted);
    }

    private static DryRunResult renderFreeze(Command.Freeze f) {
        Map<String, Object> body = ordered();
        body.put("replSetFreeze", f.seconds());
        String summary = f.seconds() == 0
                ? String.format("Unfreeze %s.", f.host())
                : String.format("Freeze %s for %d seconds.", f.host(), f.seconds());
        String predicted = f.seconds() == 0
                ? String.format("%s becomes eligible for election again.", f.host())
                : String.format("%s will refuse to become primary for %d seconds.", f.host(), f.seconds());
        return finalise(f.name(), body, summary, predicted);
    }

    private static DryRunResult renderKillOp(Command.KillOp k) {
        Map<String, Object> body = ordered();
        body.put("killOp", 1);
        body.put("op", k.opid());
        String summary = String.format("Terminate op %d on %s.", k.opid(), k.host());
        String predicted = String.format(
                "Terminates the current operation on %s. Committed work is retained.", k.host());
        return finalise(k.name(), body, summary, predicted);
    }

    private static DryRunResult renderMoveChunk(Command.MoveChunk m) {
        Map<String, Object> body = ordered();
        body.put("moveChunk", m.ns());
        body.put("bounds", List.of(m.boundsMin(), m.boundsMax()));
        body.put("to", m.toShard());
        body.put("_waitForDelete", m.waitForDelete());
        body.put("writeConcern", Map.of("w", m.writeConcern()));
        String summary = String.format("Move chunk in %s to shard %s.", m.ns(), m.toShard());
        String predicted = String.format(
                "Migrates one chunk of %s to shard %s. Write concern: %s.",
                m.ns(), m.toShard(), m.writeConcern());
        return finalise(m.name(), body, summary, predicted);
    }

    private static DryRunResult renderBalancer(String clusterName, boolean starting) {
        // Real wire commands: {balancerStart: 1} / {balancerStop: 1} on admin (4.4+).
        // Using them in the preview means OpsExecutor can forward commandBson() as-is.
        Map<String, Object> body = ordered();
        body.put(starting ? "balancerStart" : "balancerStop", 1);
        String summary = starting
                ? String.format("Start balancer on %s.", clusterName)
                : String.format("Stop balancer on %s.", clusterName);
        String predicted = starting
                ? "Balancer resumes migrations on the configured window."
                : "Balancer halts migrations; existing in-flight migrations continue until completion.";
        String name = starting ? "sh.startBalancer" : "sh.stopBalancer";
        return finalise(name, body, summary, predicted);
    }

    private static DryRunResult renderBalancerWindow(Command.BalancerWindow w) {
        Map<String, Object> body = ordered();
        Map<String, Object> settings = ordered();
        Map<String, Object> activeWindow = ordered();
        activeWindow.put("start", w.startHhmm());
        activeWindow.put("stop",  w.stopHhmm());
        settings.put("activeWindow", activeWindow);
        body.put("update",   "config.settings");
        body.put("filter",   Map.of("_id", "balancer"));
        body.put("update_spec", Map.of("$set", settings));
        String summary = String.format("Set balancer window to %s → %s UTC.", w.startHhmm(), w.stopHhmm());
        String predicted = String.format(
                "Balancer runs between %s UTC and %s UTC only.", w.startHhmm(), w.stopHhmm());
        return finalise(w.name(), body, summary, predicted);
    }

    private static DryRunResult renderTagRange(String ns, Map<String, Object> min,
                                               Map<String, Object> max, String zone) {
        // Real wire command: admin.{updateZoneKeyRange: ns, min, max, zone}.
        // Matching the server shape means OpsExecutor forwards commandBson() as-is.
        Map<String, Object> body = ordered();
        body.put("updateZoneKeyRange", ns);
        body.put("min",  min);
        body.put("max",  max);
        body.put("zone", zone);
        String summary = String.format("Add tag range [%s, %s) → zone %s on %s.", min, max, zone, ns);
        String predicted = String.format(
                "Documents in %s with shard-key in [%s, %s) are constrained to zone %s.",
                ns, min, max, zone);
        return finalise("sh.addTagRange", body, summary, predicted);
    }

    private static DryRunResult renderRemoveTagRange(String ns, Map<String, Object> min,
                                                     Map<String, Object> max) {
        // Removing a zone key range is the same admin command with zone = null.
        Map<String, Object> body = ordered();
        body.put("updateZoneKeyRange", ns);
        body.put("min", min);
        body.put("max", max);
        body.put("zone", null);
        String summary = String.format("Remove tag range [%s, %s) on %s.", min, max, ns);
        String predicted = String.format(
                "Documents in %s with shard-key in [%s, %s) are no longer zone-constrained.", ns, min, max);
        return finalise("sh.removeTagRange", body, summary, predicted);
    }

    private static DryRunResult renderKillSwitch(boolean engaging) {
        Map<String, Object> body = ordered();
        body.put("toggle", engaging ? "engage" : "disengage");
        String summary = engaging
                ? "Engage kill-switch — hides every destructive button."
                : "Disengage kill-switch — destructive actions become available.";
        String predicted = engaging
                ? "Every destructive dispatcher rejects until the kill-switch is flipped off."
                : "Dispatchers re-enabled; individual role + typed-confirm gates still apply.";
        return finalise("ui.kill_switch", body, summary, predicted);
    }

    /* ================== shared finalise ================== */

    private static DryRunResult finalise(String name, Map<String, Object> body,
                                         String summary, String predicted) {
        String json = CommandJson.render(body);
        String hash = sha256(json);
        Document bson = new Document(body);
        return new DryRunResult(name, bson, json, summary, predicted, hash);
    }

    /** Order-preserving backing map; canonical ordering is applied at render time. */
    private static Map<String, Object> ordered() { return new LinkedHashMap<>(); }

    public static String sha256(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(digest.length * 2);
            for (byte b : digest) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }
}

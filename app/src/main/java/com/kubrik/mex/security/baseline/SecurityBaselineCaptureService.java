package com.kubrik.mex.security.baseline;

import com.kubrik.mex.core.MongoService;
import com.kubrik.mex.security.access.UsersRolesFetcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * v2.6 Q2.6-B4 — captures a {@link SecurityBaseline} for a connection and
 * persists it via {@link SecurityBaselineDao}. Fed by the RoleMatrixPane's
 * Capture baseline action; also callable from a headless CLI entry (lands
 * with Q2.6-K).
 *
 * <p>Deliberately tiny: the fetcher + DAO do the work. This service owns
 * the capture contract — what goes into the snapshot payload, and in what
 * shape — so future phases (drift engine, CIS scan) read one authoritative
 * structure.</p>
 */
public final class SecurityBaselineCaptureService {

    private static final Logger log = LoggerFactory.getLogger(SecurityBaselineCaptureService.class);

    private final UsersRolesFetcher fetcher;
    private final SecurityBaselineDao dao;
    private final Clock clock;

    public SecurityBaselineCaptureService(UsersRolesFetcher fetcher,
                                           SecurityBaselineDao dao,
                                           Clock clock) {
        this.fetcher = fetcher;
        this.dao = dao;
        this.clock = clock == null ? Clock.systemUTC() : clock;
    }

    public record Result(long baselineId, String sha256, int userCount, int roleCount) {}

    /**
     * Snapshots the current users + roles for {@code connectionId} and
     * writes one {@code sec_baselines} row.
     *
     * @param svc           admin-connected MongoService for the target
     * @param connectionId  owning connection id (foreign key in the
     *                      {@code sec_baselines} row)
     * @param capturedBy    caller identity (shown in history + drift
     *                      ack panel)
     * @param notes         free-text (e.g., "pre-upgrade snapshot")
     */
    public Result capture(MongoService svc, String connectionId,
                          String capturedBy, String notes) {
        UsersRolesFetcher.Snapshot snap = fetcher.fetch(svc,
                UsersRolesFetcher.FetchOptions.forBaseline());
        return persist(connectionId, capturedBy, notes, snap);
    }

    /** Split out for testability — callers with a pre-built snapshot
     *  (fixture tests, the drift-driven "recapture for comparison" flow)
     *  skip the fetch hop and hand the snapshot in directly. */
    public Result persist(String connectionId, String capturedBy, String notes,
                          UsersRolesFetcher.Snapshot snap) {
        Map<String, Object> payload = toPayload(snap);
        SecurityBaseline baseline = new SecurityBaseline(
                SecurityBaseline.CURRENT_VERSION, connectionId,
                clock.millis(), capturedBy == null ? "" : capturedBy,
                notes == null ? "" : notes, payload);
        SecurityBaselineDao.Row row = dao.insert(baseline);
        int userCount = snap == null ? 0 : snap.users().size();
        int roleCount = snap == null ? 0 : snap.roles().size();
        log.info("sec baseline captured for {} — users={} roles={} sha256={}",
                connectionId, userCount, roleCount, row.sha256());
        return new Result(row.id(), row.sha256(), userCount, roleCount);
    }

    /* ============================= payload ============================== */

    /**
     * Canonical snapshot payload. Every field deserves a fixed key the
     * drift engine can path into ({@code users.dba@admin.roleBindings}).
     * Ordered maps are fine — {@link com.kubrik.mex.cluster.dryrun.CommandJson}
     * sorts keys at render time.
     */
    static Map<String, Object> toPayload(UsersRolesFetcher.Snapshot snap) {
        if (snap == null) return Map.of("users", Map.of(), "roles", Map.of());
        Map<String, Object> users = new LinkedHashMap<>();
        snap.users().forEach(u -> users.put(u.fullyQualified(), renderUser(u)));
        Map<String, Object> roles = new LinkedHashMap<>();
        snap.roles().forEach(r -> roles.put(r.fullyQualified(), renderRole(r)));
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("users", users);
        out.put("roles", roles);
        return out;
    }

    private static Map<String, Object> renderUser(com.kubrik.mex.security.access.UserRecord u) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("db", u.db());
        out.put("user", u.user());
        out.put("roleBindings", u.roleBindings().stream()
                .map(b -> Map.of("role", b.role(), "db", b.db())).toList());
        out.put("inheritedPrivileges", u.inheritedPrivileges().stream()
                .map(SecurityBaselineCaptureService::renderPrivilege).toList());
        out.put("authenticationRestrictions", u.authenticationRestrictions().stream()
                .map(r -> Map.of(
                        "clientSource",  r.clientSource(),
                        "serverAddress", r.serverAddress())).toList());
        return out;
    }

    private static Map<String, Object> renderRole(com.kubrik.mex.security.access.RoleRecord r) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("db", r.db());
        out.put("role", r.role());
        out.put("builtin", r.builtin());
        out.put("directRoles", r.directRoles().stream()
                .map(b -> Map.of("role", b.role(), "db", b.db())).toList());
        out.put("directPrivileges", r.directPrivileges().stream()
                .map(SecurityBaselineCaptureService::renderPrivilege).toList());
        out.put("inheritedRoles", r.inheritedRoles().stream()
                .map(b -> Map.of("role", b.role(), "db", b.db())).toList());
        out.put("inheritedPrivileges", r.inheritedPrivileges().stream()
                .map(SecurityBaselineCaptureService::renderPrivilege).toList());
        return out;
    }

    private static Map<String, Object> renderPrivilege(com.kubrik.mex.security.access.Privilege p) {
        var res = p.resource();
        Map<String, Object> resource;
        if (res.cluster()) resource = Map.of("cluster", true);
        else if (res.anyResource()) resource = Map.of("anyResource", true);
        else resource = Map.of("db", res.db(), "collection", res.collection());
        return Map.of("resource", resource, "actions", p.actions());
    }

    /** Helper for tests that want to assert payload shape without a live
     *  capture. Mirrors {@link SecurityBaselineCaptureService#toPayload}. */
    public static List<String> fieldsCapturedPerUser() {
        return List.of("db", "user", "roleBindings", "inheritedPrivileges",
                "authenticationRestrictions");
    }
}

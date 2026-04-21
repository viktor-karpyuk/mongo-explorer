package com.kubrik.mex.security.access;

import com.kubrik.mex.core.MongoService;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * v2.6 Q2.6-B1 — reads {@code usersInfo} + {@code rolesInfo} off an
 * admin-connected {@link MongoService} and returns sanitised
 * {@link UserRecord} / {@link RoleRecord} lists.
 *
 * <p>Sanitisation rules:
 * <ul>
 *   <li>{@code credentials} (SCRAM-SHA-1 / SCRAM-SHA-256 password hashes)
 *       are never fetched — we pass {@code showCredentials: false}
 *       explicitly. Older servers default-exclude them; this makes it
 *       explicit across 4.x → 7.x.</li>
 *   <li>{@code customData} is skipped because operators sometimes stash
 *       secrets there; the UI doesn't need it to render the matrix.</li>
 *   <li>Built-in roles (name is a fixed list; we also trust the server's
 *       {@code isBuiltin} flag) are excluded from baseline captures but
 *       fetched in full for matrix rendering — gated by the
 *       {@link FetchOptions#includeBuiltinRoles} flag.</li>
 * </ul>
 */
public final class UsersRolesFetcher {

    private static final Logger log = LoggerFactory.getLogger(UsersRolesFetcher.class);

    public record FetchOptions(boolean includeBuiltinRoles) {
        public static FetchOptions forMatrix()   { return new FetchOptions(true); }
        public static FetchOptions forBaseline() { return new FetchOptions(false); }
    }

    public record Snapshot(List<UserRecord> users, List<RoleRecord> roles) {
        public Snapshot {
            users = users == null ? List.of() : List.copyOf(users);
            roles = roles == null ? List.of() : List.copyOf(roles);
        }
    }

    public Snapshot fetch(MongoService svc, FetchOptions opts) {
        Document usersReply = runSafe(svc, new Document("usersInfo", 1)
                .append("forAllDBs", true)
                .append("showPrivileges", true)
                .append("showCredentials", false));
        Document rolesReply = runSafe(svc, new Document("rolesInfo", 1)
                .append("showPrivileges", true)
                .append("showBuiltinRoles", opts.includeBuiltinRoles()));
        return parse(usersReply, rolesReply);
    }

    /** Package-private parser used by both {@link #fetch} and the unit
     *  tests that feed fixture reply documents without a live server. */
    static Snapshot parse(Document usersReply, Document rolesReply) {
        List<UserRecord> users = parseUsers(usersReply);
        List<RoleRecord> roles = parseRoles(rolesReply);
        return new Snapshot(users, roles);
    }

    /* ============================= users ================================= */

    private static List<UserRecord> parseUsers(Document reply) {
        if (reply == null) return List.of();
        List<Document> raw = reply.getList("users", Document.class, List.of());
        List<UserRecord> out = new ArrayList<>(raw.size());
        for (Document u : raw) out.add(parseUser(u));
        return out;
    }

    private static UserRecord parseUser(Document u) {
        String db = u.getString("db");
        String user = u.getString("user");
        List<RoleBinding> roles = parseBindings(u.getList("roles", Document.class, List.of()));
        List<Privilege> privs = parsePrivileges(
                u.getList("inheritedPrivileges", Document.class, List.of()));
        List<AuthenticationRestriction> restrictions = parseRestrictions(
                u.getList("authenticationRestrictions", Document.class, List.of()));
        return new UserRecord(db, user, roles, privs, restrictions);
    }

    /* ============================= roles ================================= */

    private static List<RoleRecord> parseRoles(Document reply) {
        if (reply == null) return List.of();
        List<Document> raw = reply.getList("roles", Document.class, List.of());
        List<RoleRecord> out = new ArrayList<>(raw.size());
        for (Document r : raw) out.add(parseRole(r));
        return out;
    }

    private static RoleRecord parseRole(Document r) {
        String db = r.getString("db");
        String role = r.getString("role");
        List<RoleBinding> direct = parseBindings(r.getList("roles", Document.class, List.of()));
        List<RoleBinding> inheritedR = parseBindings(
                r.getList("inheritedRoles", Document.class, List.of()));
        List<Privilege> directP = parsePrivileges(
                r.getList("privileges", Document.class, List.of()));
        List<Privilege> inheritedP = parsePrivileges(
                r.getList("inheritedPrivileges", Document.class, List.of()));
        boolean builtin = Boolean.TRUE.equals(r.getBoolean("isBuiltin"));
        return new RoleRecord(db, role, direct, directP, inheritedR, inheritedP, builtin);
    }

    /* ============================= shared ================================ */

    private static List<RoleBinding> parseBindings(List<Document> raw) {
        List<RoleBinding> out = new ArrayList<>(raw.size());
        for (Document d : raw) {
            String role = d.getString("role");
            String db = d.getString("db");
            if (role != null) out.add(new RoleBinding(role, db));
        }
        return out;
    }

    private static List<Privilege> parsePrivileges(List<Document> raw) {
        List<Privilege> out = new ArrayList<>(raw.size());
        for (Document d : raw) {
            Document resource = d.get("resource", Document.class);
            if (resource == null) continue;
            Privilege.Resource r;
            if (Boolean.TRUE.equals(resource.getBoolean("cluster"))) {
                r = Privilege.Resource.ofCluster();
            } else if (Boolean.TRUE.equals(resource.getBoolean("anyResource"))) {
                r = Privilege.Resource.ofAnyResource();
            } else {
                r = Privilege.Resource.ofNamespace(
                        safeString(resource.getString("db")),
                        safeString(resource.getString("collection")));
            }
            List<String> actions = d.getList("actions", String.class, List.of());
            out.add(new Privilege(r, actions));
        }
        return out;
    }

    private static List<AuthenticationRestriction> parseRestrictions(List<Document> raw) {
        List<AuthenticationRestriction> out = new ArrayList<>(raw.size());
        for (Document d : raw) {
            List<String> cs = d.getList("clientSource", String.class, List.of());
            List<String> sa = d.getList("serverAddress", String.class, List.of());
            out.add(new AuthenticationRestriction(cs, sa));
        }
        return out;
    }

    private static String safeString(String s) { return s == null ? "" : s; }

    private static Document runSafe(MongoService svc, Document cmd) {
        try {
            return svc.database("admin").runCommand(cmd);
        } catch (Exception e) {
            log.debug("{} failed: {}", cmd.keySet().iterator().next(), e.getMessage());
            return null;
        }
    }
}

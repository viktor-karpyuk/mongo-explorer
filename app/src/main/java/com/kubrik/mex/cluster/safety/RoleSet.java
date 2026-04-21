package com.kubrik.mex.cluster.safety;

import org.bson.Document;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * v2.4 ROLE-1..4 — parsed view of {@code connectionStatus({showPrivileges:true})}.
 * Roles are normalised to their bare name ({@code "clusterManager"}, {@code "root"}, ...);
 * the {@code db} they're held on is ignored for the purposes of the gate —
 * anything on {@code admin} is enough in MongoDB's role model.
 */
public final class RoleSet {

    public static final RoleSet EMPTY = new RoleSet(Set.of());

    private final Set<String> roles;

    public RoleSet(Set<String> roles) {
        this.roles = roles == null ? Set.of() : Set.copyOf(roles);
    }

    public Set<String> roles() { return roles; }

    /** Case-sensitive role name lookup. */
    public boolean hasRole(String name) { return roles.contains(name); }

    /** True iff the holder has any of the supplied roles. Empty list → false. */
    public boolean hasAny(List<String> names) {
        if (names == null || names.isEmpty()) return false;
        for (String n : names) if (roles.contains(n)) return true;
        return false;
    }

    /** True iff a destructive {@link Command} is allowed by the current role set. */
    public boolean allows(Command cmd) {
        List<String> required = cmd.requiredRoles();
        if (required.isEmpty()) return true;
        return hasAny(required);
    }

    /**
     * Parse the Mongo {@code connectionStatus} reply. Accepts the 4.0..7.0
     * shape where {@code authInfo.authenticatedUserRoles} is a list of
     * {@code {role, db}} documents. Unknown / missing → {@link #EMPTY}.
     */
    public static RoleSet parse(Document connectionStatusReply) {
        if (connectionStatusReply == null) return EMPTY;
        Document authInfo = asDoc(connectionStatusReply.get("authInfo"));
        if (authInfo.isEmpty()) return EMPTY;
        Object rolesObj = authInfo.get("authenticatedUserRoles");
        if (!(rolesObj instanceof Iterable<?> it)) return EMPTY;
        Set<String> out = new HashSet<>();
        for (Object o : it) {
            Document d = asDoc(o);
            String name = d.getString("role");
            if (name != null && !name.isBlank()) out.add(name);
        }
        return new RoleSet(out);
    }

    private static Document asDoc(Object o) {
        if (o instanceof Document d) return d;
        return new Document();
    }

    @Override
    public String toString() { return "RoleSet" + roles; }
}

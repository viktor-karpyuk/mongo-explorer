package com.kubrik.mex.security.access;

import java.util.List;
import java.util.Objects;

/**
 * v2.6 Q2.6-B1 — captures the subset of {@code rolesInfo} that matters to
 * the security surface. Built-in roles (read, readWrite, dbAdmin, root, …)
 * can be fetched by the {@link UsersRolesFetcher} on demand; the baseline
 * capture pass skips them so the snapshot only reflects operator-defined
 * roles that actually drift.
 *
 * @param db                role database
 * @param role              role name
 * @param directRoles       roles this role inherits from directly
 * @param directPrivileges  privileges declared on this role
 * @param inheritedRoles    full transitive set of roles (direct + indirect)
 * @param inheritedPrivileges flattened privilege list post-inheritance
 * @param builtin           true when this is a MongoDB built-in role
 */
public record RoleRecord(
        String db,
        String role,
        List<RoleBinding> directRoles,
        List<Privilege> directPrivileges,
        List<RoleBinding> inheritedRoles,
        List<Privilege> inheritedPrivileges,
        boolean builtin
) {
    public RoleRecord {
        Objects.requireNonNull(db, "db");
        Objects.requireNonNull(role, "role");
        directRoles = directRoles == null ? List.of() : List.copyOf(directRoles);
        directPrivileges = directPrivileges == null ? List.of() : List.copyOf(directPrivileges);
        inheritedRoles = inheritedRoles == null ? List.of() : List.copyOf(inheritedRoles);
        inheritedPrivileges = inheritedPrivileges == null ? List.of() : List.copyOf(inheritedPrivileges);
    }

    public String fullyQualified() { return role + "@" + db; }
}

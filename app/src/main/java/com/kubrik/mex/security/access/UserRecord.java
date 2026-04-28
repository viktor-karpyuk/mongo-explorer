package com.kubrik.mex.security.access;

import java.util.List;
import java.util.Objects;

/**
 * v2.6 Q2.6-B1 — captures the subset of {@code usersInfo} that matters to
 * the security surface. Credential fields (password hashes, SCRAM salts,
 * LDAP DN passwords) are deliberately <em>not</em> fetched — the
 * {@link UsersRolesFetcher} builds this record from a sanitised reply
 * tree.
 *
 * @param db               authentication database
 * @param user             username
 * @param roleBindings     roles granted directly to the user
 * @param inheritedPrivileges privilege tuples produced after role
 *                            inheritance (flattened — the matrix pane
 *                            does not need to walk the hierarchy)
 * @param authenticationRestrictions  structured restrictions (client-source /
 *                            server-address IP allow-lists); never a secret
 */
public record UserRecord(
        String db,
        String user,
        List<RoleBinding> roleBindings,
        List<Privilege> inheritedPrivileges,
        List<AuthenticationRestriction> authenticationRestrictions
) {
    public UserRecord {
        Objects.requireNonNull(db, "db");
        Objects.requireNonNull(user, "user");
        roleBindings = roleBindings == null ? List.of() : List.copyOf(roleBindings);
        inheritedPrivileges = inheritedPrivileges == null ? List.of() : List.copyOf(inheritedPrivileges);
        authenticationRestrictions = authenticationRestrictions == null
                ? List.of() : List.copyOf(authenticationRestrictions);
    }

    public String fullyQualified() { return user + "@" + db; }
}

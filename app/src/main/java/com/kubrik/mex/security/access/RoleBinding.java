package com.kubrik.mex.security.access;

/** v2.6 Q2.6-B1 — {@code {role, db}} tuple as it appears in usersInfo /
 *  rolesInfo replies. Keeping this as its own record (rather than a
 *  {@code Pair}) makes the matrix pane's sorting + group-by read
 *  cleanly. */
public record RoleBinding(String role, String db) {
    public RoleBinding {
        if (role == null || role.isBlank()) throw new IllegalArgumentException("role");
        if (db == null) db = "";
    }

    public String fullyQualified() { return role + "@" + db; }
}

package com.kubrik.mex.security.access;

import javafx.beans.property.BooleanProperty;
import javafx.beans.property.SimpleBooleanProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.beans.property.StringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;

import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * v2.6 Q2.6-B2 — headless state for the Role Matrix pane. Keeping the
 * filtering + lookups in a plain JavaFX-property model means the pane
 * stays a thin view and tests can assert the matrix output without a
 * live FX runtime.
 *
 * <p>Two filters combine with AND semantics:
 * <ul>
 *   <li>{@link #textFilter} — case-insensitive substring match against
 *       a row's identity (user@db or role@db) plus the bound role names,
 *       so typing "read" surfaces users that have any role name
 *       containing it.</li>
 *   <li>{@link #includeBuiltinRoles} — hides MongoDB's built-in roles
 *       by default (noise for audit work) and shows them on demand for
 *       privilege-lookup questions.</li>
 * </ul>
 */
public final class RoleMatrixModel {

    private final ObservableList<UserRecord> users = FXCollections.observableArrayList();
    private final ObservableList<RoleRecord> roles = FXCollections.observableArrayList();

    private final StringProperty textFilter = new SimpleStringProperty("");
    private final BooleanProperty includeBuiltinRoles = new SimpleBooleanProperty(false);

    /** Replace the full data set. Callers typically invoke once per
     *  capture cycle; the matrix pane re-renders via the observable lists
     *  returned from {@link #filteredUsers()} / {@link #filteredRoles()}. */
    public void load(UsersRolesFetcher.Snapshot snapshot) {
        if (snapshot == null) {
            users.clear();
            roles.clear();
            return;
        }
        users.setAll(snapshot.users().stream()
                .sorted(Comparator.comparing(UserRecord::db).thenComparing(UserRecord::user))
                .toList());
        roles.setAll(snapshot.roles().stream()
                .sorted(Comparator.comparing(RoleRecord::db).thenComparing(RoleRecord::role))
                .toList());
    }

    public StringProperty textFilterProperty() { return textFilter; }
    public BooleanProperty includeBuiltinRolesProperty() { return includeBuiltinRoles; }

    /** Users matching the current filters, in a stable display order
     *  (db asc, user asc). Mutations to the underlying list don't cascade
     *  into this return value — callers observe
     *  {@link #textFilterProperty()} / {@link #includeBuiltinRolesProperty()}
     *  to re-query. */
    public List<UserRecord> filteredUsers() {
        String needle = normalise(textFilter.get());
        if (needle.isEmpty()) return List.copyOf(users);
        return users.stream().filter(u -> matchesUser(u, needle)).toList();
    }

    public List<RoleRecord> filteredRoles() {
        String needle = normalise(textFilter.get());
        boolean withBuiltins = includeBuiltinRoles.get();
        return roles.stream()
                .filter(r -> withBuiltins || !r.builtin())
                .filter(r -> needle.isEmpty() || matchesRole(r, needle))
                .toList();
    }

    /** Direct + transitively-inherited role bindings for a user, deduped
     *  by (role, db). Used by the user detail drawer (Q2.6-B3). */
    public List<RoleBinding> effectiveRoles(UserRecord user) {
        // usersInfo doesn't directly expose "inheritedRoles" on a user (it
        // gives inheritedPrivileges), so we walk from direct role bindings
        // through roles[].inheritedRoles to produce the effective set.
        java.util.LinkedHashMap<String, RoleBinding> effective = new java.util.LinkedHashMap<>();
        for (RoleBinding b : user.roleBindings()) {
            effective.putIfAbsent(b.fullyQualified(), b);
            roleLookup(b).ifPresent(r -> {
                for (RoleBinding inh : r.inheritedRoles()) {
                    effective.putIfAbsent(inh.fullyQualified(), inh);
                }
            });
        }
        return List.copyOf(effective.values());
    }

    /** Flattened privileges effective on {@code user} after role inheritance —
     *  the set {@code usersInfo} returned as {@code inheritedPrivileges}, kept
     *  in stable order keyed on rendered resource. */
    public List<Privilege> effectivePrivileges(UserRecord user) {
        return user.inheritedPrivileges().stream()
                .sorted(Comparator.comparing(p -> p.resource().render()))
                .toList();
    }

    /** Users grouped by authentication database, most populous first.
     *  Drives the left-hand "DBs" filter sidebar (future polish, already
     *  exposed as an unused helper so the pane can consume it later). */
    public Map<String, Long> usersByDb() {
        TreeMap<String, Long> out = new TreeMap<>();
        for (UserRecord u : users) out.merge(u.db(), 1L, Long::sum);
        return out;
    }

    public Optional<RoleRecord> roleLookup(RoleBinding binding) {
        return roles.stream()
                .filter(r -> r.role().equals(binding.role()) && r.db().equals(binding.db()))
                .findFirst();
    }

    /* ============================== helpers ============================== */

    private static String normalise(String s) {
        return s == null ? "" : s.trim().toLowerCase(Locale.ROOT);
    }

    private static boolean matchesUser(UserRecord u, String needle) {
        if (u.fullyQualified().toLowerCase(Locale.ROOT).contains(needle)) return true;
        for (RoleBinding b : u.roleBindings()) {
            if (b.role().toLowerCase(Locale.ROOT).contains(needle)) return true;
        }
        return false;
    }

    private static boolean matchesRole(RoleRecord r, String needle) {
        return r.fullyQualified().toLowerCase(Locale.ROOT).contains(needle);
    }
}

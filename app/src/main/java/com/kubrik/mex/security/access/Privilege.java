package com.kubrik.mex.security.access;

import java.util.List;
import java.util.Objects;

/**
 * v2.6 Q2.6-B1 — one entry in the {@code inheritedPrivileges} / {@code
 * privileges} arrays from {@code usersInfo} / {@code rolesInfo}.
 *
 * @param resource  scoped target: cluster, whole-db, or db.collection.
 *                  {@code null} fields inside {@link Resource} encode
 *                  the "any" wild-card as they appear in MongoDB's own
 *                  reply — we keep them as-is so the diff engine's
 *                  path-based compare matches the server shape.
 * @param actions   action names like {@code find}, {@code insert},
 *                  {@code createRole}. Sorted to keep the canonical
 *                  JSON stable.
 */
public record Privilege(Resource resource, List<String> actions) {
    public Privilege {
        Objects.requireNonNull(resource, "resource");
        actions = actions == null ? List.of() : List.copyOf(actions.stream().sorted().toList());
    }

    /** Encodes the three privilege-scope shapes MongoDB emits:
     *  <ul>
     *    <li>{@code cluster: true} — server-wide</li>
     *    <li>{@code {db, collection:""}} — whole DB</li>
     *    <li>{@code {db, collection}} — specific collection; either may be
     *         empty-string to mean "any"</li>
     *  </ul> */
    public record Resource(boolean cluster, String db, String collection, boolean anyResource) {
        public Resource {
            if (db == null) db = "";
            if (collection == null) collection = "";
        }

        public static Resource ofCluster()     { return new Resource(true, "", "", false); }
        public static Resource ofAnyResource() { return new Resource(false, "", "", true); }
        public static Resource ofDb(String db) { return new Resource(false, db, "", false); }
        public static Resource ofNamespace(String db, String coll) {
            return new Resource(false, db, coll, false);
        }

        public String render() {
            if (cluster) return "cluster";
            if (anyResource) return "<any>";
            if (collection.isEmpty() && db.isEmpty()) return "<any>";
            if (collection.isEmpty()) return db + ".*";
            return db + "." + collection;
        }
    }
}

package com.kubrik.mex.backup.spec;

import java.util.List;

/**
 * v2.5 BKP-POLICY-4 — scope selector for a backup policy. Sealed so the
 * validator + runner handle every kind; adding a new scope (e.g., matching
 * regex) means extending the permit list + updating both consumers.
 */
public sealed interface Scope {

    record WholeCluster() implements Scope {}

    record Databases(List<String> names) implements Scope {
        public Databases {
            if (names == null || names.isEmpty())
                throw new IllegalArgumentException("databases list");
            names = List.copyOf(names);
        }
    }

    record Namespaces(List<String> namespaces) implements Scope {
        public Namespaces {
            if (namespaces == null || namespaces.isEmpty())
                throw new IllegalArgumentException("namespaces list");
            for (String ns : namespaces) {
                if (ns == null || !ns.contains("."))
                    throw new IllegalArgumentException("expected db.collection, got: " + ns);
            }
            namespaces = List.copyOf(namespaces);
        }
    }

    /**
     * v2.6 Q2.6-L6 — fans a multi-entry scope out into one single-entry scope
     * per entry. mongodump accepts at most one {@code --db} (or db+coll) per
     * invocation, so the runner has to loop; this helper is the one authoritative
     * decomposition both the runner and its unit tests read.
     *
     * <p>Returns a one-element list for WholeCluster, Databases(1), and
     * Namespaces(1). Multi-entry variants expand to one Scope per entry.</p>
     */
    static List<Scope> fanOut(Scope scope) {
        return switch (scope) {
            case WholeCluster w -> List.of(w);
            case Databases d -> d.names().stream()
                    .map(name -> (Scope) new Databases(List.of(name)))
                    .toList();
            case Namespaces ns -> ns.namespaces().stream()
                    .map(n -> (Scope) new Namespaces(List.of(n)))
                    .toList();
        };
    }
}

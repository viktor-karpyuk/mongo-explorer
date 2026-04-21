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
}

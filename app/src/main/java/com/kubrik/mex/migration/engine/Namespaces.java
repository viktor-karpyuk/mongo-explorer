package com.kubrik.mex.migration.engine;

/** Utilities for MongoDB namespaces of the form {@code db.collection}. */
public final class Namespaces {

    private Namespaces() {}

    public record Ns(String db, String coll) {
        public String full() { return db + "." + coll; }
    }

    public static Ns parse(String ns) {
        if (ns == null) throw new IllegalArgumentException("namespace is null");
        int dot = ns.indexOf('.');
        if (dot <= 0 || dot == ns.length() - 1) {
            throw new IllegalArgumentException("invalid namespace: " + ns);
        }
        return new Ns(ns.substring(0, dot), ns.substring(dot + 1));
    }

    /** A namespace is system/internal if its DB or collection prefix is well-known-internal. */
    public static boolean isSystem(String ns) {
        Ns n = parse(ns);
        return n.db().equalsIgnoreCase("admin")
                || n.db().equalsIgnoreCase("local")
                || n.db().equalsIgnoreCase("config")
                || n.coll().startsWith("system.");
    }
}

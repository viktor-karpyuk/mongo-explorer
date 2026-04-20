package com.kubrik.mex.monitoring.util;

import org.bson.Document;

/**
 * Null-safe / type-tolerant getters over BSON {@link Document}s. MongoDB returns
 * numeric fields as {@code Integer}, {@code Long}, {@code Double}, or {@code Decimal128}
 * interchangeably across versions — these helpers normalise to {@code long} / {@code double}.
 */
public final class DocUtil {

    private DocUtil() {}

    public static Document sub(Document d, String field) {
        if (d == null) return new Document();
        Object v = d.get(field);
        return v instanceof Document s ? s : new Document();
    }

    public static long longVal(Document d, String field, long fallback) {
        if (d == null) return fallback;
        Object v = d.get(field);
        return switch (v) {
            case null -> fallback;
            case Number n -> n.longValue();
            case Boolean b -> b ? 1L : 0L;
            default -> fallback;
        };
    }

    public static double doubleVal(Document d, String field, double fallback) {
        if (d == null) return fallback;
        Object v = d.get(field);
        return switch (v) {
            case null -> fallback;
            case Number n -> n.doubleValue();
            case Boolean b -> b ? 1.0 : 0.0;
            default -> fallback;
        };
    }

    public static boolean boolVal(Document d, String field, boolean fallback) {
        if (d == null) return fallback;
        Object v = d.get(field);
        return switch (v) {
            case null -> fallback;
            case Boolean b -> b;
            case Number n -> n.doubleValue() != 0;
            default -> fallback;
        };
    }

    public static String stringVal(Document d, String field, String fallback) {
        if (d == null) return fallback;
        Object v = d.get(field);
        return v instanceof String s ? s : fallback;
    }
}

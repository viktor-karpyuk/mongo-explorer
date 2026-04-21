package com.kubrik.mex.cluster.ops;

import org.bson.Document;

/**
 * v2.4 OP-1..6 — view-model row for the currentOp table. The raw {@link Document}
 * is retained so the detail drawer can render the full operation JSON without a
 * second server round-trip.
 */
public record CurrentOpRow(
        long opid,
        String host,
        String ns,
        String op,
        long secsRunning,
        boolean waitingForLock,
        String waitingForLockReason,
        String client,
        String user,
        String comment,
        String planSummary,
        boolean active,
        Document raw
) {
    public static CurrentOpRow fromRaw(Document d) {
        long opid = switch (d.get("opid")) {
            case Number n -> n.longValue();
            case String s -> parseLong(s);
            case null, default -> 0L;
        };
        long secs = switch (d.get("secs_running")) {
            case Number n -> n.longValue();
            case null, default -> 0L;
        };
        String ns = d.getString("ns");
        String op = d.getString("op");
        String host = d.getString("host");
        String client = d.getString("client");
        if (client == null) client = d.getString("client_s");
        String user = firstUser(d);
        String comment = firstComment(d);
        String plan = d.getString("planSummary");
        boolean active = Boolean.TRUE.equals(d.getBoolean("active"));
        boolean waiting = Boolean.TRUE.equals(d.getBoolean("waitingForLock"));
        String waitingReason = null;
        if (waiting && d.get("lockStats") instanceof Document ls) {
            waitingReason = ls.keySet().stream().findFirst().orElse(null);
        }
        return new CurrentOpRow(opid, host == null ? "" : host, ns == null ? "" : ns,
                op == null ? "" : op, secs, waiting, waitingReason,
                client == null ? "" : client, user == null ? "" : user,
                comment == null ? "" : comment, plan == null ? "" : plan, active, d);
    }

    @SuppressWarnings("unchecked")
    private static String firstUser(Document d) {
        Object users = d.get("effectiveUsers");
        if (users instanceof java.util.List<?> l && !l.isEmpty() && l.get(0) instanceof Document u) {
            return u.getString("user");
        }
        return null;
    }

    private static String firstComment(Document d) {
        Object c = d.get("command");
        if (c instanceof Document cmd) {
            Object comment = cmd.get("$comment");
            if (comment instanceof String s) return s;
            comment = cmd.get("comment");
            if (comment instanceof String s) return s;
        }
        return null;
    }

    private static long parseLong(String s) {
        try { return Long.parseLong(s); }
        catch (NumberFormatException e) { return 0L; }
    }
}

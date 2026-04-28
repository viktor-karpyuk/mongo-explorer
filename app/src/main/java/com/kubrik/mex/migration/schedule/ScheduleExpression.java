package com.kubrik.mex.migration.schedule;

import java.time.Duration;

/**
 * Minimal schedule-expression parser for UX-7. v2.0.0 supports the interval
 * shorthand — {@code @every Ns|m|h|d} and three named shorthands ({@code @hourly},
 * {@code @daily}, {@code @weekly}). Full 5-field cron is deferred.
 *
 * <p>All computation is millisecond-precise, using the caller-supplied reference
 * timestamp so tests can be deterministic.
 */
public final class ScheduleExpression {

    /** Parse and return the next fire time relative to {@code nowMs}. Throws on unknown
     *  syntax — callers should surface the error to the user so they can correct it. */
    public static long nextAfter(String expression, long nowMs) {
        Duration interval = intervalOf(expression);
        return nowMs + interval.toMillis();
    }

    /** The effective interval an expression fires on. */
    public static Duration intervalOf(String expression) {
        if (expression == null) throw new IllegalArgumentException("null schedule expression");
        String s = expression.trim();
        if (s.isEmpty()) throw new IllegalArgumentException("empty schedule expression");
        return switch (s) {
            case "@hourly" -> Duration.ofHours(1);
            case "@daily"  -> Duration.ofDays(1);
            case "@weekly" -> Duration.ofDays(7);
            default -> parseEvery(s);
        };
    }

    private static Duration parseEvery(String s) {
        if (!s.startsWith("@every ")) {
            throw new IllegalArgumentException(
                    "unsupported schedule expression (v2.0 accepts @every N[smhd], @hourly, @daily, @weekly): " + s);
        }
        String body = s.substring("@every ".length()).trim();
        if (body.length() < 2) throw bad(s);
        char unit = body.charAt(body.length() - 1);
        long n;
        try { n = Long.parseLong(body.substring(0, body.length() - 1)); }
        catch (NumberFormatException nfe) { throw bad(s); }
        if (n <= 0) throw bad(s);
        return switch (unit) {
            case 's' -> Duration.ofSeconds(n);
            case 'm' -> Duration.ofMinutes(n);
            case 'h' -> Duration.ofHours(n);
            case 'd' -> Duration.ofDays(n);
            default  -> throw bad(s);
        };
    }

    private static IllegalArgumentException bad(String s) {
        return new IllegalArgumentException("invalid @every expression: " + s);
    }

    private ScheduleExpression() {}
}

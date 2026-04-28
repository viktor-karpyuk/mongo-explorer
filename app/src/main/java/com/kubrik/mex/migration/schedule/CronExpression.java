package com.kubrik.mex.migration.schedule;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/** Minimal 5-field cron parser: {@code minute hour day-of-month month day-of-week}. Supports
 *  {@code *}, literal integers, {@code a-b} ranges, {@code a,b,c} lists, {@code * / step}
 *  (zero-based from the field's lower bound), and {@code a-b / step}.
 *  <p>
 *  When both day-of-month and day-of-week are constrained the schedule fires on the OR of
 *  the two — that is, whenever either matches — matching the Vixie cron convention. When one
 *  is {@code *} and the other is a pattern, only the pattern applies (so {@code "0 2 * * 1"}
 *  fires at 02:00 on Mondays rather than every day at 02:00 and every Monday). */
public final class CronExpression {

    private final BitSet minute;   // 0..59
    private final BitSet hour;     // 0..23
    private final BitSet dom;      // 1..31
    private final BitSet month;    // 1..12
    private final BitSet dow;      // 0..6 (Sunday=0)
    private final boolean domStar;
    private final boolean dowStar;

    private CronExpression(BitSet minute, BitSet hour, BitSet dom, BitSet month, BitSet dow,
                           boolean domStar, boolean dowStar) {
        this.minute = minute;
        this.hour = hour;
        this.dom = dom;
        this.month = month;
        this.dow = dow;
        this.domStar = domStar;
        this.dowStar = dowStar;
    }

    public static CronExpression parse(String expr) {
        if (expr == null) throw new IllegalArgumentException("cron expression is null");
        String[] parts = expr.trim().split("\\s+");
        if (parts.length != 5) {
            throw new IllegalArgumentException(
                    "cron expression must have 5 fields (m h dom mon dow), got: " + expr);
        }
        BitSet minute = parseField(parts[0], 0, 59);
        BitSet hour   = parseField(parts[1], 0, 23);
        BitSet dom    = parseField(parts[2], 1, 31);
        BitSet month  = parseField(parts[3], 1, 12);
        BitSet dow    = parseField(parts[4], 0, 6);
        return new CronExpression(minute, hour, dom, month, dow,
                "*".equals(parts[2]), "*".equals(parts[4]));
    }

    /** Returns the earliest {@link Instant} strictly after {@code after} (in the given zone)
     *  whose local minute matches this expression. Seconds are discarded.
     *  <p>
     *  Walks minute-by-minute with an upper bound of ~4 years — guards against impossible
     *  schedules like {@code "0 0 31 2 *"} that never fire. */
    public Instant nextFireAfter(Instant after, ZoneId zone) {
        ZonedDateTime zdt = after.atZone(zone).withSecond(0).withNano(0).plusMinutes(1);
        final int maxSteps = 60 * 24 * 366 * 4;   // 4 years of minutes
        for (int i = 0; i < maxSteps; i++) {
            if (matches(zdt)) return zdt.toInstant();
            zdt = zdt.plusMinutes(1);
        }
        throw new IllegalStateException("cron expression has no match within 4 years — "
                + "impossible schedule?");
    }

    /** Does this cron expression match the given instant (interpreted in {@code zone})? */
    public boolean matches(Instant at, ZoneId zone) {
        return matches(at.atZone(zone).withSecond(0).withNano(0));
    }

    private boolean matches(ZonedDateTime zdt) {
        if (!minute.get(zdt.getMinute())) return false;
        if (!hour.get(zdt.getHour())) return false;
        if (!month.get(zdt.getMonthValue())) return false;
        int domVal = zdt.getDayOfMonth();
        // ZonedDateTime: MONDAY=1..SUNDAY=7. Vixie cron: SUNDAY=0..SATURDAY=6.
        int dowVal = zdt.getDayOfWeek().getValue() % 7;
        boolean domMatch = dom.get(domVal);
        boolean dowMatch = dow.get(dowVal);
        // Vixie: when both are restricted, OR them. When only one is restricted, AND them
        // (which collapses to "the restricted one matches" since the starred field is always
        // true).
        if (!domStar && !dowStar) return domMatch || dowMatch;
        return domMatch && dowMatch;
    }

    // --- field parsing -----------------------------------------------------------

    private static BitSet parseField(String field, int lo, int hi) {
        BitSet out = new BitSet(hi + 1);
        for (String term : field.split(",")) {
            parseTerm(term.trim(), lo, hi, out);
        }
        if (out.isEmpty()) {
            throw new IllegalArgumentException("cron field `" + field + "` matches no values");
        }
        return out;
    }

    private static void parseTerm(String term, int lo, int hi, BitSet out) {
        int slash = term.indexOf('/');
        int step = 1;
        String rangePart = term;
        if (slash >= 0) {
            try { step = Integer.parseInt(term.substring(slash + 1)); }
            catch (NumberFormatException e) {
                throw new IllegalArgumentException("invalid step in `" + term + "`");
            }
            if (step <= 0) throw new IllegalArgumentException("step must be > 0 in `" + term + "`");
            rangePart = term.substring(0, slash);
        }

        int rlo, rhi;
        if ("*".equals(rangePart)) {
            rlo = lo; rhi = hi;
        } else if (rangePart.contains("-")) {
            int dash = rangePart.indexOf('-');
            rlo = parseInt(rangePart.substring(0, dash), lo, hi);
            rhi = parseInt(rangePart.substring(dash + 1), lo, hi);
            if (rlo > rhi) throw new IllegalArgumentException("inverted range in `" + term + "`");
        } else {
            // bare integer — if a step was supplied, it means `N/step` → N, N+step, … up to hi.
            rlo = parseInt(rangePart, lo, hi);
            rhi = slash >= 0 ? hi : rlo;
        }
        for (int v = rlo; v <= rhi; v += step) out.set(v);
    }

    private static int parseInt(String s, int lo, int hi) {
        int v;
        try { v = Integer.parseInt(s.trim()); }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException("not a number: `" + s + "`");
        }
        if (v < lo || v > hi) {
            throw new IllegalArgumentException("value " + v + " out of range [" + lo + "," + hi + "]");
        }
        return v;
    }

    /** Convenience: list of set bits — useful for debugging / error messages. */
    List<Integer> values(BitSet set) {
        List<Integer> out = new ArrayList<>();
        for (int i = set.nextSetBit(0); i >= 0; i = set.nextSetBit(i + 1)) out.add(i);
        return out;
    }
}

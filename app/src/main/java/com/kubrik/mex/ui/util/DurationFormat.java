package com.kubrik.mex.ui.util;

import java.time.Duration;

/** OBS-7 duration formatter. Compact human-readable labels for the Migrations tab
 *  and Job Details view. */
public final class DurationFormat {

    private DurationFormat() {}

    /** Format a duration as {@code <1s} / {@code 12.3s} / {@code 12m 04s} /
     *  {@code 1h 23m 04s} / {@code 3d 04h 12m}. Null and negative inputs render as em-dash. */
    public static String format(Duration d) {
        if (d == null) return "—";
        long millis = d.toMillis();
        if (millis < 0) return "—";
        if (millis < 1_000) return "<1s";
        long totalSeconds = millis / 1_000;
        if (totalSeconds < 60) {
            double s = millis / 1_000.0;
            return String.format("%.1fs", s);
        }
        long minutes = totalSeconds / 60;
        long seconds = totalSeconds % 60;
        if (minutes < 60) return String.format("%dm %02ds", minutes, seconds);
        long hours = minutes / 60;
        long remMinutes = minutes % 60;
        if (hours < 24) return String.format("%dh %02dm %02ds", hours, remMinutes, seconds);
        long days = hours / 24;
        long remHours = hours % 24;
        return String.format("%dd %02dh %02dm", days, remHours, remMinutes);
    }

    /** Wall-clock vs. active-time breakdown for a Duration cell tooltip (OBS-7). */
    public static String breakdown(long wallMillis, long activeMillis) {
        long paused = Math.max(0L, wallMillis - activeMillis);
        return "wall: " + format(Duration.ofMillis(wallMillis))
                + "  /  active: " + format(Duration.ofMillis(activeMillis))
                + "  /  paused: " + format(Duration.ofMillis(paused));
    }
}

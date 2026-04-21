package com.kubrik.mex.backup.runner;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * v2.5 BKP-POLICY-6 — substitutes the placeholders in an
 * {@code outputDirTemplate} like {@code "<policy>/<yyyy-MM-dd_HH-mm-ss>"}.
 *
 * <p>Supported placeholders:</p>
 * <ul>
 *   <li>{@code <policy>} — policy name (with unsafe chars scrubbed)</li>
 *   <li>{@code <yyyy-MM-dd_HH-mm-ss>} — UTC timestamp</li>
 *   <li>{@code <yyyy-MM-dd>} — UTC date</li>
 *   <li>{@code <connection>} — connection id</li>
 * </ul>
 *
 * <p>Characters not matching {@code [A-Za-z0-9._-]} in substituted values are
 * replaced with dashes so the rendered path is always filesystem-safe.</p>
 */
public final class OutputDirTemplate {

    private static final DateTimeFormatter FULL =
            DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss").withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter DATE =
            DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC);

    private OutputDirTemplate() {}

    public static String render(String template, String policyName,
                                 String connectionId, Instant when) {
        if (template == null) template = "<policy>/<yyyy-MM-dd_HH-mm-ss>";
        String policy = safe(policyName);
        String conn = safe(connectionId);
        String full = FULL.format(when);
        String date = DATE.format(when);
        return template
                .replace("<policy>", policy)
                .replace("<connection>", conn)
                .replace("<yyyy-MM-dd_HH-mm-ss>", full)
                .replace("<yyyy-MM-dd>", date);
    }

    private static String safe(String raw) {
        if (raw == null) return "";
        return raw.replaceAll("[^A-Za-z0-9._-]", "-");
    }
}

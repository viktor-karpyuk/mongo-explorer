package com.kubrik.mex.migration.log;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Scrubs sensitive strings before they hit any log. Applied at the source — there is no
 *  "disable redaction" toggle (docs/mvp-technical-spec.md §12.2, §19.4).
 *  <p>
 *  Known limitation: URL-encoded special characters inside the URI password slot may not
 *  match the regex verbatim; the JSON key pass catches most real-world cases. */
public final class Redactor {

    /** mongodb://user:pass@host or mongodb+srv://user:pass@host — password is capture group 2. */
    private static final Pattern URI_PATTERN = Pattern.compile(
            "(mongodb(?:\\+srv)?://[^:@/\\s]+):([^@/\\s]+)@");

    private static final List<String> DEFAULT_PII_KEYS = List.of(
            "password", "pwd", "ssn", "token", "apiKey", "apikey", "secret");

    private final List<Pattern> jsonKeyPatterns;

    public Redactor(List<String> piiKeys) {
        this.jsonKeyPatterns = (piiKeys == null || piiKeys.isEmpty() ? DEFAULT_PII_KEYS : piiKeys)
                .stream()
                .map(k -> Pattern.compile(
                        "(\"" + Pattern.quote(k) + "\"\\s*:\\s*)(\"[^\"]*\"|\\S+)",
                        Pattern.CASE_INSENSITIVE))
                .toList();
    }

    public static Redactor defaultInstance() { return new Redactor(DEFAULT_PII_KEYS); }

    public String redact(String input) {
        if (input == null || input.isEmpty()) return input;
        String out = redactUri(input);
        for (Pattern p : jsonKeyPatterns) out = redactJsonKey(out, p);
        return out;
    }

    private static String redactUri(String s) {
        Matcher m = URI_PATTERN.matcher(s);
        if (!m.find()) return s;
        StringBuilder sb = new StringBuilder(s.length());
        int prev = 0;
        do {
            sb.append(s, prev, m.start()).append(m.group(1)).append(":***@");
            prev = m.end();
        } while (m.find());
        sb.append(s, prev, s.length());
        return sb.toString();
    }

    private static String redactJsonKey(String s, Pattern p) {
        Matcher m = p.matcher(s);
        if (!m.find()) return s;
        StringBuilder sb = new StringBuilder(s.length());
        int prev = 0;
        do {
            sb.append(s, prev, m.start()).append(m.group(1));
            String value = m.group(2);
            sb.append(value.startsWith("\"") ? "\"***\"" : "***");
            prev = m.end();
        } while (m.find());
        sb.append(s, prev, s.length());
        return sb.toString();
    }
}

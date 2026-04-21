package com.kubrik.mex.backup.runner;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * v2.5 BKP-RUN-4 — parsed view of a mongodump stderr progress line.
 *
 * <p>Mongodump emits tqdm-style progress lines roughly once per collection
 * tick:</p>
 * <pre>
 * 2026-04-21T10:15:03.123+0000   writing db.coll to dump/db/coll.bson.gz
 * 2026-04-21T10:15:03.456+0000   [####....................]   db.coll    12345/987654   (1.3%)
 * 2026-04-21T10:15:04.789+0000   done dumping db.coll (987654 documents)
 * </pre>
 *
 * <p>{@link #parse} extracts the current namespace + docs-processed counter
 * from the two shapes we actually care about (the <em>writing</em> header
 * and the <em>progress</em> bar). Other lines return an empty {@link Optional},
 * so consumers can safely ignore anything they don't recognise.</p>
 */
public record ProgressLine(String namespace, long docsProcessed, long totalDocs,
                            boolean starting, boolean done, String raw) {

    private static final Pattern WRITING = Pattern.compile(
            "writing\\s+([\\w$.-]+\\.[\\w$.-]+)\\s+to\\s+");
    private static final Pattern PROGRESS = Pattern.compile(
            "\\[[^\\]]+\\]\\s+([\\w$.-]+\\.[\\w$.-]+)\\s+(\\d+)/(\\d+)");
    private static final Pattern DONE = Pattern.compile(
            "done dumping\\s+([\\w$.-]+\\.[\\w$.-]+)\\s+\\((\\d+)\\s+documents?\\)");

    public static java.util.Optional<ProgressLine> parse(String line) {
        if (line == null || line.isEmpty()) return java.util.Optional.empty();
        Matcher m;
        if ((m = PROGRESS.matcher(line)).find()) {
            return java.util.Optional.of(new ProgressLine(m.group(1),
                    Long.parseLong(m.group(2)), Long.parseLong(m.group(3)),
                    false, false, line));
        }
        if ((m = DONE.matcher(line)).find()) {
            long docs = Long.parseLong(m.group(2));
            return java.util.Optional.of(new ProgressLine(m.group(1), docs, docs,
                    false, true, line));
        }
        if ((m = WRITING.matcher(line)).find()) {
            return java.util.Optional.of(new ProgressLine(m.group(1), 0L, 0L,
                    true, false, line));
        }
        return java.util.Optional.empty();
    }
}

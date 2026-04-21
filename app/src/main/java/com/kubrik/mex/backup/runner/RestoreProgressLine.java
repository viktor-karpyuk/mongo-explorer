package com.kubrik.mex.backup.runner;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * v2.5 Q2.5-E — mongorestore stderr progress parsing. Mongorestore emits
 * slightly different lines from mongodump:
 *
 * <pre>
 * 2026-04-21T10:15:03.123+0000  restoring db.coll from dump/db/coll.bson.gz
 * 2026-04-21T10:15:04.456+0000  [##....]  db.coll  12345/98765  (12.5%)
 * 2026-04-21T10:15:05.789+0000  finished restoring db.coll (98765 documents, 0 failures)
 * </pre>
 *
 * <p>Three shapes handled: <em>restoring</em> header, progress bar, and
 * <em>finished restoring</em> completion line. Every other line returns
 * empty.</p>
 */
public record RestoreProgressLine(String namespace, long docsProcessed, long totalDocs,
                                   boolean starting, boolean done, long failures,
                                   String raw) {

    private static final Pattern RESTORING = Pattern.compile(
            "restoring\\s+([\\w$.-]+\\.[\\w$.-]+)\\s+from\\s+");
    private static final Pattern PROGRESS = Pattern.compile(
            "\\[[^\\]]+\\]\\s+([\\w$.-]+\\.[\\w$.-]+)\\s+(\\d+)/(\\d+)");
    private static final Pattern DONE = Pattern.compile(
            "finished restoring\\s+([\\w$.-]+\\.[\\w$.-]+)\\s+\\((\\d+)\\s+documents?,\\s+(\\d+)\\s+failures?\\)");

    public static Optional<RestoreProgressLine> parse(String line) {
        if (line == null || line.isEmpty()) return Optional.empty();
        Matcher m;
        if ((m = PROGRESS.matcher(line)).find()) {
            return Optional.of(new RestoreProgressLine(m.group(1),
                    Long.parseLong(m.group(2)), Long.parseLong(m.group(3)),
                    false, false, 0L, line));
        }
        if ((m = DONE.matcher(line)).find()) {
            long docs = Long.parseLong(m.group(2));
            long failures = Long.parseLong(m.group(3));
            return Optional.of(new RestoreProgressLine(m.group(1), docs, docs,
                    false, true, failures, line));
        }
        if ((m = RESTORING.matcher(line)).find()) {
            return Optional.of(new RestoreProgressLine(m.group(1), 0L, 0L,
                    true, false, 0L, line));
        }
        return Optional.empty();
    }
}

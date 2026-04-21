package com.kubrik.mex.backup.runner;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * v2.5 Q2.5-E — builds the {@code mongorestore} argv from
 * {@link MongorestoreOptions}. Extracted from {@link MongorestoreRunner} so
 * the arg list is unit-testable without spawning a subprocess.
 *
 * <p>Shape:</p>
 * <pre>
 * mongorestore
 *   --uri=&lt;uri&gt;
 *   --dir=&lt;sourceDir&gt;
 *   [--gzip]
 *   [--oplogReplay]
 *   [--dryRun]
 *   [--drop]
 *   [--nsFrom=&lt;from&gt; --nsTo=&lt;to&gt;]*
 *   --numParallelCollections=&lt;n&gt;
 *   --quiet
 * </pre>
 */
public final class MongorestoreCommandBuilder {

    private MongorestoreCommandBuilder() {}

    public static List<String> build(String binary, MongorestoreOptions opts) {
        List<String> argv = new ArrayList<>();
        argv.add(binary);
        argv.add("--uri=" + opts.uri());
        argv.add("--dir=" + opts.sourceDir().toString());
        if (opts.gzip()) argv.add("--gzip");
        if (opts.oplogReplay()) argv.add("--oplogReplay");
        if (opts.dryRun()) argv.add("--dryRun");
        if (opts.dropBeforeRestore()) argv.add("--drop");
        for (Map.Entry<String, String> e : opts.nsRename().entrySet()) {
            argv.add("--nsFrom=" + e.getKey());
            argv.add("--nsTo=" + e.getValue());
        }
        argv.add("--numParallelCollections=" + opts.parallelCollections());
        argv.add("--quiet");
        return argv;
    }

    /** Same URI-redaction policy as the mongodump builder — never log the
     *  raw connection string. */
    public static List<String> redactUri(List<String> argv) {
        return MongodumpCommandBuilder.redactUri(argv);
    }
}

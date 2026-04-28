package com.kubrik.mex.backup.runner;

import com.kubrik.mex.backup.spec.Scope;

import java.util.ArrayList;
import java.util.List;

/**
 * v2.5 BKP-RUN-1 — builds the {@code mongodump} argv from
 * {@link MongodumpOptions}. Extracted from {@link MongodumpRunner} so the
 * arg list is unit-testable without spawning a subprocess.
 *
 * <p>Argument shape (matches technical-spec §4.1):</p>
 * <pre>
 * mongodump
 *   --uri=&lt;uri&gt;
 *   --out=&lt;outDir&gt;
 *   [--gzip]
 *   [--oplog]
 *   [--db=&lt;db&gt;] [--collection=&lt;coll&gt;]
 *   --numParallelCollections=&lt;n&gt;
 *   --quiet
 * </pre>
 *
 * <p>The scope's {@code WholeCluster} branch emits no {@code --db} / {@code --collection};
 * {@code Databases} emits one {@code --db} per name (mongodump will then
 * combine them in one pass); {@code Namespaces} emits {@code --db} +
 * {@code --collection} pairs. Multiple {@code --db} flags aren't natively
 * supported by mongodump in one invocation, so callers handling
 * {@code Databases(N)} should either loop per-db or fall back to
 * {@code WholeCluster} with a post-filter — noted in the runner's docblock.</p>
 */
public final class MongodumpCommandBuilder {

    private MongodumpCommandBuilder() {}

    public static List<String> build(String binary, MongodumpOptions opts) {
        List<String> argv = new ArrayList<>();
        argv.add(binary);
        argv.add("--uri=" + opts.uri());
        argv.add("--out=" + opts.outDir().toString());
        if (opts.archive().gzip()) argv.add("--gzip");
        if (opts.includeOplog()) argv.add("--oplog");
        switch (opts.scope()) {
            case Scope.WholeCluster w -> { /* no db/coll flags */ }
            case Scope.Databases d -> {
                // Mongodump supports at most one --db per invocation; if the
                // list has one entry pass it directly, otherwise the runner
                // should iterate. We emit the first here so the command is
                // always well-formed; the caller is responsible for looping
                // over the remainder.
                if (!d.names().isEmpty()) argv.add("--db=" + d.names().get(0));
            }
            case Scope.Namespaces ns -> {
                // Same single-namespace constraint; caller iterates for multi-ns policies.
                if (!ns.namespaces().isEmpty()) {
                    String first = ns.namespaces().get(0);
                    int dot = first.indexOf('.');
                    argv.add("--db=" + first.substring(0, dot));
                    argv.add("--collection=" + first.substring(dot + 1));
                }
            }
        }
        argv.add("--numParallelCollections=" + opts.parallelCollections());
        argv.add("--quiet");
        return argv;
    }

    /** Utility: redact the {@code --uri} argument for logs so credentials
     *  never leak into audit rows or stderr tails. */
    public static List<String> redactUri(List<String> argv) {
        List<String> out = new ArrayList<>(argv.size());
        for (String a : argv) {
            if (a.startsWith("--uri=")) out.add("--uri=<redacted>");
            else out.add(a);
        }
        return out;
    }
}

package com.kubrik.mex.migration.preflight;

import com.kubrik.mex.core.ConnectionManager;
import com.kubrik.mex.core.MongoService;
import com.kubrik.mex.migration.engine.CollectionPlan;
import com.kubrik.mex.migration.engine.Namespaces;
import com.kubrik.mex.migration.spec.ConflictMode;
import com.kubrik.mex.migration.spec.MigrationKind;
import com.kubrik.mex.migration.spec.MigrationSpec;
import com.kubrik.mex.migration.spec.ScopeSpec;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Resolves the scope into a concrete list of {@link CollectionPlan}s and emits warnings.
 *  <p>
 *  Covers the full SAFE-1 surface: scope resolution, version + same-cluster guards, conflict
 *  detection, index-portability warnings, disk-size estimates, and an Atlas-friendly ping so
 *  preflight doesn't hang the UI when a cluster is unreachable. */
public final class PreflightChecker {

    private final ConnectionManager manager;

    public PreflightChecker(ConnectionManager manager) {
        this.manager = manager;
    }

    public PreflightReport check(MigrationSpec spec) {
        List<String> warnings = new ArrayList<>();
        List<String> errors = new ArrayList<>();

        if (spec.kind() == MigrationKind.VERSIONED) {
            com.kubrik.mex.migration.versioned.ScriptRepo.ScanResult scan = null;
            if (spec.scriptsFolder() == null || spec.scriptsFolder().isBlank()) {
                errors.add("Versioned migration requires a scripts folder.");
            } else {
                scan = new com.kubrik.mex.migration.versioned.ScriptRepo()
                        .scan(java.nio.file.Path.of(spec.scriptsFolder()));
                warnings.addAll(scan.warnings());
                errors.addAll(scan.errors());
                if (!scan.hasErrors() && scan.scripts().isEmpty()) {
                    warnings.add("Scripts folder contains no V* migration files.");
                }
            }
            if (spec.target().database() == null || spec.target().database().isBlank()) {
                errors.add("Versioned migration requires a target database.");
            }

            // VER-4 — surface checksum drift before the user clicks Start. We compare the
            // eligible scripts on disk against whatever is recorded in the target's
            // _mongo_explorer_migrations. Fresh targets (no tracking collection yet) can't have
            // drift by definition, so the read-only peek leaves them untouched.
            if (scan != null && !scan.hasErrors() && !scan.scripts().isEmpty()
                    && spec.target().database() != null && !spec.target().database().isBlank()) {
                checkVersionedDrift(spec, scan, warnings, errors);
            }
            return new PreflightReport(List.of(), warnings, errors);
        }

        MongoService src = manager.service(spec.source().connectionId());
        if (src == null) {
            errors.add("Source connection not active.");
            return new PreflightReport(List.of(), warnings, errors);
        }
        MongoService tgt = manager.service(spec.target().connectionId());
        if (tgt == null) {
            errors.add("Target connection not active.");
            return new PreflightReport(List.of(), warnings, errors);
        }

        // Cross-version warnings (SRC-3)
        checkServerVersions(src, tgt, warnings, errors);

        // Endpoint reachability (SAFE-1 Atlas smoke). A targeted ping so an unreachable target
        // fails here instead of deep inside the pipeline 30 s later.
        checkReachable(src, "Source", warnings, errors);
        checkReachable(tgt, "Target", warnings, errors);

        // Same-cluster detection (SRC-6). Cheap certain case first: same connection ID. Only
        // fall back to hello-based detection for confirmed replica sets — for two distinct
        // standalone servers the hello response doesn't carry enough identity to tell them
        // apart reliably, so we prefer false negatives over false positives here.
        boolean sameCluster = spec.source().connectionId().equals(spec.target().connectionId())
                || isSameCluster(src, tgt);

        // SEC-USR-1 — Users copy is an optional tail stage (§4.3). Probe both endpoints for
        // sufficient privileges and warn on SCRAM mechanism drift; missing privileges become
        // hard errors because createUser would fail mid-job after documents are already at
        // the target.
        if (spec.scope() != null && spec.scope().migrateUsers()) {
            checkUsersCopyPreconditions(src, tgt, spec.scope(), warnings, errors);
        }

        List<SourceEntry> sourceNamespaces = resolveSource(src, spec.scope());
        Map<String, String> renameMap = buildRenameMap(spec.scope());
        Set<String> targetsSeen = new LinkedHashSet<>();

        List<CollectionPlan> plans = new ArrayList<>(sourceNamespaces.size());
        for (SourceEntry entry : sourceNamespaces) {
            String ns = entry.ns();
            String target = renameMap.getOrDefault(ns, ns);
            if (!targetsSeen.add(target)) {
                errors.add("Duplicate target namespace: " + target);
                continue;
            }
            if (Namespaces.isSystem(target)) {
                warnings.add("Skipping system namespace: " + target);
                continue;
            }
            ConflictMode mode = spec.options().conflict().modeFor(ns);

            // SRC-6 — same cluster + same namespace would destroy the source.
            if (sameCluster && ns.equals(target)) {
                errors.add("Source and target are the same namespace on the same cluster: " + ns);
                continue;
            }

            if (entry.isView()) {
                // SCOPE-6 — views carry no data and no indexes; skip count / index / size
                // warnings and emit a VIEW-flagged plan that ViewCreator picks up after all
                // regular collections have migrated.
                plans.add(new CollectionPlan(ns, target, mode, true));
                continue;
            }

            // Non-empty target (SAFE-2) — surface as warning unless mode is ABORT, in which
            // case it's a hard error.
            Namespaces.Ns tgtNs = Namespaces.parse(target);
            long existing = approxCollectionCount(tgt, tgtNs);
            if (existing > 0) {
                String msg = "Target `" + target + "` already contains " + existing + " document(s).";
                if (mode == ConflictMode.ABORT) {
                    errors.add(msg + " Choose a conflict mode to continue.");
                } else {
                    warnings.add(msg + " Conflict mode: " + mode + ".");
                }
            }
            plans.add(new CollectionPlan(ns, target, mode));

            // SAFE-1 — index portability warnings per collection. Cheap list-only scan.
            collectIndexPortabilityWarnings(src, Namespaces.parse(ns), target, warnings);
        }

        // SAFE-1 — estimated disk need per target DB vs free space on the target volume. Best
        // effort: older servers and Atlas shared tiers don't expose fsTotalSize/fsUsedSize, in
        // which case we report the estimate alone and move on.
        collectDiskSizeWarnings(src, tgt, plans, warnings);

        // EXT-2 — validate the file sink destination (path writable, no silent clobber).
        // Runs even when the Mongo target lookup passed, because sinks fully bypass writes to
        // `tgt` — errors here are independent of target-cluster state.
        checkSinkDestination(spec, plans, warnings, errors);

        return new PreflightReport(plans, warnings, errors);
    }

    /** Validates that the configured file sink can accept output: path non-blank, is (or can be
     *  made into) a writable directory, and existing output files — which the NDJSON sink
     *  overwrites on open — are flagged as warnings so the user isn't surprised.
     *  Package-private so the sink-branch unit tests can call it without a live manager. */
    static void checkSinkDestination(MigrationSpec spec,
                                     List<CollectionPlan> plans,
                                     List<String> warnings,
                                     List<String> errors) {
        List<com.kubrik.mex.migration.spec.SinkSpec> sinks = spec.options().sinks();
        if (sinks == null || sinks.isEmpty()) return;
        if (sinks.size() > 1) {
            errors.add("Multiple sinks configured — v2.0.0 supports a single sink per job.");
            return;
        }
        var sink = sinks.get(0);
        if (sink.path() == null || sink.path().isBlank()) {
            errors.add("Sink `" + sink.kind() + "` is configured but has no output path.");
            return;
        }

        java.nio.file.Path path = java.nio.file.Path.of(sink.path());
        if (java.nio.file.Files.exists(path)) {
            if (!java.nio.file.Files.isDirectory(path)) {
                errors.add("Sink path is not a directory: " + path);
                return;
            }
            if (!java.nio.file.Files.isWritable(path)) {
                errors.add("Sink directory is not writable: " + path);
                return;
            }
        } else {
            java.nio.file.Path parent = path.toAbsolutePath().getParent();
            if (parent == null || !java.nio.file.Files.exists(parent)) {
                errors.add("Sink path's parent directory does not exist: " + path);
                return;
            }
            if (!java.nio.file.Files.isWritable(parent)) {
                errors.add("Cannot create sink directory — parent is not writable: " + parent);
                return;
            }
            warnings.add("Sink directory will be created at " + path + ".");
        }

        // Flag existing output files that the sink will overwrite. One file per planned
        // target namespace; extension depends on the sink kind — must stay in sync with the
        // concrete sink implementations under `com.kubrik.mex.migration.sink`. Plugin kinds
        // delegate extension resolution to the registered factory; a missing plugin is a
        // hard error because the whole job will fail at sink-build time otherwise.
        String extension;
        switch (sink.kind()) {
            case NDJSON     -> extension = ".ndjson";
            case JSON_ARRAY -> extension = ".json";
            case CSV        -> extension = ".csv";
            case BSON_DUMP  -> extension = ".bson";
            case PLUGIN     -> {
                var factory = com.kubrik.mex.migration.sink.PluginSinkRegistry.resolve(sink.pluginName());
                if (factory == null) {
                    errors.add("Sink plugin `" + sink.pluginName() + "` is not registered. "
                            + "Registered: " + com.kubrik.mex.migration.sink.PluginSinkRegistry.registered());
                    return;
                }
                extension = factory.extension();
            }
            default -> { return; }
        }
        if (extension == null || extension.isEmpty()) return;
        for (CollectionPlan plan : plans) {
            Namespaces.Ns t = plan.target();
            java.nio.file.Path out = path.resolve(t.db() + "." + t.coll() + extension);
            if (java.nio.file.Files.exists(out)) {
                warnings.add("Sink will overwrite existing file: " + out);
            }
        }
    }

    private void checkVersionedDrift(MigrationSpec spec,
                                     com.kubrik.mex.migration.versioned.ScriptRepo.ScanResult scan,
                                     List<String> warnings, List<String> errors) {
        MongoService tgt = manager.service(spec.target().connectionId());
        if (tgt == null) {
            errors.add("Target connection not active.");
            return;
        }
        Map<String, String> stored;
        try {
            stored = com.kubrik.mex.migration.versioned.AppliedMigrations
                    .peekSuccessfulChecksums(tgt.database(spec.target().database()));
        } catch (Exception e) {
            warnings.add("Could not read applied migrations on target: " + e.getMessage());
            return;
        }
        if (stored.isEmpty()) return;

        String specEnv = spec.options().environment();
        int pending = 0;
        List<String> driftMessages = new ArrayList<>();
        for (var s : scan.scripts()) {
            if (!s.runsIn(specEnv)) continue;
            String storedChecksum = stored.get(s.version());
            if (storedChecksum == null) {
                pending++;
                continue;
            }
            if (!storedChecksum.equals(s.checksum())) {
                driftMessages.add("Checksum drift on V" + s.version() + " (" + s.description() + "): "
                        + "stored " + storedChecksum + ", current " + s.checksum());
            }
        }
        if (driftMessages.isEmpty()) return;

        boolean ignoreDrift = spec.options().ignoreDrift();
        for (String msg : driftMessages) {
            if (pending > 0 && !ignoreDrift) {
                errors.add(msg + " — refusing to apply " + pending
                        + " pending script(s). Tick \"Acknowledge checksum drift\" in Options to proceed.");
            } else {
                warnings.add(msg);
            }
        }
    }

    private static void checkReachable(MongoService svc, String role,
                                       List<String> warnings, List<String> errors) {
        try {
            svc.runCommand("admin", "{\"ping\": 1}");
        } catch (Exception e) {
            errors.add(role + " endpoint is not responding to ping: " + e.getMessage());
        }
    }

    /** Flags index definitions that may behave differently between source and target because of
     *  partial filters, non-default text language, collation, or wildcard projections. The engine
     *  copies the options verbatim, so these are informational — not blocking. */
    private static void collectIndexPortabilityWarnings(MongoService src, Namespaces.Ns source,
                                                        String target, List<String> warnings) {
        List<org.bson.Document> indexes;
        try {
            indexes = src.listIndexes(source.db(), source.coll());
        } catch (Exception e) {
            return;
        }
        for (org.bson.Document idx : indexes) {
            String name = idx.getString("name");
            if (name == null || "_id_".equals(name)) continue;
            String nsLabel = target + " / index `" + name + "`";

            if (idx.containsKey("partialFilterExpression")) {
                warnings.add(nsLabel + ": partial filter — operators must be supported by target.");
            }
            if (idx.containsKey("collation")) {
                warnings.add(nsLabel + ": non-default collation — confirm target server version.");
            }
            String lang = idx.getString("default_language");
            if (lang != null && !"english".equalsIgnoreCase(lang)) {
                warnings.add(nsLabel + ": text index uses default_language=" + lang + ".");
            }
            if (idx.containsKey("wildcardProjection") || hasWildcardKey(idx)) {
                warnings.add(nsLabel + ": wildcard index — requires target MongoDB 4.2+.");
            }
        }
    }

    private static boolean hasWildcardKey(org.bson.Document idx) {
        Object keyObj = idx.get("key");
        if (!(keyObj instanceof org.bson.Document key)) return false;
        for (String k : key.keySet()) {
            if (k.endsWith("$**") || "$**".equals(k)) return true;
        }
        return false;
    }

    private static void collectDiskSizeWarnings(MongoService src, MongoService tgt,
                                                List<CollectionPlan> plans, List<String> warnings) {
        // Sum source storageSize per *target* DB so cross-DB renames aggregate correctly.
        Map<String, Long> needPerTargetDb = new HashMap<>();
        for (CollectionPlan plan : plans) {
            if (plan.isView()) continue;   // views carry no storage
            Namespaces.Ns s = plan.source();
            Namespaces.Ns t = plan.target();
            long bytes = sizeOf(src, s);
            if (bytes <= 0) continue;
            needPerTargetDb.merge(t.db(), bytes, Long::sum);
        }
        for (Map.Entry<String, Long> e : needPerTargetDb.entrySet()) {
            String targetDb = e.getKey();
            long need = e.getValue();
            Long free = freeBytesOnVolume(tgt, targetDb);
            String estimate = humanBytes(need);
            if (free == null) {
                warnings.add("Target DB `" + targetDb + "`: ~" + estimate
                        + " estimated write. Free-space data is not available on this deployment.");
            } else if (need * 2 > free) {
                warnings.add("Target DB `" + targetDb + "`: ~" + estimate
                        + " estimated write vs ~" + humanBytes(free) + " free on volume.");
            }
        }
    }

    private static long sizeOf(MongoService svc, Namespaces.Ns ns) {
        try {
            org.bson.Document stats = svc.collStats(ns.db(), ns.coll());
            long storage = numberAsLong(stats.get("storageSize"));
            long indexes = numberAsLong(stats.get("totalIndexSize"));
            return storage + indexes;
        } catch (Exception e) {
            return 0L;
        }
    }

    private static Long freeBytesOnVolume(MongoService svc, String db) {
        try {
            org.bson.Document stats = svc.dbStats(db);
            Object total = stats.get("fsTotalSize");
            Object used = stats.get("fsUsedSize");
            if (total == null || used == null) return null;
            return Math.max(0L, numberAsLong(total) - numberAsLong(used));
        } catch (Exception e) {
            return null;
        }
    }

    private static long numberAsLong(Object o) {
        return o instanceof Number n ? n.longValue() : 0L;
    }

    private static String humanBytes(long bytes) {
        if (bytes < 1024) return bytes + " B";
        double v = bytes;
        String[] units = { "KB", "MB", "GB", "TB" };
        int i = -1;
        do { v /= 1024.0; i++; } while (v >= 1024 && i < units.length - 1);
        return String.format("%.1f %s", v, units[i]);
    }

    private static void checkServerVersions(MongoService src, MongoService tgt,
                                            List<String> warnings, List<String> errors) {
        int srcMajor = majorVersion(src.serverVersion());
        int tgtMajor = majorVersion(tgt.serverVersion());
        if (srcMajor > 0 && srcMajor < 5) {
            errors.add("Source MongoDB version " + src.serverVersion() + " is below the 5.0 floor.");
        }
        if (tgtMajor > 0 && tgtMajor < 5) {
            errors.add("Target MongoDB version " + tgt.serverVersion() + " is below the 5.0 floor.");
        }
        if (srcMajor > 0 && tgtMajor > 0 && srcMajor != tgtMajor) {
            warnings.add("Major-version difference: source " + src.serverVersion()
                    + " vs target " + tgt.serverVersion()
                    + " — review compatibility of server-specific BSON types and features.");
        }
    }

    private static int majorVersion(String v) {
        if (v == null || v.isBlank()) return 0;
        int dot = v.indexOf('.');
        try { return Integer.parseInt(dot > 0 ? v.substring(0, dot) : v); }
        catch (NumberFormatException e) { return 0; }
    }

    private static boolean isSameCluster(MongoService a, MongoService b) {
        try {
            var ha = a.hello();
            var hb = b.hello();
            String idA = replicaSetIdentity(ha);
            String idB = replicaSetIdentity(hb);
            return idA != null && idA.equals(idB);
        } catch (Exception e) {
            return false;
        }
    }

    private static String replicaSetIdentity(org.bson.Document hello) {
        // Replica sets carry a stable setName in the hello response; this is the most reliable
        // single-field identity we can extract cheaply. Standalones don't have one — we
        // intentionally don't try harder (see isSameCluster caller).
        String set = hello.getString("setName");
        if (set == null) return null;
        List<?> hosts = hello.get("hosts") instanceof List<?> l ? l : List.of();
        return "rs:" + set + ":" + hosts;
    }

    private static long approxCollectionCount(MongoService svc, Namespaces.Ns ns) {
        try {
            return com.kubrik.mex.migration.engine.ApproxCount.of(
                    svc.database(ns.db()), svc.rawCollection(ns.db(), ns.coll()));
        } catch (Exception e) {
            return 0L;
        }
    }

    /** SEC-USR-1 — best-effort probe. {@code usersInfo} on source verifies read access to the
     *  SCRAM credentials; running it against target checks that the caller has {@code userAdmin}
     *  (createUser is available when usersInfo is). SCRAM mechanism drift becomes a warning. */
    private void checkUsersCopyPreconditions(MongoService src,
                                             MongoService tgt,
                                             ScopeSpec scope,
                                             List<String> warnings,
                                             List<String> errors) {
        java.util.LinkedHashSet<String> dbs = new java.util.LinkedHashSet<>();
        if (scope instanceof ScopeSpec.Databases d) dbs.addAll(d.databases());
        else if (scope instanceof ScopeSpec.Collections c) {
            for (var ns : c.namespaces()) dbs.add(ns.db());
        }
        // Server scope copies every non-system DB — probe against "admin" as a proxy.
        if (dbs.isEmpty()) dbs.add("admin");

        for (String db : dbs) {
            try {
                src.database(db).runCommand(new org.bson.Document("usersInfo", 1));
            } catch (Exception e) {
                errors.add("SEC-USR-1: source lacks privilege to read users on `" + db
                        + "` (Migrate users is enabled): " + e.getMessage());
                continue;
            }
            try {
                tgt.database(db).runCommand(new org.bson.Document("usersInfo", 1));
            } catch (Exception e) {
                errors.add("SEC-USR-1: target lacks userAdmin on `" + db
                        + "` (Migrate users is enabled): " + e.getMessage());
            }
        }

        // SCRAM mechanism drift — compare the admin-level mechanism list.
        try {
            org.bson.Document srcParam = src.database("admin").runCommand(
                    new org.bson.Document("getParameter", 1).append("authenticationMechanisms", 1));
            org.bson.Document tgtParam = tgt.database("admin").runCommand(
                    new org.bson.Document("getParameter", 1).append("authenticationMechanisms", 1));
            Object srcMech = srcParam.get("authenticationMechanisms");
            Object tgtMech = tgtParam.get("authenticationMechanisms");
            if (srcMech != null && tgtMech != null && !srcMech.equals(tgtMech)) {
                warnings.add("SEC-USR-1: source/target SCRAM mechanisms differ "
                        + "(" + srcMech + " vs " + tgtMech + "); copied credentials may not authenticate.");
            }
        } catch (Exception ignored) {
            // Probe is advisory — fall back silently when getParameter isn't available.
        }
    }

    /** A resolved source namespace annotated with its MongoDB entity type. SCOPE-6 routes
     *  views through {@link com.kubrik.mex.migration.engine.ViewCreator}; regular collections
     *  stay on the {@link com.kubrik.mex.migration.engine.CollectionPipeline} path. */
    record SourceEntry(String ns, boolean isView) {}

    private List<SourceEntry> resolveSource(MongoService svc, ScopeSpec scope) {
        List<SourceEntry> out = new ArrayList<>();
        switch (scope) {
            case ScopeSpec.Server s -> {
                for (String db : svc.listDatabaseNames()) {
                    if (isSystemDb(db)) continue;
                    for (org.bson.Document entry : listNonSystemEntries(svc, db)) {
                        String fullNs = db + "." + entry.getString("name");
                        if (matches(fullNs, s.include(), s.exclude())) {
                            out.add(new SourceEntry(fullNs, isViewEntry(entry)));
                        }
                    }
                }
            }
            case ScopeSpec.Databases d -> {
                for (String db : d.databases()) {
                    for (org.bson.Document entry : listNonSystemEntries(svc, db)) {
                        String fullNs = db + "." + entry.getString("name");
                        if (matches(fullNs, d.include(), d.exclude())) {
                            out.add(new SourceEntry(fullNs, isViewEntry(entry)));
                        }
                    }
                }
            }
            case ScopeSpec.Collections c -> {
                // COLLECTIONS scope: the user supplied the exact list, so query per-entry to
                // discover view-ness rather than enumerating the whole DB.
                for (var ns : c.namespaces()) {
                    String fullNs = ns.toString();
                    if (!matches(fullNs, c.include(), c.exclude())) continue;
                    org.bson.Document entry = svc.collectionInfo(ns.db(), ns.coll());
                    out.add(new SourceEntry(fullNs, isViewEntry(entry)));
                }
            }
        }
        return out;
    }

    private static List<org.bson.Document> listNonSystemEntries(MongoService svc, String db) {
        List<org.bson.Document> out = new ArrayList<>();
        svc.database(db).listCollections().forEach(out::add);
        return out;
    }

    private static boolean isViewEntry(org.bson.Document entry) {
        return entry != null && "view".equalsIgnoreCase(entry.getString("type"));
    }

    private static boolean isSystemDb(String db) {
        return "admin".equalsIgnoreCase(db) || "local".equalsIgnoreCase(db)
                || "config".equalsIgnoreCase(db);
    }

    private static Map<String, String> buildRenameMap(ScopeSpec scope) {
        Map<String, String> out = new HashMap<>();
        for (ScopeSpec.Rename r : scope.renames()) out.put(r.from(), r.to());
        return out;
    }

    /** Very small glob matcher — covers `**`, `*`, `?`. Negation/brackets land later. */
    static boolean matches(String ns, List<String> include, List<String> exclude) {
        if (include == null || include.isEmpty()) include = List.of("**");
        boolean included = false;
        for (String p : include) {
            if (glob(p, ns)) { included = true; break; }
        }
        if (!included) return false;
        if (exclude != null) {
            for (String p : exclude) {
                if (glob(p, ns)) return false;
            }
        }
        return true;
    }

    static boolean glob(String pattern, String input) {
        String regex = pattern
                .replace(".", "\\.")
                .replace("**", "§§")
                .replace("*", "[^.]*")
                .replace("§§", ".*")
                .replace("?", "[^.]");
        return input.matches("^" + regex + "$");
    }
}

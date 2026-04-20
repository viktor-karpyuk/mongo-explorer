package com.kubrik.mex.migration.versioned;

import com.kubrik.mex.core.MongoService;
import com.kubrik.mex.migration.engine.JobContext;
import com.kubrik.mex.migration.spec.ExecutionMode;
import com.mongodb.client.MongoDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Applies pending versioned scripts to a target database.
 *  <p>
 *  Algorithm (docs/mvp-technical-spec.md §7.2):
 *  <ol>
 *    <li>Scan the scripts folder.</li>
 *    <li>Load successful rows from {@code _mongo_explorer_migrations} on the target.</li>
 *    <li>For each script whose version is not in the successful set, mark IN_PROGRESS,
 *        execute ops via {@link OpExecutor}, then mark SUCCESS.</li>
 *    <li>Any failure aborts the job and marks the offending row FAILED.</li>
 *  </ol>
 *  <p>
 *  Dry-run mode skips execution and never writes to
 *  {@code _mongo_explorer_migrations}. */
public final class VersionedMigrator {

    private static final Logger log = LoggerFactory.getLogger(VersionedMigrator.class);
    private static final String ENGINE_VERSION = "1.1.0";

    private final JobContext ctx;
    private final MongoService target;
    private final String database;
    private final String scriptsFolder;
    private final String appliedBy;

    public VersionedMigrator(JobContext ctx,
                             MongoService target,
                             String database,
                             String scriptsFolder,
                             String appliedBy) {
        this.ctx = ctx;
        this.target = target;
        this.database = database;
        this.scriptsFolder = scriptsFolder;
        this.appliedBy = appliedBy;
    }

    public Summary run() {
        ScriptRepo.ScanResult scan = new ScriptRepo().scan(java.nio.file.Path.of(scriptsFolder));
        if (scan.hasErrors()) {
            throw new IllegalStateException("Script repo errors: " + String.join("; ", scan.errors()));
        }

        MongoDatabase db = target.database(database);
        boolean dryRun = ctx.spec().options().executionMode() == ExecutionMode.DRY_RUN;
        AppliedMigrations applied = new AppliedMigrations(db, ENGINE_VERSION, appliedBy);
        Map<String, String> appliedChecksums = dryRun ? Map.of() : applied.loadSuccessfulChecksums();
        Set<String> successful = appliedChecksums.keySet();

        // VER-8 — drop env-gated scripts early so drift/pending calculations only consider the
        // scripts that are actually eligible for this environment.
        String specEnv = ctx.spec().options().environment();
        List<MigrationScript> eligibleScripts = new ArrayList<>(scan.scripts().size());
        int envSkipped = 0;
        for (MigrationScript s : scan.scripts()) {
            if (s.runsIn(specEnv)) {
                eligibleScripts.add(s);
            } else {
                envSkipped++;
                log.info("skip V{} (env filter `{}` does not match environment `{}`)",
                        s.version(), s.envFilter(), specEnv);
            }
        }

        // VER-4 — detect drift between the file on disk and what was applied earlier. We do
        // this regardless of dry-run so the operator sees drift before attempting a real run.
        List<DriftEntry> drift = detectDrift(eligibleScripts, appliedChecksums);
        boolean hasPendingWork = eligibleScripts.stream().anyMatch(s -> !successful.contains(s.version()));
        boolean ignoreDrift = ctx.spec().options().ignoreDrift();
        List<String> warnings = new ArrayList<>(scan.warnings());
        for (DriftEntry d : drift) {
            warnings.add("Checksum drift on V" + d.version() + " (" + d.description() + "): "
                    + "stored " + d.storedChecksum() + ", current " + d.currentChecksum());
        }
        if (!drift.isEmpty() && hasPendingWork && !ignoreDrift) {
            throw new IllegalStateException(
                    "Checksum drift detected on " + drift.size() + " previously-applied script(s); "
                    + "refusing to apply " + countPending(eligibleScripts, successful) + " pending script(s). "
                    + "Review the drift, then re-run with ignoreDrift enabled to proceed.");
        }

        OpExecutor executor = new OpExecutor(db);

        int appliedCount = 0;
        int skippedCount = envSkipped;
        if (envSkipped > 0) {
            warnings.add(envSkipped + " script(s) skipped because their `env` filter did not match `"
                    + (specEnv == null ? "<unset>" : specEnv) + "`.");
        }
        for (MigrationScript s : eligibleScripts) {
            if (ctx.stopping()) break;
            if (successful.contains(s.version())) {
                log.info("skip V{} (already applied)", s.version());
                skippedCount++;
                continue;
            }
            log.info("{} V{} — {}", dryRun ? "would apply" : "applying", s.version(), s.description());
            if (!dryRun) applied.markInProgress(s);
            long t0 = System.currentTimeMillis();
            try {
                if (!dryRun) executor.execute(s.ops());
                long dur = System.currentTimeMillis() - t0;
                if (!dryRun) applied.markSuccess(s, dur);
                ctx.metrics().addDocs(s.ops().size()); // coarse counter for UI
                appliedCount++;
            } catch (Exception e) {
                if (!dryRun) applied.markFailure(s, e);
                throw new RuntimeException(
                        "Script V" + s.version() + " failed: " + e.getMessage(), e);
            }
        }
        return new Summary(appliedCount, skippedCount, warnings, drift);
    }

    private static List<DriftEntry> detectDrift(List<MigrationScript> scripts,
                                                Map<String, String> appliedChecksums) {
        List<DriftEntry> out = new ArrayList<>();
        for (MigrationScript s : scripts) {
            String stored = appliedChecksums.get(s.version());
            if (stored != null && !stored.equals(s.checksum())) {
                out.add(new DriftEntry(s.version(), s.description(), stored, s.checksum()));
            }
        }
        return out;
    }

    private static int countPending(List<MigrationScript> scripts, Set<String> successful) {
        int n = 0;
        for (MigrationScript s : scripts) if (!successful.contains(s.version())) n++;
        return n;
    }

    public record DriftEntry(String version, String description,
                             String storedChecksum, String currentChecksum) {}

    public record Summary(int applied, int skipped, List<String> warnings, List<DriftEntry> drift) {}
}

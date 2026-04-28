package com.kubrik.mex.migration.versioned;

import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Runs the {@code U*} rollback scripts above a target version, newest-first, and marks
 *  their applied-migration rows {@code ROLLED_BACK} — audit rows are preserved (T-10).
 *  <p>
 *  See docs/mvp-technical-spec.md §7.4. */
public final class Rollback {

    private static final Logger log = LoggerFactory.getLogger(Rollback.class);

    public record Request(MongoDatabase targetDb, Path scriptsFolder, String toVersion, String user) {}

    public record Result(List<String> rolledBackVersions, List<String> warnings) {}

    public Result rollback(Request req) {
        ScriptRepo.ScanResult scan = new ScriptRepo().scan(req.scriptsFolder);
        if (scan.hasErrors()) {
            throw new IllegalStateException("Script repo errors: " + String.join("; ", scan.errors()));
        }

        AppliedMigrations applied = new AppliedMigrations(req.targetDb, "1.1.0", req.user);
        OpExecutor executor = new OpExecutor(req.targetDb);

        long toOrderKey = MigrationScript.computeOrderKey(req.toVersion);
        List<String> rolledBack = new ArrayList<>();
        List<String> warnings = new ArrayList<>(scan.warnings());

        for (Document row : applied.successfulAbove(toOrderKey)) {
            String version = row.getString("_id");
            MigrationScript u = scan.rollbacks().get(version);
            if (u == null) {
                throw new IllegalStateException(
                        "Version V" + version + " cannot be rolled back because `U" + version
                                + "__*.json` is missing.");
            }
            log.info("rolling back V{}", version);
            try {
                executor.execute(u.ops());
                applied.markRolledBack(version);
                rolledBack.add(version);
            } catch (Exception e) {
                // Record the failure on the rolled-back row and halt.
                applied.markFailure(new MigrationScript(version, 0L, "rollback", "", List.of(), null, null), e);
                throw new RuntimeException("Rollback of V" + version + " failed: " + e.getMessage(), e);
            }
        }
        return new Result(rolledBack, warnings);
    }

    /** Marker for convenience — some callers want to know the number of rolled-back scripts. */
    public static int count(Map<?, ?> m) { return m.size(); }
}

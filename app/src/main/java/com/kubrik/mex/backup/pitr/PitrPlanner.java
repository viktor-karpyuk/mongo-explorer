package com.kubrik.mex.backup.pitr;

import com.kubrik.mex.backup.store.BackupCatalogDao;
import com.kubrik.mex.backup.store.BackupCatalogRow;
import com.kubrik.mex.backup.store.BackupStatus;

import java.util.List;
import java.util.Optional;

/**
 * v2.5 Q2.5-F — point-in-time recovery planner built on the v2.4 oplog
 * window captured by {@link com.kubrik.mex.backup.runner.BackupRunner}.
 *
 * <p>Planning rule: pick the most-recent OK catalog row for the connection
 * whose {@code oplog_first_ts} is on or before the target ts AND whose
 * {@code oplog_last_ts} covers it. Ties break toward the newest started_at
 * so the oplog replay range is minimal. If no row qualifies, return a
 * refusal with a human-readable reason; the UI renders the refusal in amber
 * instead of a restore plan.</p>
 */
public final class PitrPlanner {

    private final BackupCatalogDao catalog;

    public PitrPlanner(BackupCatalogDao catalog) { this.catalog = catalog; }

    public RestorePlan plan(String connectionId, long targetEpochSec) {
        if (connectionId == null || connectionId.isBlank()) {
            return RestorePlan.refused("connectionId missing");
        }
        List<BackupCatalogRow> rows = catalog.listForConnection(connectionId, 500);
        if (rows.isEmpty()) {
            return RestorePlan.refused("no backups recorded for this connection");
        }

        Optional<BackupCatalogRow> best = rows.stream()
                .filter(r -> r.status() == BackupStatus.OK)
                .filter(r -> r.oplogFirstTs() != null && r.oplogLastTs() != null)
                .filter(r -> r.oplogFirstTs() <= targetEpochSec
                        && r.oplogLastTs() >= targetEpochSec)
                .findFirst();  // rows are newest-first from the DAO

        if (best.isPresent()) {
            return RestorePlan.of(best.get(), targetEpochSec);
        }

        // Diagnostic refusal: tell the user whether the target is in the
        // future vs. before the earliest oplog vs. inside a gap between
        // backups — or whether no backups carry an oplog window at all.
        long earliest = rows.stream()
                .filter(r -> r.oplogFirstTs() != null)
                .mapToLong(BackupCatalogRow::oplogFirstTs)
                .min().orElse(Long.MAX_VALUE);
        long latest = rows.stream()
                .filter(r -> r.oplogLastTs() != null)
                .mapToLong(BackupCatalogRow::oplogLastTs)
                .max().orElse(Long.MIN_VALUE);

        if (earliest == Long.MAX_VALUE || latest == Long.MIN_VALUE) {
            // No catalog row captured an oplog slice (includeOplog = false,
            // or pre-Q2.5-F backups). A PITR target is meaningless until a
            // fresh backup runs with oplog capture enabled.
            return RestorePlan.refused(
                    "no backup in the catalog captured an oplog window — "
                            + "enable includeOplog on the policy and run a fresh backup");
        }
        if (targetEpochSec < earliest) {
            return RestorePlan.refused(
                    "target " + targetEpochSec + " is older than the earliest oplog window ("
                            + earliest + ")");
        }
        if (targetEpochSec > latest) {
            return RestorePlan.refused(
                    "target " + targetEpochSec + " is newer than the latest oplog window ("
                            + latest + ") — take a fresh backup first");
        }
        return RestorePlan.refused(
                "target " + targetEpochSec + " falls in a gap between backup oplog windows");
    }
}

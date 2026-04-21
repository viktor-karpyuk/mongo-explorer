package com.kubrik.mex.backup.pitr;

import com.kubrik.mex.backup.store.BackupCatalogRow;

import java.util.Optional;

/**
 * v2.5 Q2.5-F — result of a PITR planning request. Either a concrete plan
 * (backup row + oplog limit) or a {@code refusal} explaining why no
 * recovery is possible to the target timestamp.
 */
public record RestorePlan(Optional<BackupCatalogRow> source, long oplogLimitTs,
                           String refusal) {

    public boolean feasible() { return source.isPresent(); }

    public static RestorePlan of(BackupCatalogRow source, long oplogLimitTs) {
        return new RestorePlan(Optional.of(source), oplogLimitTs, null);
    }

    public static RestorePlan refused(String reason) {
        return new RestorePlan(Optional.empty(), 0L, reason);
    }
}

package com.kubrik.mex.cluster.audit;

/**
 * v2.4 AUD-1..3 — a persisted row of {@code ops_audit}. Every field maps 1:1 to
 * a column in the {@code Database.migrate} schema. {@code id} is assigned on
 * insert; callers receive the populated record back from {@code OpsAuditDao.insert}.
 *
 * <p>The {@code commandJsonRedacted} field carries the exact server command
 * body the user saw during dry-run, with secrets scrubbed (driver connection
 * strings, passwords, KMS keys). {@code previewHash} is the SHA-256 over that
 * JSON so the audit trail can prove integrity between preview and dispatch
 * (see {@code PreviewHashChecker}).</p>
 */
public record OpsAuditRecord(
        long id,
        String connectionId,
        String db,
        String coll,
        String commandName,
        String commandJsonRedacted,
        String previewHash,
        Outcome outcome,
        String serverMessage,
        String roleUsed,
        long startedAt,
        Long finishedAt,
        Long latencyMs,
        String callerHost,
        String callerUser,
        String uiSource,
        boolean paste,
        boolean killSwitch
) {
    public OpsAuditRecord {
        if (connectionId == null || connectionId.isBlank())
            throw new IllegalArgumentException("connectionId");
        if (commandName == null || commandName.isBlank())
            throw new IllegalArgumentException("commandName");
        if (commandJsonRedacted == null)
            throw new IllegalArgumentException("commandJsonRedacted");
        if (previewHash == null || previewHash.isBlank())
            throw new IllegalArgumentException("previewHash");
        if (outcome == null) throw new IllegalArgumentException("outcome");
        if (uiSource == null || uiSource.isBlank())
            throw new IllegalArgumentException("uiSource");
    }

    /** Builder-style copy with a new {@code id}. Used by DAO insert paths. */
    public OpsAuditRecord withId(long newId) {
        return new OpsAuditRecord(newId, connectionId, db, coll, commandName,
                commandJsonRedacted, previewHash, outcome, serverMessage, roleUsed,
                startedAt, finishedAt, latencyMs, callerHost, callerUser, uiSource,
                paste, killSwitch);
    }
}

package com.kubrik.mex.backup.spec;

/**
 * v2.5 BKP-POLICY-1 — canonical record for a backup policy row. Names are
 * constrained by {@link PolicyValidator#NAME_PATTERN}; cron is optional
 * (null = manual-only run); {@code sinkId} references
 * {@code storage_sinks.id}. Scope / archive / retention ship as typed
 * records so the DAO's JSON round-trip is the only place the shape changes.
 */
public record BackupPolicy(
        long id,
        String connectionId,
        String name,
        boolean enabled,
        String scheduleCron,
        Scope scope,
        ArchiveSpec archive,
        RetentionSpec retention,
        long sinkId,
        boolean includeOplog,
        long createdAt,
        long updatedAt
) {
    public BackupPolicy {
        if (connectionId == null || connectionId.isBlank())
            throw new IllegalArgumentException("connectionId");
        if (name == null) throw new IllegalArgumentException("name");
        if (scope == null) throw new IllegalArgumentException("scope");
        if (archive == null) archive = ArchiveSpec.defaults();
        if (retention == null) retention = RetentionSpec.defaults();
    }

    public BackupPolicy withId(long newId) {
        return new BackupPolicy(newId, connectionId, name, enabled, scheduleCron,
                scope, archive, retention, sinkId, includeOplog, createdAt, updatedAt);
    }
}

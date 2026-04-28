package com.kubrik.mex.backup.spec;

/**
 * v2.5 BKP-POLICY-5 — how many backups / how far back to keep. The catalog
 * janitor applies the tighter of the two constraints so a 30-day / 60-count
 * policy with 100 backups older than 30 days retains only the newest 30
 * (age wins), while 400 backups in the last week retains only 60 (count wins).
 */
public record RetentionSpec(int maxCount, int maxAgeDays) {

    public RetentionSpec {
        if (maxCount < 1 || maxCount > 1000)
            throw new IllegalArgumentException("maxCount must be 1..1000, got " + maxCount);
        if (maxAgeDays < 1 || maxAgeDays > 3650)
            throw new IllegalArgumentException("maxAgeDays must be 1..3650, got " + maxAgeDays);
    }

    public static RetentionSpec defaults() {
        return new RetentionSpec(30, 30);
    }
}

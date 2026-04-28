package com.kubrik.mex.migration.resume;

import org.bson.BsonValue;

import java.util.Set;

/** Hydrated form of {@link ResumeFile} ready to drive the JobRunner on resume.
 *  A {@code null} instance means "fresh start". */
public record ResumeState(
        Set<String> completedCollections,
        String inProgressCollection,
        BsonValue inProgressLastId
) {
    public static ResumeState fresh() { return new ResumeState(Set.of(), null, null); }

    public boolean isCompleted(String sourceNs) { return completedCollections.contains(sourceNs); }
}

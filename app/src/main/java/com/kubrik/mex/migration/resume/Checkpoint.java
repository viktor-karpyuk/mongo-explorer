package com.kubrik.mex.migration.resume;

import org.bson.BsonValue;

/** Last successfully-written document in a partition. Persisted to {@code resume.json}. */
public record Checkpoint(BsonValue lastId, long docsWritten) {

    public static Checkpoint initial() {
        return new Checkpoint(null, 0L);
    }

    public Checkpoint advance(BsonValue newLastId, long addedDocs) {
        return new Checkpoint(newLastId, docsWritten + addedDocs);
    }
}

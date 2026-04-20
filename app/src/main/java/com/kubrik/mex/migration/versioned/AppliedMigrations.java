package com.kubrik.mex.migration.versioned;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.ReplaceOptions;
import org.bson.Document;

import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** CRUD over the {@code _mongo_explorer_migrations} collection living **inside each target
 *  database** (Option A from Q2 — docs/mvp-spec-review.md §7). Shape and status values are
 *  specified in docs/mvp-technical-spec.md §4.2. */
public final class AppliedMigrations {

    public static final String COLLECTION = "_mongo_explorer_migrations";
    public static final String STATUS_IN_PROGRESS = "IN_PROGRESS";
    public static final String STATUS_SUCCESS     = "SUCCESS";
    public static final String STATUS_FAILED      = "FAILED";
    public static final String STATUS_ROLLED_BACK = "ROLLED_BACK";

    private final MongoDatabase db;
    private final String engineVersion;
    private final String appliedBy;

    public AppliedMigrations(MongoDatabase db, String engineVersion, String appliedBy) {
        this.db = db;
        this.engineVersion = engineVersion;
        this.appliedBy = appliedBy;
    }

    /** Lazy-init the collection and indexes on first write. */
    private MongoCollection<Document> coll() {
        MongoCollection<Document> c = db.getCollection(COLLECTION);
        try {
            c.createIndex(Indexes.ascending("orderKey"),
                    new IndexOptions().name("orderKey_1"));
            c.createIndex(Indexes.compoundIndex(Indexes.ascending("status"),
                            Indexes.descending("appliedAt")),
                    new IndexOptions().name("status_appliedAt_-1"));
        } catch (Exception ignored) {
            // Indexes may already exist or user may lack createIndex on a replica set
            // secondary — either way, CRUD still works.
        }
        return c;
    }

    public Set<String> loadSuccessful() {
        Set<String> out = new HashSet<>();
        for (Document d : coll().find(Filters.eq("status", STATUS_SUCCESS))) {
            out.add(d.getString("_id"));
        }
        return out;
    }

    /** Read-only variant: returns stored checksums without lazy-initialising the collection or
     *  its indexes. Safe to call from preflight against a target that has not yet been migrated
     *  — a missing collection yields an empty map instead of being created as a side-effect. */
    public static Map<String, String> peekSuccessfulChecksums(MongoDatabase db) {
        Map<String, String> out = new HashMap<>();
        boolean present = false;
        for (String name : db.listCollectionNames()) {
            if (COLLECTION.equals(name)) { present = true; break; }
        }
        if (!present) return out;
        for (Document d : db.getCollection(COLLECTION).find(Filters.eq("status", STATUS_SUCCESS))) {
            String v = d.getString("_id");
            String c = d.getString("checksum");
            if (v != null && c != null) out.put(v, c);
        }
        return out;
    }

    /** Version → stored checksum for every {@code SUCCESS} row. Used by VER-4 drift detection
     *  to compare the on-disk script against what was applied earlier. */
    public Map<String, String> loadSuccessfulChecksums() {
        Map<String, String> out = new HashMap<>();
        for (Document d : coll().find(Filters.eq("status", STATUS_SUCCESS))) {
            String v = d.getString("_id");
            String c = d.getString("checksum");
            if (v != null && c != null) out.put(v, c);
        }
        return out;
    }

    public void markInProgress(MigrationScript s) {
        Document doc = new Document("_id", s.version())
                .append("orderKey", s.orderKey())
                .append("description", s.description())
                .append("checksum", s.checksum())
                .append("status", STATUS_IN_PROGRESS)
                .append("appliedAt", Date.from(Instant.now()))
                .append("appliedBy", appliedBy)
                .append("engineVersion", engineVersion);
        coll().replaceOne(Filters.eq("_id", s.version()), doc,
                new ReplaceOptions().upsert(true));
    }

    public void markSuccess(MigrationScript s, long executionMs) {
        coll().updateOne(Filters.eq("_id", s.version()),
                new Document("$set", new Document("status", STATUS_SUCCESS)
                        .append("executionMs", executionMs)
                        .append("appliedAt", Date.from(Instant.now()))));
    }

    public void markFailure(MigrationScript s, Throwable cause) {
        coll().updateOne(Filters.eq("_id", s.version()),
                new Document("$set", new Document("status", STATUS_FAILED)
                        .append("error", String.valueOf(cause.getMessage()))));
    }

    public void markRolledBack(String version) {
        coll().updateOne(Filters.eq("_id", version),
                new Document("$set", new Document("status", STATUS_ROLLED_BACK)
                        .append("rolledBackAt", Date.from(Instant.now()))
                        .append("rolledBackBy", appliedBy)));
    }

    /** All successful rows with orderKey > the given version's orderKey, sorted descending —
     *  the order they should be rolled back in. */
    public Iterable<Document> successfulAbove(long toOrderKey) {
        return coll()
                .find(Filters.and(
                        Filters.eq("status", STATUS_SUCCESS),
                        Filters.gt("orderKey", toOrderKey)))
                .sort(new Document("orderKey", -1));
    }
}

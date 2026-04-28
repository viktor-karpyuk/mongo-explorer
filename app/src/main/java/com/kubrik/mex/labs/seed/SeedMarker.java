package com.kubrik.mex.labs.seed;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.Document;

/**
 * v2.8.4 LAB-SEED-5 — Idempotency sentinel for the seed step.
 * A marker document is written to {@code <targetDb>._mex_labs}
 * on successful seed; the runner's first check on a restart
 * skips re-seeding.
 *
 * <p>Lives inside the Lab's volume so it's tombstoned by
 * {@code docker compose down -v}. A destroyed-then-recreated Lab
 * seeds from scratch.</p>
 */
public final class SeedMarker {

    public static final String COLL = "_mex_labs";
    public static final String MARKER_ID = "seeded";

    /** Fast check — opens a direct client, looks for the marker, and
     *  returns true if present. Swallows all exceptions as "no
     *  marker" so a transient connection error falls back to a
     *  re-seed attempt rather than silently skipping. */
    public boolean isSeeded(String uri, String targetDb) {
        try (MongoClient c = MongoClients.create(uri)) {
            Document d = c.getDatabase(targetDb)
                    .getCollection(COLL)
                    .find(new Document("_id", MARKER_ID))
                    .first();
            return d != null;
        } catch (Exception e) {
            return false;
        }
    }

    /** Stamp the marker. Called after the seed import completes. */
    public void markSeeded(String uri, String targetDb, String locator) {
        try (MongoClient c = MongoClients.create(uri)) {
            c.getDatabase(targetDb).getCollection(COLL)
                    .insertOne(new Document("_id", MARKER_ID)
                            .append("seeded_at", System.currentTimeMillis())
                            .append("locator", locator));
        } catch (com.mongodb.MongoWriteException dupe) {
            // Duplicate key = already seeded. Benign — two parallel
            // seed calls raced; the loser's write lost to the winner.
        }
    }
}

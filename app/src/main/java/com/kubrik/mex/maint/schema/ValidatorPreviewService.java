package com.kubrik.mex.maint.schema;

import com.kubrik.mex.maint.model.ValidatorSpec;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

/**
 * v2.7 SCHV-3 — Runs a proposed {@code $jsonSchema} validator
 * against a $sample of existing docs and reports which would fail.
 *
 * <p>Approach: server-side aggregate pipeline using {@code $match} +
 * {@code $nor} + {@code $jsonSchema}. This keeps the data on the
 * server and returns only the offending IDs + a short summary — no
 * need to stream whole docs across the wire when the user is just
 * assessing blast radius.</p>
 *
 * <p>Sample size defaults to 500 (per SCHV-3) but is configurable so
 * the wizard can bump it up for large collections when the operator
 * wants more confidence.</p>
 */
public final class ValidatorPreviewService {

    /** SCHV-3 default. */
    public static final int DEFAULT_SAMPLE_SIZE = 500;

    /** Max offenders returned to the UI — the list is for surfacing
     *  example failures, not for bulk export. */
    public static final int MAX_FIRST_FEW = 10;

    public ValidatorSpec.PreviewResult preview(MongoClient client,
                                               ValidatorSpec.Rollout rollout) {
        return preview(client, rollout, DEFAULT_SAMPLE_SIZE);
    }

    public ValidatorSpec.PreviewResult preview(MongoClient client,
                                               ValidatorSpec.Rollout rollout,
                                               int sampleSize) {
        MongoDatabase db = client.getDatabase(rollout.db());
        MongoCollection<Document> coll = db.getCollection(rollout.coll());

        Document proposed = Document.parse(rollout.proposedValidatorJson());
        Document schema = proposed.get("$jsonSchema", Document.class);
        if (schema == null) {
            // Raw-shape validator (e.g. uses $expr or field-level
            // predicates without $jsonSchema). Fall back to matching
            // the raw validator directly.
            schema = proposed;
        }

        // Stage 1: cap the work at sampleSize — $sample is uniform
        // random, cheap enough on a 1 M-doc collection.
        // Stage 2: find docs that DO NOT match the proposed schema.
        // Stage 3: return up to MAX_FIRST_FEW whole offenders. We used
        // to emit a server-side $toString summary but $toString refuses
        // object inputs (ConversionFailure error 241); rendering the
        // summary client-side is the reliable path.
        List<Document> pipeline = List.of(
                new Document("$sample", new Document("size", sampleSize)),
                new Document("$match", new Document("$nor",
                        List.of(new Document("$jsonSchema", schema)))),
                new Document("$limit", MAX_FIRST_FEW)
        );

        List<ValidatorSpec.FailedDoc> first = new ArrayList<>();
        for (Document d : coll.aggregate(pipeline)) {
            Object id = d.get("_id");
            String summary = d.toJson();
            if (summary.length() > 200) {
                summary = summary.substring(0, 200) + "…";
            }
            first.add(new ValidatorSpec.FailedDoc(
                    id == null ? "<null>" : id.toString(),
                    summary));
        }

        // Second pass to get the full count of failing sampled docs —
        // $count over a parallel pipeline. The two passes are both
        // bounded by sampleSize + the server's cache so the wall-time
        // stays predictable.
        List<Document> countPipeline = List.of(
                new Document("$sample", new Document("size", sampleSize)),
                new Document("$match", new Document("$nor",
                        List.of(new Document("$jsonSchema", schema)))),
                new Document("$count", "failed")
        );
        int failedCount = 0;
        for (Document d : coll.aggregate(countPipeline)) {
            Integer n = d.getInteger("failed");
            if (n != null) failedCount = n;
        }

        return new ValidatorSpec.PreviewResult(sampleSize, failedCount, first);
    }
}

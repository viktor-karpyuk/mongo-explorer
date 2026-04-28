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

        // Single-pass pipeline via $facet: one $sample feeds both the
        // offenders list AND the total count, so `firstFew` is always
        // a subset of the docs counted in `failedCount`. Earlier draft
        // ran two parallel pipelines with independent $samples — the
        // numbers didn't correspond and the server did 2x the work.
        //
        // Summary is still rendered client-side via Document.toJson()
        // because $toString refuses object inputs (ConversionFailure
        // error 241).
        Document facetSpec = new Document()
                .append("firstFew", List.of(
                        new Document("$limit", MAX_FIRST_FEW)))
                .append("total", List.of(
                        new Document("$count", "n")));
        List<Document> pipeline = List.of(
                new Document("$sample", new Document("size", sampleSize)),
                new Document("$match", new Document("$nor",
                        List.of(new Document("$jsonSchema", schema)))),
                new Document("$facet", facetSpec)
        );

        List<ValidatorSpec.FailedDoc> first = new ArrayList<>();
        int failedCount = 0;
        for (Document wrapper : coll.aggregate(pipeline)) {
            @SuppressWarnings("unchecked")
            List<Document> offenders = (List<Document>) wrapper.get(
                    "firstFew", List.class);
            if (offenders != null) {
                for (Document d : offenders) {
                    Object id = d.get("_id");
                    String summary = d.toJson();
                    if (summary.length() > 200) {
                        summary = summary.substring(0, 200) + "…";
                    }
                    first.add(new ValidatorSpec.FailedDoc(
                            id == null ? "<null>" : id.toString(),
                            summary));
                }
            }
            @SuppressWarnings("unchecked")
            List<Document> totals = (List<Document>) wrapper.get(
                    "total", List.class);
            if (totals != null && !totals.isEmpty()) {
                Integer n = totals.get(0).getInteger("n");
                if (n != null) failedCount = n;
            }
        }

        return new ValidatorSpec.PreviewResult(sampleSize, failedCount, first);
    }
}

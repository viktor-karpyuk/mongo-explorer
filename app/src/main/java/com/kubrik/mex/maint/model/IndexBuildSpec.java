package com.kubrik.mex.maint.model;

import org.bson.Document;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * v2.7 Q2.7-C — Input to the rolling index builder. Captures every
 * {@code createIndexes} option the wizard surfaces (IDX-BLD-2) plus
 * the target collection.
 */
public record IndexBuildSpec(
        String db,
        String coll,
        Document keys,
        String name,
        boolean unique,
        boolean sparse,
        Optional<Long> expireAfterSeconds,
        Optional<Document> partialFilterExpression,
        Optional<Document> collation,
        Optional<Document> weights,
        Optional<String> defaultStorageEngine
) {
    public IndexBuildSpec {
        Objects.requireNonNull(db, "db");
        Objects.requireNonNull(coll, "coll");
        Objects.requireNonNull(keys, "keys");
        Objects.requireNonNull(name, "name");
        if (name.isBlank()) throw new IllegalArgumentException("index name is blank");
        expireAfterSeconds = expireAfterSeconds == null
                ? Optional.empty() : expireAfterSeconds;
        partialFilterExpression = partialFilterExpression == null
                ? Optional.empty() : partialFilterExpression;
        collation = collation == null ? Optional.empty() : collation;
        weights = weights == null ? Optional.empty() : weights;
        defaultStorageEngine = defaultStorageEngine == null
                ? Optional.empty() : defaultStorageEngine;
    }

    /** Build the {@code indexSpec} document the server wants — same
     *  shape as the legacy {@code db.collection.createIndex} options
     *  block. */
    public Document toIndexSpecDocument() {
        Document d = new Document("key", keys).append("name", name);
        if (unique) d.append("unique", true);
        if (sparse) d.append("sparse", true);
        expireAfterSeconds.ifPresent(ttl -> d.append("expireAfterSeconds", ttl));
        partialFilterExpression.ifPresent(pfe -> d.append("partialFilterExpression", pfe));
        collation.ifPresent(c -> d.append("collation", c));
        weights.ifPresent(w -> d.append("weights", w));
        defaultStorageEngine.ifPresent(se -> d.append("storageEngine",
                new Document(se, new Document())));
        return d;
    }

    public List<Document> asCreateIndexesArgs() {
        return List.of(toIndexSpecDocument());
    }
}

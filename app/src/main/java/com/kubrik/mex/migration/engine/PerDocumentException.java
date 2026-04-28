package com.kubrik.mex.migration.engine;

import org.bson.BsonValue;

/** A single-document transform failure. Counted toward
 *  {@link com.kubrik.mex.migration.spec.ErrorPolicy} thresholds; never causes retry. */
public final class PerDocumentException extends RuntimeException {
    private final BsonValue docId;
    private final String field;

    public PerDocumentException(BsonValue docId, String field, String reason) {
        super("doc " + docId + " field `" + field + "`: " + reason);
        this.docId = docId;
        this.field = field;
    }

    public BsonValue docId() { return docId; }
    public String field() { return field; }
}

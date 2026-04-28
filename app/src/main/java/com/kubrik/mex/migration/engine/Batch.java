package com.kubrik.mex.migration.engine;

import org.bson.BsonValue;
import org.bson.RawBsonDocument;

import java.util.List;

/** A bounded group of documents flowing through the reader → transformer → writer pipeline.
 *  {@link #POISON} signals end-of-stream to downstream stages. */
public record Batch(List<RawBsonDocument> docs, long bytes, BsonValue lastId) {

    public static final Batch POISON = new Batch(List.of(), 0L, null);

    public boolean isPoison() { return this == POISON; }

    public int size() { return docs.size(); }
}

package com.kubrik.mex.migration.engine;

import com.mongodb.client.model.Filters;
import org.bson.BsonValue;
import org.bson.conversions.Bson;

/** An {@code _id} range for partitioned reads. A bound of {@code null} is unbounded on that side.
 *  The FULL partition means "no range filter". */
public record Partition(BsonValue minIdInclusive, BsonValue maxIdExclusive, int index) {

    public static final Partition FULL = new Partition(null, null, 0);

    public boolean isFull() {
        return minIdInclusive == null && maxIdExclusive == null;
    }

    /** Returns a Bson filter matching this partition; null if the partition is full. */
    public Bson toFilter() {
        if (minIdInclusive == null && maxIdExclusive == null) return null;
        if (minIdInclusive != null && maxIdExclusive != null) {
            return Filters.and(Filters.gte("_id", minIdInclusive), Filters.lt("_id", maxIdExclusive));
        }
        if (minIdInclusive != null) return Filters.gte("_id", minIdInclusive);
        return Filters.lt("_id", maxIdExclusive);
    }
}

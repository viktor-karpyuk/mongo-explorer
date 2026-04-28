package com.kubrik.mex.migration.spec;

import java.util.List;
import java.util.Map;

/** Per-collection transformation pipeline (XFORM-1…XFORM-3).
 *  All fields are nullable / may be empty. Stored as JSON maps so spec round-tripping
 *  via Jackson preserves field ordering for canonical serialisation (see ProfileCodec).
 */
public record TransformSpec(
        String filterJson,             // XFORM-2 — MongoDB query as JSON, nullable
        String projectionJson,         // XFORM-3 — MongoDB projection as JSON, nullable
        Map<String, String> rename,    // XFORM-1 — oldField → newField, possibly empty
        List<String> drop,             // XFORM-1 — field names to remove, possibly empty
        List<CastOp> cast,             // XFORM-1 — type casts, possibly empty
        String sourceAggregationJson   // XFORM-5 — source-side aggregation pipeline as JSON array
) {
    /** Back-compat ctor: TransformSpec before XFORM-5 did not carry the aggregation field. */
    public TransformSpec(String filterJson, String projectionJson,
                         Map<String, String> rename, List<String> drop, List<CastOp> cast) {
        this(filterJson, projectionJson, rename, drop, cast, null);
    }

    public static TransformSpec empty() {
        return new TransformSpec(null, null, Map.of(), List.of(), List.of(), null);
    }

    public boolean isEmpty() {
        return (filterJson == null || filterJson.isBlank())
                && (projectionJson == null || projectionJson.isBlank())
                && rename.isEmpty()
                && drop.isEmpty()
                && cast.isEmpty()
                && (sourceAggregationJson == null || sourceAggregationJson.isBlank());
    }

    /** XFORM-5 — when a source-side aggregation is declared, the reader bypasses
     *  partitioning and resume-after injection for that collection. */
    public boolean hasSourceAggregation() {
        return sourceAggregationJson != null && !sourceAggregationJson.isBlank();
    }
}

package com.kubrik.mex.migration;

import com.kubrik.mex.migration.spec.CastOp;
import com.kubrik.mex.migration.spec.TransformSpec;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class TransformSpecTest {

    @Test
    void sourceAggregationPredicateReflectsPresence() {
        TransformSpec none = new TransformSpec(null, null, Map.of(), List.of(), List.of());
        assertFalse(none.hasSourceAggregation());

        TransformSpec withAgg = new TransformSpec(null, null, Map.of(), List.of(), List.of(),
                "[{\"$match\":{\"active\":true}}]");
        assertTrue(withAgg.hasSourceAggregation());
        assertFalse(withAgg.isEmpty());
    }

    @Test
    void fiveArgConstructorIsBackCompat() {
        TransformSpec legacy = new TransformSpec("{}", null, Map.of(), List.of(), List.<CastOp>of());
        assertNull(legacy.sourceAggregationJson(),
                "pre-XFORM-5 callers must construct TransformSpec without the aggregation arg");
    }

    @Test
    void emptyPredicateCoversAllFields() {
        assertTrue(TransformSpec.empty().isEmpty());
        TransformSpec nonEmptyAgg = new TransformSpec(null, null, Map.of(), List.of(), List.of(),
                "[{\"$project\":{\"x\":1}}]");
        assertFalse(nonEmptyAgg.isEmpty());
    }
}

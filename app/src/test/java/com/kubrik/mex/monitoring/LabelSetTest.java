package com.kubrik.mex.monitoring;

import com.kubrik.mex.monitoring.model.LabelSet;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class LabelSetTest {

    @Test
    void emptyLabelSetHasCanonicalJson() {
        assertEquals("{}", LabelSet.EMPTY.toJson());
    }

    @Test
    void jsonIsSortedByKey() {
        // Input order is intentionally not sorted; canonical form must be.
        Map<String, String> in = new LinkedHashMap<>();
        in.put("shard", "s1");
        in.put("db", "prod");
        in.put("coll", "events");
        String json = new LabelSet(in).toJson();
        assertEquals("{\"coll\":\"events\",\"db\":\"prod\",\"shard\":\"s1\"}", json);
    }

    @Test
    void rejectsNonTopologyKeys() {
        // BR-8: labels may only carry topology identifiers.
        assertThrows(IllegalArgumentException.class,
                () -> LabelSet.of("password", "hunter2"));
        assertThrows(IllegalArgumentException.class,
                () -> LabelSet.of("filter", "{x:1}"));
    }

    @Test
    void escapesQuoteAndBackslashInValues() {
        LabelSet s = LabelSet.of("coll", "a\"b\\c");
        assertEquals("{\"coll\":\"a\\\"b\\\\c\"}", s.toJson());
    }
}

package com.kubrik.mex.maint.schema;

import com.kubrik.mex.maint.model.ValidatorSpec;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.7 Q2.7-B — Covers the pure parse helpers on {@link ValidatorFetcher}.
 * The live {@code listCollections} path is covered by the IT.
 */
class ValidatorFetcherTest {

    @Test
    void parseLevel_handles_every_wire_form() {
        assertEquals(ValidatorSpec.Level.STRICT, ValidatorFetcher.parseLevel("strict"));
        assertEquals(ValidatorSpec.Level.MODERATE, ValidatorFetcher.parseLevel("moderate"));
        assertEquals(ValidatorSpec.Level.OFF, ValidatorFetcher.parseLevel("off"));
        // Unknown values land on OFF — the safest default when we can't
        // classify a server reply. An alarm in the UI surfaces a mismatch.
        assertEquals(ValidatorSpec.Level.OFF, ValidatorFetcher.parseLevel("weird"));
    }

    @Test
    void parseAction_handles_every_wire_form() {
        assertEquals(ValidatorSpec.Action.WARN, ValidatorFetcher.parseAction("warn"));
        assertEquals(ValidatorSpec.Action.ERROR, ValidatorFetcher.parseAction("error"));
        // Anything unrecognised → error (fail closed).
        assertEquals(ValidatorSpec.Action.ERROR, ValidatorFetcher.parseAction("???"));
    }
}

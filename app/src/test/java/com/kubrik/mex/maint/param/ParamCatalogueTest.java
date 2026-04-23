package com.kubrik.mex.maint.param;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.7 Q2.7-F — Covers the public accessors on {@link ParamCatalogue}.
 * Each entry is exercised by {@link RecommenderTest} through the
 * recommender; this class just pins the registry.
 */
class ParamCatalogueTest {

    @Test
    void all_returns_the_five_curated_entries() {
        var names = ParamCatalogue.all().stream()
                .map(ParamCatalogue.Entry::name).toList();
        assertEquals(5, names.size());
        assertTrue(names.contains("wiredTigerConcurrentReadTransactions"));
        assertTrue(names.contains("notablescan"));
    }

    @Test
    void byName_is_case_sensitive_and_returns_the_match() {
        assertTrue(ParamCatalogue.byName("notablescan").isPresent());
        // Real mongod parameter names are camelCase — case mismatch is
        // a caller bug, not something the catalogue should paper over.
        assertTrue(ParamCatalogue.byName("NOTABLESCAN").isEmpty());
        assertTrue(ParamCatalogue.byName("does-not-exist").isEmpty());
    }

    @Test
    void range_present_for_numeric_entries() {
        var reads = ParamCatalogue.byName(
                "wiredTigerConcurrentReadTransactions").orElseThrow();
        assertTrue(reads.range().isPresent());
        assertTrue(reads.range().get().contains(128));
        assertFalse(reads.range().get().contains(0));
        assertFalse(reads.range().get().contains(10_000));
    }
}

package com.kubrik.mex.maint.index;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.7 IDX-BLD-5 — coverage for the pure
 * {@link BuildProgressTailer.Progress#fraction()} accessor. Live
 * $currentOp polling lives in the IT rig.
 */
class BuildProgressTailerTest {

    @Test
    void fraction_is_NaN_when_total_unknown() {
        BuildProgressTailer.Progress p = new BuildProgressTailer.Progress(
                1, /*totalDocs=*/0, /*processedDocs=*/100, "IXBUILD", 1000);
        assertTrue(Double.isNaN(p.fraction()),
                "total=0 → fraction undefined so the UI shows an "
                        + "indeterminate bar, not a spurious 0%");
    }

    @Test
    void fraction_clamps_to_1_when_processed_exceeds_total() {
        BuildProgressTailer.Progress p = new BuildProgressTailer.Progress(
                1, /*total=*/100, /*processed=*/150, "IXBUILD", 1000);
        assertEquals(1.0, p.fraction());
    }

    @Test
    void fraction_mid_build() {
        BuildProgressTailer.Progress p = new BuildProgressTailer.Progress(
                1, /*total=*/400, /*processed=*/100, "IXBUILD", 1000);
        assertEquals(0.25, p.fraction());
    }
}

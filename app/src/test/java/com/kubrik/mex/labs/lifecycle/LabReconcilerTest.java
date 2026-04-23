package com.kubrik.mex.labs.lifecycle;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.8.4 LAB-LIFECYCLE-5 — covers the pure parse helpers on
 * {@link LabReconciler}. The live compose-ls path is covered by
 * a cross-platform smoke suite that needs Docker.
 */
class LabReconcilerTest {

    @Test
    void parseStatus_classifies_common_compose_ls_strings() {
        // `docker compose ls` Status column examples.
        assertEquals(LabReconciler.ComposeStatus.RUNNING,
                LabReconciler.parseStatus("running(3)"));
        assertEquals(LabReconciler.ComposeStatus.RUNNING,
                LabReconciler.parseStatus("running"));
        assertEquals(LabReconciler.ComposeStatus.STOPPED,
                LabReconciler.parseStatus("exited(1)"));
        assertEquals(LabReconciler.ComposeStatus.STOPPED,
                LabReconciler.parseStatus("created"));
        assertEquals(LabReconciler.ComposeStatus.UNKNOWN,
                LabReconciler.parseStatus(null));
        assertEquals(LabReconciler.ComposeStatus.UNKNOWN,
                LabReconciler.parseStatus("paused(2)"));
    }
}

package com.kubrik.mex.k8s.teardown;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CascadePlanTest {

    @Test
    void prod_defaults_keep_data() {
        CascadePlan p = CascadePlan.prodDefaults();
        assertTrue(p.deleteCr());
        assertFalse(p.deleteSecrets());
        assertFalse(p.deletePvcs());
    }

    @Test
    void dev_defaults_wipe_everything() {
        CascadePlan p = CascadePlan.devDefaults();
        assertTrue(p.deleteCr());
        assertTrue(p.deleteSecrets());
        assertTrue(p.deletePvcs());
    }

    @Test
    void summary_lists_selected_actions() {
        assertEquals("delete CR", CascadePlan.prodDefaults().summary());
        assertEquals("delete CR, delete Secrets, delete PVCs",
                CascadePlan.devDefaults().summary());
    }

    @Test
    void no_op_summary_when_everything_off() {
        assertEquals("no-op",
                new CascadePlan(false, false, false).summary());
    }
}

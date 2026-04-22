package com.kubrik.mex.maint.compact;

import com.kubrik.mex.maint.model.CompactSpec;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CompactRunnerTest {

    @Test
    void primary_host_match_refused_client_side() {
        assertTrue(CompactRunner.wouldTargetPrimary("h1:27017", "h1:27017"));
        assertFalse(CompactRunner.wouldTargetPrimary("h2:27017", "h1:27017"));
    }

    @Test
    void compact_spec_requires_at_least_one_collection() {
        assertThrows(IllegalArgumentException.class,
                () -> new CompactSpec.Compact("h2:27017", "app",
                        List.of(), false, false));
    }

    @Test
    void resync_spec_rejects_null_host() {
        assertThrows(NullPointerException.class,
                () -> new CompactSpec.Resync(null, true));
    }
}

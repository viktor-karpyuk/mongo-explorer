package com.kubrik.mex.monitoring;

import com.kubrik.mex.monitoring.sampler.BackoffTracker;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class BackoffTrackerTest {

    @Test
    void startsInNormalMode() {
        BackoffTracker b = new BackoffTracker();
        assertFalse(b.isBackedOff());
        assertEquals(Duration.ofSeconds(1), b.effectiveInterval(Duration.ofSeconds(1)));
    }

    @Test
    void backsOffWhenAnyRecentPollExceedsThreshold() {
        BackoffTracker b = new BackoffTracker();
        for (int i = 0; i < 5; i++) b.record(20);
        assertFalse(b.isBackedOff());
        b.record(150); // over SLOW_THRESHOLD_MS
        assertTrue(b.isBackedOff());
        assertEquals(Duration.ofSeconds(2), b.effectiveInterval(Duration.ofSeconds(1)));
    }

    @Test
    void recoversAfterThreeFastPolls() {
        BackoffTracker b = new BackoffTracker();
        b.record(200);
        assertTrue(b.isBackedOff());
        b.record(40);
        assertTrue(b.isBackedOff());
        b.record(40);
        assertTrue(b.isBackedOff());
        b.record(40);
        assertFalse(b.isBackedOff(), "three consecutive <50ms polls must clear back-off");
    }

    @Test
    void aSlowPollMidRecoveryResetsTheStreak() {
        BackoffTracker b = new BackoffTracker();
        b.record(200);
        b.record(40);
        b.record(40);
        b.record(60); // slow enough to reset the fast streak
        b.record(40);
        b.record(40);
        assertTrue(b.isBackedOff(), "only a full streak of three fast polls clears back-off");
        b.record(40);
        assertFalse(b.isBackedOff());
    }
}

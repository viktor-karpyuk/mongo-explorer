package com.kubrik.mex.monitoring;

import com.kubrik.mex.monitoring.alerting.AlertRule;
import com.kubrik.mex.monitoring.alerting.Comparator;
import com.kubrik.mex.monitoring.alerting.SustainTracker;
import com.kubrik.mex.monitoring.model.MetricId;
import com.kubrik.mex.monitoring.model.Severity;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class SustainTrackerTest {

    private final AlertRule rule = new AlertRule(
            "r1", null, MetricId.WT_3, Map.of(),
            Comparator.GT, 0.9, 0.97, Duration.ofSeconds(10),
            true, null);

    @Test
    void firstBreachEntersPendingDoesNotFire() {
        SustainTracker s = new SustainTracker();
        var t = s.observe(rule, Map.of(), Severity.WARN, 1_000);
        assertNotNull(t);
        assertFalse(t.fired());
        assertEquals(SustainTracker.State.WARN_PENDING, t.to());
    }

    @Test
    void sustainedBreachFiresAfterWindow() {
        SustainTracker s = new SustainTracker();
        s.observe(rule, Map.of(), Severity.WARN, 1_000);
        var t = s.observe(rule, Map.of(), Severity.WARN, 1_000 + 10_000); // exactly at sustain
        assertNotNull(t);
        assertTrue(t.fired());
        assertEquals(SustainTracker.State.WARN, t.to());
    }

    @Test
    void recoveryClearsState() {
        SustainTracker s = new SustainTracker();
        s.observe(rule, Map.of(), Severity.WARN, 1_000);
        s.observe(rule, Map.of(), Severity.WARN, 12_000);
        var cleared = s.observe(rule, Map.of(), Severity.OK, 13_000);
        assertNotNull(cleared);
        assertTrue(cleared.cleared());
    }

    @Test
    void critOverridesPendingWarn() {
        SustainTracker s = new SustainTracker();
        s.observe(rule, Map.of(), Severity.WARN, 1_000);
        var toCrit = s.observe(rule, Map.of(), Severity.CRIT, 2_000);
        assertNotNull(toCrit);
        assertEquals(SustainTracker.State.CRIT_PENDING, toCrit.to());
    }
}

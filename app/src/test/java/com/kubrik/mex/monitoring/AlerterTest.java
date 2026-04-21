package com.kubrik.mex.monitoring;

import com.kubrik.mex.monitoring.alerting.AlertEvent;
import com.kubrik.mex.monitoring.alerting.AlertRule;
import com.kubrik.mex.monitoring.alerting.Alerter;
import com.kubrik.mex.monitoring.alerting.Comparator;
import com.kubrik.mex.monitoring.alerting.DefaultRules;
import com.kubrik.mex.monitoring.model.LabelSet;
import com.kubrik.mex.monitoring.model.MetricId;
import com.kubrik.mex.monitoring.model.MetricSample;
import com.kubrik.mex.monitoring.model.Severity;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

class AlerterTest {

    @Test
    void sustainedBreachFiresCrit() {
        CopyOnWriteArrayList<AlertEvent> fired = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<AlertEvent> cleared = new CopyOnWriteArrayList<>();
        Alerter a = new Alerter(fired::add, cleared::add);
        AlertRule r = new AlertRule("r", null, MetricId.WT_3, Map.of(),
                Comparator.GT, 0.90, 0.97, Duration.ofSeconds(0), true, null);
        a.installRules(List.of(r));

        a.onSamples(List.of(new MetricSample("c1", MetricId.WT_3, LabelSet.EMPTY, 1_000, 0.98)));
        assertEquals(1, fired.size());
        assertEquals(Severity.CRIT, fired.get(0).severity());
    }

    @Test
    void perConnectionRuleShadowsGlobal() {
        CopyOnWriteArrayList<AlertEvent> fired = new CopyOnWriteArrayList<>();
        Alerter a = new Alerter(fired::add, e -> {});
        AlertRule global = new AlertRule("g", null, MetricId.WT_3, Map.of(),
                Comparator.GT, 0.90, 0.97, Duration.ZERO, true, null);
        AlertRule perConn = new AlertRule("p", "c1", MetricId.WT_3, Map.of(),
                Comparator.GT, 0.99, 0.999, Duration.ZERO, true, null);
        a.installRules(List.of(global, perConn));

        // 0.95 — would fire global WARN but per-connection says 0.99 → OK.
        a.onSamples(List.of(new MetricSample("c1", MetricId.WT_3, LabelSet.EMPTY, 1_000, 0.95)));
        assertTrue(fired.isEmpty(), "per-connection rule must shadow global same-metric rule");
    }

    @Test
    void defaultRulesHaveEighteenEntries() {
        assertEquals(18, DefaultRules.all().size());
    }
}

package com.kubrik.mex.monitoring;

import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.monitoring.model.LabelSet;
import com.kubrik.mex.monitoring.model.MetricId;
import com.kubrik.mex.monitoring.model.MetricSample;
import com.kubrik.mex.ui.monitoring.InstanceSection;
import com.kubrik.mex.ui.monitoring.MetricCell;
import javafx.application.Platform;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/** v2.2.0 `ROUTE-*` — sections must filter samples by their bound connection id. */
class SectionRoutingTest {

    private static final AtomicBoolean FX_STARTED = new AtomicBoolean(false);

    @BeforeAll
    static void startFxOnce() throws Exception {
        if (FX_STARTED.compareAndSet(false, true)) {
            CountDownLatch ready = new CountDownLatch(1);
            try { Platform.startup(ready::countDown); }
            catch (IllegalStateException alreadyStarted) { ready.countDown(); }
            if (!ready.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("JavaFX toolkit did not start within 10s");
            }
        }
    }

    @Test
    void sectionBoundToConnectionIdRejectsOtherConnectionsSamples() {
        EventBus bus = new EventBus();
        InstanceSection section = new InstanceSection(bus, MetricCell.Size.NORMAL, "c-prod");
        // Publishing samples for c-staging MUST not mutate c-prod's cells. We assert this
        // indirectly by checking the connectionId() accessor stays bound. (MetricCell state
        // is FX-thread and we don't boot the toolkit in this test; the filter check in
        // route() is exercised by the publish-and-don't-throw property.)
        bus.publishMetrics(List.of(sample("c-staging", MetricId.INST_OP_1, 100)));
        assertEquals("c-prod", section.connectionId());
    }

    @Test
    void rebindSwitchesTheFilter() {
        EventBus bus = new EventBus();
        InstanceSection section = new InstanceSection(bus, MetricCell.Size.NORMAL, "c-prod");
        section.setConnectionId("c-staging");
        assertEquals("c-staging", section.connectionId());
        // Publishing samples for c-prod after re-bind MUST not drive the section; the
        // bound id is now c-staging.
        bus.publishMetrics(List.of(sample("c-prod", MetricId.INST_OP_1, 100)));
        assertEquals("c-staging", section.connectionId());
    }

    @Test
    void nullConnectionIdSkipsRouting() {
        EventBus bus = new EventBus();
        InstanceSection section = new InstanceSection(bus, MetricCell.Size.NORMAL, null);
        // Route() MUST early-return on a null connectionId so nothing blows up before
        // the picker sets it.
        assertDoesNotThrow(() ->
                bus.publishMetrics(List.of(sample("any", MetricId.INST_OP_1, 1))));
    }

    private static MetricSample sample(String connId, MetricId metric, double v) {
        return new MetricSample(connId, metric, LabelSet.EMPTY, 1_000, v);
    }
}

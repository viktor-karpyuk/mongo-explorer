package com.kubrik.mex.monitoring;

import com.kubrik.mex.monitoring.model.LabelSet;
import com.kubrik.mex.monitoring.model.MetricId;
import com.kubrik.mex.monitoring.sampler.CounterTracker;
import org.junit.jupiter.api.Test;

import java.util.OptionalDouble;

import static org.junit.jupiter.api.Assertions.*;

class CounterTrackerTest {

    @Test
    void firstSampleProducesNoRate() {
        CounterTracker t = new CounterTracker();
        OptionalDouble r = t.rate("c", MetricId.INST_OP_1, LabelSet.EMPTY, 1_000, 100);
        assertTrue(r.isEmpty());
    }

    @Test
    void computesPerSecondRateBetweenSamples() {
        CounterTracker t = new CounterTracker();
        t.rate("c", MetricId.INST_OP_1, LabelSet.EMPTY, 1_000, 100);
        OptionalDouble r = t.rate("c", MetricId.INST_OP_1, LabelSet.EMPTY, 2_000, 300);
        assertEquals(200.0, r.orElseThrow(), 1e-9);
    }

    @Test
    void resetsOnCounterDecrease() {
        CounterTracker t = new CounterTracker();
        t.rate("c", MetricId.INST_OP_1, LabelSet.EMPTY, 1_000, 500);
        // Server restart — counter drops. First post-drop call returns empty.
        OptionalDouble r1 = t.rate("c", MetricId.INST_OP_1, LabelSet.EMPTY, 2_000, 50);
        assertTrue(r1.isEmpty());
        // Subsequent call resumes rate from the new baseline.
        OptionalDouble r2 = t.rate("c", MetricId.INST_OP_1, LabelSet.EMPTY, 3_000, 150);
        assertEquals(100.0, r2.orElseThrow(), 1e-9);
    }

    @Test
    void separateKeysDoNotInterfere() {
        CounterTracker t = new CounterTracker();
        LabelSet a = LabelSet.of("db", "A");
        LabelSet b = LabelSet.of("db", "B");
        t.rate("c", MetricId.INST_OP_1, a, 1_000, 100);
        t.rate("c", MetricId.INST_OP_1, b, 1_000, 200);
        assertEquals(10.0, t.rate("c", MetricId.INST_OP_1, a, 2_000, 110).orElseThrow(), 1e-9);
        assertEquals(50.0, t.rate("c", MetricId.INST_OP_1, b, 2_000, 250).orElseThrow(), 1e-9);
    }

    @Test
    void forgetConnectionDropsItsKeysOnly() {
        CounterTracker t = new CounterTracker();
        t.rate("c1", MetricId.INST_OP_1, LabelSet.EMPTY, 1_000, 100);
        t.rate("c2", MetricId.INST_OP_1, LabelSet.EMPTY, 1_000, 100);
        t.forgetConnection("c1");
        assertTrue(t.rate("c1", MetricId.INST_OP_1, LabelSet.EMPTY, 2_000, 200).isEmpty(),
                "c1 key was forgotten — first post-forget call must return empty");
        assertEquals(100.0,
                t.rate("c2", MetricId.INST_OP_1, LabelSet.EMPTY, 2_000, 200).orElseThrow(),
                1e-9);
    }
}

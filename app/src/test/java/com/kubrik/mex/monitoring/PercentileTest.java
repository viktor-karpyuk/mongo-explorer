package com.kubrik.mex.monitoring;

import com.kubrik.mex.monitoring.util.Percentile;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PercentileTest {

    @Test
    void handlesEmptyArray() {
        assertTrue(Double.isNaN(Percentile.of(new double[0], 0.5)));
    }

    @Test
    void singleElementReturnsItself() {
        assertEquals(42.0, Percentile.of(new double[] { 42.0 }, 0.99));
    }

    @Test
    void p50OfSortedArrayIsMedian() {
        double[] v = { 1, 2, 3, 4, 5 };
        assertEquals(3.0, Percentile.of(v, 0.5), 1e-9);
    }

    @Test
    void p99InterpolatesBetweenTopTwo() {
        double[] v = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        double p = Percentile.of(v, 0.99);
        assertTrue(p > 9.5 && p < 10.0, "expected ~9.91, got " + p);
    }
}

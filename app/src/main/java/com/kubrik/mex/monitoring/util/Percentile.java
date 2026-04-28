package com.kubrik.mex.monitoring.util;

import java.util.Arrays;

/**
 * Linear-interpolation percentile over a sorted double array. Small enough that
 * an HdrHistogram dependency isn't justified at rollup-batch sizes (≤ 100 samples
 * per window).
 */
public final class Percentile {

    private Percentile() {}

    /** {@code pct} in [0.0, 1.0]. {@code values} is mutated (sorted) in-place. */
    public static double of(double[] values, double pct) {
        if (values.length == 0) return Double.NaN;
        if (values.length == 1) return values[0];
        Arrays.sort(values);
        double idx = pct * (values.length - 1);
        int lo = (int) Math.floor(idx);
        int hi = (int) Math.ceil(idx);
        if (lo == hi) return values[lo];
        double w = idx - lo;
        return values[lo] * (1 - w) + values[hi] * w;
    }
}

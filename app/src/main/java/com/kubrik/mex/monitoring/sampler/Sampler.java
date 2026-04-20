package com.kubrik.mex.monitoring.sampler;

import com.kubrik.mex.monitoring.model.MetricSample;

import java.time.Instant;
import java.util.List;

/**
 * Produces a burst of {@link MetricSample}s from one MongoDB diagnostic command.
 * One sampler instance is bound to a single connection. The {@link SamplerScheduler}
 * invokes {@link #sample(Instant)} from a dedicated virtual thread.
 *
 * <p>Implementations must be side-effect free (no writes to the cluster, per BR-1)
 * and must never throw — errors are communicated via the returned list being empty
 * and rethrowing the underlying {@code MongoException} so the scheduler can record
 * it and apply SAFE-3 back-off.
 */
public interface Sampler {
    SamplerKind kind();

    /** Connection id this sampler belongs to. */
    String connectionId();

    /** Run one poll and return zero-or-more samples. */
    List<MetricSample> sample(Instant now) throws Exception;
}

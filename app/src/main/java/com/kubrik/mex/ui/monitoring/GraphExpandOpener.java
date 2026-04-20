package com.kubrik.mex.ui.monitoring;

import com.kubrik.mex.monitoring.model.MetricId;

/**
 * Top-level opener for the expand-metric flow. Implemented by the host view
 * ({@code MainView}) which owns the {@code TabPane} + {@code Window}. Sections
 * adapt this to a {@link MetricExpander} by binding the active connection id.
 */
@FunctionalInterface
public interface GraphExpandOpener {
    void open(MetricId metric, String connectionId, MetricExpander.Mode mode);
}

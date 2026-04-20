package com.kubrik.mex.ui.monitoring;

import com.kubrik.mex.monitoring.model.MetricId;

/**
 * Opens the expand view for a single metric (GRAPH-EXPAND-*). Implementations
 * live at app-tab scope — modal opens a non-blocking {@code Stage}, tab opens an
 * app-level tab in the main {@code TabPane}. Sections receive a
 * connection-id-aware adapter of the top-level {@link GraphExpandOpener}; cells
 * only see this simpler contract.
 */
@FunctionalInterface
public interface MetricExpander {

    enum Mode { MODAL, TAB }

    void open(MetricId metric, Mode mode);
}

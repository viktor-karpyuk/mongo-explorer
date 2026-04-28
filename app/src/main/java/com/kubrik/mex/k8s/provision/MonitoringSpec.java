package com.kubrik.mex.k8s.provision;

/**
 * v2.8.1 Q2.8.1-D1 — ServiceMonitor / Prometheus scrape toggles.
 *
 * <p>Prod defaults to {@code true}; Dev/Test opt-in. A {@code
 * scrape} = true on a cluster that has no Prometheus Operator
 * installed is a pre-flight WARN, not a FAIL — the CR is still
 * valid, the ServiceMonitor just sits unattended until somebody
 * installs the operator.</p>
 */
public record MonitoringSpec(boolean serviceMonitor) {

    public static MonitoringSpec devDefaults() { return new MonitoringSpec(false); }
    public static MonitoringSpec prodDefaults() { return new MonitoringSpec(true); }
}

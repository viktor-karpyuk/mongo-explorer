package com.kubrik.mex.monitoring.alerting;

import com.kubrik.mex.monitoring.model.Severity;

import java.util.Map;

/** A firing or cleared alert event. */
public record AlertEvent(
        String id,
        String ruleId,
        String connectionId,
        Severity severity,
        long firedAtMs,
        Long clearedAtMs,      // null while still active
        double valueAtFire,
        Map<String, String> labels,
        String message
) {}

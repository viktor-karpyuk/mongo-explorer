package com.kubrik.mex.monitoring.alerting;

import com.kubrik.mex.monitoring.model.LabelSet;
import com.kubrik.mex.monitoring.model.MetricId;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;

/**
 * A single alerting rule bound to one metric. Connection-scoped when
 * {@code connectionId != null}; otherwise global (applies to every connection
 * unless a more-specific rule overrides it).
 */
public record AlertRule(
        String id,
        String connectionId,           // null = global default
        MetricId metric,
        Map<String, String> labelFilter, // subset match against sample labels; empty = any
        Comparator comparator,
        Double warnThreshold,
        Double critThreshold,
        Duration sustain,
        boolean enabled,
        String source                  // "default-v2.1.0" for the shipped defaults; null otherwise
) {
    public AlertRule {
        Objects.requireNonNull(id);
        Objects.requireNonNull(metric);
        Objects.requireNonNull(comparator);
        Objects.requireNonNull(sustain);
        labelFilter = labelFilter == null ? Map.of() : Map.copyOf(labelFilter);
    }

    /** Does the provided {@link LabelSet} match this rule's label filter? */
    public boolean matches(LabelSet lbl) {
        for (var e : labelFilter.entrySet()) {
            if (!e.getValue().equals(lbl.labels().get(e.getKey()))) return false;
        }
        return true;
    }
}

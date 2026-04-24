package com.kubrik.mex.k8s.compute;

import java.util.Objects;

/**
 * v2.8.2 Q2.8.2-A — Pod toleration entry.
 *
 * <p>Renders 1:1 to a {@code spec.tolerations[]} entry on the Mongo
 * pod spec. Operator-specific adapters lift this into the operator's
 * CR — MCO's {@code spec.statefulSet.spec.template.spec.tolerations},
 * PSMDB's per-replset {@code podSpec.tolerations}, etc.</p>
 *
 * <p>v2.8.2 scope: {@code NoSchedule} and {@code NoExecute} effects
 * (open question 2 of milestone-v2.8.2.md). {@code PreferNoSchedule}
 * is deferred because the "proceed anyway" semantics don't match
 * the Mongo-dedicated promise.</p>
 */
public record Toleration(String key, String value, Effect effect) {

    public enum Effect { NO_SCHEDULE, NO_EXECUTE }

    public Toleration {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(effect, "effect");
    }

    public String effectYamlValue() {
        return switch (effect) {
            case NO_SCHEDULE -> "NoSchedule";
            case NO_EXECUTE -> "NoExecute";
        };
    }
}

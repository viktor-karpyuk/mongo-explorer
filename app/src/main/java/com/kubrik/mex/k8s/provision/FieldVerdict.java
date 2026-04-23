package com.kubrik.mex.k8s.provision;

import java.util.Objects;
import java.util.Optional;

/**
 * v2.8.1 Q2.8.1-D2 — The wizard UI's answer to "what should this
 * field look like?"
 *
 * <p>Produced by {@link ProfileEnforcer#verdict} per {@code
 * (model, fieldId)}. The wizard renders a control as editable /
 * required / locked based on the verdict, and shows the rationale
 * as a tooltip so the user understands <em>why</em> Prod locks a
 * setting.</p>
 *
 * <p>{@code lockedValue} is the canonical value the Prod contract
 * demands; when {@code required} is true without a lock, the user
 * must pick a value themselves (e.g. storage class).</p>
 */
public record FieldVerdict(
        boolean required,
        Optional<Object> lockedValue,
        Optional<Object> defaultValue,
        Optional<String> rationale) {

    public FieldVerdict {
        Objects.requireNonNull(lockedValue, "lockedValue");
        Objects.requireNonNull(defaultValue, "defaultValue");
        Objects.requireNonNull(rationale, "rationale");
    }

    public static FieldVerdict optional() {
        return new FieldVerdict(false, Optional.empty(), Optional.empty(), Optional.empty());
    }

    public static FieldVerdict requiredWith(Object dflt, String why) {
        return new FieldVerdict(true, Optional.empty(),
                Optional.ofNullable(dflt), Optional.of(why));
    }

    public static FieldVerdict locked(Object value, String why) {
        return new FieldVerdict(true, Optional.of(value), Optional.of(value),
                Optional.of(why));
    }

    public boolean isLocked() { return lockedValue.isPresent(); }
}

package com.kubrik.mex.k8s.rollout;

import java.util.Objects;
import java.util.Optional;

/**
 * v2.8.1 Q2.8.1-H — One streamed event from the rollout pipeline.
 *
 * <p>Sources:</p>
 * <ul>
 *   <li>{@code POD} — Kubernetes Pod event (e.g. Scheduled, Pulled,
 *       Started).</li>
 *   <li>{@code PVC} — PersistentVolumeClaim event (e.g. ProvisioningFailed).</li>
 *   <li>{@code CR_STATUS} — operator's {@code status} subresource transition.</li>
 *   <li>{@code APPLY} — synthetic event the orchestrator emits at each
 *       dependency-ordered apply step.</li>
 * </ul>
 *
 * <p>{@code diagnosisHint} is populated by {@link DiagnosisEngine}
 * when a pattern matches — the raw reason/message are preserved
 * verbatim so operators can correlate with {@code kubectl events}.</p>
 */
public record RolloutEvent(
        long provisioningId,
        long at,
        Source source,
        Severity severity,
        Optional<String> reason,
        Optional<String> message,
        Optional<String> diagnosisHint) {

    public enum Source { POD, PVC, CR_STATUS, APPLY }
    public enum Severity { INFO, WARN, ERROR }

    public RolloutEvent {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(severity, "severity");
        Objects.requireNonNull(reason, "reason");
        Objects.requireNonNull(message, "message");
        Objects.requireNonNull(diagnosisHint, "diagnosisHint");
    }

    public RolloutEvent withDiagnosis(String hint) {
        return new RolloutEvent(provisioningId, at, source, severity,
                reason, message, Optional.ofNullable(hint));
    }
}

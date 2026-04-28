package com.kubrik.mex.k8s.rollout;

import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * v2.8.1 Q2.8.1-H — Pattern-table decorator: raw pod / PVC / CR
 * event → human-readable hint.
 *
 * <p>A small table of regex patterns + fixed advice strings. Covers
 * the failures we see most often in the blessed matrix:
 * ImagePullBackOff, PVC stuck Pending, CrashLoopBackOff, webhook
 * rejection, etc.</p>
 *
 * <p>New patterns are added by editing the list — the engine is a
 * pure function of {@link RolloutEvent} with no external state so
 * it's trivially testable.</p>
 */
public final class DiagnosisEngine {

    /** A pattern fires when EITHER the reason or message matches. */
    public record Pattern_(Pattern regex, String hint, RolloutEvent.Source scope) {}

    public static final List<Pattern_> DEFAULT_PATTERNS = List.of(
            new Pattern_(Pattern.compile("ImagePull(BackOff|Err)"),
                    "Image pull failed. Check the operator's imagePullSecret + network egress; private registries need a Secret.",
                    RolloutEvent.Source.POD),
            new Pattern_(Pattern.compile("ErrImagePull"),
                    "Image pull failed. Verify the image tag exists in your registry.",
                    RolloutEvent.Source.POD),
            new Pattern_(Pattern.compile("CrashLoopBackOff"),
                    "Pod is crash-looping. Inspect container logs; common causes: bad config, unmet resource request, missing volume.",
                    RolloutEvent.Source.POD),
            new Pattern_(Pattern.compile("(?i)ProvisioningFailed"),
                    "PVC binding failed. StorageClass may be missing or not default; also check IAM / CSI driver.",
                    RolloutEvent.Source.PVC),
            new Pattern_(Pattern.compile("(?i)pod has unbound immediate PersistentVolumeClaims"),
                    "Pod is waiting for its PVC. StorageClass needs to provision the PV — verify the class exists.",
                    RolloutEvent.Source.POD),
            new Pattern_(Pattern.compile("(?i)FailedScheduling"),
                    "Scheduler couldn't place the pod. Node capacity / taints / affinity — check events for the specific predicate.",
                    RolloutEvent.Source.POD),
            new Pattern_(Pattern.compile("(?i)admission webhook.*denied"),
                    "Admission webhook rejected the CR. Usually a policy-engine failure — check the webhook's rejection reason.",
                    RolloutEvent.Source.CR_STATUS),
            new Pattern_(Pattern.compile("(?i)no Issuer found"),
                    "cert-manager Issuer missing. Re-check the Issuer name in TLS step; pre-flight should have caught this.",
                    RolloutEvent.Source.CR_STATUS),
            new Pattern_(Pattern.compile("(?i)OOMKilled"),
                    "Container was OOM-killed. Raise memory limit or optimise the workload.",
                    RolloutEvent.Source.POD),
            new Pattern_(Pattern.compile("(?i)replset initialized"),
                    "Replica set initialized — PRIMARY election in progress.",
                    RolloutEvent.Source.CR_STATUS));

    private final List<Pattern_> patterns;

    public DiagnosisEngine() { this(DEFAULT_PATTERNS); }

    DiagnosisEngine(List<Pattern_> patterns) { this.patterns = List.copyOf(patterns); }

    /** Decorate an event with a hint. Returns the original event when nothing matches. */
    public RolloutEvent decorate(RolloutEvent e) {
        String reason = e.reason().orElse("");
        String msg = e.message().orElse("");
        for (Pattern_ p : patterns) {
            if (p.scope() != e.source()) continue;
            if (p.regex().matcher(reason).find() || p.regex().matcher(msg).find()) {
                return e.withDiagnosis(p.hint());
            }
        }
        return e;
    }

    public Optional<String> diagnose(RolloutEvent.Source source,
                                        String reason, String message) {
        String r = reason == null ? "" : reason;
        String m = message == null ? "" : message;
        for (Pattern_ p : patterns) {
            if (p.scope() != source) continue;
            if (p.regex().matcher(r).find() || p.regex().matcher(m).find()) {
                return Optional.of(p.hint());
            }
        }
        return Optional.empty();
    }
}

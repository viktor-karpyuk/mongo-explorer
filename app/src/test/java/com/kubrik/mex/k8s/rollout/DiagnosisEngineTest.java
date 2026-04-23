package com.kubrik.mex.k8s.rollout;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class DiagnosisEngineTest {

    private final DiagnosisEngine engine = new DiagnosisEngine();

    @Test
    void image_pull_backoff_matches_on_reason() {
        Optional<String> hint = engine.diagnose(RolloutEvent.Source.POD,
                "ImagePullBackOff", "back-off pulling image");
        assertTrue(hint.isPresent());
        assertTrue(hint.get().contains("Image pull"));
    }

    @Test
    void crashloop_on_pod_scope_matches() {
        Optional<String> hint = engine.diagnose(RolloutEvent.Source.POD,
                "CrashLoopBackOff", "restart count 8");
        assertTrue(hint.isPresent());
        assertTrue(hint.get().contains("crash"));
    }

    @Test
    void pvc_provisioning_failure_matches_only_on_pvc_scope() {
        Optional<String> hint = engine.diagnose(RolloutEvent.Source.PVC,
                "ProvisioningFailed", "failed to provision volume");
        assertTrue(hint.isPresent());

        // Same message under POD scope — shouldn't match because the
        // pattern is scoped to PVC.
        Optional<String> noHint = engine.diagnose(RolloutEvent.Source.POD,
                "ProvisioningFailed", "failed to provision volume");
        assertFalse(noHint.isPresent(),
                "pattern scope must prevent cross-source matches");
    }

    @Test
    void webhook_denial_matches_on_cr_status_scope() {
        Optional<String> hint = engine.diagnose(RolloutEvent.Source.CR_STATUS,
                "Invalid", "admission webhook \"validation.myorg.com\" denied the request");
        assertTrue(hint.isPresent());
        assertTrue(hint.get().toLowerCase().contains("webhook"));
    }

    @Test
    void no_match_returns_empty() {
        assertTrue(engine.diagnose(RolloutEvent.Source.POD,
                "Scheduled", "Pod assigned to node").isEmpty());
    }

    @Test
    void decorate_stamps_hint_on_matching_event() {
        RolloutEvent raw = new RolloutEvent(1L, 0L,
                RolloutEvent.Source.POD, RolloutEvent.Severity.ERROR,
                Optional.of("ImagePullBackOff"),
                Optional.of("back-off pulling"),
                Optional.empty());
        RolloutEvent decorated = engine.decorate(raw);
        assertTrue(decorated.diagnosisHint().isPresent());
    }

    @Test
    void decorate_leaves_non_matching_event_alone() {
        RolloutEvent raw = new RolloutEvent(1L, 0L,
                RolloutEvent.Source.POD, RolloutEvent.Severity.INFO,
                Optional.of("Scheduled"),
                Optional.of("assigned"),
                Optional.empty());
        RolloutEvent decorated = engine.decorate(raw);
        assertTrue(decorated.diagnosisHint().isEmpty());
    }
}

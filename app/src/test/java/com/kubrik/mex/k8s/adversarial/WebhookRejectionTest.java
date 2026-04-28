package com.kubrik.mex.k8s.adversarial;

import com.kubrik.mex.k8s.rollout.DiagnosisEngine;
import com.kubrik.mex.k8s.rollout.RolloutEvent;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.8.1 Q2.8.1-L — Adversarial: a webhook that rejects the CR
 * should land in the rollout viewer with a readable diagnosis,
 * not a raw API exception dump.
 */
class WebhookRejectionTest {

    private final DiagnosisEngine engine = new DiagnosisEngine();

    @Test
    void webhook_denial_message_from_api_server_gets_a_hint() {
        Optional<String> hint = engine.diagnose(RolloutEvent.Source.CR_STATUS,
                "Invalid",
                "admission webhook \"policy.kyverno.io\" denied the request: "
                + "validation error: require tls.enabled=true");
        assertTrue(hint.isPresent(),
                "webhook rejections must produce a readable diagnosis");
        assertTrue(hint.get().toLowerCase().contains("webhook"));
    }

    @Test
    void noise_in_the_webhook_message_doesnt_block_match() {
        Optional<String> hint = engine.diagnose(RolloutEvent.Source.CR_STATUS,
                "Invalid",
                "stuff\nadmission webhook blah denied the request\nmore stuff");
        assertTrue(hint.isPresent());
    }

    @Test
    void a_success_message_never_matches_the_denial_pattern() {
        Optional<String> hint = engine.diagnose(RolloutEvent.Source.CR_STATUS,
                "Ready",
                "admission webhook approved, admission webhook accepted");
        // "denied" not in the message — should NOT trigger the rejection hint.
        assertTrue(hint.isEmpty(),
                "acceptance messages shouldn't fire the rejection hint");
    }
}

package com.kubrik.mex.cluster.model;

import java.util.List;

/**
 * v2.4 HEALTH-1..6 — 0..100 score derived from a {@link TopologySnapshot} with
 * a human-readable breakdown of any points subtracted.
 *
 * <p>{@code band} maps the score to the UI pill treatment: ≥ 90 {@code GREEN},
 * 70–89 {@code AMBER}, &lt; 70 {@code RED}. Standalone clusters bypass the
 * weighted decomposition and always return 100 / {@code GREEN} or 0 /
 * {@code RED}.</p>
 */
public record HealthScore(int score, Band band, List<String> negatives) {

    public HealthScore {
        if (score < 0 || score > 100) throw new IllegalArgumentException("score");
        if (band == null) throw new IllegalArgumentException("band");
        negatives = negatives == null ? List.of() : List.copyOf(negatives);
    }

    public enum Band { GREEN, AMBER, RED;
        public static Band forScore(int score) {
            if (score >= 90) return GREEN;
            if (score >= 70) return AMBER;
            return RED;
        }
    }

    public static HealthScore of(int score, List<String> negatives) {
        return new HealthScore(score, Band.forScore(score), negatives);
    }
}

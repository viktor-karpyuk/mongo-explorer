package com.kubrik.mex.cluster.service;

import com.kubrik.mex.cluster.model.ClusterKind;
import com.kubrik.mex.cluster.model.HealthScore;
import com.kubrik.mex.cluster.model.Member;
import com.kubrik.mex.cluster.model.MemberState;
import com.kubrik.mex.cluster.model.Shard;
import com.kubrik.mex.cluster.model.TopologySnapshot;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * v2.4 HEALTH-2 — weighted health scorer.
 *
 * <pre>
 *   primary present           → +40
 *   quorum (majority reachable)→ +30
 *   all lag &lt; 10 s             → +15
 *   version consistent         → +10
 *   no unreachable shards      → +5
 * </pre>
 *
 * <p>Every subtracted point appears in
 * {@link HealthScore#negatives()} as a plain-English sentence.</p>
 */
public final class HealthScorer {

    public static final int W_PRIMARY     = 40;
    public static final int W_QUORUM      = 30;
    public static final int W_LAG         = 15;
    public static final int W_VERSION     = 10;
    public static final int W_SHARDS      = 5;
    public static final long LAG_THRESHOLD_MS = 10_000L;

    private HealthScorer() {}

    public static HealthScore score(TopologySnapshot snap) {
        if (snap == null) return new HealthScore(0, HealthScore.Band.RED, List.of("No topology sample yet."));
        if (snap.clusterKind() == ClusterKind.STANDALONE) return scoreStandalone(snap);
        List<String> negatives = new ArrayList<>();
        int score = 0;

        score += scorePrimary(allMembers(snap), negatives);
        score += scoreQuorum(allMembers(snap), negatives);
        score += scoreLag(allMembers(snap), negatives);
        score += scoreVersionConsistency(snap, negatives);
        score += scoreShards(snap, negatives);

        return HealthScore.of(score, negatives);
    }

    /* --------------------------- components ---------------------------- */

    private static HealthScore scoreStandalone(TopologySnapshot snap) {
        // Standalone has a degenerate decomposition: 100 if any member is known non-DOWN, else 0.
        for (Member m : snap.members()) {
            if (m.state() != MemberState.DOWN && m.state() != MemberState.UNKNOWN) {
                return new HealthScore(100, HealthScore.Band.GREEN, List.of());
            }
        }
        return new HealthScore(0, HealthScore.Band.RED, List.of("Standalone node is unreachable."));
    }

    private static int scorePrimary(List<Member> members, List<String> neg) {
        for (Member m : members) if (m.isPrimary()) return W_PRIMARY;
        neg.add("No primary elected.");
        return 0;
    }

    private static int scoreQuorum(List<Member> members, List<String> neg) {
        int voting = 0, healthy = 0;
        for (Member m : members) {
            Integer v = m.votes();
            int vv = v == null ? (m.isArbiter() || m.isPrimary() || m.isSecondary() ? 1 : 0) : v;
            if (vv > 0) {
                voting += vv;
                if (m.state() == MemberState.PRIMARY || m.state() == MemberState.SECONDARY
                        || m.state() == MemberState.ARBITER) healthy += vv;
            }
        }
        if (voting == 0) { neg.add("No voting members visible."); return 0; }
        int majority = (voting / 2) + 1;
        if (healthy >= majority) return W_QUORUM;
        neg.add("Quorum not met: " + healthy + " of " + voting + " voting members reachable.");
        return 0;
    }

    private static int scoreLag(List<Member> members, List<String> neg) {
        boolean allGood = true;
        for (Member m : members) {
            Long lag = m.lagMs();
            if (m.isSecondary() && lag != null && lag > LAG_THRESHOLD_MS) {
                neg.add("Secondary " + m.host() + " is " + (lag / 1000) + " s behind primary.");
                allGood = false;
            }
        }
        return allGood ? W_LAG : 0;
    }

    private static int scoreVersionConsistency(TopologySnapshot snap, List<String> neg) {
        Set<String> versions = new HashSet<>();
        for (Member m : allMembers(snap)) {
            // Version per member is not in the snapshot today; we compare against cluster version only.
            // When Q2.4-A's sharding follow-up lands, this widens to per-member versions.
        }
        // With only cluster-level version available we treat it as consistent. Placeholder
        // for the per-member comparison that v2.4-A follow-up adds.
        if (snap.version() == null || snap.version().isBlank()) {
            neg.add("Cluster version unreported.");
            return 0;
        }
        return W_VERSION;
    }

    private static int scoreShards(TopologySnapshot snap, List<String> neg) {
        if (snap.clusterKind() != ClusterKind.SHARDED) return W_SHARDS;
        boolean allReachable = true;
        for (Shard s : snap.shards()) {
            boolean anyUp = false;
            for (Member m : s.members()) {
                if (m.state() == MemberState.PRIMARY || m.state() == MemberState.SECONDARY) {
                    anyUp = true;
                    break;
                }
            }
            if (!anyUp && !s.members().isEmpty()) {
                neg.add("Shard " + s.id() + " has no reachable members.");
                allReachable = false;
            }
        }
        return allReachable ? W_SHARDS : 0;
    }

    /** Flattens REPLSET members + per-shard members into a single list for aggregate questions. */
    private static List<Member> allMembers(TopologySnapshot snap) {
        if (snap.clusterKind() != ClusterKind.SHARDED) return snap.members();
        List<Member> out = new ArrayList<>();
        for (Shard s : snap.shards()) out.addAll(s.members());
        return out;
    }
}

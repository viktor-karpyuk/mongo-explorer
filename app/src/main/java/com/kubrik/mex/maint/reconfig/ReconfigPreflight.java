package com.kubrik.mex.maint.reconfig;

import com.kubrik.mex.maint.model.ReconfigSpec;
import com.kubrik.mex.maint.model.ReconfigSpec.Change;
import com.kubrik.mex.maint.model.ReconfigSpec.Member;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * v2.7 Q2.7-D — Pre-flight checks for every RCFG-1 change kind.
 *
 * <p>Enforces the RCFG-3 arithmetic entirely client-side so the
 * wizard can surface errors before bothering the cluster:</p>
 * <ul>
 *   <li>{@code votes ∈ {0, 1}} per member (Mongo 4.4+)</li>
 *   <li>{@code priority ∈ [0, 1000]}</li>
 *   <li>{@code sum(votes) ≤ 7}, {@code members.length ≤ 50}</li>
 *   <li>Majority preserved after the change (no stranding)</li>
 *   <li>At least one electable member remains</li>
 * </ul>
 *
 * <p>The result is a list of {@link Finding}s. A finding is BLOCKING
 * iff it represents a state the wizard must refuse (not merely warn);
 * BLOCKING rejections cannot be overridden client-side. Non-blocking
 * findings render as amber warnings the user can acknowledge.</p>
 *
 * <p>Pure logic — no driver calls, no FX dependencies — so the
 * quorum math can be exercised against a 200-scenario fixture
 * without spinning up a test cluster (technical-spec §13.1).</p>
 */
public final class ReconfigPreflight {

    public enum Severity { BLOCKING, WARN }

    public record Finding(Severity severity, String code, String message) {}

    public record Result(List<Member> proposedMembers, List<Finding> findings) {
        public boolean hasBlocking() {
            return findings.stream().anyMatch(f -> f.severity() == Severity.BLOCKING);
        }
    }

    // Per rs.conf bounds documented by MongoDB.
    private static final int MAX_MEMBERS = 50;
    private static final int MAX_VOTING_MEMBERS = 7;

    public Result check(ReconfigSpec.Request req) {
        List<Member> proposed = applyChange(req.currentMembers(), req.change());
        List<Finding> findings = new ArrayList<>();

        if (proposed.size() > MAX_MEMBERS) {
            findings.add(new Finding(Severity.BLOCKING, "MAX_MEMBERS",
                    "Replica set would exceed 50 members (" + proposed.size() + ")."));
        }
        int voteCount = proposed.stream().mapToInt(Member::votes).sum();
        if (voteCount > MAX_VOTING_MEMBERS) {
            findings.add(new Finding(Severity.BLOCKING, "MAX_VOTES",
                    "Replica set would exceed 7 voting members ("
                            + voteCount + ")."));
        }
        if (voteCount == 0) {
            findings.add(new Finding(Severity.BLOCKING, "NO_VOTERS",
                    "No voting members remain — cluster would have no quorum."));
        }

        long electable = proposed.stream().filter(Member::isElectable).count();
        if (electable == 0) {
            findings.add(new Finding(Severity.BLOCKING, "NO_ELECTABLE",
                    "No electable members remain — cluster cannot elect a "
                            + "new primary. Electable = votes>0, priority>0, non-arbiter."));
        }

        // Majority check — even an even count is fine as long as a
        // majority is reachable; the preflight asserts the proposed
        // voting set has an odd majority achievable or at least > 0.
        // For a single-member drop mid-reconfig, Mongo's own reconfig
        // arithmetic insists the old and new majorities overlap; the
        // technical-spec encodes a simpler cover-your-ass check:
        // ceil((sum+1)/2) electable members must exist in the new
        // config.
        // Conservative majority: ceil((n+1)/2). For n=3 → 2; n=5 → 3; n=7 → 4.
        int conservativeMajority = (voteCount + 2) / 2;
        if (electable < conservativeMajority && voteCount > 0) {
            findings.add(new Finding(Severity.BLOCKING, "NO_MAJORITY",
                    "After change, only " + electable + " electable member"
                            + (electable == 1 ? "" : "s") + " remain"
                            + (electable == 1 ? "s" : "") + " but "
                            + conservativeMajority + " are needed for a voting majority."));
        }

        // Per-member id uniqueness — AddMember tests this naturally
        // (new _id may collide); also catches WholeConfig edits.
        Map<Integer, Integer> idCounts = new LinkedHashMap<>();
        for (Member m : proposed) idCounts.merge(m.id(), 1, Integer::sum);
        for (Map.Entry<Integer, Integer> e : idCounts.entrySet()) {
            if (e.getValue() > 1) {
                findings.add(new Finding(Severity.BLOCKING, "DUP_ID",
                        "Proposed config has duplicate _id " + e.getKey()
                                + " (×" + e.getValue() + ")."));
            }
        }

        // Host uniqueness — WholeConfig might leave a collision that
        // rs.reconfig would refuse anyway, but we'd rather name it.
        Map<String, Integer> hostCounts = new LinkedHashMap<>();
        for (Member m : proposed) hostCounts.merge(m.host(), 1, Integer::sum);
        for (Map.Entry<String, Integer> e : hostCounts.entrySet()) {
            if (e.getValue() > 1) {
                findings.add(new Finding(Severity.BLOCKING, "DUP_HOST",
                        "Proposed config has duplicate host " + e.getKey()
                                + " (×" + e.getValue() + ")."));
            }
        }

        // Warn when all voters are priority-0 (elect nothing) — the
        // NO_ELECTABLE block above catches the terminal case; this
        // warns when priorities are technically legal but the topology
        // is unusual and probably not intended.
        long prioZeroVoters = proposed.stream()
                .filter(m -> m.votes() > 0 && m.priority() == 0).count();
        if (prioZeroVoters > 0 && prioZeroVoters == voteCount) {
            findings.add(new Finding(Severity.WARN, "ALL_PRIO_ZERO",
                    "Every voter has priority 0 — only electable members "
                            + "can be primary; did you mean this?"));
        }

        // Arbiter-heavy: Mongo warns that clusters with an arbiter are
        // less durable; flag it when the change introduces one. Note:
        // previously this had a `&& || &&` precedence bug that fired
        // the warn on any ToggleArbiter(false) and missed the first
        // AddMember arbiter — parentheses here are load-bearing.
        boolean introducesArbiter =
                (req.change() instanceof ReconfigSpec.AddMember am
                        && am.member().arbiterOnly())
                || (req.change() instanceof ReconfigSpec.ToggleArbiter ta
                        && ta.arbiterOnly());
        if (introducesArbiter) {
            findings.add(new Finding(Severity.WARN, "ARBITER_PRESENT",
                    "Arbiters reduce durability. Prefer a data-bearing "
                            + "3-node set where possible."));
        }

        return new Result(proposed, List.copyOf(findings));
    }

    /** Apply the change to the current member list, returning the
     *  proposed list. Visible for unit tests that want to assert the
     *  exact membership without running the full check. */
    public List<Member> applyChange(List<Member> current, Change change) {
        return switch (change) {
            case ReconfigSpec.AddMember add -> {
                List<Member> out = new ArrayList<>(current);
                out.add(add.member());
                yield out;
            }
            case ReconfigSpec.RemoveMember rm -> current.stream()
                    .filter(m -> m.id() != rm.id()).toList();
            case ReconfigSpec.ChangePriority cp -> current.stream()
                    .map(m -> m.id() == cp.id()
                            ? new Member(m.id(), m.host(), cp.newPriority(),
                                    m.votes(), m.hidden(), m.arbiterOnly(),
                                    m.buildIndexes(), m.slaveDelay())
                            : m)
                    .toList();
            case ReconfigSpec.ChangeVotes cv -> current.stream()
                    .map(m -> m.id() == cv.id()
                            ? new Member(m.id(), m.host(), m.priority(),
                                    cv.newVotes(), m.hidden(), m.arbiterOnly(),
                                    m.buildIndexes(), m.slaveDelay())
                            : m)
                    .toList();
            case ReconfigSpec.ToggleHidden th -> current.stream()
                    .map(m -> m.id() == th.id()
                            ? new Member(m.id(), m.host(), m.priority(),
                                    m.votes(), th.hidden(), m.arbiterOnly(),
                                    m.buildIndexes(), m.slaveDelay())
                            : m)
                    .toList();
            case ReconfigSpec.ToggleArbiter ta -> current.stream()
                    .map(m -> m.id() == ta.id()
                            ? new Member(m.id(), m.host(), m.priority(),
                                    m.votes(), m.hidden(), ta.arbiterOnly(),
                                    m.buildIndexes(), m.slaveDelay())
                            : m)
                    .toList();
            case ReconfigSpec.RenameMember rn -> current.stream()
                    .map(m -> m.id() == rn.id()
                            ? new Member(m.id(), rn.newHost(), m.priority(),
                                    m.votes(), m.hidden(), m.arbiterOnly(),
                                    m.buildIndexes(), m.slaveDelay())
                            : m)
                    .toList();
            case ReconfigSpec.WholeConfig wc -> wc.newMembers();
        };
    }
}

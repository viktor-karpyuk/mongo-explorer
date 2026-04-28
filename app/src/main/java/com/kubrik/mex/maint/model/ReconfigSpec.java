package com.kubrik.mex.maint.model;

import java.util.List;
import java.util.Objects;

/**
 * v2.7 Q2.7-D — Typed, parsed shape of a proposed {@code rs.reconfig}
 * change. The wizard funnels every RCFG-1 kind into one of the
 * {@link Change} variants; the preflight runs on the variant, and the
 * runner serializes it into a BSON {@code rs.reconfig} payload.
 *
 * <p>Separating the spec from the raw BSON keeps preflight unit-
 * testable without a MongoDB fixture, keeps the UI wizard decoupled
 * from the driver API, and lets us version the spec independently of
 * the server's rs.reconfig format (which differs slightly across
 * 4.4–7.0 per technical-spec §15).</p>
 */
public final class ReconfigSpec {

    /** Minimal view of a replica-set member as rendered by
     *  {@code rs.conf()}. Voting + priority drive the preflight math;
     *  the remaining flags affect which preview copy we render. */
    public record Member(
            int id,
            String host,
            int priority,
            int votes,
            boolean hidden,
            boolean arbiterOnly,
            boolean buildIndexes,
            double slaveDelay
    ) {
        public Member {
            Objects.requireNonNull(host, "host");
            if (priority < 0 || priority > 1000)
                throw new IllegalArgumentException("priority must be in [0, 1000]");
            if (votes < 0 || votes > 1)
                throw new IllegalArgumentException("votes must be 0 or 1");
        }

        /** Electable = voter that can actually run for primary. An
         *  arbiter votes but is never electable; priority-0 voters
         *  never win an election either. Used by {@link #hasMajorityAfter}. */
        public boolean isElectable() {
            return votes > 0 && priority > 0 && !arbiterOnly;
        }
    }

    /** One of the RCFG-1 change kinds. Sealed so the preflight's
     *  switch is exhaustive — a new kind lands as a compile error
     *  everywhere that dispatches on this. */
    public sealed interface Change permits
            AddMember, RemoveMember, ChangePriority, ChangeVotes,
            ToggleHidden, ToggleArbiter, RenameMember, WholeConfig {}

    public record AddMember(Member member) implements Change {
        public AddMember { Objects.requireNonNull(member, "member"); }
    }

    public record RemoveMember(int id) implements Change {}

    public record ChangePriority(int id, int newPriority) implements Change {
        public ChangePriority {
            if (newPriority < 0 || newPriority > 1000)
                throw new IllegalArgumentException("priority must be in [0, 1000]");
        }
    }

    public record ChangeVotes(int id, int newVotes) implements Change {
        public ChangeVotes {
            if (newVotes < 0 || newVotes > 1)
                throw new IllegalArgumentException("votes must be 0 or 1");
        }
    }

    public record ToggleHidden(int id, boolean hidden) implements Change {}

    public record ToggleArbiter(int id, boolean arbiterOnly) implements Change {}

    public record RenameMember(int id, String newHost) implements Change {
        public RenameMember {
            Objects.requireNonNull(newHost, "newHost");
            if (newHost.isBlank())
                throw new IllegalArgumentException("newHost is blank");
        }
    }

    /** Escape-hatch for reviewers who need to edit the whole config
     *  as JSON. The wizard warns loudly before exposing this. */
    public record WholeConfig(List<Member> newMembers) implements Change {
        public WholeConfig {
            newMembers = List.copyOf(newMembers);
        }
    }

    /** The current config + the proposed change. The preflight's input;
     *  the runner's serializer reads both to build the new {@code rs.conf}
     *  document. */
    public record Request(
            String replicaSetName,
            int currentConfigVersion,
            List<Member> currentMembers,
            Change change
    ) {
        public Request {
            Objects.requireNonNull(replicaSetName, "replicaSetName");
            Objects.requireNonNull(change, "change");
            currentMembers = List.copyOf(currentMembers);
        }
    }

    private ReconfigSpec() {}
}

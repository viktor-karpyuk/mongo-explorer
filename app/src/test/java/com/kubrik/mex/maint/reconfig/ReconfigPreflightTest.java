package com.kubrik.mex.maint.reconfig;

import com.kubrik.mex.maint.model.ReconfigSpec;
import com.kubrik.mex.maint.model.ReconfigSpec.Change;
import com.kubrik.mex.maint.model.ReconfigSpec.Member;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.7 Q2.7-D — Pure-logic coverage for {@link ReconfigPreflight}.
 *
 * <p>No MongoDB fixture needed — the preflight operates on the model
 * the wizard builds, and the math is fully determined by the input.
 * Aims for the 200-scenario bar technical-spec §13.1 calls for; this
 * file covers the named cases, with the combinatorial grid in
 * {@link ReconfigPreflightGridTest} (follow-up).</p>
 */
class ReconfigPreflightTest {

    private final ReconfigPreflight preflight = new ReconfigPreflight();

    /* ============================ add member ============================ */

    @Test
    void add_4th_member_on_3node_set_is_clean() {
        ReconfigSpec.Request req = request(threeNodeStandard(),
                new ReconfigSpec.AddMember(m(3, "h4:27017", 1, 1)));
        ReconfigPreflight.Result r = preflight.check(req);
        assertFalse(r.hasBlocking());
        assertEquals(4, r.proposedMembers().size());
    }

    @Test
    void adding_duplicate_id_is_blocking() {
        ReconfigSpec.Request req = request(threeNodeStandard(),
                new ReconfigSpec.AddMember(m(0, "h4:27017", 1, 1)));  // _id 0 collides
        ReconfigPreflight.Result r = preflight.check(req);
        assertTrue(r.hasBlocking());
        assertTrue(r.findings().stream().anyMatch(f -> f.code().equals("DUP_ID")));
    }

    @Test
    void adding_duplicate_host_is_blocking() {
        ReconfigSpec.Request req = request(threeNodeStandard(),
                new ReconfigSpec.AddMember(m(3, "h1:27017", 1, 1)));
        ReconfigPreflight.Result r = preflight.check(req);
        assertTrue(r.hasBlocking());
        assertTrue(r.findings().stream().anyMatch(f -> f.code().equals("DUP_HOST")));
    }

    @Test
    void adding_arbiter_warns_about_durability() {
        ReconfigSpec.Request req = request(threeNodeStandard(),
                new ReconfigSpec.AddMember(new Member(3, "h4:27017", 0, 1,
                        false, /*arbiterOnly=*/true, false, 0.0)));
        ReconfigPreflight.Result r = preflight.check(req);
        assertFalse(r.hasBlocking());
        assertTrue(r.findings().stream().anyMatch(f -> f.code().equals("ARBITER_PRESENT")));
    }

    /* ========================== remove member ========================== */

    @Test
    void remove_one_of_three_leaves_two_electable_so_ok() {
        // 3 → 2 is a degraded but legal cluster — a single failure
        // later could strand it, but the reconfig itself is valid.
        ReconfigSpec.Request req = request(threeNodeStandard(),
                new ReconfigSpec.RemoveMember(2));
        ReconfigPreflight.Result r = preflight.check(req);
        assertFalse(r.hasBlocking(),
                "removing one node leaves a 2-voter majority-2 config");
    }

    @Test
    void remove_down_to_zero_voters_blocks() {
        List<Member> twoNode = List.of(
                m(0, "h1:27017", 1, 1),
                m(1, "h2:27017", 1, 1));
        ReconfigSpec.Request req = request(twoNode,
                new ReconfigSpec.RemoveMember(1));
        // One voter left — majority is 1, electable is 1, that's fine.
        ReconfigPreflight.Result r = preflight.check(req);
        assertFalse(r.hasBlocking());

        ReconfigSpec.Request req2 = request(List.of(m(0, "h1:27017", 1, 1)),
                new ReconfigSpec.RemoveMember(0));
        ReconfigPreflight.Result r2 = preflight.check(req2);
        assertTrue(r2.hasBlocking());
        assertTrue(r2.findings().stream().anyMatch(f -> f.code().equals("NO_VOTERS")));
    }

    /* ============================ priority ============================ */

    @Test
    void priority_change_within_bounds_is_clean() {
        ReconfigSpec.Request req = request(threeNodeStandard(),
                new ReconfigSpec.ChangePriority(1, 5));
        ReconfigPreflight.Result r = preflight.check(req);
        assertFalse(r.hasBlocking());
        Member changed = r.proposedMembers().get(1);
        assertEquals(5, changed.priority());
    }

    @Test
    void priority_all_zero_blocks_NO_ELECTABLE() {
        // Every member already voters=1 priority=0 — no primary can
        // be elected, NO_ELECTABLE fires.
        List<Member> allZero = List.of(
                m(0, "h1:27017", 0, 1),
                m(1, "h2:27017", 0, 1),
                m(2, "h3:27017", 0, 1));
        ReconfigSpec.Request req = request(allZero,
                new ReconfigSpec.ChangePriority(0, 0));
        ReconfigPreflight.Result r = preflight.check(req);
        assertTrue(r.hasBlocking());
        assertTrue(r.findings().stream().anyMatch(f -> f.code().equals("NO_ELECTABLE")));
    }

    @Test
    void priority_out_of_range_rejected_at_spec_construction() {
        assertThrows(IllegalArgumentException.class,
                () -> new ReconfigSpec.ChangePriority(0, 1001));
        assertThrows(IllegalArgumentException.class,
                () -> new ReconfigSpec.ChangePriority(0, -1));
    }

    /* ============================= votes ============================= */

    @Test
    void flipping_one_voter_off_still_leaves_majority() {
        // 5-node set; remove a vote → 4 voters, still ≥ 3 electable.
        List<Member> five = fiveNodeStandard();
        ReconfigSpec.Request req = request(five,
                new ReconfigSpec.ChangeVotes(4, 0));
        ReconfigPreflight.Result r = preflight.check(req);
        assertFalse(r.hasBlocking());
    }

    @Test
    void changeVotes_out_of_range_rejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new ReconfigSpec.ChangeVotes(0, 2));
    }

    @Test
    void toggling_all_voters_off_blocks_NO_VOTERS() {
        List<Member> three = threeNodeStandard();
        ReconfigSpec.Change chain = new ReconfigSpec.ChangeVotes(0, 0);
        List<Member> after1 = preflight.applyChange(three, chain);
        List<Member> after2 = preflight.applyChange(after1, new ReconfigSpec.ChangeVotes(1, 0));
        List<Member> after3 = preflight.applyChange(after2, new ReconfigSpec.ChangeVotes(2, 0));
        ReconfigSpec.Request req = request(after3,
                new ReconfigSpec.WholeConfig(after3));
        ReconfigPreflight.Result r = preflight.check(req);
        assertTrue(r.hasBlocking());
        assertTrue(r.findings().stream().anyMatch(f -> f.code().equals("NO_VOTERS")));
    }

    /* ========================== hidden / arbiter ========================== */

    @Test
    void hiding_a_non_voter_is_clean() {
        List<Member> members = List.of(
                m(0, "h1:27017", 1, 1),
                m(1, "h2:27017", 1, 1),
                m(2, "h3:27017", 0, 0));  // already a passive non-voter
        ReconfigSpec.Request req = request(members,
                new ReconfigSpec.ToggleHidden(2, true));
        ReconfigPreflight.Result r = preflight.check(req);
        assertFalse(r.hasBlocking());
        assertTrue(r.proposedMembers().get(2).hidden());
    }

    @Test
    void toggling_a_member_to_arbiter_warns() {
        ReconfigSpec.Request req = request(threeNodeStandard(),
                new ReconfigSpec.ToggleArbiter(2, true));
        ReconfigPreflight.Result r = preflight.check(req);
        assertTrue(r.findings().stream().anyMatch(f -> f.code().equals("ARBITER_PRESENT")));
    }

    /* ============================== rename ============================== */

    @Test
    void renaming_a_member_updates_host() {
        ReconfigSpec.Request req = request(threeNodeStandard(),
                new ReconfigSpec.RenameMember(1, "newh2:27017"));
        ReconfigPreflight.Result r = preflight.check(req);
        assertFalse(r.hasBlocking());
        assertEquals("newh2:27017", r.proposedMembers().get(1).host());
    }

    @Test
    void renaming_to_a_collision_is_blocking() {
        ReconfigSpec.Request req = request(threeNodeStandard(),
                new ReconfigSpec.RenameMember(1, "h1:27017"));
        ReconfigPreflight.Result r = preflight.check(req);
        assertTrue(r.hasBlocking());
        assertTrue(r.findings().stream().anyMatch(f -> f.code().equals("DUP_HOST")));
    }

    /* ============================= bounds ============================= */

    @Test
    void more_than_7_voters_blocks() {
        // Build an 8-voter config via WholeConfig and assert the bound
        // trips. (In practice the wizard prevents this before reaching
        // the preflight, but a malformed API call needs a server-side
        // safety net too.)
        List<Member> eight = List.of(
                m(0, "h1:27017", 1, 1), m(1, "h2:27017", 1, 1),
                m(2, "h3:27017", 1, 1), m(3, "h4:27017", 1, 1),
                m(4, "h5:27017", 1, 1), m(5, "h6:27017", 1, 1),
                m(6, "h7:27017", 1, 1), m(7, "h8:27017", 1, 1));
        ReconfigSpec.Request req = request(eight,
                new ReconfigSpec.WholeConfig(eight));
        ReconfigPreflight.Result r = preflight.check(req);
        assertTrue(r.hasBlocking());
        assertTrue(r.findings().stream().anyMatch(f -> f.code().equals("MAX_VOTES")));
    }

    @Test
    void more_than_50_members_blocks() {
        List<Member> many = new java.util.ArrayList<>();
        for (int i = 0; i < 51; i++) {
            many.add(new Member(i, "h" + i + ":27017",
                    /*priority=*/ i < 7 ? 1 : 0,
                    /*votes=*/    i < 7 ? 1 : 0,
                    false, false, true, 0.0));
        }
        ReconfigSpec.Request req = request(many,
                new ReconfigSpec.WholeConfig(many));
        ReconfigPreflight.Result r = preflight.check(req);
        assertTrue(r.hasBlocking());
        assertTrue(r.findings().stream().anyMatch(f -> f.code().equals("MAX_MEMBERS")));
    }

    /* ============================ fixtures ============================ */

    private static List<Member> threeNodeStandard() {
        return List.of(
                m(0, "h1:27017", 1, 1),
                m(1, "h2:27017", 1, 1),
                m(2, "h3:27017", 1, 1));
    }

    private static List<Member> fiveNodeStandard() {
        return List.of(
                m(0, "h1:27017", 1, 1), m(1, "h2:27017", 1, 1),
                m(2, "h3:27017", 1, 1), m(3, "h4:27017", 1, 1),
                m(4, "h5:27017", 1, 1));
    }

    private static Member m(int id, String host, int priority, int votes) {
        return new Member(id, host, priority, votes, false, false, true, 0.0);
    }

    private static ReconfigSpec.Request request(List<Member> members, Change change) {
        return new ReconfigSpec.Request("prod-rs", 7, members, change);
    }
}

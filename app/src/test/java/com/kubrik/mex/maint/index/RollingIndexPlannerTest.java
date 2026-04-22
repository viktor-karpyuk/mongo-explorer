package com.kubrik.mex.maint.index;

import com.kubrik.mex.maint.model.ReconfigSpec.Member;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RollingIndexPlannerTest {

    private final RollingIndexPlanner planner = new RollingIndexPlanner();

    @Test
    void three_node_rs_plans_two_secondaries_then_primary() {
        List<Member> members = List.of(
                m(0, "h1:27017", 2),
                m(1, "h2:27017", 1),
                m(2, "h3:27017", 1));
        List<RollingIndexPlanner.Step> plan = planner.plan(members, 0);
        assertEquals(3, plan.size());
        assertEquals(1, plan.get(0).member().id());  // secondary, lower priority
        assertEquals(2, plan.get(1).member().id());
        assertEquals(0, plan.get(2).member().id());  // primary last
        assertTrue(plan.get(2).isPrimary());
    }

    @Test
    void arbiters_are_skipped() {
        List<Member> members = List.of(
                m(0, "h1:27017", 1, 1, false, false),
                m(1, "h2:27017", 1, 1, false, false),
                m(2, "h3:27017", 0, 1, false, true));  // arbiter
        List<RollingIndexPlanner.Step> plan = planner.plan(members, 0);
        assertEquals(2, plan.size());
        assertTrue(plan.stream().noneMatch(s -> s.member().id() == 2));
    }

    @Test
    void hidden_secondary_included() {
        List<Member> members = List.of(
                m(0, "h1:27017", 2),
                m(1, "h2:27017", 1),
                // hidden secondaries still hold data → index needs to
                // land there, so include them.
                new Member(2, "h3:27017", 0, 1, true, false, true, 0.0));
        List<RollingIndexPlanner.Step> plan = planner.plan(members, 0);
        assertEquals(3, plan.size());
        // Lowest priority first among secondaries → hidden (prio=0) first.
        assertEquals(2, plan.get(0).member().id());
    }

    @Test
    void secondaries_ordered_by_priority_ascending_then_id() {
        List<Member> members = List.of(
                m(0, "h1:27017", 10),
                m(1, "h2:27017", 3),
                m(2, "h3:27017", 3),
                m(3, "h4:27017", 5));
        List<RollingIndexPlanner.Step> plan = planner.plan(members, 0);
        // Secondaries first, sorted by priority asc, then id asc.
        assertEquals(1, plan.get(0).member().id());  // prio 3, id 1
        assertEquals(2, plan.get(1).member().id());  // prio 3, id 2
        assertEquals(3, plan.get(2).member().id());  // prio 5
        assertEquals(0, plan.get(3).member().id());  // primary last
    }

    private static Member m(int id, String host, int priority) {
        return new Member(id, host, priority, 1, false, false, true, 0.0);
    }

    private static Member m(int id, String host, int priority, int votes,
                             boolean hidden, boolean arbiter) {
        return new Member(id, host, priority, votes, hidden, arbiter, true, 0.0);
    }
}

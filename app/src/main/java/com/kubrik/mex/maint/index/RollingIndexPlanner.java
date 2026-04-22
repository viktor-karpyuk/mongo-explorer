package com.kubrik.mex.maint.index;

import com.kubrik.mex.maint.model.ReconfigSpec.Member;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * v2.7 IDX-BLD-4 — Orders cluster members for a rolling index build.
 *
 * <p>Rules:</p>
 * <ol>
 *   <li>Secondaries first, by priority ascending — the least-likely
 *       successor is built first, so if the build is painful and we
 *       bail, we've touched the least important member.</li>
 *   <li>Primary last. The runner step-downs before building.</li>
 *   <li>Arbiters skipped — they hold no data.</li>
 *   <li>Hidden/delayed secondaries included; the user explicitly
 *       targeted them in the wizard scope.</li>
 * </ol>
 *
 * <p>Pure function of the topology snapshot — no driver calls. The
 * runner hands this list to the dispatch loop.</p>
 */
public final class RollingIndexPlanner {

    /** Per-step entry in the schedule. {@link #isPrimary} drives the
     *  step-down branch in the runner. */
    public record Step(Member member, boolean isPrimary) {}

    public List<Step> plan(List<Member> members, int currentPrimaryId) {
        Objects.requireNonNull(members, "members");
        List<Member> dataBearing = members.stream()
                .filter(m -> !m.arbiterOnly())
                .toList();

        // Split out the primary — it goes last.
        Member primary = dataBearing.stream()
                .filter(m -> m.id() == currentPrimaryId)
                .findFirst()
                .orElse(null);

        List<Member> secondaries = new ArrayList<>(dataBearing.stream()
                .filter(m -> m.id() != currentPrimaryId)
                .toList());
        // Lowest priority first — a 0-priority hidden secondary
        // is the cheapest "oops" if the build misbehaves.
        secondaries.sort(Comparator
                .comparingInt(Member::priority)
                .thenComparingInt(Member::id));

        List<Step> out = new ArrayList<>(dataBearing.size());
        for (Member m : secondaries) out.add(new Step(m, false));
        if (primary != null) out.add(new Step(primary, true));
        return out;
    }
}

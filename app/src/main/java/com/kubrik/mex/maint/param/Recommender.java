package com.kubrik.mex.maint.param;

import com.kubrik.mex.maint.model.ClusterShape;
import com.kubrik.mex.maint.model.ParamProposal;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * v2.7 PARAM-1/2 — Runs the parameter catalogue against a cluster
 * shape + the observed current values, and emits proposals with
 * severity + rationale.
 *
 * <p>Severity:</p>
 * <ul>
 *   <li><b>ACT</b> — current value is far off recommended (eg. 64
 *       when we'd recommend 256) AND the param is known to have a
 *       material workload effect.</li>
 *   <li><b>CONSIDER</b> — different from recommended but within a
 *       reasonable band, or the param is advisory.</li>
 *   <li><b>INFO</b> — already matches recommended; surfaced for
 *       transparency (empty state feels wrong without it).</li>
 * </ul>
 */
public final class Recommender {

    public List<ParamProposal> recommend(ClusterShape shape,
                                         Map<String, String> currentValues) {
        List<ParamProposal> out = new ArrayList<>();
        for (ParamCatalogue.Entry entry : ParamCatalogue.all()) {
            if (!entry.appliesTo().test(shape)) continue;
            Optional<Object> rec = entry.recommend().apply(shape);
            if (rec.isEmpty()) continue;
            String proposed = rec.get().toString();
            String current = currentValues.getOrDefault(entry.name(), "?");

            ParamProposal.Severity severity = classify(entry, current, proposed);
            out.add(new ParamProposal(entry.name(), current, proposed,
                    severity, entry.rationale()));
        }
        return List.copyOf(out);
    }

    /** Keep the severity logic tiny — a 2-bucket numeric-delta check
     *  + an already-matches fallthrough. */
    private static ParamProposal.Severity classify(
            ParamCatalogue.Entry entry, String current, String proposed) {
        if (current.equals(proposed)) return ParamProposal.Severity.INFO;
        if (current.equals("?")) return ParamProposal.Severity.CONSIDER;

        // For numeric-ranged entries, measure the distance.
        if (entry.range().isPresent()) {
            try {
                long c = Long.parseLong(current);
                long p = Long.parseLong(proposed);
                long delta = Math.abs(c - p);
                long span = entry.range().get().max() - entry.range().get().min();
                if (span > 0 && delta * 4 > span) {
                    return ParamProposal.Severity.ACT;
                }
            } catch (NumberFormatException ignored) {}
        }
        return ParamProposal.Severity.CONSIDER;
    }
}

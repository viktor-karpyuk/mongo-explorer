package com.kubrik.mex.migration.preflight;

import com.kubrik.mex.migration.engine.CollectionPlan;

import java.util.List;

/** Output of the pre-flight check — resolved plans plus any warnings/errors the engine
 *  discovered while reconciling the spec against the live source and target clusters. */
public record PreflightReport(
        List<CollectionPlan> plans,
        List<String> warnings,
        List<String> errors
) {

    public boolean hasBlockingErrors() { return !errors.isEmpty(); }

    public static PreflightReport empty() {
        return new PreflightReport(List.of(), List.of(), List.of());
    }
}

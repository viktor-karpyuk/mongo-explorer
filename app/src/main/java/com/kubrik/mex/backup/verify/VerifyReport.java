package com.kubrik.mex.backup.verify;

import java.util.List;

/**
 * v2.5 Q2.5-D — structured result of a catalog verification pass.
 *
 * <p>{@code outcome} is the top-level verdict (single enum value for the
 * catalog row); {@code problems} is the human-readable list rendered on the
 * artefact-explorer detail pane when the verdict is anything other than
 * {@link VerifyOutcome#VERIFIED}.</p>
 */
public record VerifyReport(
        long catalogId,
        VerifyOutcome outcome,
        long filesChecked,
        long bytesChecked,
        List<String> problems
) {
    public VerifyReport {
        problems = problems == null ? List.of() : List.copyOf(problems);
    }

    public boolean ok() { return outcome == VerifyOutcome.VERIFIED; }
}

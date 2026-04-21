package com.kubrik.mex.ui.cluster;

import com.kubrik.mex.cluster.audit.Outcome;
import com.kubrik.mex.cluster.dryrun.DryRunRenderer;
import com.kubrik.mex.cluster.safety.Command;
import com.kubrik.mex.cluster.safety.DryRunResult;
import com.kubrik.mex.cluster.safety.TypedConfirmDialog;
import com.kubrik.mex.cluster.safety.TypedConfirmModel;
import com.kubrik.mex.cluster.service.OpsExecutor;
import javafx.stage.Window;

import java.util.Optional;

/**
 * v2.4 RS-6 — stepDown confirmation + dispatch for the current primary. Uses
 * the spec's documented defaults (60 s step-down window, 10 s secondary
 * catch-up) so the preview is exactly the contract example in §5. Seconds
 * customisation is deferred to the Q2.4-I polish pass; the hard-coded defaults
 * match the v2.4 acceptance test (StepDownIT).
 */
public final class StepDownDialog {

    public static final int DEFAULT_STEP_DOWN_SECS = 60;
    public static final int DEFAULT_CATCHUP_SECS   = 10;

    private StepDownDialog() {}

    public static KillOpDialog.Result show(Window owner, String connectionId, String primaryHost,
                                           OpsExecutor executor, String callerUser, String callerHost) {
        Command.StepDown cmd = new Command.StepDown(
                primaryHost, DEFAULT_STEP_DOWN_SECS, DEFAULT_CATCHUP_SECS);
        DryRunResult preview = DryRunRenderer.render(cmd);
        TypedConfirmModel model = new TypedConfirmModel(primaryHost, preview);
        Optional<TypedConfirmModel.Outcome> picked = TypedConfirmDialog.showAndWait(owner, model);
        if (picked.isEmpty() || picked.get() != TypedConfirmModel.Outcome.CONFIRMED) {
            return new KillOpDialog.Result(Outcome.CANCELLED, "user_cancelled");
        }
        OpsExecutor.Result r = executor.execute(connectionId, cmd, preview,
                model.paste(), callerUser, callerHost);
        return new KillOpDialog.Result(r.outcome(), r.serverMessage());
    }
}

package com.kubrik.mex.ui.cluster;

import com.kubrik.mex.cluster.audit.Outcome;
import com.kubrik.mex.cluster.dryrun.DryRunRenderer;
import com.kubrik.mex.cluster.ops.CurrentOpRow;
import com.kubrik.mex.cluster.safety.Command;
import com.kubrik.mex.cluster.safety.DryRunResult;
import com.kubrik.mex.cluster.safety.TypedConfirmDialog;
import com.kubrik.mex.cluster.safety.TypedConfirmModel;
import com.kubrik.mex.cluster.service.OpsExecutor;
import javafx.stage.Window;

import java.util.Optional;

/**
 * v2.4 OP-7 — glue between {@link TypedConfirmDialog} and {@link OpsExecutor}
 * for the {@code killOp} action triggered from a {@link CurrentOpPane} row.
 * Renders the dry-run preview, presents the typed confirm, and dispatches
 * only on explicit confirmation. Returns a short descriptor so the caller
 * can surface a toast / inline message.
 */
public final class KillOpDialog {

    private KillOpDialog() {}

    public static Result show(Window owner, String connectionId, CurrentOpRow row,
                              OpsExecutor executor, String callerUser, String callerHost) {
        Command.KillOp cmd = new Command.KillOp(row.host().isBlank() ? "unknown" : row.host(), row.opid());
        DryRunResult preview = DryRunRenderer.render(cmd);
        TypedConfirmModel model = new TypedConfirmModel(String.valueOf(row.opid()), preview);
        Optional<TypedConfirmModel.Outcome> picked = TypedConfirmDialog.showAndWait(owner, model);

        if (picked.isEmpty() || picked.get() != TypedConfirmModel.Outcome.CONFIRMED) {
            return new Result(Outcome.CANCELLED, "user_cancelled");
        }
        OpsExecutor.Result result = executor.execute(connectionId, cmd, preview,
                model.paste(), callerUser, callerHost);
        return new Result(result.outcome(), result.serverMessage());
    }

    public record Result(Outcome outcome, String message) {
        public boolean ok() { return outcome == Outcome.OK; }
    }
}

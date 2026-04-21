package com.kubrik.mex.ui.cluster;

import com.kubrik.mex.cluster.audit.Outcome;
import com.kubrik.mex.cluster.dryrun.DryRunRenderer;
import com.kubrik.mex.cluster.safety.Command;
import com.kubrik.mex.cluster.safety.DryRunResult;
import com.kubrik.mex.cluster.safety.TypedConfirmDialog;
import com.kubrik.mex.cluster.safety.TypedConfirmModel;
import com.kubrik.mex.cluster.service.OpsExecutor;
import javafx.scene.control.ChoiceDialog;
import javafx.stage.Window;

import java.util.List;
import java.util.Optional;

/**
 * v2.4 RS-7 — freeze / unfreeze a secondary. A small {@link ChoiceDialog} up
 * front lets the user pick a duration (30 s, 60 s, 5 min, 15 min, or
 * <em>Unfreeze</em> which maps to seconds = 0). The selection drives a
 * {@link Command.Freeze}, and the full preview + typed confirm follows the
 * same flow as {@link StepDownDialog} / {@link KillOpDialog}.
 */
public final class FreezeDialog {

    private static final List<String> CHOICES = List.of(
            "Freeze 30 s", "Freeze 60 s", "Freeze 5 min", "Freeze 15 min", "Unfreeze");

    private FreezeDialog() {}

    public static KillOpDialog.Result show(Window owner, String connectionId, String host,
                                           OpsExecutor executor, String callerUser, String callerHost) {
        ChoiceDialog<String> picker = new ChoiceDialog<>("Freeze 60 s", CHOICES);
        if (owner != null) picker.initOwner(owner);
        picker.setTitle("Freeze " + host);
        picker.setHeaderText("Choose how long to refuse election on " + host + ".");
        picker.setContentText("Duration:");
        Optional<String> choice = picker.showAndWait();
        if (choice.isEmpty()) return new KillOpDialog.Result(Outcome.CANCELLED, "user_cancelled");

        int seconds = switch (choice.get()) {
            case "Freeze 30 s"  -> 30;
            case "Freeze 60 s"  -> 60;
            case "Freeze 5 min" -> 300;
            case "Freeze 15 min"-> 900;
            case "Unfreeze"     -> 0;
            default             -> 60;
        };

        Command.Freeze cmd = new Command.Freeze(host, seconds);
        DryRunResult preview = DryRunRenderer.render(cmd);
        TypedConfirmModel model = new TypedConfirmModel(host, preview);
        Optional<TypedConfirmModel.Outcome> picked = TypedConfirmDialog.showAndWait(owner, model);
        if (picked.isEmpty() || picked.get() != TypedConfirmModel.Outcome.CONFIRMED) {
            return new KillOpDialog.Result(Outcome.CANCELLED, "user_cancelled");
        }
        OpsExecutor.Result r = executor.execute(connectionId, cmd, preview,
                model.paste(), callerUser, callerHost);
        return new KillOpDialog.Result(r.outcome(), r.serverMessage());
    }
}

package com.kubrik.mex.ui.cluster;

import com.kubrik.mex.cluster.audit.Outcome;
import com.kubrik.mex.cluster.dryrun.DryRunRenderer;
import com.kubrik.mex.cluster.safety.Command;
import com.kubrik.mex.cluster.safety.DryRunResult;
import com.kubrik.mex.cluster.safety.TypedConfirmDialog;
import com.kubrik.mex.cluster.safety.TypedConfirmModel;
import com.kubrik.mex.cluster.service.OpsExecutor;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.ButtonBar;
import javafx.scene.control.ButtonType;
import javafx.scene.control.Dialog;
import javafx.scene.control.Label;
import javafx.scene.control.TextField;
import javafx.scene.layout.GridPane;
import javafx.stage.Modality;
import javafx.stage.Window;

import java.util.Optional;

/**
 * v2.4 SHARD-5..9 (part 2) — balancer activeWindow editor. A tiny form
 * collects the start / stop HH:MM UTC strings (current values pre-filled
 * when known); validation is delegated to {@link Command.BalancerWindow}'s
 * constructor. On submit, the standard preview + typed-confirm flow runs,
 * and on confirmation {@link OpsExecutor#execute} upserts the
 * {@code config.settings} document.
 */
public final class BalancerWindowDialog {

    private BalancerWindowDialog() {}

    public static KillOpDialog.Result show(Window owner, String connectionId,
                                           String currentStart, String currentStop,
                                           OpsExecutor executor,
                                           String callerUser, String callerHost) {
        Dialog<String[]> picker = new Dialog<>();
        if (owner != null) picker.initOwner(owner);
        picker.initModality(Modality.APPLICATION_MODAL);
        picker.setTitle("Balancer window");
        ButtonType ok = new ButtonType("Preview…", ButtonBar.ButtonData.OK_DONE);
        ButtonType cancel = new ButtonType("Cancel", ButtonBar.ButtonData.CANCEL_CLOSE);
        picker.getDialogPane().getButtonTypes().setAll(cancel, ok);

        TextField start = new TextField(currentStart == null ? "00:00" : currentStart);
        TextField stop  = new TextField(currentStop  == null ? "06:00" : currentStop);
        start.setPromptText("HH:MM UTC");
        stop.setPromptText("HH:MM UTC");
        Label err = new Label("");
        err.setStyle("-fx-text-fill: #b91c1c; -fx-font-size: 11px;");

        GridPane g = new GridPane();
        g.setHgap(10);
        g.setVgap(8);
        g.setPadding(new Insets(14));
        g.add(small("start"), 0, 0); g.add(start, 1, 0);
        g.add(small("stop"),  0, 1); g.add(stop,  1, 1);
        g.add(err, 0, 2, 2, 1);
        Label help = new Label("Balancer runs only inside the window. Uses UTC.");
        help.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px;");
        g.add(help, 0, 3, 2, 1);
        GridPane.setHalignment(help, Pos.CENTER.getHpos());
        picker.getDialogPane().setContent(g);

        picker.setResultConverter(bt -> {
            if (bt != ok) return null;
            return new String[] { start.getText().trim(), stop.getText().trim() };
        });
        Optional<String[]> choice = picker.showAndWait();
        if (choice.isEmpty() || choice.get() == null) {
            return new KillOpDialog.Result(Outcome.CANCELLED, "user_cancelled");
        }
        Command.BalancerWindow cmd;
        try {
            cmd = new Command.BalancerWindow(connectionId, choice.get()[0], choice.get()[1]);
        } catch (IllegalArgumentException bad) {
            return new KillOpDialog.Result(Outcome.CANCELLED, "invalid_window: " + bad.getMessage());
        }
        DryRunResult preview = DryRunRenderer.render(cmd);
        TypedConfirmModel model = new TypedConfirmModel(connectionId, preview);
        Optional<TypedConfirmModel.Outcome> picked = TypedConfirmDialog.showAndWait(owner, model);
        if (picked.isEmpty() || picked.get() != TypedConfirmModel.Outcome.CONFIRMED) {
            return new KillOpDialog.Result(Outcome.CANCELLED, "user_cancelled");
        }
        OpsExecutor.Result r = executor.execute(connectionId, cmd, preview,
                model.paste(), callerUser, callerHost);
        return new KillOpDialog.Result(r.outcome(), r.serverMessage());
    }

    private static Label small(String s) {
        Label l = new Label(s);
        l.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px;");
        return l;
    }
}

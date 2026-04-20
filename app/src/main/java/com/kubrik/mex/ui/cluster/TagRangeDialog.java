package com.kubrik.mex.ui.cluster;

import com.kubrik.mex.cluster.audit.Outcome;
import com.kubrik.mex.cluster.dryrun.DryRunRenderer;
import com.kubrik.mex.cluster.ops.TagRange;
import com.kubrik.mex.cluster.safety.Command;
import com.kubrik.mex.cluster.safety.DryRunResult;
import com.kubrik.mex.cluster.safety.TypedConfirmDialog;
import com.kubrik.mex.cluster.safety.TypedConfirmModel;
import com.kubrik.mex.cluster.service.OpsExecutor;
import javafx.geometry.Insets;
import javafx.scene.control.ButtonBar;
import javafx.scene.control.ButtonType;
import javafx.scene.control.Dialog;
import javafx.scene.control.Label;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.layout.GridPane;
import javafx.stage.Modality;
import javafx.stage.Window;
import org.bson.BsonDocument;
import org.bson.Document;

import java.util.Map;
import java.util.Optional;

/**
 * v2.4 SHARD-14..16 — add / remove a single tag range. The forms share a
 * layout: namespace, min / max bounds as JSON, and (for add) a zone. Every
 * dispatch goes through {@link OpsExecutor} so the preview hash + audit row
 * land even if the server rejects the range.
 */
public final class TagRangeDialog {

    private TagRangeDialog() {}

    /** Build an {@code addTagRange} via a fresh form. */
    public static KillOpDialog.Result showAdd(Window owner, String connectionId,
                                              OpsExecutor executor,
                                              String callerUser, String callerHost) {
        return runForm(owner, connectionId, executor, callerUser, callerHost,
                "Add tag range", "", "", "", "", /*removeMode*/ false);
    }

    /** Pre-fill a form from an existing row; used for the "Remove" flow. */
    public static KillOpDialog.Result showRemove(Window owner, String connectionId,
                                                  TagRange row,
                                                  OpsExecutor executor,
                                                  String callerUser, String callerHost) {
        return runForm(owner, connectionId, executor, callerUser, callerHost,
                "Remove tag range", row.ns(), row.minJson(), row.maxJson(),
                row.tag(), /*removeMode*/ true);
    }

    /* ============================ internals ============================ */

    private static KillOpDialog.Result runForm(Window owner, String connectionId,
                                                OpsExecutor executor, String callerUser, String callerHost,
                                                String title, String ns, String min, String max,
                                                String zone, boolean removeMode) {
        Dialog<Form> picker = new Dialog<>();
        if (owner != null) picker.initOwner(owner);
        picker.initModality(Modality.APPLICATION_MODAL);
        picker.setTitle(title);
        ButtonType ok = new ButtonType("Preview…", ButtonBar.ButtonData.OK_DONE);
        ButtonType cancel = new ButtonType("Cancel", ButtonBar.ButtonData.CANCEL_CLOSE);
        picker.getDialogPane().getButtonTypes().setAll(cancel, ok);

        TextField nsField = new TextField(ns);
        nsField.setPromptText("db.collection");
        TextArea minField = new TextArea(min);
        minField.setPromptText("{ shardKey: MinKey() }");
        minField.setPrefRowCount(3);
        TextArea maxField = new TextArea(max);
        maxField.setPromptText("{ shardKey: MaxKey() }");
        maxField.setPrefRowCount(3);
        TextField zoneField = new TextField(zone);
        zoneField.setPromptText("zone name");
        zoneField.setDisable(removeMode);
        if (removeMode) {
            nsField.setDisable(true);
            minField.setDisable(true);
            maxField.setDisable(true);
        }

        GridPane g = new GridPane();
        g.setHgap(10);
        g.setVgap(8);
        g.setPadding(new Insets(14));
        g.add(small("namespace"), 0, 0); g.add(nsField, 1, 0);
        g.add(small("min"),       0, 1); g.add(minField, 1, 1);
        g.add(small("max"),       0, 2); g.add(maxField, 1, 2);
        g.add(small("zone"),      0, 3); g.add(zoneField, 1, 3);
        picker.getDialogPane().setContent(g);

        picker.setResultConverter(bt -> {
            if (bt != ok) return null;
            return new Form(nsField.getText().trim(), minField.getText(),
                    maxField.getText(), zoneField.getText().trim());
        });
        Optional<Form> picked = picker.showAndWait();
        if (picked.isEmpty() || picked.get() == null) {
            return new KillOpDialog.Result(Outcome.CANCELLED, "user_cancelled");
        }
        Form f = picked.get();

        Command cmd;
        String confirmTarget;
        try {
            Map<String, Object> minMap = parseBounds(f.min);
            Map<String, Object> maxMap = parseBounds(f.max);
            if (removeMode) {
                cmd = new Command.RemoveTagRange(f.ns, minMap, maxMap);
                confirmTarget = f.ns;
            } else {
                cmd = new Command.AddTagRange(f.ns, minMap, maxMap, f.zone);
                confirmTarget = f.zone;
            }
        } catch (Exception parseErr) {
            return new KillOpDialog.Result(Outcome.CANCELLED, "parse_error: " + parseErr.getMessage());
        }
        DryRunResult preview = DryRunRenderer.render(cmd);
        TypedConfirmModel model = new TypedConfirmModel(confirmTarget, preview);
        Optional<TypedConfirmModel.Outcome> confirmed = TypedConfirmDialog.showAndWait(owner, model);
        if (confirmed.isEmpty() || confirmed.get() != TypedConfirmModel.Outcome.CONFIRMED) {
            return new KillOpDialog.Result(Outcome.CANCELLED, "user_cancelled");
        }
        OpsExecutor.Result r = executor.execute(connectionId, cmd, preview,
                model.paste(), callerUser, callerHost);
        return new KillOpDialog.Result(r.outcome(), r.serverMessage());
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> parseBounds(String json) {
        if (json == null || json.isBlank()) return Map.of();
        Document d = Document.parse(BsonDocument.parse(json).toJson());
        return (Map<String, Object>) (Map<?, ?>) d;
    }

    private static Label small(String s) {
        Label l = new Label(s);
        l.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px;");
        return l;
    }

    private record Form(String ns, String min, String max, String zone) {}
}

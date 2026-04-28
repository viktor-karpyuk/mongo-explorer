package com.kubrik.mex.ui.cluster;

import com.kubrik.mex.cluster.audit.Outcome;
import com.kubrik.mex.cluster.dryrun.DryRunRenderer;
import com.kubrik.mex.cluster.safety.Command;
import com.kubrik.mex.cluster.safety.DryRunResult;
import com.kubrik.mex.cluster.safety.TypedConfirmDialog;
import com.kubrik.mex.cluster.safety.TypedConfirmModel;
import com.kubrik.mex.cluster.service.ChunkService;
import com.kubrik.mex.cluster.service.OpsExecutor;
import com.kubrik.mex.core.ConnectionManager;
import com.kubrik.mex.core.MongoService;
import javafx.geometry.Insets;
import javafx.scene.control.ButtonBar;
import javafx.scene.control.ButtonType;
import javafx.scene.control.ChoiceBox;
import javafx.scene.control.Dialog;
import javafx.scene.control.Label;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.layout.GridPane;
import javafx.stage.Modality;
import javafx.stage.Window;
import org.bson.BsonDocument;
import org.bson.Document;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * v2.4 SHARD-17 — moveChunk dispatcher. A form captures the namespace,
 * min / max bounds (BSON JSON), and target shard (choice loaded from
 * {@code listShards}). The standard preview + typed-confirm + dispatch flow
 * follows. Write concern defaults to {@code majority}; we don't expose it
 * in the v2.4 form — per the spec, majority is non-negotiable for moveChunk.
 */
public final class MoveChunkDialog {

    private MoveChunkDialog() {}

    public static KillOpDialog.Result show(Window owner, String connectionId,
                                           ConnectionManager connManager,
                                           OpsExecutor executor,
                                           String callerUser, String callerHost) {
        MongoService svc = connManager.service(connectionId);
        if (svc == null) {
            return new KillOpDialog.Result(Outcome.FAIL, "not_connected");
        }
        List<String> shards = ChunkService.listShards(svc);
        if (shards.isEmpty()) {
            return new KillOpDialog.Result(Outcome.FAIL, "no_shards_available");
        }

        Dialog<Form> picker = new Dialog<>();
        if (owner != null) picker.initOwner(owner);
        picker.initModality(Modality.APPLICATION_MODAL);
        picker.setTitle("Move chunk");
        ButtonType ok = new ButtonType("Preview…", ButtonBar.ButtonData.OK_DONE);
        ButtonType cancel = new ButtonType("Cancel", ButtonBar.ButtonData.CANCEL_CLOSE);
        picker.getDialogPane().getButtonTypes().setAll(cancel, ok);

        TextField ns = new TextField();
        ns.setPromptText("db.collection");
        TextArea minField = new TextArea();
        minField.setPromptText("{ shardKey: MinKey() }");
        minField.setPrefRowCount(3);
        TextArea maxField = new TextArea();
        maxField.setPromptText("{ shardKey: MaxKey() }");
        maxField.setPrefRowCount(3);
        ChoiceBox<String> toShard = new ChoiceBox<>();
        toShard.getItems().setAll(shards);
        toShard.setValue(shards.get(0));
        Label err = new Label("");
        err.setStyle("-fx-text-fill: #b91c1c; -fx-font-size: 11px;");

        GridPane g = new GridPane();
        g.setHgap(10);
        g.setVgap(8);
        g.setPadding(new Insets(14));
        g.add(small("namespace"), 0, 0); g.add(ns,       1, 0);
        g.add(small("min bound"), 0, 1); g.add(minField, 1, 1);
        g.add(small("max bound"), 0, 2); g.add(maxField, 1, 2);
        g.add(small("to shard"),  0, 3); g.add(toShard,  1, 3);
        g.add(err, 0, 4, 2, 1);
        picker.getDialogPane().setContent(g);

        picker.setResultConverter(bt -> {
            if (bt != ok) return null;
            return new Form(ns.getText().trim(), minField.getText(), maxField.getText(),
                    toShard.getValue());
        });

        Optional<Form> picked = picker.showAndWait();
        if (picked.isEmpty() || picked.get() == null) {
            return new KillOpDialog.Result(Outcome.CANCELLED, "user_cancelled");
        }
        Form f = picked.get();
        Command.MoveChunk cmd;
        try {
            Map<String, Object> minMap = parseBounds(f.min);
            Map<String, Object> maxMap = parseBounds(f.max);
            cmd = new Command.MoveChunk(f.ns, minMap, maxMap, f.toShard, false, "majority");
        } catch (Exception parseErr) {
            return new KillOpDialog.Result(Outcome.CANCELLED, "parse_error: " + parseErr.getMessage());
        }
        DryRunResult preview = DryRunRenderer.render(cmd);
        TypedConfirmModel model = new TypedConfirmModel(f.toShard, preview);
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

    private record Form(String ns, String min, String max, String toShard) {}
}

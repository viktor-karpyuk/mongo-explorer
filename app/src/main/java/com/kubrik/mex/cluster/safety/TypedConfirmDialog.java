package com.kubrik.mex.cluster.safety;

import javafx.geometry.Insets;
import javafx.scene.control.ButtonBar;
import javafx.scene.control.ButtonType;
import javafx.scene.control.Dialog;
import javafx.scene.control.Label;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.input.KeyCode;
import javafx.scene.layout.VBox;
import javafx.stage.Modality;
import javafx.stage.Window;

import java.util.Optional;

/**
 * v2.4 SAFE-OPS-5..7 — thin JavaFX view over {@link TypedConfirmModel}.
 *
 * <p>Behavioural rules (all enforced by the model):</p>
 * <ul>
 *   <li><em>Execute</em> is disabled until the input trim-matches the expected
 *       string. ESC and the explicit <em>Cancel</em> produce
 *       {@link TypedConfirmModel.Outcome#CANCELLED}.</li>
 *   <li>Backdrop clicks do not dismiss the dialog (modality APPLICATION_MODAL).</li>
 *   <li>Paste events on the input field set the audit-visible {@code paste}
 *       flag via the model.</li>
 * </ul>
 */
public final class TypedConfirmDialog {

    private TypedConfirmDialog() {}

    public static Optional<TypedConfirmModel.Outcome> showAndWait(Window owner, TypedConfirmModel model) {
        Dialog<TypedConfirmModel.Outcome> d = new Dialog<>();
        if (owner != null) d.initOwner(owner);
        d.initModality(Modality.APPLICATION_MODAL);
        d.setTitle("Confirm — " + model.preview().commandName());

        ButtonType executeBt = new ButtonType("Execute", ButtonBar.ButtonData.OK_DONE);
        ButtonType cancelBt  = new ButtonType("Cancel",  ButtonBar.ButtonData.CANCEL_CLOSE);
        d.getDialogPane().getButtonTypes().setAll(cancelBt, executeBt);

        Label summary = new Label(model.preview().summary());
        summary.setWrapText(true);
        summary.setStyle("-fx-font-weight: 600;");
        Label predicted = new Label(model.preview().predictedEffect());
        predicted.setWrapText(true);
        predicted.setStyle("-fx-text-fill: #4b5563;");

        TextArea previewJson = new TextArea(model.preview().commandJson());
        previewJson.setEditable(false);
        previewJson.setWrapText(true);
        previewJson.setPrefRowCount(6);
        previewJson.setStyle("-fx-font-family: 'JetBrains Mono','Menlo',monospace; -fx-font-size: 12px;");

        Label prompt = new Label("Type " + model.expected() + " to confirm:");
        TextField input = new TextField();
        input.setPromptText(model.expected());
        input.textProperty().addListener((o, a, b) -> model.setInput(b));
        input.setOnKeyPressed(e -> {
            if (e.getCode() == KeyCode.ESCAPE) { model.cancel(); d.setResult(TypedConfirmModel.Outcome.CANCELLED); d.close(); }
            if (e.isShortcutDown() && (e.getCode() == KeyCode.V || e.getCode() == KeyCode.INSERT)) model.markPaste();
        });

        Label hashFooter = new Label("preview hash  sha-256 · " + shortHash(model.preview().previewHash()));
        hashFooter.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px;");

        VBox body = new VBox(10, summary, predicted, previewJson, prompt, input, hashFooter);
        body.setPadding(new Insets(16));
        body.setPrefWidth(520);
        d.getDialogPane().setContent(body);

        javafx.scene.control.Button executeButton =
                (javafx.scene.control.Button) d.getDialogPane().lookupButton(executeBt);
        executeButton.setDisable(true);
        model.onMatchChanged(match -> javafx.application.Platform.runLater(
                () -> executeButton.setDisable(!match)));

        d.setResultConverter(bt -> {
            if (bt == executeBt && model.matches()) { model.confirm(); return TypedConfirmModel.Outcome.CONFIRMED; }
            model.cancel();
            return TypedConfirmModel.Outcome.CANCELLED;
        });
        return d.showAndWait();
    }

    private static String shortHash(String full) {
        if (full == null || full.length() < 12) return full == null ? "" : full;
        return full.substring(0, 4) + "…" + full.substring(full.length() - 4);
    }
}

package com.kubrik.mex.maint.ui;

import com.kubrik.mex.maint.model.ValidatorSpec;
import com.kubrik.mex.maint.schema.StarterTemplates;
import com.kubrik.mex.maint.schema.ValidatorFetcher;
import com.kubrik.mex.maint.schema.ValidatorPreviewService;
import com.kubrik.mex.maint.schema.ValidatorRolloutRunner;
import com.mongodb.client.MongoClient;
import javafx.application.Platform;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.ChoiceBox;
import javafx.scene.control.Label;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.control.Tooltip;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import javafx.util.Duration;

/**
 * v2.7 SCHV-* UI — Schema-validator editor + preview + rollout.
 *
 * <p>Workflow: pick db/collection → Load (fetches current validator
 * via {@link ValidatorFetcher}) → edit in the JSON area → Preview
 * (runs {@link ValidatorPreviewService} against a $sample) → Apply
 * (opens rollout dialog with level + action + typed confirm).</p>
 */
public final class SchemaValidatorPane extends BorderPane {

    private final ValidatorFetcher fetcher = new ValidatorFetcher();
    private final ValidatorPreviewService previewService = new ValidatorPreviewService();
    private final ValidatorRolloutRunner rolloutRunner = new ValidatorRolloutRunner();

    private final java.util.function.Supplier<MongoClient> clientSupplier;

    private final TextField dbField = new TextField();
    private final TextField collField = new TextField();
    private final TextArea editor = new TextArea();
    private final ChoiceBox<String> templatePicker = new ChoiceBox<>();
    private final ChoiceBox<ValidatorSpec.Level> levelPicker = new ChoiceBox<>();
    private final ChoiceBox<ValidatorSpec.Action> actionPicker = new ChoiceBox<>();
    private final ObservableList<ValidatorSpec.FailedDoc> offenders =
            FXCollections.observableArrayList();
    private final TableView<ValidatorSpec.FailedDoc> offendersTable =
            new TableView<>(offenders);
    private final Label statusLabel = new Label("—");

    public SchemaValidatorPane(java.util.function.Supplier<MongoClient> clientSupplier) {
        this.clientSupplier = clientSupplier;
        setStyle("-fx-background-color: -color-bg-default;");
        setPadding(new Insets(14, 16, 14, 16));
        setAccessibleText("Schema validator editor");
        setAccessibleHelp(
                "Edit a collection's $jsonSchema validator. Preview runs "
                + "the proposed schema against a $sample; Apply dispatches "
                + "collMod through the approval workflow.");

        setTop(buildHeader());
        setCenter(buildCenter());
        setBottom(buildActions());
    }

    /* =============================== layout =============================== */

    private Region buildHeader() {
        Label title = new Label("Schema validator");
        title.setStyle("-fx-font-size: 14px; -fx-font-weight: 700;");
        Label hint = new Label(
                "Edit a collection's $jsonSchema validator with a live "
                + "preview of how many sampled docs would fail. Apply "
                + "through a rollout dialog gated by the approval "
                + "workflow.");
        hint.setStyle("-fx-text-fill: -color-fg-muted; -fx-font-size: 11px;");
        hint.setWrapText(true);

        templatePicker.setTooltip(tip(
                "Starter templates — pick one and edit, then Preview."));
        for (var t : StarterTemplates.all()) templatePicker.getItems().add(t.name());
        templatePicker.setOnAction(e -> {
            String name = templatePicker.getValue();
            if (name == null) return;
            editor.setText(StarterTemplates.byName(name).json());
        });

        dbField.setPromptText("app");
        collField.setPromptText("users");

        GridPane g = new GridPane();
        g.setHgap(8); g.setVgap(6);
        g.add(small("Database"), 0, 0); g.add(dbField, 1, 0);
        g.add(small("Collection"), 2, 0); g.add(collField, 3, 0);
        g.add(small("Starter"), 0, 1); g.add(templatePicker, 1, 1);

        VBox v = new VBox(6, title, hint, g);
        v.setPadding(new Insets(0, 0, 10, 0));
        return v;
    }

    private Region buildCenter() {
        editor.setPromptText("Paste or edit the $jsonSchema validator here.");
        editor.setPrefRowCount(14);
        editor.setStyle("-fx-font-family: 'Menlo', 'Courier New', monospace; -fx-font-size: 12px;");

        Label offendersLabel = new Label("Offending sampled docs");
        offendersLabel.setStyle("-fx-text-fill: -color-fg-default; -fx-font-size: 11px; -fx-font-weight: 600;");
        offendersTable.setPlaceholder(new Label("Run Preview to populate."));
        offendersTable.getColumns().setAll(
                col("_id", 140, ValidatorSpec.FailedDoc::id),
                col("Summary", 480, ValidatorSpec.FailedDoc::summary));
        offendersTable.setPrefHeight(160);

        VBox v = new VBox(6, editor, offendersLabel, offendersTable);
        VBox.setVgrow(editor, Priority.ALWAYS);
        return v;
    }

    private Region buildActions() {
        levelPicker.getItems().addAll(ValidatorSpec.Level.values());
        levelPicker.setValue(ValidatorSpec.Level.MODERATE);
        actionPicker.getItems().addAll(ValidatorSpec.Action.values());
        actionPicker.setValue(ValidatorSpec.Action.ERROR);

        Button loadBtn = new Button("Load current");
        loadBtn.setOnAction(e -> onLoad());
        Button previewBtn = new Button("Preview");
        previewBtn.setOnAction(e -> onPreview());
        Button applyBtn = new Button("Apply…");
        applyBtn.setTooltip(tip(
                "Opens the rollout dialog — confirms the collection name "
                + "(typed-confirm) and goes through the approval gate."));
        applyBtn.setOnAction(e -> onApply());

        Region grow = new Region();
        HBox.setHgrow(grow, Priority.ALWAYS);
        HBox actions = new HBox(8,
                small("Level"), levelPicker,
                small("Action"), actionPicker,
                grow, loadBtn, previewBtn, applyBtn);
        actions.setPadding(new Insets(10, 0, 0, 0));

        statusLabel.setStyle("-fx-text-fill: -color-fg-muted; -fx-font-size: 11px;");
        statusLabel.setWrapText(true);
        VBox v = new VBox(6, actions, statusLabel);
        return v;
    }

    /* =============================== actions =============================== */

    private void onLoad() {
        MongoClient client = clientSupplier.get();
        if (client == null) { fail("No active connection."); return; }
        String db = dbField.getText();
        String coll = collField.getText();
        if (blank(db) || blank(coll)) { fail("db + collection required"); return; }
        try {
            var current = fetcher.fetch(client, db.trim(), coll.trim());
            if (current.isEmpty()) {
                fail("Collection not found: " + db + "." + coll);
                return;
            }
            editor.setText(current.get().validatorJson());
            levelPicker.setValue(current.get().level());
            actionPicker.setValue(current.get().action());
            ok("Loaded current validator for " + db + "." + coll);
        } catch (Exception ex) {
            fail("Load failed: " + ex.getMessage());
        }
    }

    private void onPreview() {
        MongoClient client = clientSupplier.get();
        if (client == null) { fail("No active connection."); return; }
        String db = dbField.getText();
        String coll = collField.getText();
        String text = editor.getText();
        if (blank(db) || blank(coll) || blank(text)) {
            fail("db, collection, and a non-empty schema required");
            return;
        }
        statusLabel.setText("Running preview…");
        Thread.startVirtualThread(() -> {
            try {
                ValidatorSpec.Rollout rollout = new ValidatorSpec.Rollout(
                        db.trim(), coll.trim(), text,
                        levelPicker.getValue(), actionPicker.getValue());
                var result = previewService.preview(client, rollout);
                Platform.runLater(() -> {
                    offenders.setAll(result.firstFew());
                    ok("Sampled " + result.sampled() + " docs — "
                            + result.failedCount() + " would fail.");
                });
            } catch (Exception ex) {
                Platform.runLater(() -> fail("Preview failed: " + ex.getMessage()));
            }
        });
    }

    private void onApply() {
        MongoClient client = clientSupplier.get();
        if (client == null) { fail("No active connection."); return; }
        String db = dbField.getText();
        String coll = collField.getText();
        String text = editor.getText();
        if (blank(db) || blank(coll) || blank(text)) {
            fail("db, collection, and a non-empty schema required");
            return;
        }

        // Typed-confirm the collection name per SCHV-4.
        javafx.scene.control.TextInputDialog confirm = new javafx.scene.control.TextInputDialog();
        confirm.setHeaderText("Apply validator to " + db + "." + coll);
        confirm.setContentText("Type the collection name to confirm:");
        confirm.showAndWait().ifPresent(typed -> {
            if (!coll.trim().equals(typed.trim())) {
                fail("Typed-confirm mismatch — aborting.");
                return;
            }
            Thread.startVirtualThread(() -> {
                try {
                    ValidatorSpec.Rollout rollout = new ValidatorSpec.Rollout(
                            db.trim(), coll.trim(), text,
                            levelPicker.getValue(), actionPicker.getValue());
                    var outcome = rolloutRunner.apply(client, rollout);
                    Platform.runLater(() -> {
                        if (outcome instanceof ValidatorRolloutRunner.Outcome.Ok) {
                            ok("Validator applied to " + db + "." + coll + ".");
                        } else if (outcome instanceof ValidatorRolloutRunner.Outcome.Failed f) {
                            fail("Apply failed: " + f.code() + " — " + f.message());
                        }
                    });
                } catch (Exception ex) {
                    Platform.runLater(() -> fail("Apply failed: " + ex.getMessage()));
                }
            });
        });
    }

    /* =============================== helpers =============================== */

    private void ok(String msg) {
        statusLabel.setText(msg);
        statusLabel.setStyle(
                "-fx-text-fill: -color-success-emphasis; -fx-font-size: 11px; -fx-font-weight: 600;");
    }

    private void fail(String msg) {
        statusLabel.setText(msg);
        statusLabel.setStyle(
                "-fx-text-fill: -color-danger-emphasis; -fx-font-size: 11px; -fx-font-weight: 600;");
    }

    private static boolean blank(String s) { return s == null || s.isBlank(); }

    private static Label small(String text) {
        Label l = new Label(text);
        l.setStyle("-fx-text-fill: -color-fg-muted; -fx-font-size: 11px;");
        return l;
    }

    private static Tooltip tip(String body) {
        Tooltip t = new Tooltip(body);
        t.setShowDelay(Duration.millis(250));
        t.setShowDuration(Duration.seconds(20));
        t.setWrapText(true);
        t.setMaxWidth(360);
        return t;
    }

    private static <T> TableColumn<T, String> col(String title, int width,
                                                   java.util.function.Function<T, String> getter) {
        TableColumn<T, String> c = new TableColumn<>(title);
        c.setPrefWidth(width);
        c.setCellValueFactory(cd -> new SimpleStringProperty(getter.apply(cd.getValue())));
        return c;
    }
}

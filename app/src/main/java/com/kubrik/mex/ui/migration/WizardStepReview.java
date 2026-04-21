package com.kubrik.mex.ui.migration;

import com.kubrik.mex.migration.MigrationService;
import com.kubrik.mex.migration.engine.CollectionPlan;
import com.kubrik.mex.migration.preflight.PreflightReport;
import com.kubrik.mex.migration.spec.ConflictMode;
import javafx.application.Platform;
import javafx.beans.binding.Bindings;
import javafx.beans.binding.BooleanBinding;
import javafx.beans.property.SimpleBooleanProperty;
import javafx.beans.property.SimpleObjectProperty;
import javafx.geometry.Insets;
import javafx.scene.control.Label;
import javafx.scene.control.ScrollPane;
import javafx.scene.control.TextField;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import javafx.scene.paint.Color;

import java.util.List;

/** Step 5: shows the preflight report and collects any required typed confirmations
 *  before Start is enabled. Preflight runs on demand via {@link #refresh()}. */
public final class WizardStepReview implements WizardStep {

    private final WizardModel model;
    private final MigrationService service;

    private final VBox root = new VBox(12);
    private final Label summary = new Label();
    private final VBox collections = new VBox(4);
    private final VBox warnings = new VBox(4);
    private final VBox errors = new VBox(4);
    private final VBox confirmations = new VBox(8);
    private final TextField dropConfirmField = new TextField();
    private final SimpleObjectProperty<PreflightReport> reportProp = new SimpleObjectProperty<>();
    private final SimpleBooleanProperty dropConfirmed = new SimpleBooleanProperty(true);
    private final SimpleBooleanProperty preflightOk = new SimpleBooleanProperty(false);

    public WizardStepReview(WizardModel model, MigrationService service) {
        this.model = model;
        this.service = service;

        summary.setWrapText(true);
        root.setPadding(new Insets(16));
        ScrollPane scroll = new ScrollPane();
        VBox content = new VBox(12, summary, section("Collections", collections),
                section("Warnings", warnings), section("Errors", errors), confirmations);
        content.setPadding(new Insets(0, 8, 0, 0));
        scroll.setContent(content);
        scroll.setFitToWidth(true);
        scroll.setPrefHeight(420);
        root.getChildren().setAll(new Label("Press Refresh to run preflight."), scroll,
                refreshButton());
    }

    private javafx.scene.control.Button refreshButton() {
        javafx.scene.control.Button b = new javafx.scene.control.Button("Refresh preflight");
        b.setOnAction(e -> refresh());
        return b;
    }

    private Region section(String title, VBox body) {
        Label header = new Label(title);
        header.setStyle("-fx-font-weight: bold;");
        VBox box = new VBox(4, header, body);
        return box;
    }

    public void refresh() {
        summary.setText("Running preflight…");
        collections.getChildren().clear();
        warnings.getChildren().clear();
        errors.getChildren().clear();
        confirmations.getChildren().clear();
        preflightOk.set(false);
        dropConfirmed.set(true);
        dropConfirmField.clear();

        // Preflight touches MongoDB — run it off the FX thread.
        Thread.ofVirtual().name("preflight").start(() -> {
            try {
                PreflightReport report = service.preflight(model.toSpec());
                Platform.runLater(() -> render(report));
            } catch (Exception e) {
                Platform.runLater(() -> summary.setText("Preflight failed: " + e.getMessage()));
            }
        });
    }

    private void render(PreflightReport report) {
        reportProp.set(report);
        summary.setText(String.format("Plans: %d · Warnings: %d · Errors: %d",
                report.plans().size(), report.warnings().size(), report.errors().size()));

        for (CollectionPlan p : report.plans()) {
            Label row = new Label(p.sourceNs() + "  →  " + p.targetNs() + "   [" + p.conflictMode() + "]");
            collections.getChildren().add(row);
        }
        for (String w : report.warnings()) warnings.getChildren().add(warn(w));
        for (String err : report.errors()) errors.getChildren().add(err(err));

        // Drop-and-recreate typed confirmations (one field per namespace in that mode).
        List<String> toConfirm = report.plans().stream()
                .filter(p -> p.conflictMode() == ConflictMode.DROP_AND_RECREATE)
                .map(CollectionPlan::targetNs)
                .toList();
        if (!toConfirm.isEmpty()) {
            dropConfirmed.set(false);
            Label header = new Label("Type each target namespace to confirm drop-and-recreate:");
            header.setStyle("-fx-text-fill: #b45309;");
            confirmations.getChildren().add(header);
            for (String ns : toConfirm) {
                TextField f = new TextField();
                f.setPromptText(ns);
                f.textProperty().addListener((o, a, b) -> {
                    // all confirmations must match to flip the flag
                    boolean allOk = true;
                    for (javafx.scene.Node n : confirmations.getChildren()) {
                        if (n instanceof HBox h && h.getChildren().size() >= 2
                                && h.getChildren().get(1) instanceof TextField tf) {
                            Label lbl = (Label) h.getChildren().get(0);
                            if (!lbl.getText().equals(tf.getText())) allOk = false;
                        }
                    }
                    dropConfirmed.set(allOk);
                });
                HBox row = new HBox(8, new Label(ns), f);
                HBox.setHgrow(f, javafx.scene.layout.Priority.ALWAYS);
                confirmations.getChildren().add(row);
            }
        }
        preflightOk.set(!report.hasBlockingErrors());
    }

    private Label warn(String text) {
        Label l = new Label("⚠ " + text);
        l.setTextFill(Color.web("#b45309"));
        l.setWrapText(true);
        return l;
    }

    private Label err(String text) {
        Label l = new Label("✗ " + text);
        l.setTextFill(Color.web("#dc2626"));
        l.setWrapText(true);
        return l;
    }

    @Override public void onEnter() { refresh(); }

    @Override public String title() { return "5. Review"; }
    @Override public Region view() { return root; }

    @Override public BooleanBinding validProperty() {
        return Bindings.createBooleanBinding(
                () -> preflightOk.get() && dropConfirmed.get(),
                preflightOk, dropConfirmed);
    }
}

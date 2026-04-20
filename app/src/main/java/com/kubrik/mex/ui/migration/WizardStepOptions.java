package com.kubrik.mex.ui.migration;

import com.kubrik.mex.migration.spec.ConflictMode;
import com.kubrik.mex.migration.spec.ExecutionMode;
import com.kubrik.mex.migration.spec.MigrationKind;
import com.kubrik.mex.migration.spec.PerfSpec;
import com.kubrik.mex.migration.spec.SinkSpec;
import javafx.beans.binding.Bindings;
import javafx.beans.binding.BooleanBinding;
import javafx.collections.FXCollections;
import javafx.geometry.Insets;
import javafx.scene.Node;
import javafx.scene.control.Button;
import javafx.scene.control.CheckBox;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Label;
import javafx.scene.control.RadioButton;
import javafx.scene.control.Spinner;
import javafx.scene.control.TextField;
import javafx.scene.control.ToggleGroup;
import javafx.scene.control.Tooltip;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.stage.DirectoryChooser;

import java.io.File;

/** Step 4: execution mode, conflict mode, performance knobs. Transforms + verification live
 *  in their default form for MVP; exposing them is a follow-up. */
public final class WizardStepOptions implements WizardStep {

    private final WizardModel model;
    private final GridPane root = new GridPane();

    public WizardStepOptions(WizardModel model) {
        this.model = model;

        // --- execution mode ------------------------------------------------------
        RadioButton runBtn = new RadioButton("Run");
        RadioButton dryBtn = new RadioButton("Dry-run (no writes)");
        ToggleGroup execGroup = new ToggleGroup();
        runBtn.setToggleGroup(execGroup);
        dryBtn.setToggleGroup(execGroup);
        runBtn.setSelected(model.executionMode.get() == ExecutionMode.RUN);
        dryBtn.setSelected(model.executionMode.get() == ExecutionMode.DRY_RUN);
        execGroup.selectedToggleProperty().addListener((o, a, b) ->
                model.executionMode.set(b == dryBtn ? ExecutionMode.DRY_RUN : ExecutionMode.RUN));
        HBox execRow = new HBox(16, runBtn, dryBtn);

        // --- conflict mode -------------------------------------------------------
        ComboBox<ConflictMode> conflictBox = new ComboBox<>(
                FXCollections.observableArrayList(ConflictMode.values()));
        conflictBox.setValue(model.conflictMode.get());
        conflictBox.valueProperty().addListener((o, a, b) -> model.conflictMode.set(b));

        // --- performance ---------------------------------------------------------
        PerfSpec d = model.perf.get();
        Spinner<Integer> parallel = intSpinner(1, 16, d.parallelCollections());
        Spinner<Integer> batchDocs = intSpinner(10, 100_000, d.batchDocs());
        Spinner<Integer> rateLimit = intSpinner(0, 1_000_000, (int) d.rateLimitDocsPerSec());
        Spinner<Integer> retry = intSpinner(0, 20, d.retryAttempts());

        Runnable syncPerf = () -> model.perf.set(new PerfSpec(
                parallel.getValue(),
                d.partitionThreshold(),
                batchDocs.getValue(),
                d.batchBytes(),
                rateLimit.getValue().longValue(),
                retry.getValue()));
        parallel.valueProperty().addListener((o, a, b) -> syncPerf.run());
        batchDocs.valueProperty().addListener((o, a, b) -> syncPerf.run());
        rateLimit.valueProperty().addListener((o, a, b) -> syncPerf.run());
        retry.valueProperty().addListener((o, a, b) -> syncPerf.run());

        // --- data-transfer-only: file sink destination (EXT-2) ------------------
        // A null kind ("MongoDB target") means the existing Mongo-to-Mongo path; any other
        // kind flips the writer to a file sink and bypasses the target cluster entirely.
        Label destLabel = new Label("Destination:");
        ComboBox<SinkKindChoice> destBox = new ComboBox<>(FXCollections.observableArrayList(
                SinkKindChoice.MONGODB,
                new SinkKindChoice(SinkSpec.SinkKind.NDJSON,     "NDJSON file (one doc per line)"),
                new SinkKindChoice(SinkSpec.SinkKind.JSON_ARRAY, "JSON array file"),
                new SinkKindChoice(SinkSpec.SinkKind.CSV,        "CSV (flat, top-level columns)"),
                new SinkKindChoice(SinkSpec.SinkKind.BSON_DUMP,  "BSON dump (mongorestore-compatible)")));
        destBox.getSelectionModel().select(
                model.sinkKind.get() == null
                        ? SinkKindChoice.MONGODB
                        : SinkKindChoice.of(model.sinkKind.get()));
        destBox.valueProperty().addListener((o, a, b) ->
                model.sinkKind.set(b == null ? null : b.kind));

        Label sinkPathLabel = new Label("Output folder:");
        TextField sinkPathField = new TextField();
        sinkPathField.setPromptText("e.g. /tmp/export");
        sinkPathField.setTooltip(new Tooltip(
                "Each migrated collection writes to <folder>/<db>.<coll>.<extension>. "
              + "The folder will be created if it doesn't exist."));
        sinkPathField.textProperty().bindBidirectional(model.sinkPath);
        Button pickFolder = new Button("Browse…");
        pickFolder.setOnAction(e -> {
            DirectoryChooser dc = new DirectoryChooser();
            String current = model.sinkPath.get();
            if (current != null && !current.isBlank()) {
                File dir = new File(current);
                if (dir.isDirectory()) dc.setInitialDirectory(dir);
            }
            File chosen = dc.showDialog(pickFolder.getScene() == null ? null : pickFolder.getScene().getWindow());
            if (chosen != null) model.sinkPath.set(chosen.getAbsolutePath());
        });
        HBox sinkPathRow = new HBox(8, sinkPathField, pickFolder);
        HBox.setHgrow(sinkPathField, Priority.ALWAYS);

        // --- versioned-only: environment (VER-8) + ignore drift (VER-4) ---------
        Label envLabel = new Label("Environment:");
        TextField envField = new TextField();
        envField.setPromptText("optional — e.g. dev, staging, prod");
        envField.setTooltip(new Tooltip(
                "Scripts annotated with an @env filter run only when they match this value. "
              + "Leave blank to disable environment filtering."));
        envField.textProperty().bindBidirectional(model.environment);

        Label driftLabel = new Label("Drift policy:");
        CheckBox ignoreDriftBox = new CheckBox("Acknowledge checksum drift and apply pending scripts");
        ignoreDriftBox.setTooltip(new Tooltip(
                "When a previously-applied script's file has changed on disk, the engine refuses "
              + "to apply any newer scripts. Tick to acknowledge and proceed — the drift warning "
              + "is still reported in the job summary."));
        ignoreDriftBox.selectedProperty().bindBidirectional(model.ignoreDrift);

        root.setHgap(12);
        root.setVgap(10);
        root.setPadding(new Insets(16));
        int r = 0;
        root.addRow(r++, new Label("Execution mode:"), execRow);
        root.addRow(r++, new Label("Conflict mode:"), conflictBox);
        root.addRow(r++, new Label("Parallel collections:"), parallel);
        root.addRow(r++, new Label("Batch size (docs):"), batchDocs);
        root.addRow(r++, new Label("Rate limit (docs/sec, 0 = unlimited):"), rateLimit);
        root.addRow(r++, new Label("Retry attempts:"), retry);
        root.addRow(r++, destLabel, destBox);
        int sinkPathRowIdx = r++;
        root.addRow(sinkPathRowIdx, sinkPathLabel, sinkPathRow);
        int envRow = r++;
        int driftRow = r++;
        root.addRow(envRow, envLabel, envField);
        root.addRow(driftRow, driftLabel, ignoreDriftBox);

        conflictBox.setMaxWidth(Double.MAX_VALUE);
        envField.setMaxWidth(Double.MAX_VALUE);
        destBox.setMaxWidth(Double.MAX_VALUE);
        GridPane.setHgrow(conflictBox, Priority.ALWAYS);
        GridPane.setHgrow(envField, Priority.ALWAYS);
        GridPane.setHgrow(destBox, Priority.ALWAYS);

        // Destination section is only relevant for data-transfer; versioned migrations target
        // a Mongo database by definition (they apply DDL-like ops).
        Runnable syncDestVisibility = () -> {
            boolean dataTransfer = model.kind.get() == MigrationKind.DATA_TRANSFER;
            boolean needsPath = dataTransfer && model.sinkKind.get() != null;
            destLabel.setVisible(dataTransfer);  destLabel.setManaged(dataTransfer);
            destBox.setVisible(dataTransfer);    destBox.setManaged(dataTransfer);
            sinkPathLabel.setVisible(needsPath); sinkPathLabel.setManaged(needsPath);
            sinkPathRow.setVisible(needsPath);   sinkPathRow.setManaged(needsPath);
        };
        model.kind.addListener((o, a, b) -> syncDestVisibility.run());
        model.sinkKind.addListener((o, a, b) -> syncDestVisibility.run());
        syncDestVisibility.run();

        Runnable syncVersionedVisibility = () -> {
            boolean versioned = model.kind.get() == MigrationKind.VERSIONED;
            for (Node n : new Node[] { envLabel, envField, driftLabel, ignoreDriftBox }) {
                n.setVisible(versioned);
                n.setManaged(versioned);
            }
        };
        model.kind.addListener((o, a, b) -> syncVersionedVisibility.run());
        syncVersionedVisibility.run();
    }

    /** Dropdown entry. {@code null} kind renders as "MongoDB target" (the default, keeps the
     *  existing Mongo-to-Mongo path); other entries flip to a file sink. */
    private record SinkKindChoice(SinkSpec.SinkKind kind, String label) {
        static final SinkKindChoice MONGODB = new SinkKindChoice(null, "MongoDB target");

        static SinkKindChoice of(SinkSpec.SinkKind k) {
            return new SinkKindChoice(k, switch (k) {
                case NDJSON     -> "NDJSON file (one doc per line)";
                case JSON_ARRAY -> "JSON array file";
                case CSV        -> "CSV (flat, top-level columns)";
                case BSON_DUMP  -> "BSON dump (mongorestore-compatible)";
                // Plugin kinds aren't offered in the dropdown — users configure them via
                // profile YAML. Rendered here only to keep the switch exhaustive.
                case PLUGIN     -> "Plugin sink";
            });
        }

        @Override public String toString() { return label; }
    }

    private static Spinner<Integer> intSpinner(int min, int max, int initial) {
        Spinner<Integer> s = new Spinner<>(min, max, initial);
        s.setEditable(true);
        s.setPrefWidth(140);
        return s;
    }

    @Override public String title() { return "4. Options"; }
    @Override public Region view() { return root; }
    @Override public BooleanBinding validProperty() {
        return Bindings.createBooleanBinding(() -> true); // all fields have defaults
    }
}

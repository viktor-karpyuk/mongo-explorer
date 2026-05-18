package com.kubrik.mex.ui.migration;

import com.kubrik.mex.core.ConnectionManager;
import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.migration.JobId;
import com.kubrik.mex.migration.MigrationService;
import com.kubrik.mex.migration.spec.ExecutionMode;
import com.kubrik.mex.migration.spec.MigrationSpec;
import com.kubrik.mex.store.ConnectionStore;
import javafx.application.Platform;
import javafx.beans.binding.Bindings;
import javafx.beans.property.SimpleIntegerProperty;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TextInputDialog;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Region;
import javafx.scene.layout.StackPane;
import javafx.scene.layout.VBox;
import javafx.stage.Modality;
import javafx.stage.Stage;
import javafx.stage.Window;

import java.util.ArrayList;
import java.util.List;

/** Non-modal wizard window (docs/mvp-functional-spec.md §6). Users can pop one of these
 *  open alongside the main app; closing after Step 6 begins detaches the view — the job
 *  keeps running in the background. */
public final class MigrationWizard extends Stage {

    private final WizardModel model = new WizardModel();
    private final List<WizardStep> steps = new ArrayList<>();
    private final StackPane content = new StackPane();
    private final SimpleIntegerProperty currentStep = new SimpleIntegerProperty(0);
    private final Label stepIndicator = new Label();
    private final Button backBtn = new Button("Back");
    private final Button nextBtn = new Button("Next");
    private final Button saveBtn = new Button("Save as profile");
    private final Button cancelBtn = new Button("Close");

    private final MigrationService service;
    private final WizardStepReview reviewStep;
    private final WizardStepRun runStep;

    public MigrationWizard(Window owner,
                           MigrationService service,
                           ConnectionStore connectionStore,
                           ConnectionManager manager,
                           EventBus bus) {
        this.service = service;
        initOwner(owner);
        initModality(Modality.NONE);
        setTitle("New migration");

        steps.add(new WizardStepSource(model, connectionStore, manager, bus));
        steps.add(new WizardStepTarget(model, connectionStore, manager, bus));
        steps.add(new WizardStepScope(model, manager));
        steps.add(new WizardStepOptions(model));
        reviewStep = new WizardStepReview(model, service);
        runStep = new WizardStepRun(service, bus);
        steps.add(reviewStep);
        steps.add(runStep);

        for (WizardStep s : steps) {
            Region v = s.view();
            v.setVisible(false);
            content.getChildren().add(v);
        }

        stepIndicator.setStyle("-fx-font-weight: bold; -fx-font-size: 14px;");
        HBox buttonRow = new HBox(8, saveBtn, spacer(), backBtn, nextBtn, cancelBtn);
        buttonRow.setPadding(new Insets(12));
        buttonRow.setAlignment(Pos.CENTER_LEFT);

        VBox header = new VBox(stepIndicator);
        header.setPadding(new Insets(12, 16, 6, 16));

        BorderPane root = new BorderPane();
        root.setTop(header);
        root.setCenter(content);
        root.setBottom(buttonRow);

        Scene scene = new Scene(root, 960, 720);
        setScene(scene);
        setMinWidth(960);
        setMinHeight(640);

        // UX-13 — when the user closes the wizard while a job is still running, show a toast
        // so it's obvious the migration continues in the background.
        setOnHiding(ev -> showBackgroundRunToast());

        backBtn.setOnAction(e -> go(currentStep.get() - 1));
        nextBtn.setOnAction(e -> onNext());
        saveBtn.setOnAction(e -> onSave());
        cancelBtn.setOnAction(e -> close());

        // Keyboard shortcuts — docs/mvp-functional-spec.md §13.
        scene.addEventFilter(javafx.scene.input.KeyEvent.KEY_PRESSED, evt -> {
            if (evt.getCode() == javafx.scene.input.KeyCode.ESCAPE) {
                close();
                evt.consume();
            } else if (evt.getCode() == javafx.scene.input.KeyCode.ENTER
                    && !nextBtn.isDisabled()
                    && !(evt.getTarget() instanceof javafx.scene.control.TextArea)) {
                // Enter advances unless the user is in a multi-line editor (globs).
                onNext();
                evt.consume();
            }
        });

        currentStep.addListener((o, a, b) -> renderStep());
        renderStep();
    }

    private Region spacer() {
        Region r = new Region();
        HBox.setHgrow(r, javafx.scene.layout.Priority.ALWAYS);
        return r;
    }

    private void renderStep() {
        int idx = currentStep.get();
        for (int i = 0; i < steps.size(); i++) {
            steps.get(i).view().setVisible(i == idx);
        }
        WizardStep s = steps.get(idx);
        stepIndicator.setText(s.title() + "   of " + steps.size());
        backBtn.setDisable(idx == 0 || idx == steps.size() - 1);
        saveBtn.setDisable(idx < 2);   // Save becomes meaningful after scope is set

        // Next button label
        if (idx == steps.size() - 2) {
            nextBtn.setText(model.executionMode.get() == ExecutionMode.DRY_RUN ? "Start dry-run" : "Start migration");
        } else if (idx == steps.size() - 1) {
            nextBtn.setText("Done");
        } else {
            nextBtn.setText("Next");
        }

        // Next button validity
        nextBtn.disableProperty().unbind();
        if (idx == steps.size() - 1) {
            nextBtn.disableProperty().bind(runStep.validProperty().not());
        } else {
            nextBtn.disableProperty().bind(Bindings.not(s.validProperty()));
        }

        s.onEnter();
    }

    private void onNext() {
        int idx = currentStep.get();
        if (idx == steps.size() - 2) {
            // Start the job and jump to step 6.
            MigrationSpec spec = model.toSpec();
            try {
                boolean dryRun = spec.options().executionMode() == ExecutionMode.DRY_RUN;
                JobId id = dryRun ? service.startDryRun(spec) : service.start(spec);
                runStep.bindJob(id, dryRun);
                go(steps.size() - 1);
            } catch (Exception e) {
                showError("Unable to start migration", e.getMessage());
            }
        } else if (idx == steps.size() - 1) {
            close();
        } else {
            go(idx + 1);
        }
    }

    private void onSave() {
        try {
            TextInputDialog d = new TextInputDialog();
            d.setTitle("Save as profile");
            d.setHeaderText("Give this profile a name.");
            d.setContentText("Name:");
            d.initOwner(this);
            d.showAndWait().ifPresent(name -> {
                if (name.isBlank()) return;
                MigrationSpec spec = model.toSpec();
                service.profiles().save(name.trim(), spec);
                showInfo("Profile saved", "Saved as `" + name + "`.");
            });
        } catch (Exception e) {
            showError("Save failed", e.getMessage());
        }
    }

    private void go(int target) {
        if (target < 0 || target >= steps.size()) return;
        currentStep.set(target);
    }

    /** UX-13 one-line toast. No-op when no non-terminal job is in flight. */
    private void showBackgroundRunToast() {
        if (getOwner() == null) return;
        boolean anyLive;
        try {
            anyLive = service.list(com.kubrik.mex.migration.JobHistoryQuery.all())
                    .stream().anyMatch(r -> !r.status().isTerminal());
        } catch (Exception ignored) { return; }
        if (!anyLive) return;

        Label toast = new Label(
                "Migration continues in the background. Open it from the status bar or the Migrations tab.");
        toast.setStyle("-fx-background-color: rgba(15,23,42,0.92); -fx-text-fill: #f8fafc; "
                + "-fx-padding: 10 14; -fx-background-radius: 6; -fx-font-size: 12px;");
        javafx.stage.Popup popup = new javafx.stage.Popup();
        popup.getContent().add(toast);
        popup.setAutoHide(true);
        Window owner = getOwner();
        popup.show(owner,
                owner.getX() + owner.getWidth() - 520,
                owner.getY() + owner.getHeight() - 80);
        javafx.animation.PauseTransition t = new javafx.animation.PauseTransition(
                javafx.util.Duration.seconds(4));
        t.setOnFinished(e -> popup.hide());
        t.play();
    }

    private void showError(String title, String message) {
        javafx.scene.control.Alert a = new javafx.scene.control.Alert(
                javafx.scene.control.Alert.AlertType.ERROR, message);
        a.setHeaderText(title);
        a.initOwner(this);
        a.showAndWait();
    }

    private void showInfo(String title, String message) {
        javafx.scene.control.Alert a = new javafx.scene.control.Alert(
                javafx.scene.control.Alert.AlertType.INFORMATION, message);
        a.setHeaderText(title);
        a.initOwner(this);
        a.showAndWait();
    }

    /** Open a new wizard seeded from the connection tree's context node. Any of the
     *  arguments may be null; only the non-null ones fill the model. */
    public static void openSeeded(Window owner,
                                  MigrationService service,
                                  ConnectionStore connectionStore,
                                  ConnectionManager manager,
                                  EventBus bus,
                                  String connectionId,
                                  String db,
                                  String coll) {
        MigrationWizard w = new MigrationWizard(owner, service, connectionStore, manager, bus);
        Platform.runLater(() -> {
            if (connectionId != null) w.model.sourceConnectionId.set(connectionId);
            if (db != null) w.model.sourceDatabase.set(db);
            if (coll != null) {
                w.model.granularity.set(WizardModel.Granularity.COLLECTIONS);
                w.model.selectedCollections.clear();
                w.model.selectedCollections.add(db + "." + coll);
                // Pre-tick the DB too so if the user flips granularity to DATABASES the
                // owning database is already checked.
                if (db != null) {
                    w.model.selectedDatabases.setAll(db);
                }
            } else if (db != null) {
                w.model.granularity.set(WizardModel.Granularity.DATABASES);
                // Pre-check the clicked DB in the Step 3 list — WizardStepScope reads
                // selectedDatabases to restore prior ticks on first render.
                w.model.selectedDatabases.setAll(db);
            }
        });
        w.show();
    }

    /**
     * UX-8 — open the wizard from the Query History view: seed connection + collection and
     * install the history entry's body as the XFORM-2 filter for that collection. Callers
     * pass the raw JSON query body; this helper does not validate it — validation happens
     * when the user advances to the wizard's Review step.
     */
    public static void openFromHistory(Window owner,
                                       MigrationService service,
                                       ConnectionStore connectionStore,
                                       ConnectionManager manager,
                                       EventBus bus,
                                       String connectionId,
                                       String db,
                                       String coll,
                                       String filterJson) {
        MigrationWizard w = new MigrationWizard(owner, service, connectionStore, manager, bus);
        Platform.runLater(() -> {
            if (connectionId != null) w.model.sourceConnectionId.set(connectionId);
            if (db != null) w.model.sourceDatabase.set(db);
            if (coll != null && db != null) {
                w.model.granularity.set(WizardModel.Granularity.COLLECTIONS);
                w.model.selectedCollections.clear();
                w.model.selectedCollections.add(db + "." + coll);
                if (filterJson != null && !filterJson.isBlank()) {
                    w.model.transforms.put(db + "." + coll, new com.kubrik.mex.migration.spec.TransformSpec(
                            filterJson, null, java.util.Map.of(), java.util.List.of(), java.util.List.of()));
                }
            }
        });
        w.show();
    }

    /** Open a new wizard already seeded with the given spec — used by profile "Run". */
    public static void openPrefilled(Window owner,
                                     MigrationService service,
                                     ConnectionStore connectionStore,
                                     ConnectionManager manager,
                                     EventBus bus,
                                     MigrationSpec spec) {
        MigrationWizard w = new MigrationWizard(owner, service, connectionStore, manager, bus);
        Platform.runLater(() -> {
            w.model.sourceConnectionId.set(spec.source().connectionId());
            w.model.readPreference.set(spec.source().readPreference());
            w.model.targetConnectionId.set(spec.target().connectionId());
            if (spec.target().database() != null) w.model.targetDatabase.set(spec.target().database());
            w.model.kind.set(spec.kind());
            w.model.executionMode.set(spec.options().executionMode());
            w.model.conflictMode.set(spec.options().conflict().defaultMode());
            w.model.perf.set(spec.options().performance());
        });
        w.show();
    }
}

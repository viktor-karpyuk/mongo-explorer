package com.kubrik.mex.labs.ui;

import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.labs.docker.DockerClient;
import com.kubrik.mex.labs.lifecycle.LabLifecycleService;
import com.kubrik.mex.labs.model.EngineStatus;
import com.kubrik.mex.labs.model.LabDeployment;
import com.kubrik.mex.labs.model.LabStatus;
import com.kubrik.mex.labs.model.LabTemplate;
import com.kubrik.mex.labs.store.LabDeploymentDao;
import com.kubrik.mex.labs.templates.LabTemplateRegistry;
import javafx.application.Platform;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.ButtonType;
import javafx.scene.control.Label;
import javafx.scene.control.ListView;
import javafx.scene.control.ProgressIndicator;
import javafx.scene.control.SplitPane;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextInputDialog;
import javafx.scene.control.TextArea;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;

import java.time.Instant;

/**
 * v2.8.4 UI-LAB-* — Top-level Labs tab. Three panes:
 * <ul>
 *   <li>Left: template catalogue (ListView of the 6 shipped
 *       templates; clicking Apply kicks off a new Lab).</li>
 *   <li>Centre: running-labs table (ObservableList mirrors
 *       {@code lab_deployments.listLive()}; refreshes on
 *       lifecycle events).</li>
 *   <li>Bottom: rollout / detail drawer — progress line during
 *       Apply, then the selected-row event log.</li>
 * </ul>
 *
 * <p>Docker-not-available fallback renders
 * {@link DockerEmptyStatePane} in place of the three-pane layout
 * until the user fixes the engine.</p>
 */
public final class LabsTab extends BorderPane {

    private final DockerClient docker;
    private final LabTemplateRegistry registry;
    private final LabLifecycleService lifecycle;
    private final LabDeploymentDao deploymentDao;
    private final EventBus eventBus;

    private final ObservableList<LabDeployment> runningRows =
            FXCollections.observableArrayList();
    private final TableView<LabDeployment> runningTable = new TableView<>(runningRows);
    private final TextArea progressArea = new TextArea();
    private final Label statusLabel = new Label("Pick a template to spin up.");
    private final ProgressIndicator busySpinner = new ProgressIndicator();
    private EventBus.Subscription labSubscription;

    public LabsTab(DockerClient docker, LabTemplateRegistry registry,
                    LabLifecycleService lifecycle, LabDeploymentDao deploymentDao,
                    EventBus eventBus) {
        this.docker = docker;
        this.registry = registry;
        this.lifecycle = lifecycle;
        this.deploymentDao = deploymentDao;
        this.eventBus = eventBus;

        setStyle("-fx-background-color: -color-bg-default;");
        setPadding(new Insets(14, 16, 14, 16));
        setAccessibleText("Labs tab");
        setAccessibleHelp(
                "Spin up local Docker-based MongoDB sandboxes from a "
                + "catalogue of pre-built templates. Destroy them when "
                + "done to reclaim the volumes.");

        busySpinner.setVisible(false);
        busySpinner.setPrefSize(18, 18);
        refreshEngineState();

        if (eventBus != null) {
            labSubscription = eventBus.onLab(evt ->
                    Platform.runLater(this::reloadRunning));
        }
    }

    /** Re-probe Docker + rebuild the tab body. Called from the
     *  empty-state's Retry button via a callback. */
    private void refreshEngineState() {
        EngineStatus status = docker.status();
        if (status != EngineStatus.READY) {
            setCenter(new DockerEmptyStatePane(docker, this::refreshEngineState));
            setTop(null);
            setBottom(null);
            return;
        }
        setTop(buildHeader());
        setCenter(buildSplit());
        setBottom(buildFooter());
        reloadRunning();
    }

    private Region buildHeader() {
        Label title = new Label("Labs");
        title.setStyle("-fx-font-size: 14px; -fx-font-weight: 700;");
        Label hint = new Label(
                "Local Docker sandboxes for training, evaluation, and "
                + "testing. Each Lab creates its own compose project + "
                + "auto-adds a Mongo Explorer connection.");
        hint.setStyle("-fx-text-fill: -color-fg-muted; -fx-font-size: 11px;");
        hint.setWrapText(true);
        VBox v = new VBox(4, title, hint);
        v.setPadding(new Insets(0, 0, 10, 0));
        return v;
    }

    private Region buildSplit() {
        SplitPane split = new SplitPane();
        split.getItems().addAll(buildCatalogue(), buildRunningPane());
        split.setDividerPositions(0.35);
        return split;
    }

    private Region buildCatalogue() {
        Label head = new Label("Templates");
        head.setStyle("-fx-text-fill: -color-fg-default; -fx-font-size: 11px; -fx-font-weight: 600;");

        ListView<LabTemplate> list = new ListView<>(
                FXCollections.observableArrayList(registry.all()));
        list.setCellFactory(lv -> new javafx.scene.control.ListCell<>() {
            @Override protected void updateItem(LabTemplate t, boolean empty) {
                super.updateItem(t, empty);
                if (empty || t == null) { setText(null); setGraphic(null); return; }
                Label name = new Label(t.displayName());
                name.setStyle("-fx-font-weight: 600;");
                Label meta = new Label("~" + t.estMemoryMib() + " MiB · "
                        + t.estStartupSeconds() + "s startup");
                meta.setStyle("-fx-text-fill: -color-fg-subtle; -fx-font-size: 10px;");
                Label desc = new Label(t.description());
                desc.setStyle("-fx-text-fill: -color-fg-muted; -fx-font-size: 11px;");
                desc.setWrapText(true);
                VBox box = new VBox(2, name, meta, desc);
                box.setPadding(new Insets(4, 4, 8, 4));
                setGraphic(box);
                setText(null);
            }
        });

        Button applyBtn = new Button("Apply…");
        applyBtn.setOnAction(e -> {
            LabTemplate sel = list.getSelectionModel().getSelectedItem();
            if (sel == null) {
                statusLabel.setText("Pick a template first.");
                return;
            }
            onApply(sel);
        });

        VBox v = new VBox(6, head, list, applyBtn);
        VBox.setVgrow(list, Priority.ALWAYS);
        v.setPadding(new Insets(0, 10, 0, 0));
        return v;
    }

    private Region buildRunningPane() {
        Label head = new Label("Running Labs");
        head.setStyle("-fx-text-fill: -color-fg-default; -fx-font-size: 11px; -fx-font-weight: 600;");

        runningTable.setPlaceholder(new Label("No Labs yet — apply a template from the left."));
        runningTable.getColumns().setAll(
                col("Name", 200, LabDeployment::displayName),
                col("Template", 120, LabDeployment::templateId),
                col("Status", 100, d -> d.status().name()),
                col("Created", 160, d -> Instant.ofEpochMilli(d.createdAt()).toString()));
        runningTable.getSelectionModel().selectedItemProperty().addListener(
                (o, a, b) -> renderDetail(b));

        progressArea.setEditable(false);
        progressArea.setPrefRowCount(8);
        progressArea.setWrapText(true);
        progressArea.setStyle("-fx-font-family: 'Menlo', 'Courier New', monospace; -fx-font-size: 11px;");

        Button stopBtn = new Button("Stop");
        stopBtn.setOnAction(e -> onLifecycle(LabLifecycleService::stop, "Stop"));
        Button startBtn = new Button("Start");
        startBtn.setOnAction(e -> onLifecycle(LabLifecycleService::start, "Start"));
        Button destroyBtn = new Button("Destroy…");
        destroyBtn.setOnAction(e -> onDestroy());

        HBox actions = new HBox(8, stopBtn, startBtn, destroyBtn);
        actions.setPadding(new Insets(8, 0, 0, 0));

        VBox v = new VBox(6, head, runningTable, new Label("Detail"), progressArea, actions);
        VBox.setVgrow(runningTable, Priority.ALWAYS);
        return v;
    }

    private Region buildFooter() {
        statusLabel.setStyle("-fx-text-fill: -color-fg-muted; -fx-font-size: 11px;");
        statusLabel.setWrapText(true);
        HBox h = new HBox(8, busySpinner, statusLabel);
        h.setPadding(new Insets(8, 0, 0, 0));
        return h;
    }

    /* ============================== actions ============================== */

    private void onApply(LabTemplate template) {
        TextInputDialog dlg = new TextInputDialog(
                template.displayName() + " (Lab)");
        dlg.setHeaderText("Name your new " + template.displayName() + " Lab");
        dlg.setContentText("Display name:");
        dlg.showAndWait().ifPresent(name -> {
            busySpinner.setVisible(true);
            statusLabel.setText("Creating " + name + "…");
            progressArea.clear();

            LabLifecycleService.ApplyOptions opts =
                    new LabLifecycleService.ApplyOptions(name, null, false, false);

            lifecycle.apply(template, opts, line ->
                    Platform.runLater(() -> {
                        progressArea.appendText(line + "\n");
                        statusLabel.setText(line);
                    }))
                    .whenComplete((lab, err) -> Platform.runLater(() -> {
                        busySpinner.setVisible(false);
                        if (err != null) {
                            statusLabel.setText("Apply failed: " + err.getMessage());
                        } else if (lab != null) {
                            statusLabel.setText("Lab " + lab.displayName()
                                    + " is " + lab.status().name() + ".");
                        }
                        reloadRunning();
                    }));
        });
    }

    private void onLifecycle(LifecycleOp op, String label) {
        LabDeployment sel = runningTable.getSelectionModel().getSelectedItem();
        if (sel == null) { statusLabel.setText("Pick a Lab first."); return; }
        busySpinner.setVisible(true);
        statusLabel.setText(label + "…");
        Thread.startVirtualThread(() -> {
            LabLifecycleService.TransitionResult r;
            try {
                r = op.apply(lifecycle, sel.id());
            } catch (Throwable t) {
                r = new LabLifecycleService.TransitionResult.Failed(
                        t.getClass().getSimpleName() + ": " + t.getMessage());
            }
            var result = r;
            Platform.runLater(() -> {
                busySpinner.setVisible(false);
                if (result instanceof LabLifecycleService.TransitionResult.Rejected rj) {
                    statusLabel.setText(label + " rejected: " + rj.reason());
                } else if (result instanceof LabLifecycleService.TransitionResult.Failed f) {
                    statusLabel.setText(label + " failed: " + f.reason());
                } else if (result instanceof LabLifecycleService.TransitionResult.Ok ok) {
                    statusLabel.setText(label + " OK — " + ok.lab().status());
                }
                reloadRunning();
            });
        });
    }

    private void onDestroy() {
        LabDeployment sel = runningTable.getSelectionModel().getSelectedItem();
        if (sel == null) { statusLabel.setText("Pick a Lab first."); return; }
        TextInputDialog confirm = new TextInputDialog();
        confirm.setHeaderText("Destroy " + sel.displayName() + "?");
        confirm.setContentText("Type the Lab name to confirm — this removes "
                + "containers AND volumes:");
        confirm.showAndWait().ifPresent(typed -> {
            if (!sel.displayName().equals(typed.trim())) {
                statusLabel.setText("Typed-confirm mismatch — aborting.");
                return;
            }
            onLifecycle(LabLifecycleService::destroy, "Destroy");
        });
    }

    private void renderDetail(LabDeployment lab) {
        if (lab == null) { progressArea.clear(); return; }
        progressArea.setText(
                "id: " + lab.id() + "\n"
                + "compose project: " + lab.composeProject() + "\n"
                + "compose file: " + lab.composeFilePath() + "\n"
                + "status: " + lab.status() + "\n"
                + "mongo image: " + lab.mongoImageTag() + "\n"
                + "ports:\n" + lab.portMap().toJson() + "\n"
                + "connection id: " + lab.connectionId().orElse("(none)"));
    }

    private void reloadRunning() {
        runningRows.setAll(deploymentDao.listLive());
    }

    /* ============================ helpers ============================ */

    @FunctionalInterface
    private interface LifecycleOp {
        LabLifecycleService.TransitionResult apply(LabLifecycleService svc, long id);
    }

    private static <T> TableColumn<T, String> col(String title, int width,
                                                    java.util.function.Function<T, String> getter) {
        TableColumn<T, String> c = new TableColumn<>(title);
        c.setPrefWidth(width);
        c.setCellValueFactory(cd -> new SimpleStringProperty(getter.apply(cd.getValue())));
        return c;
    }
}

package com.kubrik.mex.k8s.ui;

import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.k8s.apply.ProvisioningRecordDao;
import com.kubrik.mex.k8s.events.ProvisionEvent;
import com.kubrik.mex.k8s.model.K8sClusterRef;
import com.kubrik.mex.k8s.model.ProvisioningRecord;
import com.kubrik.mex.k8s.teardown.CascadePlan;
import com.kubrik.mex.k8s.teardown.TearDownService;
import javafx.application.Platform;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.scene.control.Button;
import javafx.scene.control.ButtonType;
import javafx.scene.control.CheckBox;
import javafx.scene.control.Dialog;
import javafx.scene.control.Label;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextField;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.VBox;
import javafx.stage.FileChooser;

import java.nio.file.Files;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * v2.8.1 Q2.8.1-L — Historical-provisions panel for the Clusters
 * tab.
 *
 * <p>Shows every {@code provisioning_records} row across all
 * clusters. Per-row actions:</p>
 * <ul>
 *   <li><b>Tear down…</b> — typed-confirm + cascade-plan dialog
 *       driving {@link TearDownService}.</li>
 *   <li><b>Toggle protection</b> — flip
 *       {@code deletion_protection} so a Prod row becomes deletable.</li>
 * </ul>
 *
 * <p>The live view re-renders on every {@link ProvisionEvent}
 * terminal (Started / Ready / Failed) so a new deployment lands
 * here seconds after Apply.</p>
 */
public final class ProvisionsPanel extends VBox {

    private final ProvisioningRecordDao recordDao;
    private final TearDownService tearDown;
    private final EventBus events;
    private final ClusterLookup clusterLookup;

    private final ObservableList<ProvisioningRecord> rows =
            FXCollections.observableArrayList();
    private final TableView<ProvisioningRecord> table = new TableView<>(rows);
    private final Label statusLabel = new Label("Provisioned MongoDB deployments.");
    private EventBus.Subscription sub;

    public ProvisionsPanel(ProvisioningRecordDao recordDao,
                             TearDownService tearDown,
                             EventBus events,
                             ClusterLookup clusterLookup) {
        super(6);
        this.recordDao = Objects.requireNonNull(recordDao);
        this.tearDown = Objects.requireNonNull(tearDown);
        this.events = Objects.requireNonNull(events);
        this.clusterLookup = Objects.requireNonNull(clusterLookup);
        setPadding(new Insets(10, 0, 0, 0));

        Label head = new Label("Provisions");
        head.setStyle("-fx-text-fill: -color-fg-default; -fx-font-size: 11px; -fx-font-weight: 600;");

        table.setPlaceholder(new Label(
                "No provisions yet. Click \"Provision…\" above to apply one."));
        table.setAccessibleText("Provisioning records table");
        table.getColumns().setAll(
                col("Name", 150, ProvisioningRecord::name),
                col("Namespace", 110, ProvisioningRecord::namespace),
                col("Operator", 80, ProvisioningRecord::operator),
                col("Topology", 90, ProvisioningRecord::topology),
                col("Profile", 80, ProvisioningRecord::profile),
                colStatus(),
                col("Created", 160, r -> Instant.ofEpochMilli(r.createdAt()).toString()),
                col("Protection", 80, r -> r.deletionProtection() ? "on" : "off"));

        Button refreshBtn = new Button("Refresh");
        refreshBtn.setOnAction(e -> reload());
        refreshBtn.setAccessibleText("Reload the provisioning records list");

        Button tearDownBtn = new Button("Tear down…");
        tearDownBtn.setOnAction(e -> onTearDown());
        tearDownBtn.getStyleClass().add("danger");
        tearDownBtn.setAccessibleText("Delete the selected provision (typed confirm + cascade plan)");

        Button toggleProtectionBtn = new Button("Toggle protection");
        toggleProtectionBtn.setOnAction(e -> onToggleProtection());
        toggleProtectionBtn.setAccessibleText("Flip deletion_protection on the selected row");

        Button exportBtn = new Button("Export spec…");
        exportBtn.setOnAction(e -> onExport());
        exportBtn.setAccessibleText("Export this provision's CR YAML to disk");

        statusLabel.setStyle("-fx-text-fill: -color-fg-muted; -fx-font-size: 11px;");
        statusLabel.setWrapText(true);

        HBox actions = new HBox(8, refreshBtn, tearDownBtn, toggleProtectionBtn, exportBtn);
        VBox.setVgrow(table, Priority.ALWAYS);
        getChildren().addAll(head, table, actions, statusLabel);

        sub = events.onProvision(evt -> Platform.runLater(() -> {
            if (evt instanceof ProvisionEvent.Ready
                    || evt instanceof ProvisionEvent.Failed
                    || evt instanceof ProvisionEvent.Started) {
                reload();
            }
        }));
        reload();
    }

    public void close() {
        if (sub != null) sub.close();
    }

    /* ============================ actions ============================ */

    private void reload() {
        try {
            rows.setAll(recordDao.listAll());
        } catch (SQLException sqle) {
            statusLabel.setText("Reload failed: " + sqle.getMessage());
        }
    }

    private void onTearDown() {
        ProvisioningRecord sel = table.getSelectionModel().getSelectedItem();
        if (sel == null) { statusLabel.setText("Pick a provision first."); return; }
        if (sel.deletionProtection()) {
            statusLabel.setText("Deletion protection is on for " + sel.name()
                    + " — click Toggle protection first.");
            return;
        }
        if (sel.status() == ProvisioningRecord.Status.DELETED) {
            statusLabel.setText(sel.name() + " is already DELETED.");
            return;
        }
        CascadePlan plan = sel.profile().equalsIgnoreCase("PROD")
                ? CascadePlan.prodDefaults()
                : CascadePlan.devDefaults();

        Dialog<CascadePlan> dlg = new Dialog<>();
        dlg.setTitle("Tear down " + sel.name());
        dlg.setHeaderText("Type the deployment name to confirm");
        dlg.getDialogPane().getButtonTypes().setAll(ButtonType.OK, ButtonType.CANCEL);
        if (getScene() != null && getScene().getWindow() != null) {
            dlg.initOwner(getScene().getWindow());
        }
        TextField confirmField = new TextField();
        confirmField.setPromptText(sel.name());
        CheckBox cb1 = new CheckBox("Delete CR");
        cb1.setSelected(plan.deleteCr());
        CheckBox cb2 = new CheckBox("Delete Secrets");
        cb2.setSelected(plan.deleteSecrets());
        CheckBox cb3 = new CheckBox("Delete PVCs (destroys data)");
        cb3.setSelected(plan.deletePvcs());
        cb3.setStyle("-fx-text-fill: -color-danger-emphasis; -fx-font-weight: 600;");
        VBox content = new VBox(6,
                new Label("Cascade plan — uncheck to preserve each kind:"),
                cb1, cb2, cb3,
                new Label("Type the deployment name to confirm:"),
                confirmField);
        content.setPadding(new Insets(8));
        dlg.getDialogPane().setContent(content);

        javafx.scene.Node okBtn = dlg.getDialogPane().lookupButton(ButtonType.OK);
        okBtn.setDisable(true);
        confirmField.textProperty().addListener((o, a, b) ->
                okBtn.setDisable(!sel.name().equals(b)));

        dlg.setResultConverter(bt -> bt == ButtonType.OK
                ? new CascadePlan(cb1.isSelected(), cb2.isSelected(), cb3.isSelected())
                : null);

        dlg.showAndWait().ifPresent(picked -> runTearDown(sel, picked));
    }

    private void runTearDown(ProvisioningRecord row, CascadePlan plan) {
        K8sClusterRef ref = clusterLookup.lookup(row.k8sClusterId());
        if (ref == null) {
            statusLabel.setText("Cluster " + row.k8sClusterId()
                    + " is no longer attached; can't tear down.");
            return;
        }
        statusLabel.setText("Tearing down " + row.name() + "…");
        Thread.startVirtualThread(() -> {
            try {
                TearDownService.TearDownResult r =
                        tearDown.tearDown(ref, row.id(), plan);
                Platform.runLater(() -> {
                    statusLabel.setText(row.name() + " teardown " + r.summary()
                            + " (" + r.deletedCount() + " resource(s) deleted).");
                    reload();
                });
            } catch (Throwable t) {
                Platform.runLater(() ->
                        statusLabel.setText("Tear-down failed: " + t.getMessage()));
            }
        });
    }

    private void onToggleProtection() {
        ProvisioningRecord sel = table.getSelectionModel().getSelectedItem();
        if (sel == null) { statusLabel.setText("Pick a provision first."); return; }
        Thread.startVirtualThread(() -> {
            try {
                recordDao.setDeletionProtection(sel.id(), !sel.deletionProtection());
                Platform.runLater(() -> {
                    statusLabel.setText("Deletion protection "
                            + (!sel.deletionProtection() ? "on" : "off")
                            + " for " + sel.name() + ".");
                    reload();
                });
            } catch (Throwable t) {
                Platform.runLater(() ->
                        statusLabel.setText("Toggle failed: " + t.getMessage()));
            }
        });
    }

    private void onExport() {
        ProvisioningRecord sel = table.getSelectionModel().getSelectedItem();
        if (sel == null) { statusLabel.setText("Pick a provision first."); return; }
        FileChooser fc = new FileChooser();
        fc.setTitle("Export deployment CR");
        fc.setInitialFileName(sel.name() + "-cr.yaml");
        fc.getExtensionFilters().add(new FileChooser.ExtensionFilter("YAML", "*.yaml", "*.yml"));
        java.io.File target = fc.showSaveDialog(getScene() == null ? null : getScene().getWindow());
        if (target == null) return;
        try {
            Files.writeString(target.toPath(), sel.crYaml());
            statusLabel.setText("Exported CR to " + target.getAbsolutePath() + ".");
        } catch (java.io.IOException ioe) {
            statusLabel.setText("Export failed: " + ioe.getMessage());
        }
    }

    /* ============================ helpers ============================ */

    private static <T> TableColumn<T, String> col(String title, int width,
                                                    Function<T, String> getter) {
        TableColumn<T, String> c = new TableColumn<>(title);
        c.setPrefWidth(width);
        c.setCellValueFactory(cd ->
                new SimpleStringProperty(getter.apply(cd.getValue())));
        return c;
    }

    private static TableColumn<ProvisioningRecord, String> colStatus() {
        TableColumn<ProvisioningRecord, String> c = new TableColumn<>("Status");
        c.setPrefWidth(90);
        c.setCellValueFactory(cd ->
                new SimpleStringProperty(cd.getValue().status().name()));
        c.setCellFactory(col -> new javafx.scene.control.TableCell<>() {
            @Override protected void updateItem(String item, boolean empty) {
                super.updateItem(item, empty);
                if (empty || item == null) { setText(null); setStyle(""); return; }
                setText(item);
                setStyle(switch (item) {
                    case "READY" -> "-fx-text-fill: -color-success-emphasis; -fx-font-weight: 600;";
                    case "APPLYING", "DELETING" ->
                            "-fx-text-fill: -color-accent-emphasis; -fx-font-weight: 600;";
                    case "FAILED" ->
                            "-fx-text-fill: -color-danger-emphasis; -fx-font-weight: 600;";
                    default -> "-fx-text-fill: -color-fg-muted;";
                });
            }
        });
        return c;
    }

    /** Small seam so the panel can resolve a ProvisioningRecord's cluster_id back to the ref. */
    @FunctionalInterface
    public interface ClusterLookup {
        K8sClusterRef lookup(long clusterId);
    }
}

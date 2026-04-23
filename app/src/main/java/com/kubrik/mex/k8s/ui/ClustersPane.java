package com.kubrik.mex.k8s.ui;

import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.k8s.cluster.KubeClusterService;
import com.kubrik.mex.k8s.discovery.DiscoveryService;
import com.kubrik.mex.k8s.events.ClusterEvent;
import com.kubrik.mex.k8s.model.ClusterProbeResult;
import com.kubrik.mex.k8s.model.K8sClusterRef;
import com.kubrik.mex.k8s.portforward.PortForwardService;
import com.kubrik.mex.k8s.secret.SecretPickupService;
import com.kubrik.mex.store.ConnectionStore;
import javafx.application.Platform;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.ButtonType;
import javafx.scene.control.Label;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextArea;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * v2.8.1 Q2.8.1-A5 — Top-level Clusters pane.
 *
 * <p>Single table of attached Kubernetes clusters with:
 * <ul>
 *   <li>Add cluster — opens {@link AddClusterDialog}, which picks a
 *       kubeconfig context and writes it to {@code k8s_clusters}.</li>
 *   <li>Probe — runs {@link KubeClusterService#probe} and updates
 *       the status chip via {@link ClusterEvent.Probed} on the bus.</li>
 *   <li>Remove — calls {@link KubeClusterService#remove}; refuses
 *       with an alert if live provisioning rows point here.</li>
 * </ul>
 * The detail area below the table surfaces the last probe's server
 * version, node count, and error message when applicable.</p>
 *
 * <p>Consistent with LabsTab: all Kubernetes API calls happen on
 * virtual threads; Platform.runLater hops the result back to the
 * JavaFX thread before touching scene nodes.</p>
 */
public final class ClustersPane extends BorderPane {

    private final KubeClusterService service;
    private final EventBus events;
    private final DiscoveryPanel discoveryPanel;

    private final ObservableList<K8sClusterRef> rows = FXCollections.observableArrayList();
    private final TableView<K8sClusterRef> table = new TableView<>(rows);
    private final Label statusLabel = new Label("Attach a Kubernetes cluster to get started.");
    private final TextArea detailArea = new TextArea();
    private final Map<Long, ClusterProbeResult> probeByCluster = new ConcurrentHashMap<>();
    private EventBus.Subscription busSubscription;

    public ClustersPane(KubeClusterService service, EventBus events,
                         DiscoveryService discoveryService,
                         SecretPickupService secretService,
                         PortForwardService portForwardService,
                         ConnectionStore connectionStore) {
        this.service = service;
        this.events = events;
        this.discoveryPanel = new DiscoveryPanel(
                discoveryService, secretService, portForwardService,
                connectionStore, events);

        setStyle("-fx-background-color: -color-bg-default;");
        setPadding(new Insets(14, 16, 14, 16));
        setAccessibleText("Clusters tab");
        setAccessibleHelp(
                "Attach a Kubernetes cluster by kubeconfig context and probe "
                + "it for connectivity. Provisioning MongoDB into a cluster "
                + "builds on the rows registered here.");

        setTop(buildHeader());
        setCenter(buildBody());
        setBottom(buildFooter());
        reload();

        if (events != null) {
            busSubscription = events.onKubeCluster(evt ->
                    Platform.runLater(() -> onEvent(evt)));
        }
    }

    public void close() {
        if (busSubscription != null) busSubscription.close();
        if (discoveryPanel != null) discoveryPanel.close();
    }

    private Region buildHeader() {
        Label title = new Label("Kubernetes clusters");
        title.setStyle("-fx-font-size: 14px; -fx-font-weight: 700;");
        Label hint = new Label(
                "Register kubeconfig contexts once. Mongo Explorer will "
                + "re-read the file on every connect, so cert rotations "
                + "and kubectl set-context operations take effect without "
                + "re-registering here.");
        hint.setStyle("-fx-text-fill: -color-fg-muted; -fx-font-size: 11px;");
        hint.setWrapText(true);
        VBox v = new VBox(4, title, hint);
        v.setPadding(new Insets(0, 0, 10, 0));
        return v;
    }

    private Region buildBody() {
        Label placeholder = new Label(
                "No clusters attached yet.\nClick \"Add cluster\" to pick a kubeconfig context.");
        placeholder.setStyle("-fx-text-fill: -color-fg-muted; -fx-text-alignment: center;");
        placeholder.setWrapText(true);
        table.setPlaceholder(placeholder);
        table.setAccessibleText("Kubernetes clusters table");
        table.getColumns().setAll(
                col("Name", 180, K8sClusterRef::displayName),
                col("Context", 180, K8sClusterRef::contextName),
                col("Default ns", 120, r -> r.defaultNamespace().orElse("—")),
                colStatus("Status", 130, r -> probeByCluster.get(r.id())),
                col("Server", 200, r -> {
                    ClusterProbeResult p = probeByCluster.get(r.id());
                    if (p == null) return r.serverUrl().orElse("—");
                    return p.serverVersion().orElse(r.serverUrl().orElse("—"));
                }));
        table.getSelectionModel().selectedItemProperty().addListener(
                (o, a, b) -> {
                    renderDetail(b);
                    discoveryPanel.bindCluster(b);
                });

        detailArea.setEditable(false);
        detailArea.setPrefRowCount(6);
        detailArea.setWrapText(true);
        detailArea.setStyle("-fx-font-family: 'Menlo', 'Courier New', monospace; -fx-font-size: 11px;");

        Button addBtn = new Button("Add cluster…");
        addBtn.setOnAction(e -> onAdd());
        addBtn.setAccessibleText("Add a Kubernetes cluster by kubeconfig context");
        addBtn.getStyleClass().add("accent");
        Button probeBtn = new Button("Probe");
        probeBtn.setOnAction(e -> onProbe());
        probeBtn.setAccessibleText("Probe the selected cluster for reachability");
        Button probeAllBtn = new Button("Probe all");
        probeAllBtn.setOnAction(e -> onProbeAll());
        Button removeBtn = new Button("Forget…");
        removeBtn.setOnAction(e -> onRemove());
        removeBtn.setAccessibleText("Forget the selected cluster (doesn't touch the kubeconfig file)");
        removeBtn.getStyleClass().add("danger");

        HBox actions = new HBox(8, addBtn, probeBtn, probeAllBtn, removeBtn);
        actions.setPadding(new Insets(8, 0, 0, 0));

        javafx.scene.control.SplitPane split = new javafx.scene.control.SplitPane();
        split.setOrientation(javafx.geometry.Orientation.VERTICAL);
        VBox top = new VBox(6, table, new Label("Detail"), detailArea, actions);
        VBox.setVgrow(table, Priority.ALWAYS);
        split.getItems().addAll(top, discoveryPanel);
        split.setDividerPositions(0.55);
        return split;
    }

    private Region buildFooter() {
        statusLabel.setStyle("-fx-text-fill: -color-fg-muted; -fx-font-size: 11px;");
        statusLabel.setWrapText(true);
        HBox h = new HBox(8, statusLabel);
        h.setPadding(new Insets(8, 0, 0, 0));
        return h;
    }

    /* ============================== actions ============================== */

    private void onAdd() {
        AddClusterDialog dlg = new AddClusterDialog();
        // Anchor the dialog to the owning window so it doesn't hide
        // behind the main stage on macOS multi-display setups.
        if (getScene() != null && getScene().getWindow() != null) {
            dlg.initOwner(getScene().getWindow());
        }
        Optional<AddClusterDialog.Choice> pick = dlg.showAndWait();
        if (pick.isEmpty()) return;

        AddClusterDialog.Choice c = pick.get();
        Thread.startVirtualThread(() -> {
            try {
                K8sClusterRef ref = service.add(
                        c.displayName(),
                        c.kubeconfigPath(),
                        c.contextName(),
                        c.defaultNamespace(),
                        c.serverUrl());
                Platform.runLater(() -> {
                    statusLabel.setText("Attached " + ref.displayName() + ".");
                    reload();
                    // Kick off a probe immediately so the new row lights up.
                    probe(ref);
                });
            } catch (SQLException sqle) {
                boolean duplicate = sqle.getMessage() != null
                        && sqle.getMessage().toLowerCase().contains("unique");
                Platform.runLater(() -> {
                    if (duplicate) {
                        statusLabel.setText("This kubeconfig + context is already attached.");
                    } else {
                        statusLabel.setText("Add failed: " + sqle.getMessage());
                    }
                });
            } catch (Throwable t) {
                Platform.runLater(() ->
                        statusLabel.setText("Add failed: " + t.getMessage()));
            }
        });
    }

    private void onProbe() {
        K8sClusterRef sel = table.getSelectionModel().getSelectedItem();
        if (sel == null) { statusLabel.setText("Pick a cluster first."); return; }
        probe(sel);
    }

    private void onProbeAll() {
        List<K8sClusterRef> all = List.copyOf(rows);
        if (all.isEmpty()) { statusLabel.setText("Nothing to probe."); return; }
        statusLabel.setText("Probing " + all.size() + " cluster"
                + (all.size() == 1 ? "" : "s") + "…");
        for (K8sClusterRef r : all) probe(r);
    }

    private void probe(K8sClusterRef ref) {
        Thread.startVirtualThread(() -> {
            ClusterProbeResult r = service.probe(ref);
            // The service already published on the bus; we keep this
            // assignment so the status chip refreshes even if the bus
            // listener is temporarily detached during a tab rebuild.
            probeByCluster.put(ref.id(), r);
            Platform.runLater(() -> {
                table.refresh();
                if (ref.equals(table.getSelectionModel().getSelectedItem())) {
                    renderDetail(ref);
                }
                statusLabel.setText(ref.displayName() + ": " + renderStatus(r));
            });
        });
    }

    private void onRemove() {
        K8sClusterRef sel = table.getSelectionModel().getSelectedItem();
        if (sel == null) { statusLabel.setText("Pick a cluster first."); return; }
        Alert confirm = new Alert(Alert.AlertType.CONFIRMATION,
                "Forget cluster \"" + sel.displayName() + "\"?\n\n"
                        + "This removes Mongo Explorer's record of the context. "
                        + "The kubeconfig file on disk is not touched.",
                ButtonType.CANCEL, ButtonType.OK);
        confirm.setHeaderText(null);
        confirm.showAndWait().ifPresent(bt -> {
            if (bt != ButtonType.OK) return;
            Thread.startVirtualThread(() -> {
                try {
                    service.remove(sel.id());
                    Platform.runLater(() -> {
                        statusLabel.setText("Forgot " + sel.displayName() + ".");
                        probeByCluster.remove(sel.id());
                        reload();
                    });
                } catch (IllegalStateException ise) {
                    Platform.runLater(() -> statusLabel.setText(ise.getMessage()));
                } catch (Throwable t) {
                    Platform.runLater(() ->
                            statusLabel.setText("Forget failed: " + t.getMessage()));
                }
            });
        });
    }

    private void onEvent(ClusterEvent e) {
        switch (e) {
            case ClusterEvent.Added a -> reload();
            case ClusterEvent.Removed r -> reload();
            case ClusterEvent.Probed p -> {
                probeByCluster.put(p.clusterId(), p.result());
                table.refresh();
                K8sClusterRef sel = table.getSelectionModel().getSelectedItem();
                if (sel != null && sel.id() == p.clusterId()) renderDetail(sel);
            }
            case ClusterEvent.AuthRefreshFailed af ->
                    statusLabel.setText("Auth refresh failed: " + af.message());
        }
    }

    /* ============================= helpers ============================= */

    private void reload() {
        try {
            rows.setAll(service.list());
        } catch (SQLException sqle) {
            statusLabel.setText("Load failed: " + sqle.getMessage());
        }
    }

    private void renderDetail(K8sClusterRef ref) {
        if (ref == null) { detailArea.clear(); return; }
        ClusterProbeResult p = probeByCluster.get(ref.id());
        StringBuilder sb = new StringBuilder();
        sb.append("id: ").append(ref.id()).append('\n');
        sb.append("kubeconfig: ").append(ref.kubeconfigPath()).append('\n');
        sb.append("context: ").append(ref.contextName()).append('\n');
        sb.append("default ns: ").append(ref.defaultNamespace().orElse("(none)")).append('\n');
        sb.append("server URL: ").append(ref.serverUrl().orElse("(unknown)")).append('\n');
        sb.append("last used: ").append(ref.lastUsedAt()
                .map(ms -> java.time.Instant.ofEpochMilli(ms).toString())
                .orElse("never")).append('\n');
        if (p != null) {
            sb.append("\n— last probe —\n");
            sb.append("status: ").append(p.status()).append('\n');
            p.serverVersion().ifPresent(v -> sb.append("server version: ").append(v).append('\n'));
            p.nodeCount().ifPresent(n -> sb.append("node count: ").append(n).append('\n'));
            p.errorMessage().ifPresent(m -> sb.append("error: ").append(m).append('\n'));
            sb.append("probed at: ").append(java.time.Instant.ofEpochMilli(p.probedAt())).append('\n');
        }
        detailArea.setText(sb.toString());
    }

    private static String renderStatus(ClusterProbeResult p) {
        return switch (p.status()) {
            case REACHABLE -> "OK";
            case UNREACHABLE -> "unreachable";
            case AUTH_FAILED -> "auth failed";
            case PLUGIN_MISSING -> "plugin missing";
            case TIMED_OUT -> "timed out";
        };
    }

    /**
     * Colour-coded status column — renders the text PASS/WARN/FAIL
     * using the AtlantaFX semantic tokens so the chip reads the
     * right severity at a glance in both light and dark themes.
     */
    private static TableColumn<K8sClusterRef, String> colStatus(
            String title, int width,
            Function<K8sClusterRef, ClusterProbeResult> probeFn) {
        TableColumn<K8sClusterRef, String> c = new TableColumn<>(title);
        c.setPrefWidth(width);
        c.setCellValueFactory(cd -> {
            ClusterProbeResult p = probeFn.apply(cd.getValue());
            return new SimpleStringProperty(p == null ? "(unprobed)" : renderStatus(p));
        });
        c.setCellFactory(col -> new javafx.scene.control.TableCell<>() {
            @Override
            protected void updateItem(String item, boolean empty) {
                super.updateItem(item, empty);
                if (empty || item == null) { setText(null); setStyle(""); return; }
                setText(item);
                setStyle(switch (item) {
                    case "OK" -> "-fx-text-fill: -color-success-emphasis; -fx-font-weight: 600;";
                    case "auth failed", "plugin missing" ->
                            "-fx-text-fill: -color-warning-emphasis; -fx-font-weight: 600;";
                    case "unreachable", "timed out" ->
                            "-fx-text-fill: -color-danger-emphasis; -fx-font-weight: 600;";
                    default -> "-fx-text-fill: -color-fg-muted;";
                });
            }
        });
        return c;
    }

    private static <T> TableColumn<T, String> col(String title, int width,
                                                    Function<T, String> getter) {
        TableColumn<T, String> c = new TableColumn<>(title);
        c.setPrefWidth(width);
        c.setCellValueFactory(cd -> new SimpleStringProperty(getter.apply(cd.getValue())));
        return c;
    }

    /**
     * Presentational snapshot returned for tests — the pane itself
     * doesn't need it, but having the map lets an IT assert the
     * post-event render without poking the scene graph.
     */
    public Map<Long, ClusterProbeResult> probeStateSnapshot() {
        return new HashMap<>(probeByCluster);
    }
}

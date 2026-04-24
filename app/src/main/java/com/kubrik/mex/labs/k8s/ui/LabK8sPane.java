package com.kubrik.mex.labs.k8s.ui;

import com.kubrik.mex.labs.k8s.distro.DistroDetector;
import com.kubrik.mex.labs.k8s.lifecycle.LabK8sLifecycleService;
import com.kubrik.mex.labs.k8s.model.LabK8sCluster;
import com.kubrik.mex.labs.k8s.model.LabK8sDistro;
import com.kubrik.mex.labs.k8s.store.LabK8sClusterDao;
import com.kubrik.mex.labs.k8s.templates.LabK8sTemplate;
import com.kubrik.mex.labs.k8s.templates.LabK8sTemplateRegistry;
import javafx.application.Platform;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.scene.control.Button;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Label;
import javafx.scene.control.ListCell;
import javafx.scene.control.ListView;
import javafx.scene.control.ProgressIndicator;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextInputDialog;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;

import java.sql.SQLException;
import java.time.Instant;
import java.util.Optional;
import java.util.function.Function;

/**
 * v2.8.1 Q2.8-N5 — Top-level Lab K8s pane.
 *
 * <p>Three stacked regions:</p>
 * <ul>
 *   <li><b>Target row</b> — distro picker (minikube / k3d) +
 *       unavailable-CLI hint.</li>
 *   <li><b>Template catalogue</b> — ListView of the five shipped
 *       templates with an inline description.</li>
 *   <li><b>Running Labs</b> — TableView over {@code lab_k8s_clusters}
 *       rows; refreshes on Apply completion.</li>
 * </ul>
 *
 * <p>Apply opens a small text input for the distro identifier
 * (profile name) + target namespace + deployment name, then runs
 * {@link LabK8sLifecycleService#apply} on a virtual thread.</p>
 */
public final class LabK8sPane extends BorderPane {

    private final DistroDetector detector;
    private final LabK8sTemplateRegistry registry;
    private final LabK8sClusterDao clusterDao;
    private final LabK8sLifecycleService lifecycle;

    private final ComboBox<LabK8sDistro> distroBox = new ComboBox<>();
    private final ListView<LabK8sTemplate> templateList = new ListView<>();
    private final ObservableList<LabK8sCluster> runningRows = FXCollections.observableArrayList();
    private final TableView<LabK8sCluster> runningTable = new TableView<>(runningRows);
    private final TextArea detailArea = new TextArea();
    private final Label statusLabel = new Label("Pick a distro + template, then click Apply.");
    private final ProgressIndicator busySpinner = new ProgressIndicator();

    public LabK8sPane(DistroDetector detector,
                        LabK8sTemplateRegistry registry,
                        LabK8sClusterDao clusterDao,
                        LabK8sLifecycleService lifecycle) {
        this.detector = detector;
        this.registry = registry;
        this.clusterDao = clusterDao;
        this.lifecycle = lifecycle;

        setStyle("-fx-background-color: -color-bg-default;");
        setPadding(new Insets(14, 16, 14, 16));
        setAccessibleText("Lab K8s pane");
        setAccessibleHelp(
                "Spin up a local Kubernetes cluster (minikube or k3d) and "
                + "apply a curated MongoDB template onto it. The production "
                + "provisioning pipeline runs against the local cluster "
                + "identically to a remote one.");

        busySpinner.setVisible(false);
        busySpinner.setPrefSize(18, 18);

        setTop(buildHeader());
        setCenter(buildBody());
        setBottom(buildFooter());
        reload();
    }

    /* ============================ layout ============================ */

    private Region buildHeader() {
        Label title = new Label("Local K8s Labs");
        title.setStyle("-fx-font-size: 14px; -fx-font-weight: 700;");
        Label hint = new Label(
                "Runs the v2.8.1 Kubernetes provisioning pipeline against a "
                + "local distro (minikube or k3d) instead of a remote "
                + "cluster — same CR renderers, same pre-flight, same "
                + "rollout viewer. Destroy a Lab to tear the distro + "
                + "deployment down together.");
        hint.setStyle("-fx-text-fill: -color-fg-muted; -fx-font-size: 11px;");
        hint.setWrapText(true);
        VBox v = new VBox(4, title, hint);
        v.setPadding(new Insets(0, 0, 10, 0));
        return v;
    }

    private Region buildBody() {
        javafx.scene.control.SplitPane split = new javafx.scene.control.SplitPane();
        split.setOrientation(javafx.geometry.Orientation.VERTICAL);
        split.getItems().addAll(buildTopHalf(), buildRunningPanel());
        split.setDividerPositions(0.55);
        return split;
    }

    private Region buildTopHalf() {
        Label distroLabel = new Label("Distro");
        distroLabel.setStyle("-fx-font-weight: 600; -fx-font-size: 11px;");

        distroBox.getItems().setAll(LabK8sDistro.values());
        distroBox.setCellFactory(lv -> distroCell());
        distroBox.setButtonCell(distroCell());
        distroBox.setValue(firstAvailable());
        HBox distroRow = new HBox(8, distroLabel, distroBox);

        Label catHead = new Label("Template");
        catHead.setStyle("-fx-font-weight: 600; -fx-font-size: 11px;");

        templateList.setItems(FXCollections.observableArrayList(registry.all()));
        templateList.setCellFactory(lv -> templateCell());
        templateList.getSelectionModel().selectFirst();

        Button applyBtn = new Button("Apply…");
        applyBtn.setOnAction(e -> onApply());
        applyBtn.getStyleClass().add("accent");
        applyBtn.setAccessibleText("Apply the selected template onto the chosen distro");

        Button refreshBtn = new Button("Refresh CLI");
        refreshBtn.setOnAction(e -> { detector.refresh(); distroBox.setValue(firstAvailable()); });

        HBox actions = new HBox(8, applyBtn, refreshBtn);
        VBox top = new VBox(6, distroRow, catHead, templateList, actions);
        VBox.setVgrow(templateList, Priority.ALWAYS);
        return top;
    }

    private Region buildRunningPanel() {
        Label head = new Label("Running local clusters");
        head.setStyle("-fx-font-weight: 600; -fx-font-size: 11px;");

        runningTable.setPlaceholder(new Label(
                "No local K8s clusters yet. Apply a template above to create one."));
        runningTable.getColumns().setAll(
                col("Distro", 90, c -> c.distro().name()),
                col("Identifier", 180, LabK8sCluster::identifier),
                col("Context", 180, LabK8sCluster::contextName),
                col("Status", 100, c -> c.status().name()),
                col("Created", 180, c -> Instant.ofEpochMilli(c.createdAt()).toString()));
        runningTable.getSelectionModel().selectedItemProperty().addListener(
                (o, a, b) -> renderDetail(b));

        detailArea.setEditable(false);
        detailArea.setPrefRowCount(6);
        detailArea.setWrapText(true);
        detailArea.setStyle(
                "-fx-font-family: 'Menlo','Monaco',monospace; -fx-font-size: 11px;");

        Button refreshRows = new Button("Reload");
        refreshRows.setOnAction(e -> reload());
        Button exportBtn = new Button("Export kubeconfig…");
        exportBtn.setOnAction(e -> onExportKubeconfig());
        exportBtn.setAccessibleText("Export the selected cluster's kubeconfig context as a standalone file");
        Button destroyBtn = new Button("Destroy…");
        destroyBtn.setOnAction(e -> onDestroy());
        destroyBtn.getStyleClass().add("danger");

        HBox actions = new HBox(8, refreshRows, exportBtn, destroyBtn);

        VBox v = new VBox(6, head, runningTable, new Label("Detail"), detailArea, actions);
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

    /* ============================ actions ============================ */

    private void onApply() {
        LabK8sDistro distro = distroBox.getValue();
        LabK8sTemplate template = templateList.getSelectionModel().getSelectedItem();
        if (distro == null) { statusLabel.setText("Pick a distro first."); return; }
        if (template == null) { statusLabel.setText("Pick a template first."); return; }
        if (!detector.isAvailable(distro)) {
            statusLabel.setText(distro + " CLI not on PATH — install " + distro.name().toLowerCase()
                    + " or pick another distro.");
            return;
        }

        TextInputDialog identifierDlg = new TextInputDialog("lab-" + template.id());
        identifierDlg.setHeaderText("Cluster identifier for " + distro.name().toLowerCase());
        identifierDlg.setContentText("Profile / cluster name:");
        if (getScene() != null && getScene().getWindow() != null) {
            identifierDlg.initOwner(getScene().getWindow());
        }
        Optional<String> identifier = identifierDlg.showAndWait();
        if (identifier.isEmpty() || identifier.get().isBlank()) return;

        String namespace = "mongo";
        String deploymentName = template.id().replace("-", "") + "-rs";

        statusLabel.setText("Bringing up " + distro + ":" + identifier.get() + "…");
        busySpinner.setVisible(true);
        Thread.startVirtualThread(() -> {
            LabK8sLifecycleService.LabK8sApplyResult r = lifecycle.apply(
                    distro, identifier.get(), template, namespace, deploymentName);
            Platform.runLater(() -> {
                busySpinner.setVisible(false);
                switch (r) {
                    case LabK8sLifecycleService.LabK8sApplyResult.Ok ok ->
                            statusLabel.setText("Applied — provisioning id "
                                    + ok.provisioningId() + " on " + ok.cluster().coordinates() + ".");
                    case LabK8sLifecycleService.LabK8sApplyResult.DistroFailed f ->
                            statusLabel.setText("Distro failed: " + f.reason());
                    case LabK8sLifecycleService.LabK8sApplyResult.ProvisionFailed f ->
                            statusLabel.setText("Provision failed on "
                                    + f.cluster().coordinates() + ": " + f.reason());
                }
                reload();
            });
        });
    }

    private void onExportKubeconfig() {
        LabK8sCluster sel = runningTable.getSelectionModel().getSelectedItem();
        if (sel == null) { statusLabel.setText("Pick a cluster first."); return; }

        javafx.stage.FileChooser fc = new javafx.stage.FileChooser();
        fc.setTitle("Export kubeconfig for " + sel.coordinates());
        fc.setInitialFileName(sel.distro().cliName() + "-"
                + sel.identifier() + "-kubeconfig.yaml");
        fc.getExtensionFilters().add(
                new javafx.stage.FileChooser.ExtensionFilter("Kubeconfig YAML", "*.yaml", "*.yml"));
        java.io.File target = fc.showSaveDialog(getScene() == null ? null : getScene().getWindow());
        if (target == null) return;

        // Read the merged kubeconfig + write only the chosen context
        // + its cluster + user entries out to a standalone file.
        // Uses the same YAML parse as KubeConfigLoader so the round-
        // trip is stable.
        Thread.startVirtualThread(() -> {
            try {
                java.nio.file.Path source = java.nio.file.Path.of(sel.kubeconfigPath());
                byte[] bytes = java.nio.file.Files.readAllBytes(source);
                com.fasterxml.jackson.databind.ObjectMapper yaml =
                        new com.fasterxml.jackson.databind.ObjectMapper(
                                new com.fasterxml.jackson.dataformat.yaml.YAMLFactory());
                @SuppressWarnings("unchecked")
                java.util.Map<String, Object> root = yaml.readValue(bytes, java.util.Map.class);

                // Filter the three arrays to only entries referenced
                // by the chosen context. The output is a valid
                // single-context kubeconfig the user can pass with
                // --kubeconfig=.
                java.util.Map<String, Object> filtered = filterKubeconfig(root, sel.contextName());
                byte[] out = yaml.writeValueAsBytes(filtered);
                java.nio.file.Files.write(target.toPath(), out);

                Platform.runLater(() -> statusLabel.setText(
                        "Exported kubeconfig for " + sel.coordinates()
                        + " to " + target.getAbsolutePath() + "."));
            } catch (Throwable t) {
                Platform.runLater(() -> statusLabel.setText(
                        "Export kubeconfig failed: " + t.getMessage()));
            }
        });
    }

    /**
     * Filter a merged kubeconfig down to the single context plus its
     * referenced cluster + user entries. Preserves apiVersion + kind;
     * sets {@code current-context} to the picked context.
     */
    @SuppressWarnings("unchecked")
    static java.util.Map<String, Object> filterKubeconfig(
            java.util.Map<String, Object> root, String contextName) {
        java.util.Map<String, Object> out = new java.util.LinkedHashMap<>();
        out.put("apiVersion", root.getOrDefault("apiVersion", "v1"));
        out.put("kind", root.getOrDefault("kind", "Config"));
        out.put("current-context", contextName);

        java.util.List<Object> contexts = asList(root.get("contexts"));
        java.util.List<Object> clusters = asList(root.get("clusters"));
        java.util.List<Object> users = asList(root.get("users"));

        java.util.Map<String, Object> ctxRow = null;
        for (Object raw : contexts) {
            if (raw instanceof java.util.Map<?, ?> r
                    && contextName.equals(r.get("name"))) {
                ctxRow = (java.util.Map<String, Object>) raw;
                break;
            }
        }
        if (ctxRow == null) {
            throw new IllegalStateException("context " + contextName + " not in kubeconfig");
        }
        java.util.Map<String, Object> ctxBody = (java.util.Map<String, Object>)
                ctxRow.getOrDefault("context", java.util.Map.of());
        String clusterName = (String) ctxBody.get("cluster");
        String userName = (String) ctxBody.get("user");

        out.put("contexts", java.util.List.of(ctxRow));
        out.put("clusters", filterByName(clusters, clusterName));
        out.put("users", filterByName(users, userName));
        return out;
    }

    @SuppressWarnings("unchecked")
    private static java.util.List<Object> filterByName(java.util.List<Object> rows, String name) {
        if (name == null) return java.util.List.of();
        for (Object raw : rows) {
            if (raw instanceof java.util.Map<?, ?> r && name.equals(r.get("name"))) {
                return java.util.List.of(raw);
            }
        }
        return java.util.List.of();
    }

    private static java.util.List<Object> asList(Object o) {
        return o instanceof java.util.List<?> l ? (java.util.List<Object>) l : java.util.List.of();
    }

    private void onDestroy() {
        LabK8sCluster sel = runningTable.getSelectionModel().getSelectedItem();
        if (sel == null) { statusLabel.setText("Pick a cluster first."); return; }

        TextInputDialog confirm = new TextInputDialog();
        confirm.setHeaderText("Destroy " + sel.coordinates() + "?");
        confirm.setContentText("Type the identifier to confirm:");
        if (getScene() != null && getScene().getWindow() != null) {
            confirm.initOwner(getScene().getWindow());
        }
        confirm.showAndWait().ifPresent(typed -> {
            if (!sel.identifier().equals(typed.trim())) {
                statusLabel.setText("Typed-confirm mismatch — aborting.");
                return;
            }
            statusLabel.setText("Destroying " + sel.coordinates() + "…");
            busySpinner.setVisible(true);
            Thread.startVirtualThread(() -> {
                String summary;
                try {
                    summary = lifecycle.destroy(sel.id());
                } catch (Throwable t) {
                    summary = "destroy errored: " + t.getMessage();
                }
                final String msg = summary;
                Platform.runLater(() -> {
                    busySpinner.setVisible(false);
                    statusLabel.setText(msg);
                    reload();
                });
            });
        });
    }

    /* ============================ helpers ============================ */

    private void reload() {
        try {
            runningRows.setAll(clusterDao.listLive());
        } catch (SQLException sqle) {
            statusLabel.setText("Reload failed: " + sqle.getMessage());
        }
    }

    private void renderDetail(LabK8sCluster c) {
        if (c == null) { detailArea.clear(); return; }
        StringBuilder sb = new StringBuilder();
        sb.append("id: ").append(c.id()).append('\n');
        sb.append("distro: ").append(c.distro()).append('\n');
        sb.append("identifier: ").append(c.identifier()).append('\n');
        sb.append("context: ").append(c.contextName()).append('\n');
        sb.append("kubeconfig: ").append(c.kubeconfigPath()).append('\n');
        sb.append("status: ").append(c.status()).append('\n');
        sb.append("created: ").append(Instant.ofEpochMilli(c.createdAt())).append('\n');
        c.lastStartedAt().ifPresent(t -> sb.append("last started: ").append(Instant.ofEpochMilli(t)).append('\n'));
        c.lastStoppedAt().ifPresent(t -> sb.append("last stopped: ").append(Instant.ofEpochMilli(t)).append('\n'));
        c.destroyedAt().ifPresent(t -> sb.append("destroyed at: ").append(Instant.ofEpochMilli(t)).append('\n'));
        c.k8sClusterId().ifPresent(k -> sb.append("k8s_clusters.id: ").append(k).append('\n'));
        detailArea.setText(sb.toString());
    }

    private LabK8sDistro firstAvailable() {
        for (LabK8sDistro d : LabK8sDistro.values()) {
            if (detector.isAvailable(d)) return d;
        }
        return LabK8sDistro.MINIKUBE;
    }

    private ListCell<LabK8sDistro> distroCell() {
        return new ListCell<>() {
            @Override protected void updateItem(LabK8sDistro d, boolean empty) {
                super.updateItem(d, empty);
                if (empty || d == null) { setText(null); return; }
                Optional<String> version = detector.detect(d);
                String suffix = version
                        .map(v -> " — " + v.split("\n", 2)[0].trim())
                        .orElse(" — CLI not installed");
                setText(d.name().toLowerCase() + suffix);
                setDisable(version.isEmpty());
            }
        };
    }

    private ListCell<LabK8sTemplate> templateCell() {
        return new ListCell<>() {
            @Override protected void updateItem(LabK8sTemplate t, boolean empty) {
                super.updateItem(t, empty);
                if (empty || t == null) { setText(null); setGraphic(null); return; }
                Label name = new Label(t.displayName());
                name.setStyle("-fx-font-weight: 600;");
                Label desc = new Label(t.description());
                desc.setStyle("-fx-text-fill: -color-fg-muted; -fx-font-size: 11px;");
                desc.setWrapText(true);
                Label tags = new Label(String.join(" · ", t.tags()));
                tags.setStyle("-fx-text-fill: -color-fg-subtle; -fx-font-size: 10px;");
                VBox box = new VBox(2, name, desc, tags);
                box.setPadding(new Insets(4, 4, 8, 4));
                setGraphic(box);
                setText(null);
            }
        };
    }

    private static <T> TableColumn<T, String> col(String title, int width,
                                                    Function<T, String> getter) {
        TableColumn<T, String> c = new TableColumn<>(title);
        c.setPrefWidth(width);
        c.setCellValueFactory(cd -> new SimpleStringProperty(getter.apply(cd.getValue())));
        return c;
    }
}

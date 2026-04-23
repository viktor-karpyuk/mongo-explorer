package com.kubrik.mex.k8s.ui;

import com.kubrik.mex.k8s.client.KubeConfigLoader;
import com.kubrik.mex.k8s.model.K8sAuthKind;
import com.kubrik.mex.k8s.model.K8sContextSummary;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.ButtonType;
import javafx.scene.control.Dialog;
import javafx.scene.control.Label;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextField;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.VBox;
import javafx.stage.FileChooser;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * v2.8.1 Q2.8.1-A5 — "Add a Kubernetes cluster" dialog.
 *
 * <p>Workflow:</p>
 * <ol>
 *   <li>Auto-discovers kubeconfig files from {@code $KUBECONFIG} /
 *       {@code ~/.kube/config}, merges their contexts into one list,
 *       and classifies each row's auth strategy (see
 *       {@link KubeConfigLoader}).</li>
 *   <li>User picks a row, optionally renames the display name, and
 *       confirms. The dialog returns a {@link Choice} record that
 *       the pane writes to {@code k8s_clusters}.</li>
 *   <li>A "Browse…" button on top lets users point at an ad-hoc
 *       kubeconfig outside the discovery set — uncommon but useful
 *       for CI-style kubeconfigs checked into a project repo.</li>
 * </ol>
 *
 * <p>We deliberately don't probe the cluster <i>inside</i> the dialog:
 * a probe that blocks the dialog is a worse UX than "add, then watch
 * the status chip light up." The pane issues the probe after
 * insert.</p>
 */
public final class AddClusterDialog extends Dialog<AddClusterDialog.Choice> {

    private final ObservableList<Row> contextRows = FXCollections.observableArrayList();
    private final TableView<Row> contextTable = new TableView<>(contextRows);
    private final TextField displayNameField = new TextField();
    private final TextField namespaceField = new TextField();
    private final Label errorLabel = new Label();

    public AddClusterDialog() {
        setTitle("Add Kubernetes cluster");
        setHeaderText("Pick a kubeconfig context to attach");
        getDialogPane().getButtonTypes().addAll(ButtonType.OK, ButtonType.CANCEL);

        errorLabel.setStyle("-fx-text-fill: -color-danger-emphasis; -fx-font-size: 11px;");
        errorLabel.setWrapText(true);

        Button browseBtn = new Button("Browse kubeconfig…");
        browseBtn.setOnAction(e -> onBrowse());

        Button reloadBtn = new Button("Reload");
        reloadBtn.setOnAction(e -> reloadDiscovery());

        HBox topRow = new HBox(8, browseBtn, reloadBtn);
        topRow.setPadding(new Insets(0, 0, 8, 0));

        contextTable.setPlaceholder(new Label(
                "No kubeconfig contexts found. Point at a file with Browse…"));
        contextTable.getColumns().setAll(
                col("Context",   170, r -> r.summary().contextName()),
                col("Cluster",   140, r -> r.summary().clusterName()),
                col("User",      140, r -> r.summary().userName()),
                col("Auth",      140, r -> renderAuth(r.summary())),
                col("Namespace",  90, r -> r.summary().namespace().orElse("—")),
                col("Server",    220, r -> r.summary().serverUrl().orElse("—")),
                col("File",      200, r -> r.sourcePath().toString()));
        contextTable.getSelectionModel().selectedItemProperty().addListener(
                (o, a, b) -> onRowSelected(b));

        displayNameField.setPromptText("Display name (defaults to context)");
        namespaceField.setPromptText("Default namespace (optional)");

        Label nameL = new Label("Name");
        Label nsL = new Label("Default namespace");

        VBox form = new VBox(4,
                nameL, displayNameField,
                nsL, namespaceField,
                errorLabel);
        form.setPadding(new Insets(8, 0, 0, 0));

        VBox root = new VBox(6, topRow, contextTable, form);
        VBox.setVgrow(contextTable, Priority.ALWAYS);
        root.setPrefWidth(960);
        root.setPrefHeight(520);
        getDialogPane().setContent(root);

        reloadDiscovery();

        setResultConverter(bt -> {
            if (bt != ButtonType.OK) return null;
            Row sel = contextTable.getSelectionModel().getSelectedItem();
            if (sel == null) return null;
            String displayName = displayNameField.getText().isBlank()
                    ? sel.summary().contextName()
                    : displayNameField.getText().trim();
            Optional<String> ns = namespaceField.getText().isBlank()
                    ? sel.summary().namespace()
                    : Optional.of(namespaceField.getText().trim());
            return new Choice(
                    displayName,
                    sel.sourcePath().toString(),
                    sel.summary().contextName(),
                    ns,
                    sel.summary().serverUrl());
        });
    }

    private void reloadDiscovery() {
        try {
            List<KubeConfigLoader.DiscoveredContext> found = KubeConfigLoader.discoverAll();
            List<Row> mapped = new ArrayList<>(found.size());
            for (KubeConfigLoader.DiscoveredContext d : found) {
                mapped.add(new Row(d.sourcePath(), d.summary()));
            }
            contextRows.setAll(mapped);
            errorLabel.setText(mapped.isEmpty()
                    ? "No kubeconfig files on $KUBECONFIG or ~/.kube/config."
                    : "");
        } catch (IOException ioe) {
            errorLabel.setText("Scan failed: " + ioe.getMessage());
        }
    }

    private void onBrowse() {
        FileChooser fc = new FileChooser();
        fc.setTitle("Select a kubeconfig file");
        java.io.File chosen = fc.showOpenDialog(getOwner());
        if (chosen == null) return;
        try {
            List<K8sContextSummary> parsed = KubeConfigLoader.listContexts(chosen.toPath());
            List<Row> merged = new ArrayList<>(contextRows);
            for (K8sContextSummary s : parsed) merged.add(new Row(chosen.toPath(), s));
            contextRows.setAll(merged);
            errorLabel.setText(parsed.isEmpty()
                    ? "Picked file has no contexts."
                    : "Added " + parsed.size() + " context" + (parsed.size() == 1 ? "" : "s")
                        + " from " + chosen.getName() + ".");
        } catch (IOException ioe) {
            errorLabel.setText("Parse failed: " + ioe.getMessage());
        }
    }

    private void onRowSelected(Row r) {
        if (r == null) return;
        displayNameField.setText(r.summary().contextName());
        namespaceField.setText(r.summary().namespace().orElse(""));
    }

    private static String renderAuth(K8sContextSummary s) {
        String base = switch (s.authKind()) {
            case EXEC_PLUGIN -> "exec";
            case OIDC -> "oidc";
            case TOKEN -> "token";
            case CLIENT_CERT -> "cert";
            case BASIC_AUTH -> "basic";
            case IN_CLUSTER -> "in-cluster";
            case UNKNOWN -> "unknown";
        };
        return s.authDetail()
                .map(d -> base + " · " + d)
                .orElse(base);
    }

    private static <T> TableColumn<T, String> col(String title, int width,
                                                    Function<T, String> getter) {
        TableColumn<T, String> c = new TableColumn<>(title);
        c.setPrefWidth(width);
        c.setCellValueFactory(cd -> new SimpleStringProperty(getter.apply(cd.getValue())));
        return c;
    }

    /**
     * Picker-output shape. Every field is pre-validated against a
     * reachable kubeconfig row, so the caller can insert without
     * re-parsing.
     */
    public record Choice(
            String displayName,
            String kubeconfigPath,
            String contextName,
            Optional<String> defaultNamespace,
            Optional<String> serverUrl) {}

    /** Row projection shown in the table — marries the source kubeconfig path to a context summary. */
    private record Row(Path sourcePath, K8sContextSummary summary) {}
}

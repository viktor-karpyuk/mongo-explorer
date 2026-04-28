package com.kubrik.mex.maint.ui;

import com.kubrik.mex.maint.drift.ConfigSnapshotDao;
import com.kubrik.mex.maint.drift.ConfigSnapshotService;
import com.kubrik.mex.maint.model.ConfigSnapshot;
import com.mongodb.client.MongoClient;
import javafx.application.Platform;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextArea;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;

import java.time.Instant;

/**
 * v2.7 DRIFT-CFG-* UI — Recent config snapshots + line-by-line diff
 * between two selected rows. Capture button triggers captureAll
 * on the current connection; the v2.6 DriftDiffEngine isn't plugged
 * in here — a dumb line-by-line diff is enough for the first pass,
 * and the evidence-signed export lives on a later story.
 */
public final class ConfigDriftPane extends BorderPane {

    private final ConfigSnapshotDao dao;
    private final ConfigSnapshotService service;
    private final java.util.function.Supplier<String> connectionIdSupplier;
    private final java.util.function.Supplier<MongoClient> clientSupplier;

    private final ObservableList<ConfigSnapshot> rows =
            FXCollections.observableArrayList();
    private final TableView<ConfigSnapshot> table = new TableView<>(rows);
    private final TextArea diffArea = new TextArea();
    private final Label statusLabel = new Label("—");

    public ConfigDriftPane(ConfigSnapshotDao dao,
                           java.util.function.Supplier<MongoClient> clientSupplier,
                           java.util.function.Supplier<String> connectionIdSupplier) {
        this.dao = dao;
        this.service = new ConfigSnapshotService(dao);
        this.clientSupplier = clientSupplier;
        this.connectionIdSupplier = connectionIdSupplier;
        setStyle("-fx-background-color: -color-bg-default;");
        setPadding(new Insets(14, 16, 14, 16));
        setAccessibleText("Config drift pane");
        setAccessibleHelp(
                "Snapshots of cluster configuration over time (parameters, "
                + "cmdline, FCV, sharding). Capture now to take a fresh "
                + "snapshot; select two rows and Diff to see what changed.");
        setTop(buildHeader());
        setCenter(buildCenter());
        setBottom(buildActions());
    }

    private Region buildHeader() {
        Label title = new Label("Config drift");
        title.setStyle("-fx-font-size: 14px; -fx-font-weight: 700;");
        Label hint = new Label(
                "Snapshots of cluster-wide parameters, cmdline (redacted), "
                + "FCV, and sharding settings. Select two rows and click "
                + "Diff to see what changed.");
        hint.setStyle("-fx-text-fill: -color-fg-muted; -fx-font-size: 11px;");
        hint.setWrapText(true);
        VBox v = new VBox(6, title, hint);
        v.setPadding(new Insets(0, 0, 10, 0));
        return v;
    }

    private Region buildCenter() {
        table.setPlaceholder(new Label("No snapshots yet."));
        table.getColumns().setAll(
                col("Captured at", 180, r -> Instant.ofEpochMilli(
                        r.capturedAt()).toString()),
                col("Kind", 110, r -> r.kind().name()),
                col("Host", 200, r -> r.host() == null ? "<cluster>" : r.host()),
                col("SHA-256 (prefix)", 140, r -> r.sha256().substring(0, 12) + "…"));
        table.setPrefHeight(180);
        table.getSelectionModel().setSelectionMode(
                javafx.scene.control.SelectionMode.MULTIPLE);

        diffArea.setEditable(false);
        diffArea.setWrapText(false);
        diffArea.setPrefRowCount(14);
        diffArea.setStyle("-fx-font-family: 'Menlo', 'Courier New', monospace; -fx-font-size: 11px;");

        Label diffLabel = new Label("Diff");
        diffLabel.setStyle("-fx-text-fill: -color-fg-default; -fx-font-size: 11px; -fx-font-weight: 600;");
        VBox v = new VBox(6, table, diffLabel, diffArea);
        VBox.setVgrow(diffArea, Priority.ALWAYS);
        return v;
    }

    private Region buildActions() {
        Button captureBtn = new Button("Capture now");
        captureBtn.setOnAction(e -> onCapture());
        Button refreshBtn = new Button("Refresh");
        refreshBtn.setOnAction(e -> onRefresh());
        Button diffBtn = new Button("Diff selected");
        diffBtn.setOnAction(e -> onDiff());

        Region grow = new Region();
        HBox.setHgrow(grow, Priority.ALWAYS);
        HBox actions = new HBox(8, captureBtn, refreshBtn, grow, diffBtn);
        actions.setPadding(new Insets(10, 0, 0, 0));

        statusLabel.setStyle("-fx-text-fill: -color-fg-muted; -fx-font-size: 11px;");
        statusLabel.setWrapText(true);
        return new VBox(6, actions, statusLabel);
    }

    private void onCapture() {
        MongoClient client = clientSupplier.get();
        String cx = connectionIdSupplier.get();
        if (client == null || cx == null) {
            fail("No active connection.");
            return;
        }
        Thread.startVirtualThread(() -> {
            try {
                var ids = service.captureAll(client, cx, null,
                        System.currentTimeMillis());
                Platform.runLater(() -> {
                    ok("Captured " + ids.size() + " snapshot(s).");
                    onRefresh();
                });
            } catch (Exception ex) {
                Platform.runLater(() -> fail("Capture failed: " + ex.getMessage()));
            }
        });
    }

    public void onRefresh() {
        String cx = connectionIdSupplier.get();
        if (cx == null) { rows.clear(); return; }
        rows.setAll(dao.listForConnection(cx, 50));
        statusLabel.setText(rows.size() + " snapshots shown.");
    }

    private void onDiff() {
        var selected = table.getSelectionModel().getSelectedItems();
        if (selected.size() != 2) {
            fail("Select exactly two rows.");
            return;
        }
        ConfigSnapshot a = selected.get(0);
        ConfigSnapshot b = selected.get(1);
        ConfigSnapshot older = a.capturedAt() < b.capturedAt() ? a : b;
        ConfigSnapshot newer = older == a ? b : a;
        if (older.sha256().equals(newer.sha256())) {
            diffArea.setText("// identical — no drift");
            ok("Identical snapshots.");
            return;
        }
        diffArea.setText(renderStructuralDiff(older, newer));
        ok("Showing diff " + older.id() + " → " + newer.id() + ".");
    }

    /** Structural path-based diff via v2.6 {@link
     *  com.kubrik.mex.security.drift.DriftDiffEngine}. Replaces the
     *  earlier line-by-line fallback which produced a near-useless
     *  wall of `-`/`+` lines for any realistic getParameter reply. */
    private static String renderStructuralDiff(ConfigSnapshot older,
                                               ConfigSnapshot newer) {
        java.util.Map<String, Object> olderMap = parseAsMap(older.snapshotJson());
        java.util.Map<String, Object> newerMap = parseAsMap(newer.snapshotJson());
        java.util.List<com.kubrik.mex.security.drift.DriftFinding> findings =
                com.kubrik.mex.security.drift.DriftDiffEngine.diff(
                        olderMap, newerMap);
        if (findings.isEmpty()) return "// no structural differences";
        StringBuilder sb = new StringBuilder();
        sb.append("# ").append(findings.size())
                .append(" drift finding").append(findings.size() == 1 ? "" : "s")
                .append(" (oldest → newest)\n\n");
        for (var f : findings) {
            switch (f.kind()) {
                case ADDED -> sb.append("+ ").append(f.path())
                        .append(" = ").append(f.after()).append('\n');
                case REMOVED -> sb.append("- ").append(f.path())
                        .append(" (was ").append(f.before()).append(")\n");
                case CHANGED -> sb.append("~ ").append(f.path())
                        .append(": ").append(f.before())
                        .append("  →  ").append(f.after()).append('\n');
            }
        }
        return sb.toString();
    }

    /** Best-effort JSON → Map parse via the Mongo BSON Document
     *  (the engine is shape-tolerant so BSON's Map-ish Document is
     *  fine). We deliberately don't pull in Jackson just for this. */
    @SuppressWarnings("unchecked")
    private static java.util.Map<String, Object> parseAsMap(String json) {
        if (json == null || json.isBlank()) return java.util.Map.of();
        try {
            org.bson.Document d = org.bson.Document.parse(json);
            return (java.util.Map<String, Object>) (java.util.Map<?, ?>) d;
        } catch (Exception e) {
            return java.util.Map.of();
        }
    }

    private void ok(String msg) {
        statusLabel.setText(msg);
        statusLabel.setStyle("-fx-text-fill: -color-success-emphasis; -fx-font-size: 11px; -fx-font-weight: 600;");
    }
    private void fail(String msg) {
        statusLabel.setText(msg);
        statusLabel.setStyle("-fx-text-fill: -color-danger-emphasis; -fx-font-size: 11px; -fx-font-weight: 600;");
    }
    private static <T> TableColumn<T, String> col(String title, int width,
                                                   java.util.function.Function<T, String> getter) {
        TableColumn<T, String> c = new TableColumn<>(title);
        c.setPrefWidth(width);
        c.setCellValueFactory(cd -> new SimpleStringProperty(getter.apply(cd.getValue())));
        return c;
    }
}

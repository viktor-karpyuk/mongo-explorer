package com.kubrik.mex.maint.ui;

import com.kubrik.mex.maint.model.UpgradePlan;
import com.kubrik.mex.maint.upgrade.RunbookRenderer;
import com.kubrik.mex.maint.upgrade.UpgradeRules;
import com.kubrik.mex.maint.upgrade.UpgradeScanner;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import javafx.stage.FileChooser;

import java.nio.file.Files;
import java.util.List;

/**
 * v2.7 UPG-* UI — Upgrade scan + runbook export.
 */
public final class UpgradePlannerPane extends BorderPane {

    private final UpgradeScanner scanner = new UpgradeScanner();
    private final RunbookRenderer renderer = new RunbookRenderer();

    private final TextField fromField = new TextField();
    private final TextField toField = new TextField();
    private final TextField hostsField = new TextField();
    private final ObservableList<UpgradePlan.Finding> findings =
            FXCollections.observableArrayList();
    private final TableView<UpgradePlan.Finding> findingsTable =
            new TableView<>(findings);
    private final TextArea preview = new TextArea();
    private final Label statusLabel = new Label("—");

    private UpgradePlan.Plan lastPlan;

    public UpgradePlannerPane() {
        setStyle("-fx-background-color: white;");
        setPadding(new Insets(14, 16, 14, 16));
        setTop(buildHeader());
        setCenter(buildCenter());
        setBottom(buildActions());
    }

    private Region buildHeader() {
        Label title = new Label("Upgrade planner");
        title.setStyle("-fx-font-size: 14px; -fx-font-weight: 700;");
        Label hint = new Label(
                "Scan the cluster for upgrade blockers + emit a rolling-"
                + "restart runbook. Supports immediate-neighbour major "
                + "hops only (4.4→5.0, 5.0→6.0, 6.0→7.0). Binary "
                + "replacement itself remains out-of-scope per NG-2.7-1.");
        hint.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px;");
        hint.setWrapText(true);

        fromField.setPromptText("6.0.0");
        toField.setPromptText("7.0.0");
        hostsField.setPromptText("h1:27017,h2:27017,h3:27017");

        GridPane g = new GridPane();
        g.setHgap(8); g.setVgap(6);
        int row = 0;
        g.add(small("From"), 0, row); g.add(fromField, 1, row);
        g.add(small("To"), 2, row); g.add(toField, 3, row++);
        g.add(small("Member hosts (primary last)"), 0, row);
        g.add(hostsField, 1, row++, 3, 1);

        VBox v = new VBox(6, title, hint, g);
        v.setPadding(new Insets(0, 0, 10, 0));
        return v;
    }

    private Region buildCenter() {
        findingsTable.setPlaceholder(new Label("Scan to populate."));
        findingsTable.getColumns().setAll(
                col("Severity", 90, f -> f.severity().name()),
                col("Code", 180, UpgradePlan.Finding::code),
                col("Title", 260, UpgradePlan.Finding::title));
        findingsTable.setPrefHeight(170);

        preview.setEditable(false);
        preview.setWrapText(true);
        preview.setPrefRowCount(14);
        preview.setStyle("-fx-font-family: 'Menlo', 'Courier New', monospace; -fx-font-size: 11px;");

        Label previewLabel = new Label("Markdown runbook preview");
        previewLabel.setStyle("-fx-text-fill: #4b5563; -fx-font-size: 11px; -fx-font-weight: 600;");
        VBox v = new VBox(6, findingsTable, previewLabel, preview);
        VBox.setVgrow(preview, Priority.ALWAYS);
        return v;
    }

    private Region buildActions() {
        Button scanBtn = new Button("Scan");
        scanBtn.setOnAction(e -> onScan());
        Button exportMdBtn = new Button("Export Markdown…");
        exportMdBtn.setOnAction(e -> onExport(true));
        Button exportHtmlBtn = new Button("Export HTML…");
        exportHtmlBtn.setOnAction(e -> onExport(false));

        Region grow = new Region();
        HBox.setHgrow(grow, Priority.ALWAYS);
        HBox actions = new HBox(8, scanBtn, grow, exportMdBtn, exportHtmlBtn);
        actions.setPadding(new Insets(10, 0, 0, 0));

        statusLabel.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px;");
        statusLabel.setWrapText(true);
        return new VBox(6, actions, statusLabel);
    }

    private void onScan() {
        try {
            UpgradePlan.Version from = UpgradePlan.Version.parse(fromField.getText().trim());
            UpgradePlan.Version to = UpgradePlan.Version.parse(toField.getText().trim());
            List<String> hosts = java.util.Arrays.stream(
                            hostsField.getText().split(","))
                    .map(String::trim).filter(s -> !s.isEmpty()).toList();
            UpgradeRules.Context ctx = new UpgradeRules.Context(from, to,
                    List.of(), List.of());
            lastPlan = scanner.scan(ctx, hosts);
            findings.setAll(lastPlan.findings());
            preview.setText(renderer.renderMarkdown(lastPlan));
            if (lastPlan.hasBlockers()) {
                fail("Blocking findings — address before exporting.");
            } else {
                ok(lastPlan.findings().size() + " findings, "
                        + lastPlan.steps().size() + " steps.");
            }
        } catch (Exception ex) {
            fail("Scan failed: " + ex.getMessage());
        }
    }

    private void onExport(boolean markdown) {
        if (lastPlan == null) { fail("Run Scan first."); return; }
        FileChooser fc = new FileChooser();
        fc.setTitle(markdown ? "Export runbook (Markdown)" : "Export runbook (HTML)");
        fc.setInitialFileName("mongo-upgrade-runbook."
                + (markdown ? "md" : "html"));
        var file = fc.showSaveDialog(getScene() == null ? null : getScene().getWindow());
        if (file == null) return;
        try {
            String body = markdown
                    ? renderer.renderMarkdown(lastPlan)
                    : renderer.renderHtml(lastPlan);
            Files.writeString(file.toPath(), body);
            ok("Wrote " + file.getName() + ".");
        } catch (Exception ex) {
            fail("Export failed: " + ex.getMessage());
        }
    }

    private void ok(String msg) {
        statusLabel.setText(msg);
        statusLabel.setStyle("-fx-text-fill: #166534; -fx-font-size: 11px; -fx-font-weight: 600;");
    }
    private void fail(String msg) {
        statusLabel.setText(msg);
        statusLabel.setStyle("-fx-text-fill: #b91c1c; -fx-font-size: 11px; -fx-font-weight: 600;");
    }
    private static Label small(String text) {
        Label l = new Label(text);
        l.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px;");
        return l;
    }
    private static <T> TableColumn<T, String> col(String title, int width,
                                                   java.util.function.Function<T, String> getter) {
        TableColumn<T, String> c = new TableColumn<>(title);
        c.setPrefWidth(width);
        c.setCellValueFactory(cd -> new SimpleStringProperty(getter.apply(cd.getValue())));
        return c;
    }
}

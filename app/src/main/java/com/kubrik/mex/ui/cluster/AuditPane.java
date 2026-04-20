package com.kubrik.mex.ui.cluster;

import com.kubrik.mex.cluster.audit.AuditExporter;
import com.kubrik.mex.cluster.audit.OpsAuditRecord;
import com.kubrik.mex.cluster.audit.Outcome;
import com.kubrik.mex.cluster.store.OpsAuditDao;
import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.ui.JsonCodeArea;
import javafx.application.Platform;
import javafx.beans.property.SimpleObjectProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.collections.transformation.FilteredList;
import javafx.collections.transformation.SortedList;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.ChoiceBox;
import javafx.scene.control.Label;
import javafx.scene.control.ScrollPane;
import javafx.scene.control.TableCell;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableRow;
import javafx.scene.control.TableView;
import javafx.scene.control.TextField;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import javafx.stage.FileChooser;
import javafx.stage.Stage;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * v2.4 AUD-4..6 — live audit pane for a connection. Loads the most-recent
 * {@code listForConnection} page on mount, then prepends rows that arrive on
 * {@link EventBus#onOpsAudit}. Filters (command name, outcome, free-text over
 * command + server message) drive a {@link FilteredList}; the pane stays
 * decoupled from the DAO so sort / filter work happens on the JavaFX thread
 * without extra DB round-trips.
 */
public final class AuditPane extends BorderPane implements AutoCloseable {

    private static final int PAGE_SIZE = 500;
    private static final DateTimeFormatter TS_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());

    private final String connectionId;
    private final OpsAuditDao dao;
    private final EventBus bus;

    private final ObservableList<OpsAuditRecord> rows = FXCollections.observableArrayList();
    private final FilteredList<OpsAuditRecord> filtered = new FilteredList<>(rows, r -> true);
    private final SortedList<OpsAuditRecord> sorted = new SortedList<>(filtered);
    private final TableView<OpsAuditRecord> table = new TableView<>(sorted);
    private final TextField search = new TextField();
    private final ChoiceBox<String> outcomeFilter = new ChoiceBox<>();
    private final ChoiceBox<String> commandFilter = new ChoiceBox<>();
    private final Label footer = new Label("—");
    private final EventBus.Subscription sub;

    public AuditPane(String connectionId, OpsAuditDao dao, EventBus bus) {
        this.connectionId = connectionId;
        this.dao = dao;
        this.bus = bus;

        setStyle("-fx-background-color: white;");
        setPadding(new Insets(14, 16, 14, 16));
        setTop(buildFilterBar());
        setCenter(buildTable());
        setBottom(footerBar());

        sub = bus.onOpsAudit(r -> {
            if (!connectionId.equals(r.connectionId())) return;
            Platform.runLater(() -> {
                rows.add(0, r);
                reapplyFilter();
            });
        });

        sceneProperty().addListener((obs, o, n) -> {
            if (n != null) reload();
        });
    }

    @Override
    public void close() {
        try { sub.close(); } catch (Exception ignored) {}
    }

    /* =========================== loading ============================== */

    private void reload() {
        Thread.startVirtualThread(() -> {
            List<OpsAuditRecord> page = dao.listForConnection(connectionId, PAGE_SIZE);
            Platform.runLater(() -> {
                rows.setAll(page);
                populateCommandFilter();
                reapplyFilter();
            });
        });
    }

    private void populateCommandFilter() {
        var names = new java.util.TreeSet<String>();
        for (OpsAuditRecord r : rows) names.add(r.commandName());
        String current = commandFilter.getValue();
        commandFilter.getItems().setAll("all commands");
        commandFilter.getItems().addAll(names);
        commandFilter.setValue(current == null ? "all commands" : current);
    }

    /* =========================== filter bar =========================== */

    private Region buildFilterBar() {
        search.setPromptText("search command / message");
        search.setPrefWidth(240);
        search.textProperty().addListener((o, a, b) -> reapplyFilter());

        outcomeFilter.getItems().addAll("all outcomes", "OK", "FAIL", "CANCELLED", "PENDING");
        outcomeFilter.setValue("all outcomes");
        outcomeFilter.valueProperty().addListener((o, a, b) -> reapplyFilter());

        commandFilter.getItems().add("all commands");
        commandFilter.setValue("all commands");
        commandFilter.valueProperty().addListener((o, a, b) -> reapplyFilter());

        Region grow = new Region();
        HBox.setHgrow(grow, Priority.ALWAYS);
        Button jsonBtn = new Button("Export JSON…");
        jsonBtn.setOnAction(e -> exportRows(true));
        Button csvBtn = new Button("Export CSV…");
        csvBtn.setOnAction(e -> exportRows(false));
        HBox row = new HBox(10, small("command"), commandFilter, small("outcome"),
                outcomeFilter, small("search"), search, grow, jsonBtn, csvBtn);
        row.setAlignment(Pos.CENTER_LEFT);
        row.setPadding(new Insets(0, 0, 10, 0));
        return row;
    }

    private void exportRows(boolean asJson) {
        List<OpsAuditRecord> toExport = table.getSelectionModel().getSelectedItems().isEmpty()
                ? new java.util.ArrayList<>(filtered)
                : new java.util.ArrayList<>(table.getSelectionModel().getSelectedItems());
        if (toExport.isEmpty()) {
            footer.setText("Nothing to export.");
            return;
        }
        FileChooser fc = new FileChooser();
        fc.setTitle("Export audit rows");
        String suggested = "ops-audit-" + Instant.now().getEpochSecond() + (asJson ? ".json" : ".csv");
        fc.setInitialFileName(suggested);
        fc.getExtensionFilters().add(asJson
                ? new FileChooser.ExtensionFilter("JSON bundle", "*.json")
                : new FileChooser.ExtensionFilter("CSV", "*.csv"));
        java.io.File out = fc.showSaveDialog(getScene() == null ? null : getScene().getWindow());
        if (out == null) return;
        Path target = Path.of(out.getAbsolutePath());
        Thread.startVirtualThread(() -> {
            try {
                if (asJson) AuditExporter.writeJson(target, toExport);
                else        AuditExporter.writeCsv(target, toExport);
                Platform.runLater(() -> footer.setText(
                        "Exported " + toExport.size() + " rows → " + out.getName()));
            } catch (IOException ex) {
                Platform.runLater(() -> footer.setText(
                        "Export failed: " + ex.getClass().getSimpleName() + " — " + ex.getMessage()));
            }
        });
    }

    private void reapplyFilter() {
        String cmd = commandFilter.getValue();
        String oc = outcomeFilter.getValue();
        String q = search.getText() == null ? "" : search.getText().trim().toLowerCase();
        filtered.setPredicate(r -> {
            if (cmd != null && !"all commands".equals(cmd) && !cmd.equals(r.commandName())) return false;
            if (oc != null && !"all outcomes".equals(oc) && !r.outcome().name().equals(oc)) return false;
            if (!q.isEmpty()) {
                String blob = (r.commandName() + " " + (r.serverMessage() == null ? "" : r.serverMessage())).toLowerCase();
                if (!blob.contains(q)) return false;
            }
            return true;
        });
        updateFooter();
    }

    private void updateFooter() {
        footer.setText(rows.size() + " rows loaded · " + filtered.size() + " visible"
                + "  ·  newest first");
    }

    /* =============================== table ============================= */

    private Region buildTable() {
        table.setPlaceholder(new Label("No audit rows yet. Destructive actions on this connection will appear here."));
        sorted.comparatorProperty().bind(table.comparatorProperty());

        table.getColumns().setAll(
                tsCol("started", 160, OpsAuditRecord::startedAt),
                textCol("command", 180, OpsAuditRecord::commandName),
                outcomeCol(),
                textCol("ui_source", 140, OpsAuditRecord::uiSource),
                textCol("role", 120, r -> r.roleUsed() == null ? "—" : r.roleUsed()),
                numCol("latency ms", 90, r -> r.latencyMs() == null ? 0L : r.latencyMs()),
                textCol("message", 320, r -> r.serverMessage() == null ? "" : r.serverMessage())
        );
        table.setRowFactory(tv -> {
            var row = new TableRow<OpsAuditRecord>();
            row.setOnMouseClicked(e -> {
                if (e.getClickCount() == 2 && !row.isEmpty()) openDetail(row.getItem());
            });
            return row;
        });
        VBox.setVgrow(table, Priority.ALWAYS);
        return table;
    }

    private Region footerBar() {
        footer.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px;");
        HBox b = new HBox(footer);
        b.setPadding(new Insets(6, 0, 0, 0));
        return b;
    }

    /* =========================== detail drawer ========================= */

    private void openDetail(OpsAuditRecord r) {
        JsonCodeArea area = new JsonCodeArea(r.commandJsonRedacted());
        area.setEditable(false);
        ScrollPane scroll = new ScrollPane(area);
        scroll.setFitToWidth(true);

        Label title = new Label(r.commandName() + "  ·  " + r.outcome().name());
        title.setStyle("-fx-font-weight: 700; -fx-font-size: 14px; -fx-padding: 14 16 4 16;");
        Label meta = new Label("started " + TS_FMT.format(Instant.ofEpochMilli(r.startedAt()))
                + (r.latencyMs() != null ? "  ·  " + r.latencyMs() + " ms" : "")
                + (r.roleUsed() != null ? "  ·  role " + r.roleUsed() : "")
                + (r.paste() ? "  ·  paste" : "")
                + (r.killSwitch() ? "  ·  kill-switch" : ""));
        meta.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px; -fx-padding: 0 16 6 16;");
        Label hash = new Label("preview hash: " + r.previewHash());
        hash.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 10px; "
                + "-fx-font-family: 'JetBrains Mono','Menlo',monospace; -fx-padding: 0 16 10 16;");
        Label msg = new Label(r.serverMessage() == null ? "" : "server: " + r.serverMessage());
        msg.setStyle("-fx-text-fill: #374151; -fx-font-size: 12px; -fx-padding: 0 16 10 16;");
        msg.setWrapText(true);

        VBox header = new VBox(title, meta, hash, msg);
        BorderPane body = new BorderPane(scroll);
        body.setTop(header);
        Scene scene = new Scene(body, 760, 560);
        Stage stage = new Stage();
        stage.setTitle("Audit row #" + r.id());
        stage.setScene(scene);
        stage.show();
    }

    /* =========================== column helpers ======================== */

    private static TableColumn<OpsAuditRecord, String> textCol(String title, int width,
                                                                java.util.function.Function<OpsAuditRecord, String> getter) {
        TableColumn<OpsAuditRecord, String> c = new TableColumn<>(title);
        c.setPrefWidth(width);
        c.setCellValueFactory(cd -> new SimpleStringProperty(getter.apply(cd.getValue())));
        return c;
    }

    private static TableColumn<OpsAuditRecord, String> tsCol(String title, int width,
                                                              java.util.function.ToLongFunction<OpsAuditRecord> getter) {
        TableColumn<OpsAuditRecord, String> c = new TableColumn<>(title);
        c.setPrefWidth(width);
        c.setCellValueFactory(cd -> new SimpleStringProperty(
                TS_FMT.format(Instant.ofEpochMilli(getter.applyAsLong(cd.getValue())))));
        c.setSortType(TableColumn.SortType.DESCENDING);
        return c;
    }

    private static TableColumn<OpsAuditRecord, Number> numCol(String title, int width,
                                                               java.util.function.ToLongFunction<OpsAuditRecord> getter) {
        TableColumn<OpsAuditRecord, Number> c = new TableColumn<>(title);
        c.setPrefWidth(width);
        c.setCellValueFactory(cd -> new SimpleObjectProperty<>(getter.applyAsLong(cd.getValue())));
        c.setCellFactory(col -> {
            TableCell<OpsAuditRecord, Number> cell = new TableCell<>() {
                @Override protected void updateItem(Number n, boolean empty) {
                    super.updateItem(n, empty);
                    setText(empty || n == null ? "" : String.valueOf(n.longValue()));
                }
            };
            cell.setAlignment(Pos.CENTER_RIGHT);
            return cell;
        });
        return c;
    }

    private static TableColumn<OpsAuditRecord, Outcome> outcomeCol() {
        TableColumn<OpsAuditRecord, Outcome> c = new TableColumn<>("outcome");
        c.setPrefWidth(100);
        c.setCellValueFactory(cd -> new SimpleObjectProperty<>(cd.getValue().outcome()));
        c.setCellFactory(col -> new TableCell<>() {
            @Override protected void updateItem(Outcome o, boolean empty) {
                super.updateItem(o, empty);
                if (empty || o == null) { setText(""); setStyle(""); return; }
                setText(o.name());
                setStyle(outcomeStyle(o));
            }
        });
        return c;
    }

    private static String outcomeStyle(Outcome o) {
        String fg;
        switch (o) {
            case OK        -> fg = "#166534";
            case FAIL      -> fg = "#991b1b";
            case CANCELLED -> fg = "#92400e";
            case PENDING   -> fg = "#374151";
            default        -> fg = "#374151";
        }
        return "-fx-text-fill: " + fg + "; -fx-font-weight: 700;";
    }

    private static Label small(String s) {
        Label l = new Label(s);
        l.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px;");
        return l;
    }
}

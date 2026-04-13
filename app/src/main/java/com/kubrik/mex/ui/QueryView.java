package com.kubrik.mex.ui;

import com.kubrik.mex.core.ConnectionManager;
import com.kubrik.mex.core.MongoService;
import com.kubrik.mex.core.QueryRunner;
import com.kubrik.mex.model.QueryRequest;
import com.kubrik.mex.model.QueryResult;
import com.kubrik.mex.store.HistoryStore;
import javafx.application.Platform;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Button;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Label;
import javafx.scene.control.ListView;
import javafx.scene.control.SplitPane;
import javafx.scene.control.Tab;
import javafx.scene.control.TabPane;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import javafx.stage.FileChooser;

import java.nio.file.Files;
import java.nio.file.Path;

public class QueryView extends VBox {

    private final ConnectionManager manager;
    private final HistoryStore history;

    private String connectionId;
    private boolean fixedNamespace;
    private final ComboBox<String> dbBox = new ComboBox<>();
    private final ComboBox<String> collBox = new ComboBox<>();

    private final TextArea filter = monoArea("{ }");
    private final TextArea projection = monoArea("");
    private final TextArea sort = monoArea("");
    { filter.setPrefRowCount(2); projection.setPrefRowCount(2); sort.setPrefRowCount(2); }
    private final TextField skip = new TextField("0");
    private final TextField limit = new TextField("50");
    private final TextField maxTime = new TextField("30000");

    private final AggregationView aggView = new AggregationView();

    private final ResultsPane results = new ResultsPane();
    private final TextArea detail = monoArea("");
    private final Label statusLabel = new Label("");
    private final Label nsLabel = new Label("");
    private org.bson.Document selectedDoc;
    private TabPane queryTabs;

    private Button prevPageBtn;
    private Button nextPageBtn;
    private final Label pageInfo = new Label("no results");
    private boolean lastHasMore = false;
    private int lastResultCount = 0;

    private SplitPane resultsSplit;
    private SplitPane mainSplit;
    private double lastDetailDivider = 0.55;
    private double lastResultsDivider = 0.42;

    public QueryView(ConnectionManager manager, HistoryStore history) {
        this.manager = manager;
        this.history = history;
        setPadding(new Insets(0));
        setSpacing(0);
        setStyle("-fx-background-color: white;");

        dbBox.setPrefWidth(160);
        collBox.setPrefWidth(200);
        dbBox.valueProperty().addListener((o, a, b) -> { reloadCollections(); updateNsLabel(); });
        collBox.valueProperty().addListener((o, a, b) -> updateNsLabel());

        /* ============ HEADER BAR ============ */
        org.kordamp.ikonli.javafx.FontIcon collIcon = new org.kordamp.ikonli.javafx.FontIcon("fth-grid");
        collIcon.setIconSize(18);
        collIcon.setIconColor(javafx.scene.paint.Color.web("#2563eb"));
        nsLabel.setStyle("-fx-font-size: 16px; -fx-font-weight: bold;");
        Region hsp = new Region();
        HBox.setHgrow(hsp, Priority.ALWAYS);
        statusLabel.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 12px;");
        HBox header = new HBox(10, collIcon, nsLabel, hsp, statusLabel);
        header.setAlignment(Pos.CENTER_LEFT);
        header.setPadding(new Insets(10, 16, 10, 16));
        header.setStyle("-fx-background-color: #f9fafb; -fx-border-color: #e5e7eb; -fx-border-width: 0 0 1 0;");

        /* ============ TOP ACTION TOOLBAR (Run, paging, doc ops, view switcher) ============ */
        Button runBtn = new Button("Run");
        org.kordamp.ikonli.javafx.FontIcon playIcon = new org.kordamp.ikonli.javafx.FontIcon("fth-play");
        playIcon.setIconColor(javafx.scene.paint.Color.WHITE);
        runBtn.setGraphic(playIcon);
        runBtn.setStyle("-fx-background-color: #16a34a; -fx-text-fill: white; -fx-font-weight: bold; -fx-padding: 6 14 6 14; -fx-background-radius: 4;");
        runBtn.setTooltip(new javafx.scene.control.Tooltip("Run query  (⌘↵)"));
        runBtn.setOnAction(e -> runActive());

        Button refreshBtn = UiHelpers.iconButton("fth-rotate-cw", "Re-run last query");
        refreshBtn.setOnAction(e -> runActive());

        prevPageBtn = UiHelpers.iconButton("fth-chevron-left", "Previous page");
        nextPageBtn = UiHelpers.iconButton("fth-chevron-right", "Next page");
        prevPageBtn.setDisable(true);
        nextPageBtn.setDisable(true);
        prevPageBtn.setOnAction(e -> { skip.setText(String.valueOf(Math.max(0, parseInt(skip.getText(), 0) - parseInt(limit.getText(), 50)))); doFind(); });
        nextPageBtn.setOnAction(e -> { skip.setText(String.valueOf(parseInt(skip.getText(), 0) + parseInt(limit.getText(), 50))); doFind(); });

        Button insert = UiHelpers.iconButton("fth-file-plus", "Insert new document");
        Button update = UiHelpers.iconButton("fth-edit-3", "Edit selected document");
        Button copyDoc = UiHelpers.iconButton("fth-copy", "Copy document to clipboard");
        Button delete = UiHelpers.iconButton("fth-trash-2", "Delete selected document");
        Button export = UiHelpers.iconButton("fth-download", "Export results to JSON");
        delete.setOnMouseEntered(e -> delete.setStyle("-fx-background-color: #fee2e2; -fx-padding: 4 8 4 8; -fx-background-radius: 4;"));
        delete.setOnMouseExited(e -> delete.setStyle("-fx-background-color: transparent; -fx-padding: 4 8 4 8; -fx-background-radius: 4;"));
        update.setDisable(true);
        copyDoc.setDisable(true);
        delete.setDisable(true);
        copyDoc.setOnAction(e -> {
            if (selectedDoc == null) return;
            String json = selectedDoc.toJson(com.kubrik.mex.core.MongoService.JSON_RELAXED);
            javafx.scene.input.ClipboardContent cc = new javafx.scene.input.ClipboardContent();
            cc.putString(json);
            javafx.scene.input.Clipboard.getSystemClipboard().setContent(cc);
            statusLabel.setText("Document copied to clipboard");
        });

        // Segmented view switcher (Table / Tree / JSON)
        javafx.scene.control.ToggleGroup viewGroup = new javafx.scene.control.ToggleGroup();
        javafx.scene.control.ToggleButton vTable = viewToggle("fth-grid", "Table", viewGroup, 0);
        javafx.scene.control.ToggleButton vTree  = viewToggle("fth-list", "Tree",  viewGroup, 1);
        javafx.scene.control.ToggleButton vJson  = viewToggle("fth-code", "JSON",  viewGroup, 2);
        vTable.setSelected(true);
        viewGroup.selectedToggleProperty().addListener((o, a, b) -> {
            if (b == null) { a.setSelected(true); return; } // prevent deselect-all
            results.getSelectionModel().select((Integer) b.getUserData());
        });
        results.getSelectionModel().selectedIndexProperty().addListener((o, a, b) -> {
            int i = b.intValue();
            javafx.scene.control.ToggleButton t = i == 0 ? vTable : i == 1 ? vTree : vJson;
            if (!t.isSelected()) t.setSelected(true);
        });
        HBox viewSwitcher = new HBox(0, vTable, vTree, vJson);
        viewSwitcher.setAlignment(Pos.CENTER);

        Region tsp = new Region();
        HBox.setHgrow(tsp, Priority.ALWAYS);
        // Toggle button: hide/show the results panel (collapse to query-only)
        Button toggleResults = UiHelpers.iconButton("fth-minimize-2", "Toggle results panel");
        toggleResults.setOnAction(e -> {
            org.kordamp.ikonli.javafx.FontIcon ico = (org.kordamp.ikonli.javafx.FontIcon) toggleResults.getGraphic();
            if (mainSplit.getItems().contains(resultsSplit)) {
                lastResultsDivider = mainSplit.getDividerPositions()[0];
                mainSplit.getItems().remove(resultsSplit);
                ico.setIconLiteral("fth-maximize-2");
            } else {
                mainSplit.getItems().add(resultsSplit);
                mainSplit.setDividerPositions(lastResultsDivider);
                ico.setIconLiteral("fth-minimize-2");
            }
        });

        HBox topToolbar = new HBox(8,
                runBtn,
                refreshBtn,
                vsep(),
                insert,
                tsp,
                viewSwitcher,
                vsep(),
                toggleResults);
        topToolbar.setAlignment(Pos.CENTER_LEFT);
        topToolbar.setPadding(new Insets(8, 16, 8, 16));
        topToolbar.setStyle("-fx-background-color: white; -fx-border-color: #e5e7eb; -fx-border-width: 0 0 1 0;");

        /* ============ NS row (only when not fixed namespace) ============ */
        HBox nsRow = new HBox(8, new Label("DB"), dbBox, new Label("Collection"), collBox);
        nsRow.setAlignment(Pos.CENTER_LEFT);
        nsRow.setPadding(new Insets(8, 16, 0, 16));

        /* ============ FIND FORM ============ */
        GridPane findForm = new GridPane();
        findForm.setHgap(8); findForm.setVgap(6);
        findForm.setPadding(new Insets(12, 16, 12, 16));
        // Make the filter/projection/sort areas vertically resizable.
        GridPane.setVgrow(filter, Priority.SOMETIMES);
        GridPane.setVgrow(projection, Priority.SOMETIMES);
        GridPane.setVgrow(sort, Priority.SOMETIMES);
        filter.setMinHeight(40);
        projection.setMinHeight(40);
        sort.setMinHeight(40);
        findForm.addRow(0, formLabel("Filter"), filter);
        findForm.addRow(1, formLabel("Projection"), projection);
        findForm.addRow(2, formLabel("Sort"), sort);
        HBox limits = new HBox(8,
                new Label("Skip"), skip,
                new Label("Limit"), limit,
                new Label("maxTimeMs"), maxTime);
        limits.setAlignment(Pos.CENTER_LEFT);
        skip.setPrefColumnCount(6); limit.setPrefColumnCount(6); maxTime.setPrefColumnCount(8);
        findForm.add(limits, 1, 3);
        javafx.scene.layout.ColumnConstraints lc = new javafx.scene.layout.ColumnConstraints();
        lc.setMinWidth(80); lc.setHalignment(javafx.geometry.HPos.RIGHT);
        javafx.scene.layout.ColumnConstraints fc = new javafx.scene.layout.ColumnConstraints();
        fc.setHgrow(Priority.ALWAYS); fc.setFillWidth(true);
        findForm.getColumnConstraints().addAll(lc, fc);
        // Row constraints so filter/projection/sort expand when the Find tab gets more space.
        javafx.scene.layout.RowConstraints rGrow = new javafx.scene.layout.RowConstraints();
        rGrow.setVgrow(Priority.ALWAYS);
        javafx.scene.layout.RowConstraints rFixed = new javafx.scene.layout.RowConstraints();
        rFixed.setVgrow(Priority.NEVER);
        findForm.getRowConstraints().addAll(rGrow, rGrow, rGrow, rFixed);

        /* ============ QUERY MODE TABS ============ */
        queryTabs = new TabPane(
                new Tab("Find", findForm),
                new Tab("Aggregate", aggView));
        queryTabs.getTabs().forEach(t -> t.setClosable(false));

        aggView.setOnRun(this::doAggregate);
        aggView.setCollectionNameSupplier(() -> collBox.getValue() == null ? "collection" : collBox.getValue());

        results.setOnSelect(d -> {
            selectedDoc = d;
            detail.setText(d == null ? "" : d.toJson(com.kubrik.mex.core.MongoService.JSON_RELAXED));
            update.setDisable(d == null);
            copyDoc.setDisable(d == null);
            delete.setDisable(d == null);
        });
        detail.setEditable(false);

        /* ============ RESULTS ============ */
        Region rfsp = new Region();
        HBox.setHgrow(rfsp, Priority.ALWAYS);
        pageInfo.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px;");

        // Toggle button: hide/show the JSON detail panel
        Button toggleDetail = UiHelpers.iconButton("fth-sidebar", "Toggle document detail panel");
        toggleDetail.setOnAction(e -> {
            if (resultsSplit.getItems().contains(detail)) {
                lastDetailDivider = resultsSplit.getDividerPositions()[0];
                resultsSplit.getItems().remove(detail);
            } else {
                resultsSplit.getItems().add(detail);
                resultsSplit.setDividerPositions(lastDetailDivider);
            }
        });

        HBox resultsFooter = new HBox(6, prevPageBtn, nextPageBtn, pageInfo, rfsp, copyDoc, update, delete, export, vsep(), toggleDetail);
        resultsFooter.setAlignment(Pos.CENTER_LEFT);
        resultsFooter.setPadding(new Insets(4, 8, 4, 8));
        resultsFooter.setStyle("-fx-background-color: #f9fafb; -fx-border-color: #e5e7eb; -fx-border-width: 1 0 0 0;");

        VBox resultsBox = new VBox(results, resultsFooter);
        VBox.setVgrow(results, Priority.ALWAYS);

        resultsSplit = new SplitPane(resultsBox, detail);
        resultsSplit.setDividerPositions(0.55);
        VBox.setVgrow(resultsSplit, Priority.ALWAYS);

        mainSplit = new SplitPane();
        mainSplit.setOrientation(javafx.geometry.Orientation.VERTICAL);
        mainSplit.getItems().addAll(queryTabs, resultsSplit);
        mainSplit.setDividerPositions(0.42);
        VBox.setVgrow(mainSplit, Priority.ALWAYS);

        getChildren().addAll(header, topToolbar, nsRow, mainSplit);

        // Cmd/Ctrl+Enter runs the active query mode from any input
        javafx.event.EventHandler<javafx.scene.input.KeyEvent> runShortcut = ev -> {
            if (ev.isShortcutDown() && ev.getCode() == javafx.scene.input.KeyCode.ENTER) {
                runActive();
                ev.consume();
            }
        };
        for (javafx.scene.Node n : new javafx.scene.Node[]{filter, projection, sort, skip, limit, maxTime}) {
            n.addEventFilter(javafx.scene.input.KeyEvent.KEY_PRESSED, runShortcut);
        }
        addEventFilter(javafx.scene.input.KeyEvent.KEY_PRESSED, runShortcut);

        insert.setOnAction(e -> doInsert());
        update.setOnAction(e -> doUpdate());
        delete.setOnAction(e -> doDelete());
        export.setOnAction(e -> doExport());

        updateNsLabel();
    }

    /* ---------------- new helpers ---------------- */

    private void runActive() {
        int idx = queryTabs.getSelectionModel().getSelectedIndex();
        if (idx == 1) aggView.run(); else doFind();
    }

    private void updateNsLabel() {
        String d = dbBox.getValue();
        String c = collBox.getValue();
        if (d != null && c != null) nsLabel.setText(d + "." + c);
        else if (d != null) nsLabel.setText(d);
        else nsLabel.setText("(no collection)");
    }

    private static javafx.scene.control.Separator vsep() {
        javafx.scene.control.Separator s = new javafx.scene.control.Separator(javafx.geometry.Orientation.VERTICAL);
        s.setPadding(new Insets(0, 4, 0, 4));
        return s;
    }

    private static Label formLabel(String text) {
        Label l = new Label(text);
        l.setStyle("-fx-text-fill: #374151; -fx-font-size: 12px;");
        return l;
    }

    private static javafx.scene.control.ToggleButton viewToggle(String iconLit, String tooltip,
                                                                javafx.scene.control.ToggleGroup group, int index) {
        org.kordamp.ikonli.javafx.FontIcon icon = new org.kordamp.ikonli.javafx.FontIcon(iconLit);
        icon.setIconSize(14);
        javafx.scene.control.ToggleButton b = new javafx.scene.control.ToggleButton();
        b.setGraphic(icon);
        b.setTooltip(new javafx.scene.control.Tooltip(tooltip));
        b.setUserData(index);
        b.setToggleGroup(group);
        b.setStyle("-fx-background-radius: 0; -fx-padding: 4 10 4 10;");
        return b;
    }

    public void setFixedNamespace(String id, String db, String coll) {
        this.connectionId = id;
        this.fixedNamespace = true;
        dbBox.setItems(FXCollections.observableArrayList(db));
        dbBox.setValue(db);
        collBox.setItems(FXCollections.observableArrayList(coll));
        collBox.setValue(coll);
        dbBox.setDisable(true);
        collBox.setDisable(true);
    }

    public void setConnection(String id, String db, String coll) {
        if (fixedNamespace) return;
        this.connectionId = id;
        MongoService svc = id != null ? manager.service(id) : null;
        if (svc == null) { dbBox.setItems(FXCollections.observableArrayList()); return; }
        Thread.startVirtualThread(() -> {
            try {
                var dbs = svc.listDatabaseNames();
                Platform.runLater(() -> {
                    dbBox.setItems(FXCollections.observableArrayList(dbs));
                    if (db != null && dbs.contains(db)) dbBox.setValue(db);
                    else if (!dbs.isEmpty()) dbBox.setValue(dbs.get(0));
                    if (coll != null) Platform.runLater(() -> collBox.setValue(coll));
                });
            } catch (Exception ignored) {}
        });
    }

    private void reloadCollections() {
        if (connectionId == null || dbBox.getValue() == null) return;
        MongoService svc = manager.service(connectionId);
        if (svc == null) return;
        String d = dbBox.getValue();
        Thread.startVirtualThread(() -> {
            try {
                var colls = svc.listCollectionNames(d);
                Platform.runLater(() -> {
                    collBox.setItems(FXCollections.observableArrayList(colls));
                });
            } catch (Exception ignored) {}
        });
    }

    private QueryRunner runner() {
        MongoService svc = manager.service(connectionId);
        return svc == null ? null : new QueryRunner(svc);
    }

    private void doFind() {
        QueryRunner r = runner();
        if (r == null || dbBox.getValue() == null || collBox.getValue() == null) return;
        QueryRequest req = new QueryRequest(
                dbBox.getValue(), collBox.getValue(),
                filter.getText(), projection.getText(), sort.getText(),
                parseInt(skip.getText(), 0), parseInt(limit.getText(), 50),
                parseLong(maxTime.getText(), 30000));
        history.add(connectionId, req.dbName(), req.collName(), "find", filter.getText());
        statusLabel.setText("Running…");
        Thread.startVirtualThread(() -> {
            QueryResult res = r.find(req);
            Platform.runLater(() -> applyResult(res));
        });
    }

    private void doAggregate(String pipelineJson) {
        QueryRunner r = runner();
        if (r == null || dbBox.getValue() == null || collBox.getValue() == null) return;
        history.add(connectionId, dbBox.getValue(), collBox.getValue(), "aggregate", pipelineJson);
        statusLabel.setText("Running…");
        String d = dbBox.getValue(), c = collBox.getValue();
        long mt = parseLong(maxTime.getText(), 30000);
        Thread.startVirtualThread(() -> {
            QueryResult res = r.aggregate(d, c, pipelineJson, mt);
            Platform.runLater(() -> applyResult(res));
        });
    }

    private void applyResult(QueryResult res) {
        if (res.error() != null) {
            statusLabel.setStyle("-fx-text-fill: #dc2626;");
            statusLabel.setText("Error: " + res.error());
            results.setDocuments(java.util.List.of());
            selectedDoc = null;
            detail.clear();
            lastHasMore = false;
            lastResultCount = 0;
            updatePagination();
            return;
        }
        statusLabel.setStyle("-fx-text-fill: #6b7280;");
        statusLabel.setText(res.documents().size() + " docs · " + res.durationMs() + " ms"
                + (res.hasMore() ? " · more available" : ""));
        results.setDocuments(res.documents());
        selectedDoc = null;
        detail.clear();
        lastHasMore = res.hasMore();
        lastResultCount = res.documents().size();
        updatePagination();
    }

    private void updatePagination() {
        int s = parseInt(skip.getText(), 0);
        boolean hasPrev = s > 0;
        boolean hasNext = lastHasMore;
        if (prevPageBtn != null) prevPageBtn.setDisable(!hasPrev);
        if (nextPageBtn != null) nextPageBtn.setDisable(!hasNext);
        if (lastResultCount == 0) {
            pageInfo.setText("no results");
        } else {
            int from = s + 1;
            int to = s + lastResultCount;
            pageInfo.setText("rows " + from + "–" + to + (hasNext ? " (more)" : ""));
        }
    }

    private void doInsert() {
        DocumentEditorDialog editor = new DocumentEditorDialog(
                getScene().getWindow(),
                "Insert document",
                "Insert a new document into " + dbBox.getValue() + "." + collBox.getValue(),
                "{\n  \n}");
        editor.showAndWait().ifPresent(json -> {
            QueryRunner r = runner();
            String db = dbBox.getValue(), coll = collBox.getValue();
            statusLabel.setText("Inserting…");
            Thread.startVirtualThread(() -> {
                try {
                    r.insert(db, coll, json);
                    Platform.runLater(this::doFind);
                } catch (Exception ex) {
                    Platform.runLater(() -> UiHelpers.error(getScene().getWindow(), ex.getMessage()));
                }
            });
        });
    }

    private void doUpdate() {
        if (selectedDoc == null) return;
        String selJson = selectedDoc.toJson(com.kubrik.mex.core.MongoService.JSON_RELAXED);
        org.bson.BsonDocument doc = org.bson.BsonDocument.parse(selJson);
        if (!doc.containsKey("_id")) {
            UiHelpers.error(getScene().getWindow(), "Selected document has no _id");
            return;
        }
        DocumentEditorDialog editor = new DocumentEditorDialog(
                getScene().getWindow(),
                "Edit document",
                "Replace document _id=" + doc.get("_id") + " in "
                        + dbBox.getValue() + "." + collBox.getValue(),
                selJson);
        editor.showAndWait().ifPresent(json -> {
            QueryRunner r = runner();
            String db = dbBox.getValue(), coll = collBox.getValue();
            org.bson.BsonDocument idFilter = new org.bson.BsonDocument("_id", doc.get("_id"));
            statusLabel.setText("Updating…");
            Thread.startVirtualThread(() -> {
                try {
                    r.replaceById(db, coll, idFilter, json);
                    Platform.runLater(this::doFind);
                } catch (Exception ex) {
                    Platform.runLater(() -> UiHelpers.error(getScene().getWindow(), ex.getMessage()));
                }
            });
        });
    }

    private void doDelete() {
        if (selectedDoc == null) return;
        org.bson.BsonDocument doc = org.bson.BsonDocument.parse(
                selectedDoc.toJson(com.kubrik.mex.core.MongoService.JSON_RELAXED));
        if (!doc.containsKey("_id")) { UiHelpers.error(getScene().getWindow(), "No _id"); return; }
        if (!UiHelpers.confirm(getScene().getWindow(), "Delete this document?")) return;
        QueryRunner r = runner();
        String db = dbBox.getValue(), coll = collBox.getValue();
        org.bson.BsonDocument idFilter = new org.bson.BsonDocument("_id", doc.get("_id"));
        statusLabel.setText("Deleting…");
        Thread.startVirtualThread(() -> {
            try {
                r.deleteById(db, coll, idFilter);
                Platform.runLater(this::doFind);
            } catch (Exception ex) {
                Platform.runLater(() -> UiHelpers.error(getScene().getWindow(), ex.getMessage()));
            }
        });
    }

    private void doExport() {
        // Pull current docs from the JSON view (kept in sync by ResultsPane).
        // Easier: rely on the last result's documents via a small accessor on ResultsPane.
        java.util.List<org.bson.Document> docs = results.currentDocuments();
        if (docs.isEmpty()) return;
        FileChooser fc = new FileChooser();
        fc.setInitialFileName("export.json");
        fc.getExtensionFilters().add(new FileChooser.ExtensionFilter("JSON", "*.json"));
        var f = fc.showSaveDialog(getScene().getWindow());
        if (f == null) return;
        try {
            StringBuilder sb = new StringBuilder("[\n");
            for (int i = 0; i < docs.size(); i++) {
                sb.append(docs.get(i).toJson(com.kubrik.mex.core.MongoService.JSON_RELAXED));
                if (i < docs.size() - 1) sb.append(",\n");
            }
            sb.append("\n]\n");
            Files.writeString(Path.of(f.toURI()), sb.toString());
        } catch (Exception ex) {
            UiHelpers.error(getScene().getWindow(), ex.getMessage());
        }
    }

    public void prefill(String db, String coll, String filterJson) {
        if (db != null) dbBox.setValue(db);
        if (coll != null) collBox.setValue(coll);
        if (filterJson != null) filter.setText(filterJson);
    }

    private static TextArea monoArea(String text) {
        TextArea t = new TextArea(text);
        t.setStyle("-fx-font-family: 'Menlo','Monaco',monospace; -fx-font-size: 12px;");
        t.setPrefRowCount(3);
        return t;
    }

    private static int parseInt(String s, int d) { try { return Integer.parseInt(s.trim()); } catch (Exception e) { return d; } }
    private static long parseLong(String s, long d) { try { return Long.parseLong(s.trim()); } catch (Exception e) { return d; } }
}

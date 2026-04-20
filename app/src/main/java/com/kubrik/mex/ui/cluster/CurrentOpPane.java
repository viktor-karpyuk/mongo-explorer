package com.kubrik.mex.ui.cluster;

import com.kubrik.mex.cluster.audit.Outcome;
import com.kubrik.mex.cluster.ops.CurrentOpRow;
import com.kubrik.mex.cluster.service.CurrentOpService;
import com.kubrik.mex.core.ConnectionManager;
import com.kubrik.mex.core.MongoService;
import com.kubrik.mex.ui.JsonCodeArea;
import javafx.animation.KeyFrame;
import javafx.animation.Timeline;
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
import javafx.scene.control.CheckBox;
import javafx.scene.control.ContextMenu;
import javafx.scene.control.Label;
import javafx.scene.control.MenuItem;
import javafx.scene.control.ScrollPane;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextField;
import javafx.scene.control.ToggleButton;
import javafx.scene.control.Tooltip;
import javafx.scene.input.Clipboard;
import javafx.scene.input.ClipboardContent;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import javafx.stage.Modality;
import javafx.stage.Stage;
import javafx.util.Duration;
import org.bson.json.JsonWriterSettings;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * v2.4 OP-1..6 — live currentOp viewer. Polls {@code $currentOp} at 2 s when
 * the pane is attached to a scene; filters (ns regex, op-type chips,
 * secs_running threshold, user, planSummary-contains, pause toggle) apply on a
 * {@link FilteredList} so polling can continue even while the user narrows the
 * view. Row double-click opens the raw document in a modal JSON viewer.
 */
public final class CurrentOpPane extends BorderPane implements AutoCloseable {

    private static final List<String> OP_TYPES =
            List.of("query", "update", "command", "getmore", "insert", "remove", "none");
    private static final Duration POLL_INTERVAL = Duration.seconds(2);

    /**
     * Callback used by the pane to drive the {@code killOp} kebab action. The
     * UI owner ({@code MainView}) supplies an implementation wired to the
     * shared {@link com.kubrik.mex.cluster.service.OpsExecutor}; the pane
     * itself stays decoupled from audit / role / dispatch wiring.
     */
    public interface KillOpHandler {
        /** {@code true} when the connected user holds a role allowing killOp. */
        boolean allowed(String connectionId);
        /** Present the confirm flow + dispatch the kill. */
        KillOpDialog.Result handle(javafx.stage.Window owner, String connectionId, CurrentOpRow row);
    }

    private final String connectionId;
    private final ConnectionManager connManager;
    private final KillOpHandler killHandler;

    private final ObservableList<CurrentOpRow> rows = FXCollections.observableArrayList();
    private final FilteredList<CurrentOpRow> filtered = new FilteredList<>(rows, r -> true);
    private final SortedList<CurrentOpRow> sorted = new SortedList<>(filtered);
    private final TableView<CurrentOpRow> table = new TableView<>(sorted);

    private final TextField nsField = new TextField();
    private final TextField secsField = new TextField();
    private final TextField userField = new TextField();
    private final TextField planField = new TextField();
    private final Set<String> activeOpTypes = new CopyOnWriteArraySet<>(OP_TYPES);
    private final java.util.List<CheckBox> opChips = new java.util.ArrayList<>();
    private final ToggleButton pauseBtn = new ToggleButton("Pause");
    private final Label footer = new Label("— ops total · — visible");
    private final Label errorBar = new Label();

    private final Timeline poller;
    private volatile boolean closed = false;

    public CurrentOpPane(String connectionId, ConnectionManager connManager, KillOpHandler killHandler) {
        this.connectionId = connectionId;
        this.connManager = connManager;
        this.killHandler = killHandler;

        setStyle("-fx-background-color: white;");
        setPadding(new Insets(14, 16, 14, 16));
        setTop(buildFilterBar());
        setCenter(buildTable());
        setBottom(buildFooter());

        sorted.comparatorProperty().bind(table.comparatorProperty());
        installFilterListeners();

        this.poller = new Timeline(new KeyFrame(POLL_INTERVAL, e -> tick()));
        poller.setCycleCount(Timeline.INDEFINITE);
        sceneProperty().addListener((obs, oldScene, newScene) -> {
            if (newScene != null) {
                tick();                // first sample on mount, no wait
                poller.playFromStart();
            } else {
                poller.stop();
            }
        });
    }

    @Override
    public void close() {
        closed = true;
        poller.stop();
    }

    /* ============================== UI ================================= */

    private Region buildFilterBar() {
        nsField.setPromptText("ns regex — e.g. reports\\..*");
        secsField.setPromptText("secs ≥");
        userField.setPromptText("user");
        planField.setPromptText("planSummary contains");
        secsField.setPrefWidth(80);
        userField.setPrefWidth(120);
        planField.setPrefWidth(200);
        nsField.setPrefWidth(200);

        HBox chipBox = new HBox(6);
        for (String t : OP_TYPES) {
            CheckBox cb = new CheckBox(t);
            cb.setSelected(true);
            cb.setStyle("-fx-font-size: 11px;");
            cb.selectedProperty().addListener((obs, o, n) -> {
                if (Boolean.TRUE.equals(n)) activeOpTypes.add(t);
                else activeOpTypes.remove(t);
                reapplyFilter();
            });
            opChips.add(cb);
            chipBox.getChildren().add(cb);
        }
        chipBox.setAlignment(Pos.CENTER_LEFT);

        pauseBtn.setTooltip(new Tooltip("Freeze the view without stopping the background poll"));
        Label reset = new Label("Reset");
        reset.setStyle("-fx-text-fill: #2563eb; -fx-cursor: hand; -fx-underline: true; -fx-font-size: 11px;");
        reset.setOnMouseClicked(e -> resetFilters());

        Region grow = new Region();
        HBox.setHgrow(grow, Priority.ALWAYS);

        HBox row1 = new HBox(8, label("namespace"), nsField, label("secs ≥"), secsField,
                label("user"), userField, label("planSummary"), planField,
                grow, pauseBtn, reset);
        row1.setAlignment(Pos.CENTER_LEFT);

        errorBar.setVisible(false);
        errorBar.managedProperty().bind(errorBar.visibleProperty());
        errorBar.setStyle("-fx-text-fill: #b91c1c; -fx-font-size: 11px;");

        VBox bar = new VBox(6, row1, chipBox, errorBar);
        bar.setPadding(new Insets(0, 0, 10, 0));
        return bar;
    }

    private static Label label(String s) {
        Label l = new Label(s);
        l.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px;");
        return l;
    }

    private Region buildTable() {
        table.setPlaceholder(new Label("No ops running."));
        table.setRowFactory(tv -> {
            var row = new javafx.scene.control.TableRow<CurrentOpRow>();
            row.setOnMouseClicked(e -> {
                if (e.getClickCount() == 2 && !row.isEmpty()) openDetail(row.getItem());
            });
            row.itemProperty().addListener((obs, oldItem, newItem) -> {
                if (newItem == null) { row.setContextMenu(null); return; }
                row.setContextMenu(buildRowMenu(newItem));
            });
            return row;
        });

        TableColumn<CurrentOpRow, Number> opidCol = numCol("opid", 80, CurrentOpRow::opid);
        TableColumn<CurrentOpRow, String> hostCol = textCol("host", 200, CurrentOpRow::host);
        TableColumn<CurrentOpRow, String> nsCol = textCol("ns", 180, CurrentOpRow::ns);
        TableColumn<CurrentOpRow, String> opCol = textCol("op", 90, CurrentOpRow::op);
        TableColumn<CurrentOpRow, Number> secsCol = numCol("secs", 70, CurrentOpRow::secsRunning);
        TableColumn<CurrentOpRow, String> waitingCol = textCol("waiting-for", 120,
                r -> r.waitingForLock() ? (r.waitingForLockReason() == null ? "lock" : r.waitingForLockReason()) : "");
        TableColumn<CurrentOpRow, String> clientCol = textCol("client", 140, CurrentOpRow::client);
        TableColumn<CurrentOpRow, String> commentCol = textCol("comment", 150, CurrentOpRow::comment);
        TableColumn<CurrentOpRow, String> planCol = textCol("planSummary", 200, CurrentOpRow::planSummary);

        table.getColumns().setAll(opidCol, hostCol, nsCol, opCol, secsCol,
                waitingCol, clientCol, commentCol, planCol);
        table.getSortOrder().setAll(secsCol);
        secsCol.setSortType(TableColumn.SortType.DESCENDING);
        VBox.setVgrow(table, Priority.ALWAYS);
        return table;
    }

    private Region buildFooter() {
        footer.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px;");
        HBox box = new HBox(footer);
        box.setPadding(new Insets(6, 0, 0, 0));
        return box;
    }

    /* ============================ polling ============================== */

    private void tick() {
        if (closed || pauseBtn.isSelected()) return;
        MongoService svc = connManager.service(connectionId);
        if (svc == null) return;
        Thread.startVirtualThread(() -> {
            try {
                List<CurrentOpRow> snap = CurrentOpService.sample(svc);
                Platform.runLater(() -> apply(snap, null));
            } catch (Exception ex) {
                Platform.runLater(() -> apply(List.of(), ex.getMessage()));
            }
        });
    }

    private void apply(List<CurrentOpRow> snap, String errorMessage) {
        if (errorMessage != null) {
            errorBar.setText("currentOp failed: " + errorMessage);
            errorBar.setVisible(true);
            return;
        }
        errorBar.setVisible(false);
        rows.setAll(snap);
        updateFooter();
    }

    /* ============================ filters ============================== */

    private void installFilterListeners() {
        nsField.textProperty().addListener((obs, o, n) -> reapplyFilter());
        secsField.textProperty().addListener((obs, o, n) -> reapplyFilter());
        userField.textProperty().addListener((obs, o, n) -> reapplyFilter());
        planField.textProperty().addListener((obs, o, n) -> reapplyFilter());
        filtered.predicateProperty().addListener((obs, o, n) -> updateFooter());
    }

    private void reapplyFilter() {
        String nsPat = nsField.getText() == null ? "" : nsField.getText().trim();
        Pattern compiled;
        try {
            compiled = nsPat.isEmpty() ? null : Pattern.compile(nsPat);
            nsField.setStyle("");
        } catch (PatternSyntaxException bad) {
            compiled = null;
            nsField.setStyle("-fx-border-color: #b91c1c;");
        }
        long minSecs = parseLongOrZero(secsField.getText());
        String user = userField.getText() == null ? "" : userField.getText().trim().toLowerCase();
        String planFrag = planField.getText() == null ? "" : planField.getText().trim().toLowerCase();
        Pattern nsFinal = compiled;
        filtered.setPredicate(r -> {
            if (!activeOpTypes.contains(r.op() == null || r.op().isBlank() ? "none" : r.op())) return false;
            if (r.secsRunning() < minSecs) return false;
            if (!user.isEmpty() && !r.user().toLowerCase().contains(user)) return false;
            if (!planFrag.isEmpty() && !r.planSummary().toLowerCase().contains(planFrag)) return false;
            if (nsFinal != null && !nsFinal.matcher(r.ns()).find()) return false;
            return true;
        });
    }

    private void resetFilters() {
        nsField.clear();
        secsField.clear();
        userField.clear();
        planField.clear();
        for (CheckBox cb : opChips) cb.setSelected(true);
        activeOpTypes.addAll(OP_TYPES);
        pauseBtn.setSelected(false);
        reapplyFilter();
    }

    private void updateFooter() {
        footer.setText(rows.size() + " ops total · " + filtered.size() + " visible");
    }

    /* ============================ row actions ========================== */

    private ContextMenu buildRowMenu(CurrentOpRow row) {
        ContextMenu m = new ContextMenu();
        MenuItem copyOpid = new MenuItem("Copy opid");
        copyOpid.setOnAction(e -> copy(String.valueOf(row.opid())));
        MenuItem copyKillOp = new MenuItem("Copy as killOp command");
        copyKillOp.setOnAction(e -> copy("db.adminCommand({ killOp: 1, op: " + row.opid() + " })"));
        MenuItem kill = new MenuItem("Kill op");
        boolean canKill = killHandler != null && killHandler.allowed(connectionId);
        if (!canKill) {
            kill.setDisable(true);
            Tooltip tip = new Tooltip("Requires role: killAnyCursor or root.");
            kill.setGraphic(new Label(""));  // required for Tooltip on MenuItem in some versions
            kill.textProperty().set("Kill op  (role-gated)");
            Tooltip.install(kill.getGraphic(), tip);
        }
        kill.setOnAction(e -> {
            if (killHandler == null) return;
            javafx.stage.Window win = getScene() == null ? null : getScene().getWindow();
            KillOpDialog.Result r = killHandler.handle(win, connectionId, row);
            if (r != null && r.outcome() == Outcome.OK) {
                tick();  // refresh immediately so the row disappears from the table
            }
        });
        m.getItems().addAll(copyOpid, copyKillOp, kill);
        return m;
    }

    private static void copy(String text) {
        ClipboardContent c = new ClipboardContent();
        c.putString(text);
        Clipboard.getSystemClipboard().setContent(c);
    }

    /* ============================ detail drawer ======================== */

    private void openDetail(CurrentOpRow row) {
        String json = row.raw().toJson(JsonWriterSettings.builder()
                .indent(true).build());
        JsonCodeArea area = new JsonCodeArea(json);
        area.setEditable(false);
        ScrollPane scroll = new ScrollPane(area);
        scroll.setFitToWidth(true);
        BorderPane body = new BorderPane(scroll);
        Label title = new Label("opid " + row.opid() + " · " + row.ns());
        title.setStyle("-fx-font-weight: 700; -fx-font-size: 14px; -fx-padding: 14 16 6 16;");
        Label meta = new Label(row.op() + " · " + row.secsRunning() + " s · " + row.host());
        meta.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px; -fx-padding: 0 16 10 16;");
        body.setTop(new VBox(title, meta));
        Scene scene = new Scene(body, 720, 540);
        Stage stage = new Stage();
        stage.setTitle("Current Op · " + row.opid());
        stage.initModality(Modality.NONE);
        stage.setScene(scene);
        stage.show();
    }

    /* ============================ helpers ============================== */

    private static <T> TableColumn<CurrentOpRow, String> textCol(String title, int width,
                                                                  java.util.function.Function<CurrentOpRow, String> getter) {
        TableColumn<CurrentOpRow, String> c = new TableColumn<>(title);
        c.setPrefWidth(width);
        c.setCellValueFactory(cd -> new SimpleStringProperty(String.valueOf(getter.apply(cd.getValue()))));
        return c;
    }

    private static TableColumn<CurrentOpRow, Number> numCol(String title, int width,
                                                            java.util.function.ToLongFunction<CurrentOpRow> getter) {
        TableColumn<CurrentOpRow, Number> c = new TableColumn<>(title);
        c.setPrefWidth(width);
        c.setCellValueFactory(cd ->
                new SimpleObjectProperty<>(getter.applyAsLong(cd.getValue())));
        return c;
    }

    private static long parseLongOrZero(String s) {
        if (s == null || s.isBlank()) return 0;
        try { return Long.parseLong(s.trim()); }
        catch (NumberFormatException e) { return 0; }
    }
}

package com.kubrik.mex.ui.cluster;

import com.kubrik.mex.cluster.ops.OplogEntry;
import com.kubrik.mex.cluster.ops.OplogGaugeStats;
import com.kubrik.mex.cluster.service.OplogService;
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
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.CheckBox;
import javafx.scene.control.Label;
import javafx.scene.control.ScrollPane;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextField;
import javafx.scene.control.ToggleButton;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import javafx.stage.FileChooser;
import javafx.stage.Stage;
import javafx.util.Duration;
import org.bson.json.JsonWriterSettings;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * v2.4 OPLOG-1..9 — oplog gauge + tail viewer.
 *
 * <p>The gauge card shows size / used / window-hours with the three colour
 * bands (&lt; 6 h red, &lt; 24 h amber, ≥ 24 h green). The tail table pulls
 * the last N entries (default 200, cap 2 000) filtered by namespace regex +
 * op-type chips; Pause freezes updates but keeps the schedule running, and a
 * JSON Lines export dumps the current selection (or all rows when none
 * selected) to disk via a streamed writer.</p>
 */
public final class OplogPane extends BorderPane implements AutoCloseable {

    private static final List<String> OP_TYPES = List.of("i", "u", "d", "c", "n");
    private static final Duration GAUGE_INTERVAL = Duration.seconds(10);
    private static final Duration TAIL_INTERVAL  = Duration.seconds(2);

    private final String connectionId;
    private final ConnectionManager connManager;

    private final Label gaugeTitle = new Label("Oplog window");
    private final Label gaugeSub = new Label("resolving…");
    private final Label gaugeBadge = new Label("…");
    private final Label gaugeDetail = new Label("");

    private final ObservableList<OplogEntry> entries = FXCollections.observableArrayList();
    private final TableView<OplogEntry> table = new TableView<>(entries);
    private final TextField nsField = new TextField();
    private final Set<String> activeOps = new CopyOnWriteArraySet<>(OP_TYPES);
    private final List<CheckBox> opChips = new ArrayList<>();
    private final ToggleButton pauseBtn = new ToggleButton("Pause");
    private final Label footer = new Label("—");

    private final Timeline gaugePoller;
    private final Timeline tailPoller;
    private volatile boolean closed = false;

    public OplogPane(String connectionId, ConnectionManager connManager) {
        this.connectionId = connectionId;
        this.connManager = connManager;

        setStyle("-fx-background-color: white;");
        setPadding(new Insets(14, 16, 14, 16));

        setTop(new VBox(12, buildGaugeCard(), buildFilterBar()));
        setCenter(buildTable());
        setBottom(buildFooter());

        gaugePoller = new Timeline(new KeyFrame(GAUGE_INTERVAL, e -> tickGauge()));
        gaugePoller.setCycleCount(Timeline.INDEFINITE);
        tailPoller  = new Timeline(new KeyFrame(TAIL_INTERVAL,  e -> tickTail()));
        tailPoller.setCycleCount(Timeline.INDEFINITE);

        sceneProperty().addListener((obs, o, n) -> {
            if (n != null) {
                tickGauge();
                tickTail();
                gaugePoller.playFromStart();
                tailPoller.playFromStart();
            } else {
                gaugePoller.stop();
                tailPoller.stop();
            }
        });
    }

    @Override
    public void close() {
        closed = true;
        gaugePoller.stop();
        tailPoller.stop();
    }

    /* ============================== gauge ============================== */

    private Region buildGaugeCard() {
        gaugeTitle.setStyle("-fx-font-size: 13px; -fx-font-weight: 700; -fx-text-fill: #1f2937;");
        gaugeSub.setStyle("-fx-font-size: 12px; -fx-text-fill: #6b7280;");
        gaugeDetail.setStyle("-fx-font-size: 11px; -fx-text-fill: #6b7280;");
        gaugeBadge.setStyle(badgeStyle("amber"));
        Region grow = new Region();
        HBox.setHgrow(grow, Priority.ALWAYS);
        HBox head = new HBox(10, gaugeTitle, grow, gaugeBadge);
        head.setAlignment(Pos.CENTER_LEFT);
        VBox card = new VBox(4, head, gaugeSub, gaugeDetail);
        card.setPadding(new Insets(12, 14, 12, 14));
        card.setStyle("-fx-background-color: #f9fafb; -fx-background-radius: 8; "
                + "-fx-border-color: #e5e7eb; -fx-border-radius: 8; -fx-border-width: 1;");
        return card;
    }

    private void tickGauge() {
        if (closed) return;
        MongoService svc = connManager.service(connectionId);
        if (svc == null) return;
        Thread.startVirtualThread(() -> {
            OplogGaugeStats stats = OplogService.sampleGauge(svc);
            Platform.runLater(() -> renderGauge(stats));
        });
    }

    private void renderGauge(OplogGaugeStats s) {
        if (!s.supported()) {
            gaugeSub.setText("Oplog unavailable — " + s.errorMessage());
            gaugeDetail.setText("");
            gaugeBadge.setText("n/a");
            gaugeBadge.setStyle(badgeStyle("neutral"));
            return;
        }
        gaugeSub.setText("First entry: " + formatTs(s.firstTsSec())
                + "  ·  Last entry: " + formatTs(s.lastTsSec()));
        gaugeDetail.setText("Size: " + formatBytes(s.sizeBytes())
                + "  ·  Used: " + formatBytes(s.usedBytes())
                + "  (" + String.format("%.0f %%", s.usageRatio() * 100) + ")");
        gaugeBadge.setText(formatDuration(s.windowHours()));
        gaugeBadge.setStyle(badgeStyle(s.band()));
    }

    /* ============================ filter bar =========================== */

    private Region buildFilterBar() {
        nsField.setPromptText("ns regex — e.g. reports\\..*");
        nsField.setPrefWidth(220);
        HBox chipBox = new HBox(6);
        for (String op : OP_TYPES) {
            CheckBox cb = new CheckBox(op);
            cb.setSelected(true);
            cb.setStyle("-fx-font-size: 11px;");
            cb.selectedProperty().addListener((o, a, b) -> {
                if (Boolean.TRUE.equals(b)) activeOps.add(op);
                else activeOps.remove(op);
                tickTail();
            });
            opChips.add(cb);
            chipBox.getChildren().add(cb);
        }
        chipBox.setAlignment(Pos.CENTER_LEFT);
        pauseBtn.setTooltip(new javafx.scene.control.Tooltip("Freeze the view without stopping the poll"));

        Button exportBtn = new Button("Export…");
        exportBtn.setOnAction(e -> exportSelected());
        Button resetBtn = new Button("Reset filters");
        resetBtn.setOnAction(e -> {
            nsField.clear();
            for (CheckBox cb : opChips) cb.setSelected(true);
            activeOps.addAll(OP_TYPES);
            pauseBtn.setSelected(false);
            tickTail();
        });

        Region grow = new Region();
        HBox.setHgrow(grow, Priority.ALWAYS);
        HBox row = new HBox(10, small("namespace"), nsField, chipBox, grow,
                pauseBtn, resetBtn, exportBtn);
        row.setAlignment(Pos.CENTER_LEFT);
        row.setPadding(new Insets(4, 0, 8, 0));
        nsField.textProperty().addListener((o, a, b) -> tickTail());
        return row;
    }

    /* ============================== table =============================== */

    private Region buildTable() {
        table.setPlaceholder(new Label(
                "No oplog entries match this filter. Loosen the namespace regex or op-type chips above."));
        table.getSelectionModel().setSelectionMode(
                javafx.scene.control.SelectionMode.MULTIPLE);
        TableColumn<OplogEntry, String> tsCol = textCol("ts", 180,
                e -> formatTs(e.tsSec()));
        TableColumn<OplogEntry, String> opCol = textCol("op", 70, OplogEntry::opLabel);
        TableColumn<OplogEntry, String> nsCol = textCol("ns", 220, OplogEntry::ns);
        TableColumn<OplogEntry, String> previewCol = textCol("preview", 480, OplogEntry::preview);
        table.getColumns().setAll(tsCol, opCol, nsCol, previewCol);
        table.setRowFactory(tv -> {
            var row = new javafx.scene.control.TableRow<OplogEntry>();
            row.setOnMouseClicked(e -> {
                if (e.getClickCount() == 2 && !row.isEmpty()) openDetail(row.getItem());
            });
            return row;
        });
        VBox.setVgrow(table, Priority.ALWAYS);
        return table;
    }

    private Region buildFooter() {
        footer.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px;");
        HBox b = new HBox(footer);
        b.setPadding(new Insets(6, 0, 0, 0));
        return b;
    }

    private void tickTail() {
        if (closed || pauseBtn.isSelected()) return;
        MongoService svc = connManager.service(connectionId);
        if (svc == null) return;
        String ns = nsField.getText() == null ? "" : nsField.getText().trim();
        List<String> ops = new ArrayList<>(activeOps);
        Thread.startVirtualThread(() -> {
            List<OplogEntry> snap = OplogService.tail(svc, 200, ns, ops);
            Platform.runLater(() -> {
                entries.setAll(snap);
                footer.setText(entries.size() + " entries  ·  newest first");
            });
        });
    }

    private void openDetail(OplogEntry row) {
        String json = row.raw().toJson(JsonWriterSettings.builder().indent(true).build());
        JsonCodeArea area = new JsonCodeArea(json);
        area.setEditable(false);
        ScrollPane scroll = new ScrollPane(area);
        scroll.setFitToWidth(true);
        BorderPane body = new BorderPane(scroll);
        Label title = new Label("oplog · " + row.opLabel() + " · " + row.ns());
        title.setStyle("-fx-font-weight: 700; -fx-font-size: 14px; -fx-padding: 14 16 6 16;");
        Label meta = new Label(formatTs(row.tsSec()));
        meta.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px; -fx-padding: 0 16 10 16;");
        body.setTop(new VBox(title, meta));
        Scene scene = new Scene(body, 720, 520);
        Stage stage = new Stage();
        stage.setTitle("Oplog entry · " + formatTs(row.tsSec()));
        stage.setScene(scene);
        stage.show();
    }

    /* ============================== export ============================= */

    private void exportSelected() {
        List<OplogEntry> rows = table.getSelectionModel().getSelectedItems().isEmpty()
                ? new ArrayList<>(entries) : new ArrayList<>(table.getSelectionModel().getSelectedItems());
        if (rows.isEmpty()) return;
        FileChooser fc = new FileChooser();
        fc.setTitle("Export oplog entries");
        fc.setInitialFileName("oplog-" + Instant.now().getEpochSecond() + ".jsonl");
        fc.getExtensionFilters().add(new FileChooser.ExtensionFilter("JSON Lines", "*.jsonl"));
        java.io.File out = fc.showSaveDialog(getScene() == null ? null : getScene().getWindow());
        if (out == null) return;
        // Stream one line per entry so > 10 000 rows don't balloon the heap.
        Thread.startVirtualThread(() -> {
            try (BufferedWriter w = Files.newBufferedWriter(out.toPath(), StandardCharsets.UTF_8)) {
                for (OplogEntry e : rows) {
                    w.write(e.raw().toJson());
                    w.newLine();
                }
            } catch (Exception ex) {
                Platform.runLater(() -> footer.setText(
                        "Export failed: " + ex.getClass().getSimpleName() + " — " + ex.getMessage()));
            }
        });
    }

    /* ============================== helpers ============================ */

    private static <T> TableColumn<OplogEntry, String> textCol(String title, int width,
                                                               java.util.function.Function<OplogEntry, String> getter) {
        TableColumn<OplogEntry, String> c = new TableColumn<>(title);
        c.setPrefWidth(width);
        c.setCellValueFactory(cd -> new SimpleStringProperty(getter.apply(cd.getValue())));
        return c;
    }

    private static Label small(String s) {
        Label l = new Label(s);
        l.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px;");
        return l;
    }

    static String formatTs(long epochSec) {
        if (epochSec <= 0) return "—";
        return DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochSecond(epochSec));
    }

    static String formatDuration(double hours) {
        if (hours <= 0) return "—";
        if (hours >= 24) return String.format("%d d %d h", (long) (hours / 24), ((long) hours) % 24);
        long h = (long) hours;
        long m = (long) ((hours - h) * 60);
        return h + " h " + m + " m";
    }

    static String formatBytes(long b) {
        double gb = b / (1024.0 * 1024 * 1024);
        if (gb >= 1) return String.format("%.1f GB", gb);
        double mb = b / (1024.0 * 1024);
        return String.format("%.1f MB", mb);
    }

    private static String badgeStyle(String band) {
        String bg, fg;
        switch (band) {
            case "green" -> { bg = "#dcfce7"; fg = "#166534"; }
            case "amber" -> { bg = "#fef3c7"; fg = "#92400e"; }
            case "red"   -> { bg = "#fee2e2"; fg = "#991b1b"; }
            default      -> { bg = "#f3f4f6"; fg = "#374151"; }
        }
        return "-fx-background-color: " + bg + "; -fx-text-fill: " + fg + "; "
                + "-fx-font-size: 11px; -fx-font-weight: 700; "
                + "-fx-padding: 2 10 2 10; -fx-background-radius: 999;";
    }

    /**
     * Suppresses "unused T" warnings from the helper generics method — the
     * compiler doesn't infer the column type parameter from the lambda alone
     * in all configs; explicit placeholder keeps the file compiling on older
     * toolchains.
     */
    @SuppressWarnings("unused")
    private static void typeParameterAnchor() {}
}

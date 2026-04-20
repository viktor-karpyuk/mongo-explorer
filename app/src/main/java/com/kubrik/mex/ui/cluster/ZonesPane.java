package com.kubrik.mex.ui.cluster;

import com.kubrik.mex.cluster.audit.Outcome;
import com.kubrik.mex.cluster.ops.TagRange;
import com.kubrik.mex.cluster.service.OpsExecutor;
import com.kubrik.mex.cluster.service.ZonesService;
import com.kubrik.mex.core.ConnectionManager;
import com.kubrik.mex.core.MongoService;
import javafx.animation.KeyFrame;
import javafx.animation.Timeline;
import javafx.application.Platform;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.ScrollPane;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.Tooltip;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.FlowPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import javafx.util.Duration;

import java.util.List;
import java.util.Map;

/**
 * v2.4 SHARD-14..16 — zones + tag ranges pane. Shows the shard → zone
 * mapping as a compact chip grid, the list of tag ranges in a table, and
 * exposes Add / Remove buttons that dispatch through {@link OpsExecutor}
 * (updateZoneKeyRange under the hood). Refreshes every 10 s while visible.
 */
public final class ZonesPane extends BorderPane implements AutoCloseable {

    private static final Duration POLL_INTERVAL = Duration.seconds(10);

    /** Callbacks for dispatching destructive edits. Mirrors the balancer
     *  handler: role gate + caller identity, dispatch routed through
     *  OpsExecutor so the three safety gates apply. */
    public interface ZonesHandler {
        boolean allowed(String connectionId);
        String callerUser();
        String callerHost();
    }

    private final String connectionId;
    private final ConnectionManager connManager;
    private final OpsExecutor executor;
    private final ZonesHandler handler;

    private final Label headline = new Label("Zones + tag ranges");
    private final Label note = new Label("");
    private final FlowPane shardZones = new FlowPane(6, 4);
    private final ObservableList<TagRange> rows = FXCollections.observableArrayList();
    private final TableView<TagRange> table = new TableView<>(rows);
    private final Button addBtn = new Button("Add tag range…");
    private final Button removeBtn = new Button("Remove tag range…");

    private final Timeline poller;
    private volatile boolean closed = false;

    public ZonesPane(String connectionId, ConnectionManager connManager,
                     OpsExecutor executor, ZonesHandler handler) {
        this.connectionId = connectionId;
        this.connManager = connManager;
        this.executor = executor;
        this.handler = handler;

        setStyle("-fx-background-color: white;");
        setPadding(new Insets(14, 16, 14, 16));
        headline.setStyle("-fx-font-weight: 700; -fx-font-size: 13px; -fx-text-fill: #374151;");
        note.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px;");

        VBox top = new VBox(8,
                headline,
                small("Shard → zones"),
                shardZones,
                buildActionRow(),
                note);
        setTop(top);
        setCenter(buildTable());

        poller = new Timeline(new KeyFrame(POLL_INTERVAL, e -> tick()));
        poller.setCycleCount(Timeline.INDEFINITE);
        sceneProperty().addListener((obs, o, n) -> {
            if (n != null) { tick(); poller.playFromStart(); }
            else { poller.stop(); }
        });
    }

    @Override
    public void close() {
        closed = true;
        poller.stop();
    }

    /* ============================== UI ================================ */

    private Region buildActionRow() {
        addBtn.setDisable(true);
        removeBtn.setDisable(true);
        addBtn.setOnAction(e -> openAdd());
        removeBtn.setOnAction(e -> openRemove());
        HBox row = new HBox(8, addBtn, removeBtn);
        row.setAlignment(Pos.CENTER_LEFT);
        row.setPadding(new Insets(6, 0, 0, 0));
        return row;
    }

    private Region buildTable() {
        table.setPlaceholder(new Label(
                "No tag ranges defined. Use \"Add tag range…\" above to constrain documents to a zone."));
        table.getColumns().setAll(
                textCol("namespace", 200, TagRange::ns),
                textCol("zone", 120, TagRange::tag),
                textCol("min", 240, TagRange::minJson),
                textCol("max", 240, TagRange::maxJson));
        VBox.setVgrow(table, Priority.ALWAYS);
        ScrollPane scroll = new ScrollPane(table);
        scroll.setFitToWidth(true);
        scroll.setFitToHeight(true);
        scroll.setStyle("-fx-background-color: transparent; -fx-background: transparent;");
        return scroll;
    }

    private void tick() {
        if (closed) return;
        MongoService svc = connManager.service(connectionId);
        if (svc == null) return;
        Thread.startVirtualThread(() -> {
            List<TagRange> ranges = ZonesService.tagRanges(svc);
            Map<String, List<String>> perShard = ZonesService.zonesPerShard(svc);
            Platform.runLater(() -> apply(ranges, perShard));
        });
    }

    private void apply(List<TagRange> ranges, Map<String, List<String>> perShard) {
        rows.setAll(ranges);
        shardZones.getChildren().clear();
        for (var e : perShard.entrySet()) {
            Label chip = new Label(e.getKey() + (e.getValue().isEmpty() ? "" : " · " + String.join(", ", e.getValue())));
            chip.setStyle(chipStyle(!e.getValue().isEmpty()));
            shardZones.getChildren().add(chip);
        }
        boolean role = handler != null && handler.allowed(connectionId);
        addBtn.setDisable(!role);
        removeBtn.setDisable(!role || table.getSelectionModel().getSelectedItem() == null);
        table.getSelectionModel().selectedItemProperty().addListener((obs, o, n) ->
                removeBtn.setDisable(!role || n == null));
        if (!role) {
            Tooltip.install(addBtn, new Tooltip("Requires clusterManager or root."));
            Tooltip.install(removeBtn, new Tooltip("Requires clusterManager or root."));
        }
    }

    /* ============================ actions ============================= */

    private void openAdd() {
        if (executor == null || handler == null) return;
        KillOpDialog.Result r = TagRangeDialog.showAdd(
                getScene() == null ? null : getScene().getWindow(),
                connectionId, executor, handler.callerUser(), handler.callerHost());
        note.setText(r.outcome() == Outcome.OK
                ? "Tag range added."
                : "Dispatch " + r.outcome() + (r.message() == null ? "" : ": " + r.message()));
        tick();
    }

    private void openRemove() {
        if (executor == null || handler == null) return;
        TagRange sel = table.getSelectionModel().getSelectedItem();
        if (sel == null) return;
        KillOpDialog.Result r = TagRangeDialog.showRemove(
                getScene() == null ? null : getScene().getWindow(),
                connectionId, sel, executor, handler.callerUser(), handler.callerHost());
        note.setText(r.outcome() == Outcome.OK
                ? "Tag range removed."
                : "Dispatch " + r.outcome() + (r.message() == null ? "" : ": " + r.message()));
        tick();
    }

    /* =========================== helpers ============================== */

    private static TableColumn<TagRange, String> textCol(String title, int width,
                                                          java.util.function.Function<TagRange, String> getter) {
        TableColumn<TagRange, String> c = new TableColumn<>(title);
        c.setPrefWidth(width);
        c.setCellValueFactory(cd -> new SimpleStringProperty(getter.apply(cd.getValue())));
        return c;
    }

    private static Label small(String s) {
        Label l = new Label(s);
        l.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px;");
        return l;
    }

    private static String chipStyle(boolean hasZone) {
        String bg = hasZone ? "#eef2ff" : "#f3f4f6";
        String fg = hasZone ? "#3730a3" : "#6b7280";
        return "-fx-background-color: " + bg + "; -fx-text-fill: " + fg + "; "
                + "-fx-font-size: 11px; -fx-padding: 2 8 2 8; -fx-background-radius: 10;";
    }
}

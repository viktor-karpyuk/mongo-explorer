package com.kubrik.mex.ui.cluster;

import com.kubrik.mex.cluster.ops.ChunkSummary;
import com.kubrik.mex.cluster.service.ChunkService;
import com.kubrik.mex.core.ConnectionManager;
import com.kubrik.mex.core.MongoService;
import javafx.animation.KeyFrame;
import javafx.animation.Timeline;
import javafx.application.Platform;
import javafx.beans.property.SimpleObjectProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Label;
import javafx.scene.control.TableCell;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import javafx.util.Duration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * v2.4 SHARD-10..13 — per-collection chunk distribution. Each row shows the
 * collection, total chunks, a compact shard → count summary, and a jumbo
 * indicator when any chunk in the collection is jumbo-flagged.
 *
 * <p>Refreshes every 15 s while visible — chunk counts change slowly and
 * {@code config.chunks} can be large on heavily-sharded clusters, so a lower
 * cadence is friendlier than the 2–5 s polls used by live-ops panes.</p>
 */
public final class ChunkDistributionPane extends BorderPane implements AutoCloseable {

    private static final Duration POLL_INTERVAL = Duration.seconds(15);

    private final String connectionId;
    private final ConnectionManager connManager;

    private final ObservableList<ChunkSummary> rows = FXCollections.observableArrayList();
    private final TableView<ChunkSummary> table = new TableView<>(rows);
    private final Label headline = new Label("Chunk distribution");
    private final Label footer = new Label("—");

    private final Timeline poller;
    private volatile boolean closed = false;

    public ChunkDistributionPane(String connectionId, ConnectionManager connManager) {
        this.connectionId = connectionId;
        this.connManager = connManager;

        setStyle("-fx-background-color: white;");
        setPadding(new Insets(14, 16, 14, 16));
        headline.setStyle("-fx-font-weight: 700; -fx-font-size: 13px; -fx-text-fill: #374151;");
        footer.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px;");

        setTop(new VBox(6, headline));
        setCenter(buildTable());
        HBox foot = new HBox(footer);
        foot.setPadding(new Insets(6, 0, 0, 0));
        setBottom(foot);

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

    /* ============================ polling ============================= */

    private void tick() {
        if (closed) return;
        MongoService svc = connManager.service(connectionId);
        if (svc == null) return;
        Thread.startVirtualThread(() -> {
            List<ChunkSummary> snap = ChunkService.distribution(svc);
            Platform.runLater(() -> apply(snap));
        });
    }

    private void apply(List<ChunkSummary> snap) {
        rows.setAll(snap);
        long total = snap.stream().mapToLong(ChunkSummary::totalChunks).sum();
        long jumbo = snap.stream().mapToLong(ChunkSummary::jumboChunks).sum();
        footer.setText(snap.size() + " collections  ·  " + total + " chunks"
                + (jumbo > 0 ? "  ·  " + jumbo + " jumbo" : ""));
    }

    /* ============================= table ============================== */

    private Region buildTable() {
        table.setPlaceholder(new Label(
                "No sharded collections reported. Run sh.shardCollection(\"db.coll\", ...) on one to see it here."));

        TableColumn<ChunkSummary, String> nsCol = new TableColumn<>("namespace");
        nsCol.setPrefWidth(260);
        nsCol.setCellValueFactory(cd -> new SimpleStringProperty(cd.getValue().ns()));

        TableColumn<ChunkSummary, Number> totalCol = new TableColumn<>("chunks");
        totalCol.setPrefWidth(80);
        totalCol.setCellValueFactory(cd -> new SimpleObjectProperty<>(cd.getValue().totalChunks()));
        totalCol.setCellFactory(col -> alignedRightCell());

        TableColumn<ChunkSummary, Number> jumboCol = new TableColumn<>("jumbo");
        jumboCol.setPrefWidth(70);
        jumboCol.setCellValueFactory(cd -> new SimpleObjectProperty<>(cd.getValue().jumboChunks()));
        jumboCol.setCellFactory(col -> new TableCell<>() {
            @Override protected void updateItem(Number n, boolean empty) {
                super.updateItem(n, empty);
                if (empty || n == null || n.longValue() == 0) {
                    setText(""); setStyle("");
                } else {
                    setText("▲ " + n.longValue());
                    setStyle("-fx-text-fill: #b45309; -fx-font-weight: 700;");
                    setAlignment(Pos.CENTER_LEFT);
                }
            }
        });

        TableColumn<ChunkSummary, String> distCol = new TableColumn<>("per shard");
        distCol.setPrefWidth(440);
        distCol.setCellValueFactory(cd -> new SimpleStringProperty(histogram(cd.getValue())));

        table.getColumns().setAll(nsCol, totalCol, jumboCol, distCol);
        VBox.setVgrow(table, Priority.ALWAYS);
        return table;
    }

    private static String histogram(ChunkSummary s) {
        long max = s.perShard().values().stream().mapToLong(Long::longValue).max().orElse(1);
        StringBuilder sb = new StringBuilder();
        for (String shard : new TreeSet<>(s.perShard().keySet())) {
            long count = s.perShard().getOrDefault(shard, 0L);
            int bars = (int) Math.round(10.0 * count / max);
            sb.append(shard).append(' ')
                    .append("▇".repeat(Math.max(0, bars)))
                    .append("  ").append(count).append("   ");
        }
        return sb.toString();
    }

    private static TableCell<ChunkSummary, Number> alignedRightCell() {
        TableCell<ChunkSummary, Number> cell = new TableCell<>() {
            @Override protected void updateItem(Number n, boolean empty) {
                super.updateItem(n, empty);
                setText(empty || n == null ? "" : String.valueOf(n.longValue()));
            }
        };
        cell.setAlignment(Pos.CENTER_RIGHT);
        return cell;
    }
}

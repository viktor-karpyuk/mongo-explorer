package com.kubrik.mex.ui.cluster;

import com.kubrik.mex.cluster.ops.ConnPoolStats;
import com.kubrik.mex.cluster.service.ConnPoolStatsService;
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
import javafx.scene.control.TableRow;
import javafx.scene.control.TableView;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.VBox;
import javafx.util.Duration;

import java.util.function.Function;
import java.util.function.ToIntFunction;

/**
 * v2.4 POOL-1..5 — connection-pool viewer. Polls {@code connPoolStats} every
 * 10 s while attached to a scene. Each row shows pool size / inUse / available
 * / waitQueue / timeouts / last-refreshed; rows with {@code waitQueueSize > 0}
 * render with an amber background so hotspots are immediately visible.
 */
public final class ConnPoolPane extends BorderPane implements AutoCloseable {

    private static final Duration POLL_INTERVAL = Duration.seconds(10);

    private final String connectionId;
    private final ConnectionManager connManager;

    private final ObservableList<ConnPoolStats.Row> rows = FXCollections.observableArrayList();
    private final TableView<ConnPoolStats.Row> table = new TableView<>(rows);
    private final Label footer = new Label("—");
    private final Label headline = new Label("Resolving pool stats…");

    private final Timeline poller;
    private volatile boolean closed = false;

    public ConnPoolPane(String connectionId, ConnectionManager connManager) {
        this.connectionId = connectionId;
        this.connManager = connManager;

        setStyle("-fx-background-color: white;");
        setPadding(new Insets(14, 16, 14, 16));

        headline.setStyle("-fx-font-weight: 700; -fx-font-size: 13px; -fx-text-fill: #374151;");
        setTop(new VBox(6, headline));
        setCenter(buildTable());

        footer.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px;");
        HBox footerBox = new HBox(footer);
        footerBox.setPadding(new Insets(6, 0, 0, 0));
        setBottom(footerBox);

        this.poller = new Timeline(new KeyFrame(POLL_INTERVAL, e -> tick()));
        poller.setCycleCount(Timeline.INDEFINITE);
        sceneProperty().addListener((obs, o, n) -> {
            if (n != null) {
                tick();
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

    private TableView<ConnPoolStats.Row> buildTable() {
        table.setPlaceholder(new Label("No pool activity yet."));
        table.getColumns().setAll(
                textCol("host", 240, ConnPoolStats.Row::host),
                numCol("pool", 70, ConnPoolStats.Row::poolSize),
                numCol("inUse", 70, ConnPoolStats.Row::inUse),
                numCol("avail", 70, ConnPoolStats.Row::available),
                numCol("wait", 70, ConnPoolStats.Row::waitQueueSize),
                numLongCol("timeouts", 80, ConnPoolStats.Row::timeouts),
                textCol("refreshed", 110, r -> formatRefreshed(r.lastRefreshedMs()))
        );
        table.setRowFactory(tv -> new TableRow<>() {
            @Override
            protected void updateItem(ConnPoolStats.Row row, boolean empty) {
                super.updateItem(row, empty);
                if (empty || row == null) {
                    setStyle("");
                    return;
                }
                if (row.waitQueueSize() > 0) {
                    setStyle("-fx-background-color: #fef3c7;");
                } else {
                    setStyle("");
                }
            }
        });
        VBox.setVgrow(table, Priority.ALWAYS);
        return table;
    }

    private void tick() {
        if (closed) return;
        MongoService svc = connManager.service(connectionId);
        if (svc == null) return;
        Thread.startVirtualThread(() -> {
            ConnPoolStats stats = ConnPoolStatsService.sample(svc);
            Platform.runLater(() -> apply(stats));
        });
    }

    private void apply(ConnPoolStats stats) {
        rows.setAll(stats.rows());
        headline.setText("Connection pools  ·  "
                + stats.rows().size() + " hosts");
        int waiting = stats.rows().stream().mapToInt(ConnPoolStats.Row::waitQueueSize).sum();
        footer.setText("totals — " + stats.totalInUse() + " in use · "
                + stats.totalAvailable() + " available · "
                + stats.totalCreated() + " created"
                + (waiting > 0 ? "  ▲  " + waiting + " queued" : ""));
    }

    /* =========================== helpers ============================== */

    private static TableColumn<ConnPoolStats.Row, String> textCol(
            String title, int width, Function<ConnPoolStats.Row, String> getter) {
        TableColumn<ConnPoolStats.Row, String> c = new TableColumn<>(title);
        c.setPrefWidth(width);
        c.setCellValueFactory(cd -> new SimpleStringProperty(getter.apply(cd.getValue())));
        return c;
    }

    private static TableColumn<ConnPoolStats.Row, Number> numCol(
            String title, int width, ToIntFunction<ConnPoolStats.Row> getter) {
        TableColumn<ConnPoolStats.Row, Number> c = new TableColumn<>(title);
        c.setPrefWidth(width);
        c.setCellValueFactory(cd -> new SimpleObjectProperty<>(getter.applyAsInt(cd.getValue())));
        c.setCellFactory(col -> alignedRightCell());
        return c;
    }

    private static TableColumn<ConnPoolStats.Row, Number> numLongCol(
            String title, int width, java.util.function.ToLongFunction<ConnPoolStats.Row> getter) {
        TableColumn<ConnPoolStats.Row, Number> c = new TableColumn<>(title);
        c.setPrefWidth(width);
        c.setCellValueFactory(cd -> new SimpleObjectProperty<>(getter.applyAsLong(cd.getValue())));
        c.setCellFactory(col -> alignedRightCell());
        return c;
    }

    private static TableCell<ConnPoolStats.Row, Number> alignedRightCell() {
        TableCell<ConnPoolStats.Row, Number> cell = new TableCell<>() {
            @Override protected void updateItem(Number n, boolean empty) {
                super.updateItem(n, empty);
                setText(empty || n == null ? "" : String.valueOf(n.longValue()));
            }
        };
        cell.setAlignment(Pos.CENTER_RIGHT);
        return cell;
    }

    private static String formatRefreshed(Long ms) {
        if (ms == null) return "—";
        long diff = System.currentTimeMillis() - ms;
        if (diff < 60_000) return (diff / 1000) + "s ago";
        if (diff < 3_600_000) return (diff / 60_000) + "m ago";
        return (diff / 3_600_000) + "h ago";
    }
}

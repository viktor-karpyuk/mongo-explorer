package com.kubrik.mex.ui.monitoring;

import com.kubrik.mex.core.ConnectionManager;
import com.kubrik.mex.core.MongoService;
import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.monitoring.model.MetricId;
import com.kubrik.mex.monitoring.model.MetricSample;
import javafx.application.Platform;
import javafx.beans.property.ReadOnlyObjectWrapper;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.control.Label;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.layout.FlowPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import org.bson.Document;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Sharding panel (M-2 / SECTION-SHARD-*). Shows:
 *
 * <ul>
 *   <li>Balancer chip (enabled / running / active window).</li>
 *   <li>Chunks-per-shard bar — one row per {@code shard} label.</li>
 *   <li>Migrations summary (success24h / failed24h / last completed).</li>
 *   <li>Jumbo-chunks count + per-namespace jumbo breakdown.</li>
 * </ul>
 *
 * <p><b>SECTION-SHARD-2:</b> the section hides itself when the active connection's
 * topology is not {@code sharded} — determined from {@link MongoService#hello()},
 * not from the presence of a SHARD-* sample (which could be stale after a
 * re-bind from a sharded to a non-sharded connection).
 */
public final class ShardingSection implements AutoCloseable {

    public static final String ID = "sharding";
    public static final String TITLE = "Sharding";

    private final EventBus bus;
    private final ConnectionManager manager;
    private final EventBus.Subscription sub;

    private final ConcurrentMap<String, Long> shardCounts = new ConcurrentHashMap<>();
    private final ObservableList<ShardRow> shardRows = FXCollections.observableArrayList();

    private final Label balancerPill = new Label("—");
    private final Label runningPill  = new Label("—");
    private final Label windowPill   = new Label("—");

    private final MetricCell imbalanceCell;
    private final MetricCell jumboCell;
    private final MetricCell mongosCell;
    private final MetricCell success24hCell;
    private final MetricCell failed24hCell;
    private final MetricCell lastMigCell;

    private final VBox root = new VBox(12);
    private final Label placeholder = new Label(
            "Active connection is not sharded. Connect to a mongos router to see shard metrics.");
    private final VBox content = new VBox(12);

    private volatile String connectionId;

    public ShardingSection(EventBus bus, ConnectionManager manager,
                           MetricCell.Size size, String connectionId) {
        this(bus, manager, size, connectionId, null);
    }

    public ShardingSection(EventBus bus, ConnectionManager manager,
                           MetricCell.Size size, String connectionId,
                           MetricExpander expander) {
        this.bus = bus;
        this.manager = manager;
        this.connectionId = connectionId;

        root.setPadding(new Insets(12));
        placeholder.setStyle("-fx-text-fill: #6b7280; -fx-font-style: italic;");
        placeholder.setPadding(new Insets(16));

        HBox balRow = new HBox(12,
                pillBox("Balancer", balancerPill),
                pillBox("Running",  runningPill),
                pillBox("Window",   windowPill));
        balRow.setAlignment(Pos.CENTER_LEFT);

        TableView<ShardRow> shardTable = new TableView<>(shardRows);
        shardTable.setPrefHeight(size == MetricCell.Size.LARGE ? 220 : 160);
        shardTable.setPlaceholder(new Label("Waiting for shard chunk samples…"));
        shardTable.getColumns().addAll(List.of(
                col("Shard",  260, (ShardRow r) -> r.shard),
                col("Chunks", 140, (ShardRow r) -> Long.toString(r.chunks))));

        FlowPane migGrid = new FlowPane(12, 12);
        imbalanceCell  = new MetricCell(MetricId.SHARD_CHK_2, "Chunk imbalance", size, expander);
        jumboCell      = new MetricCell(MetricId.SHARD_CHK_3, "Jumbo chunks",    size, expander);
        mongosCell     = new MetricCell(MetricId.SHARD_MGS_1, "Mongos count",    size, expander);
        success24hCell = new MetricCell(MetricId.SHARD_MIG_1, "Migrations OK 24h", size, expander);
        failed24hCell  = new MetricCell(MetricId.SHARD_MIG_2, "Migrations err 24h", size, expander);
        lastMigCell    = new MetricCell(MetricId.SHARD_MIG_3, "Last completed",  size, expander);
        migGrid.getChildren().addAll(imbalanceCell, jumboCell, mongosCell,
                success24hCell, failed24hCell, lastMigCell);

        content.getChildren().addAll(
                header("Balancer"), balRow,
                header("Chunks per shard"), shardTable,
                header("Migrations & mongos"), migGrid);

        root.getChildren().setAll(content);

        this.sub = bus.onMetrics(this::route);
        probeTopology();
    }

    @Override public void close() { try { sub.close(); } catch (Throwable ignored) {} }

    public Node view() { return root; }

    public String connectionId() { return connectionId; }

    /** Re-bind. Clears per-shard counts, resets cells + balancer pills, re-probes topology. */
    public void setConnectionId(String newConnectionId) {
        this.connectionId = newConnectionId;
        shardCounts.clear();
        Platform.runLater(() -> {
            shardRows.clear();
            balancerPill.setText("—"); balancerPill.setStyle(pillStyle("#f3f4f6", "#4b5563"));
            runningPill.setText("—");  runningPill.setStyle(pillStyle("#f3f4f6", "#4b5563"));
            windowPill.setText("—");   windowPill.setStyle(pillStyle("#f3f4f6", "#4b5563"));
        });
        imbalanceCell.reset(); jumboCell.reset(); mongosCell.reset();
        success24hCell.reset(); failed24hCell.reset(); lastMigCell.reset();
        probeTopology();
    }

    private void route(List<MetricSample> batch) {
        String bound = this.connectionId;
        if (bound == null) return;
        boolean shardListChanged = false;
        for (MetricSample s : batch) {
            if (!bound.equals(s.connectionId())) continue;
            MetricId id = s.metric();
            switch (id) {
                case SHARD_BAL_1 -> Platform.runLater(() -> applyBalPill(balancerPill,
                        s.value() != 0 ? "enabled" : "disabled", s.value() != 0));
                case SHARD_BAL_2 -> Platform.runLater(() -> applyBalPill(runningPill,
                        s.value() != 0 ? "running" : "idle", s.value() == 0));
                case SHARD_BAL_3 -> Platform.runLater(() -> applyBalPill(windowPill,
                        s.value() != 0 ? "in window" : "off window", s.value() != 0));
                case SHARD_CHK_1 -> {
                    String shard = s.labels().labels().get("shard");
                    if (shard != null) {
                        shardCounts.put(shard, (long) s.value());
                        shardListChanged = true;
                    }
                }
                case SHARD_CHK_2 -> imbalanceCell.onSamples(List.of(s));
                case SHARD_CHK_3 -> jumboCell.onSamples(List.of(s));
                case SHARD_MIG_1 -> success24hCell.onSamples(List.of(s));
                case SHARD_MIG_2 -> failed24hCell.onSamples(List.of(s));
                case SHARD_MIG_3 -> lastMigCell.onSamples(List.of(s));
                case SHARD_MGS_1 -> mongosCell.onSamples(List.of(s));
                default -> {}
            }
        }
        if (shardListChanged) {
            Map<String, Long> snap = new TreeMap<>(shardCounts);
            List<ShardRow> rows = new java.util.ArrayList<>();
            for (var e : snap.entrySet()) rows.add(new ShardRow(e.getKey(), e.getValue()));
            rows.sort(Comparator.comparingLong((ShardRow r) -> -r.chunks));
            Platform.runLater(() -> shardRows.setAll(rows));
        }
    }

    private void probeTopology() {
        String bound = this.connectionId;
        if (bound == null) { setVisibleInPlace(false); return; }
        Thread.startVirtualThread(() -> {
            boolean sharded = false;
            try {
                MongoService svc = manager.service(bound);
                if (svc != null) {
                    Document hello = svc.hello();
                    sharded = Boolean.TRUE.equals(hello.getBoolean("isdbgrid"))
                            || "isdbgrid".equals(hello.getString("msg"));
                }
            } catch (Throwable ignored) {}
            final boolean s = sharded;
            Platform.runLater(() -> setVisibleInPlace(s));
        });
    }

    private void setVisibleInPlace(boolean sharded) {
        root.getChildren().setAll(sharded ? content : placeholder);
    }

    private static void applyBalPill(Label pill, String text, boolean ok) {
        pill.setText(text);
        pill.setStyle(ok ? pillStyle("#dcfce7", "#166534") : pillStyle("#fee2e2", "#991b1b"));
    }

    private static String pillStyle(String bg, String fg) {
        return "-fx-background-color: " + bg
                + "; -fx-text-fill: " + fg
                + "; -fx-font-weight: bold; -fx-font-size: 11px;"
                + " -fx-padding: 3 10 3 10; -fx-background-radius: 12;";
    }

    private static VBox pillBox(String title, Label pill) {
        Label t = new Label(title);
        t.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 10px;");
        pill.setStyle(pillStyle("#f3f4f6", "#4b5563"));
        VBox v = new VBox(2, t, pill);
        v.setAlignment(Pos.TOP_LEFT);
        return v;
    }

    private static Label header(String text) {
        Label l = new Label(text);
        l.setStyle("-fx-font-weight: bold; -fx-font-size: 13px;");
        return l;
    }

    private static <R> TableColumn<R, String> col(String title, double width,
                                                  java.util.function.Function<R, String> extractor) {
        TableColumn<R, String> c = new TableColumn<>(title);
        c.setCellValueFactory(cd -> new ReadOnlyObjectWrapper<>(extractor.apply(cd.getValue())));
        c.setPrefWidth(width);
        return c;
    }

    public static final class ShardRow {
        final String shard;
        final long chunks;
        ShardRow(String shard, long chunks) { this.shard = shard; this.chunks = chunks; }
        public String getShard() { return shard; }
        public long getChunks() { return chunks; }
    }
}

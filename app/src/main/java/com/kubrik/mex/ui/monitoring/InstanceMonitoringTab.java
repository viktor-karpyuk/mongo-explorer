package com.kubrik.mex.ui.monitoring;

import com.kubrik.mex.core.ConnectionManager;
import com.kubrik.mex.core.MongoService;
import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.model.ConnectionState;
import com.kubrik.mex.model.MongoConnection;
import com.kubrik.mex.monitoring.MonitoringService;
import com.kubrik.mex.store.ConnectionStore;
import com.kubrik.mex.ui.ClusterInfoDialog;
import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.control.Button;
import javafx.scene.control.ContentDisplay;
import javafx.scene.control.Label;
import javafx.scene.control.ScrollPane;
import javafx.scene.control.TitledPane;
import javafx.scene.control.Tooltip;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import org.bson.Document;
import org.kordamp.ikonli.javafx.FontIcon;

import java.util.function.BiConsumer;

/**
 * Per-instance monitoring tab (`INSTANCE-TAB-*`). Shows all four content sections
 * bound to a single connection id — no picker, because the tab itself is the scope.
 * Maximize buttons on each section fire through the provided callback so the
 * maximized tab stays scoped to this connection.
 */
public final class InstanceMonitoringTab extends VBox implements AutoCloseable {

    private final String connectionId;
    private final java.util.List<AutoCloseable> closeables = new java.util.ArrayList<>();

    public InstanceMonitoringTab(String connectionId,
                                 EventBus bus,
                                 MonitoringService svc,
                                 ConnectionManager manager,
                                 ConnectionStore store,
                                 BiConsumer<String, String> onMaximize,
                                 GraphExpandOpener graphExpandOpener,
                                 RowExpandOpener rowExpandOpener) {
        this.connectionId = connectionId;
        MetricExpander expander = graphExpandOpener == null ? null
                : (m, mode) -> graphExpandOpener.open(m, connectionId, mode);

        MongoConnection conn = store == null ? null : store.get(connectionId);
        String displayName = conn != null ? conn.name() : connectionId;

        setPadding(new Insets(12));
        setSpacing(12);

        Label title = new Label("Monitoring · " + displayName);
        title.setStyle("-fx-font-size: 18px; -fx-font-weight: bold;");
        getChildren().add(title);

        getChildren().add(buildHeader(connectionId, displayName, manager, store));

        InstanceSection instance =    new InstanceSection   (bus, MetricCell.Size.NORMAL, connectionId, expander);
        ReplicationSection replication = new ReplicationSection(bus, MetricCell.Size.NORMAL, connectionId, expander, rowExpandOpener);
        ShardingSection sharding    = new ShardingSection   (bus, manager, MetricCell.Size.NORMAL, connectionId, expander);
        StorageSection storage      = new StorageSection    (bus, MetricCell.Size.NORMAL, connectionId, rowExpandOpener);
        WorkloadSection workload    = new WorkloadSection   (bus, svc, MetricCell.Size.NORMAL, connectionId, expander, rowExpandOpener);
        closeables.add(instance);
        closeables.add(replication);
        closeables.add(sharding);
        closeables.add(storage);
        closeables.add(workload);

        TitledPane[] panels = {
                wrapPanel(InstanceSection.ID,    InstanceSection.TITLE,    instance.view(),    true,  onMaximize),
                wrapPanel(ReplicationSection.ID, ReplicationSection.TITLE, replication.view(), false, onMaximize),
                wrapPanel(ShardingSection.ID,    ShardingSection.TITLE,    sharding.view(),    false, onMaximize),
                wrapPanel(StorageSection.ID,     StorageSection.TITLE,     storage.view(),     false, onMaximize),
                wrapPanel(WorkloadSection.ID,    WorkloadSection.TITLE,    workload.view(),    false, onMaximize),
        };
        getChildren().addAll(panels);

        // NAV-2 — digit 1..5 expands the matching panel.
        sceneProperty().addListener((o, a, b) -> { if (b != null) installPanelJumps(b, panels); });
    }

    @Override public void close() {
        for (AutoCloseable c : closeables) { try { c.close(); } catch (Throwable ignored) {} }
        closeables.clear();
    }

    private void installPanelJumps(javafx.scene.Scene scene, TitledPane[] panels) {
        scene.addEventFilter(javafx.scene.input.KeyEvent.KEY_PRESSED, e -> {
            if (getScene() == null) return;
            javafx.scene.Node owner = scene.getFocusOwner();
            if (owner instanceof javafx.scene.control.TextInputControl) return;
            int idx = switch (e.getCode()) {
                case DIGIT1, NUMPAD1 -> 0;
                case DIGIT2, NUMPAD2 -> 1;
                case DIGIT3, NUMPAD3 -> 2;
                case DIGIT4, NUMPAD4 -> 3;
                case DIGIT5, NUMPAD5 -> 4;
                default -> -1;
            };
            if (idx < 0 || idx >= panels.length || panels[idx] == null) return;
            panels[idx].setExpanded(true);
            panels[idx].requestFocus();
            e.consume();
        });
    }

    public String connectionId() { return connectionId; }

    private VBox buildHeader(String connId, String displayName,
                             ConnectionManager manager, ConnectionStore store) {
        Label host = new Label(displayName);
        host.setStyle("-fx-font-size: 13px; -fx-text-fill: #111827;");

        Label dot = new Label("●");
        ConnectionState st = manager.state(connId);
        dot.setStyle("-fx-text-fill: " + statusColour(st.status()) + "; -fx-font-size: 14px;");

        Label topo = new Label("Probing…");
        topo.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 12px;");
        Label uptime = new Label("");
        uptime.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 12px;");

        Button clusterInfo = new Button("ⓘ Cluster info");
        clusterInfo.setOnAction(e -> {
            MongoService mongo = manager.service(connId);
            if (mongo == null) return;
            ClusterInfoDialog.show(getScene() == null ? null : getScene().getWindow(),
                    mongo, displayName);
        });

        Region spacer = new Region();
        HBox.setHgrow(spacer, Priority.ALWAYS);
        HBox line1 = new HBox(8, host, spacer, clusterInfo);
        line1.setAlignment(Pos.CENTER_LEFT);
        HBox line2 = new HBox(8, dot, topo, new Label("·"), uptime);
        line2.setAlignment(Pos.CENTER_LEFT);

        VBox box = new VBox(4, line1, line2);
        box.setPadding(new Insets(6, 10, 10, 10));
        box.setStyle("-fx-border-color: #e5e7eb; -fx-border-width: 0 0 1 0;");

        if (st.status() == ConnectionState.Status.CONNECTED) {
            Thread.startVirtualThread(() -> fillLiveSummary(connId, manager, topo, uptime));
        } else {
            topo.setText("Not connected — samples paused.");
        }
        return box;
    }

    private static void fillLiveSummary(String connId, ConnectionManager manager,
                                        Label topoLabel, Label uptimeLabel) {
        String topo = "?", uptime = "";
        try {
            MongoService mongo = manager.service(connId);
            if (mongo != null) {
                Document hello = mongo.hello();
                if (Boolean.TRUE.equals(hello.getBoolean("isdbgrid"))
                        || "isdbgrid".equals(hello.getString("msg"))) {
                    topo = "Sharded";
                } else if (hello.containsKey("setName")) {
                    topo = "Replica set · " + hello.getString("setName");
                    if (hello.getString("primary") != null) topo += " · primary " + hello.getString("primary");
                } else {
                    topo = "Standalone";
                }
                topo += " · v" + mongo.serverVersion();
                try {
                    Document status = mongo.database("admin").runCommand(new Document("serverStatus", 1));
                    if (status != null && status.get("uptime") instanceof Number n) {
                        uptime = "Uptime " + humanDuration(n.longValue());
                    }
                } catch (Throwable ignored) {}
            }
        } catch (Throwable ignored) {}
        final String topoF = topo, uptimeF = uptime;
        Platform.runLater(() -> {
            topoLabel.setText(topoF);
            uptimeLabel.setText(uptimeF);
        });
    }

    private TitledPane wrapPanel(String id, String title, Node content, boolean expanded,
                                 BiConsumer<String, String> onMaximize) {
        ScrollPane scroll = new ScrollPane(content);
        scroll.setFitToWidth(true);
        TitledPane tp = new TitledPane(title, scroll);
        tp.setExpanded(expanded);

        FontIcon icon = new FontIcon("fth-maximize-2");
        icon.setIconSize(12);
        Button max = new Button();
        max.setGraphic(icon);
        max.setTooltip(new Tooltip("Open " + title + " in its own tab"));
        max.setStyle("-fx-background-color: transparent; -fx-padding: 2 6 2 6; -fx-cursor: hand;");
        max.setOnAction(e -> {
            if (onMaximize != null) onMaximize.accept(id, connectionId);
        });
        Region spacer = new Region();
        HBox.setHgrow(spacer, Priority.ALWAYS);
        HBox header = new HBox(8, spacer, max);
        header.setAlignment(Pos.CENTER_RIGHT);
        tp.setGraphic(header);
        tp.setContentDisplay(ContentDisplay.RIGHT);
        return tp;
    }

    private static String statusColour(ConnectionState.Status s) {
        return switch (s) {
            case CONNECTED    -> "#16a34a";
            case CONNECTING   -> "#d97706";
            case ERROR        -> "#b91c1c";
            case DISCONNECTED -> "#6b7280";
        };
    }

    private static String humanDuration(long seconds) {
        long days = seconds / 86400;
        long hours = (seconds % 86400) / 3600;
        long mins = (seconds % 3600) / 60;
        if (days > 0) return days + "d " + hours + "h";
        if (hours > 0) return hours + "h " + mins + "m";
        return mins + "m";
    }
}

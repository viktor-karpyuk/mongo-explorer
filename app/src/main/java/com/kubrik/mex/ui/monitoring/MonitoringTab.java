package com.kubrik.mex.ui.monitoring;

import com.kubrik.mex.core.ConnectionManager;
import com.kubrik.mex.core.MongoService;
import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.model.ConnectionState;
import com.kubrik.mex.model.MongoConnection;
import com.kubrik.mex.monitoring.MonitoringProfile;
import com.kubrik.mex.monitoring.MonitoringService;
import com.kubrik.mex.store.ConnectionStore;
import com.kubrik.mex.store.Database;
import com.kubrik.mex.ui.ClusterInfoDialog;
import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.control.Button;
import javafx.scene.control.ComboBox;
import javafx.scene.control.ContentDisplay;
import javafx.scene.control.Label;
import javafx.scene.control.ScrollPane;
import javafx.scene.control.TitledPane;
import javafx.scene.control.Tooltip;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import javafx.util.StringConverter;
import org.bson.Document;
import org.kordamp.ikonli.javafx.FontIcon;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Monitoring tab root. v2.2.0 refactor — the four content sections bind to a
 * single active connection id; switching the picker re-binds them in place.
 *
 * <ul>
 *   <li>Connection picker + instance header at the top identify which cluster
 *       the charts reflect.</li>
 *   <li>Each section's maximize button passes the active connection id
 *       through {@link #onMaximize(BiConsumer)} so the maximized tab stays scoped.</li>
 *   <li>"Monitored connections" cards open a dedicated per-instance tab via
 *       {@link #onOpenInstanceTab(Consumer)}.</li>
 * </ul>
 */
public final class MonitoringTab extends VBox {

    /** App-settings key persisting the picker selection (PERSIST-1). */
    public static final String ACTIVE_CONNECTION_KEY = "monitoring.active_connection_id";

    private final EventBus bus;
    private final MonitoringService svc;
    private final ConnectionManager manager;
    private final ConnectionStore connectionStore;
    private final Database database;

    private final ComboBox<String> picker = new ComboBox<>();
    private final Label hostLabel   = new Label("—");
    private final Label topoLabel   = new Label("—");
    private final Label uptimeLabel = new Label("—");
    private final Label healthDot   = new Label("●");
    private final VBox connectionsBox = new VBox(8);
    private final java.util.Map<String, ConnectionCard> cardsByConnId = new java.util.LinkedHashMap<>();
    private Label emptyStateLabel;

    private final InstanceSection instance;
    private final ReplicationSection replication;
    private final ShardingSection sharding;
    private final StorageSection storage;
    private final WorkloadSection workload;

    private BiConsumer<String, String> maximizeHandler;   // (sectionId, connectionId)
    private Consumer<String> openInstanceTabHandler;      // (connectionId)
    private final GraphExpandOpener graphExpandOpener;
    private final RowExpandOpener rowExpandOpener;
    private final TitledPane[] panels = new TitledPane[5];

    public MonitoringTab(EventBus bus, MonitoringService svc,
                         ConnectionManager manager, ConnectionStore connectionStore,
                         Database database, GraphExpandOpener graphExpandOpener,
                         RowExpandOpener rowExpandOpener) {
        this.bus = bus;
        this.svc = svc;
        this.manager = manager;
        this.connectionStore = connectionStore;
        this.database = database;
        this.graphExpandOpener = graphExpandOpener;
        this.rowExpandOpener = rowExpandOpener;

        setPadding(new Insets(12));
        setSpacing(12);

        Label tabTitle = new Label("Monitoring");
        tabTitle.setStyle("-fx-font-size: 18px; -fx-font-weight: bold;");
        getChildren().add(tabTitle);

        getChildren().add(buildPickerRow());
        getChildren().add(buildInstanceHeader());
        getChildren().add(buildConnectionsPanel());

        // Cells in every section resolve the *current* picker value at expand time,
        // so one expander binding works across re-binds.
        MetricExpander expander = (m, mode) -> {
            if (graphExpandOpener == null) return;
            String connId = picker.getValue();
            if (connId != null) graphExpandOpener.open(m, connId, mode);
        };
        this.instance    = new InstanceSection(bus, MetricCell.Size.NORMAL, null, expander);
        this.replication = new ReplicationSection(bus, MetricCell.Size.NORMAL, null, expander, rowExpandOpener);
        this.sharding    = new ShardingSection(bus, manager, MetricCell.Size.NORMAL, null, expander);
        this.storage     = new StorageSection(bus, MetricCell.Size.NORMAL, null, rowExpandOpener);
        this.workload    = new WorkloadSection(bus, svc, MetricCell.Size.NORMAL, null, expander, rowExpandOpener);

        TitledPane instancePane    = wrapPanel(InstanceSection.ID,    InstanceSection.TITLE,    instance.view(),    true);
        TitledPane replicationPane = wrapPanel(ReplicationSection.ID, ReplicationSection.TITLE, replication.view(), false);
        TitledPane shardingPane    = wrapPanel(ShardingSection.ID,    ShardingSection.TITLE,    sharding.view(),    false);
        TitledPane storagePane     = wrapPanel(StorageSection.ID,     StorageSection.TITLE,     storage.view(),     false);
        TitledPane workloadPane    = wrapPanel(WorkloadSection.ID,    WorkloadSection.TITLE,    workload.view(),    false);
        getChildren().addAll(instancePane, replicationPane, shardingPane, storagePane, workloadPane);
        panels[0] = instancePane;
        panels[1] = replicationPane;
        panels[2] = shardingPane;
        panels[3] = storagePane;
        panels[4] = workloadPane;

        // NAV-2 — digit 1..5 expands + scrolls into view, installed once the scene is attached.
        sceneProperty().addListener((o, a, b) -> { if (b != null) installPanelJumps(b); });

        bus.onState(s -> Platform.runLater(() -> {
            refreshPicker();
            refreshConnectionsPanel();
            refreshInstanceHeader();
        }));
        Platform.runLater(() -> {
            refreshPicker();
            refreshConnectionsPanel();
            selectInitialConnection();
            refreshInstanceHeader();
        });
    }

    public void onMaximize(BiConsumer<String, String> handler) { this.maximizeHandler = handler; }

    public void onOpenInstanceTab(Consumer<String> handler) { this.openInstanceTabHandler = handler; }

    private void installPanelJumps(javafx.scene.Scene scene) {
        scene.addEventFilter(javafx.scene.input.KeyEvent.KEY_PRESSED, e -> {
            // Only act when this tab is showing + no text input has focus.
            if (!isShowingInTab()) return;
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

    private boolean isShowingInTab() {
        javafx.scene.Node n = this;
        while (n != null) { if (n.getScene() == null) return false; n = n.getParent(); }
        return isVisible();
    }

    // ===== picker =========================================================

    private HBox buildPickerRow() {
        Label label = new Label("Connection:");
        picker.setPrefWidth(280);
        picker.setConverter(new StringConverter<>() {
            @Override public String toString(String connId) {
                if (connId == null) return "";
                MongoConnection c = connectionStore == null ? null : connectionStore.get(connId);
                String name = c != null ? c.name() : connId;
                ConnectionState st = manager.state(connId);
                return st.status() == ConnectionState.Status.CONNECTED
                        ? name
                        : name + "  (not connected)";
            }
            @Override public String fromString(String s) { return s; }
        });
        picker.valueProperty().addListener((obs, old, neu) -> {
            if (neu == null || neu.equals(old)) return;
            rebindSections(neu);
            persistActive(neu);
            refreshInstanceHeader();
        });
        HBox row = new HBox(8, label, picker);
        row.setAlignment(Pos.CENTER_LEFT);
        return row;
    }

    private void refreshPicker() {
        String current = picker.getValue();
        var profiles = svc.enabledProfiles();
        var ids = new java.util.ArrayList<String>(profiles.size());
        for (MonitoringProfile p : profiles) ids.add(p.connectionId());
        picker.getItems().setAll(ids);
        if (current != null && ids.contains(current)) picker.setValue(current);
    }

    private void selectInitialConnection() {
        if (picker.getValue() != null) return;
        Optional<String> persisted = loadPersisted();
        if (persisted.isPresent() && picker.getItems().contains(persisted.get())) {
            picker.setValue(persisted.get());
            return;
        }
        String firstConnected = null, firstAny = null;
        for (String id : picker.getItems()) {
            if (firstAny == null) firstAny = id;
            if (manager.state(id).status() == ConnectionState.Status.CONNECTED) {
                firstConnected = id;
                break;
            }
        }
        String pick = firstConnected != null ? firstConnected : firstAny;
        if (pick != null) picker.setValue(pick);
    }

    private void rebindSections(String connectionId) {
        instance.setConnectionId(connectionId);
        replication.setConnectionId(connectionId);
        sharding.setConnectionId(connectionId);
        storage.setConnectionId(connectionId);
        workload.setConnectionId(connectionId);
    }

    // ===== instance header ================================================

    private VBox buildInstanceHeader() {
        hostLabel.setStyle("-fx-text-fill: #111827; -fx-font-size: 13px;");
        topoLabel.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 12px;");
        uptimeLabel.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 12px;");
        healthDot.setStyle("-fx-text-fill: #9ca3af; -fx-font-size: 14px;");

        Button clusterInfo = new Button("ⓘ Cluster info");
        clusterInfo.setOnAction(e -> {
            String connId = picker.getValue();
            if (connId == null) return;
            MongoService mongo = manager.service(connId);
            if (mongo == null) return;
            MongoConnection c = connectionStore == null ? null : connectionStore.get(connId);
            ClusterInfoDialog.show(getScene() == null ? null : getScene().getWindow(),
                    mongo, c != null ? c.name() : connId);
        });

        Region spacer = new Region();
        HBox.setHgrow(spacer, Priority.ALWAYS);
        HBox line1 = new HBox(8, hostLabel, spacer, clusterInfo);
        line1.setAlignment(Pos.CENTER_LEFT);
        HBox line2 = new HBox(8, healthDot, topoLabel, new Label("·"), uptimeLabel);
        line2.setAlignment(Pos.CENTER_LEFT);
        VBox box = new VBox(4, line1, line2);
        box.setPadding(new Insets(6, 10, 10, 10));
        box.setStyle("-fx-border-color: #e5e7eb; -fx-border-width: 0 0 1 0;");
        return box;
    }

    private void refreshInstanceHeader() {
        String connId = picker.getValue();
        if (connId == null) {
            hostLabel.setText("(no monitored connection)");
            topoLabel.setText("—");
            uptimeLabel.setText("—");
            healthDot.setStyle("-fx-text-fill: #9ca3af; -fx-font-size: 14px;");
            return;
        }
        MongoConnection conn = connectionStore == null ? null : connectionStore.get(connId);
        String name = conn != null ? conn.name() : connId;
        ConnectionState st = manager.state(connId);
        healthDot.setStyle("-fx-text-fill: " + statusColour(st.status()) + "; -fx-font-size: 14px;");
        hostLabel.setText(name);
        topoLabel.setText("Connecting…");
        uptimeLabel.setText("");
        if (st.status() != ConnectionState.Status.CONNECTED) {
            topoLabel.setText("Not connected — samples paused.");
            return;
        }
        Thread.startVirtualThread(() -> {
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
                        Document status = mongo.database("admin")
                                .runCommand(new Document("serverStatus", 1));
                        if (status != null && status.get("uptime") instanceof Number n) {
                            uptime = "Uptime " + humanDuration(n.longValue());
                        }
                    } catch (Throwable ignored) {}
                }
            } catch (Throwable ignored) {}
            final String topoF = topo;
            final String uptimeF = uptime;
            Platform.runLater(() -> {
                topoLabel.setText(topoF);
                uptimeLabel.setText(uptimeF);
            });
        });
    }

    // ===== persistence ====================================================

    private void persistActive(String connectionId) {
        if (database == null || connectionId == null) return;
        try (PreparedStatement ps = database.connection().prepareStatement(
                "INSERT INTO app_settings(key, value) VALUES(?, ?) " +
                        "ON CONFLICT(key) DO UPDATE SET value = excluded.value")) {
            ps.setString(1, ACTIVE_CONNECTION_KEY);
            ps.setString(2, connectionId);
            ps.executeUpdate();
        } catch (Exception ignored) {}
    }

    private Optional<String> loadPersisted() {
        if (database == null) return Optional.empty();
        try (PreparedStatement ps = database.connection().prepareStatement(
                "SELECT value FROM app_settings WHERE key = ?")) {
            ps.setString(1, ACTIVE_CONNECTION_KEY);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return Optional.ofNullable(rs.getString(1));
            }
        } catch (Exception ignored) {}
        return Optional.empty();
    }

    // ===== section wrapping ===============================================

    private TitledPane wrapPanel(String id, String title, Node content, boolean expanded) {
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
            if (maximizeHandler != null) maximizeHandler.accept(id, picker.getValue());
        });
        Region spacer = new Region();
        HBox.setHgrow(spacer, Priority.ALWAYS);
        HBox header = new HBox(8, spacer, max);
        header.setAlignment(Pos.CENTER_RIGHT);
        tp.setGraphic(header);
        tp.setContentDisplay(ContentDisplay.RIGHT);
        return tp;
    }

    // ===== monitored-connections panel ====================================

    private TitledPane buildConnectionsPanel() {
        Button refresh = new Button("Refresh");
        refresh.setOnAction(e -> refreshConnectionsPanel());
        Region spacer = new Region();
        HBox.setHgrow(spacer, Priority.ALWAYS);
        HBox toolbar = new HBox(8, spacer, refresh);
        toolbar.setPadding(new Insets(0, 0, 6, 0));

        VBox body = new VBox(6, toolbar, connectionsBox);
        body.setPadding(new Insets(8));
        TitledPane pane = new TitledPane("Monitored connections", body);
        pane.setExpanded(true);
        return pane;
    }

    /**
     * Diff the current profile set against the card map — only add/remove cards
     * on set changes; existing cards self-refresh on state events, so state
     * flaps never create new subscriptions.
     */
    private void refreshConnectionsPanel() {
        var profiles = svc.enabledProfiles();
        java.util.LinkedHashSet<String> wantedIds = new java.util.LinkedHashSet<>(profiles.size());
        for (MonitoringProfile p : profiles) wantedIds.add(p.connectionId());

        // Remove cards whose profile is gone; close their subscriptions.
        var it = cardsByConnId.entrySet().iterator();
        while (it.hasNext()) {
            var entry = it.next();
            if (!wantedIds.contains(entry.getKey())) {
                try { entry.getValue().close(); } catch (Throwable ignored) {}
                it.remove();
            }
        }

        // Add cards for newly-enabled profiles.
        for (String connId : wantedIds) {
            if (cardsByConnId.containsKey(connId)) continue;
            cardsByConnId.put(connId, new ConnectionCard(
                    connId,
                    connectionStore == null ? null : connectionStore.get(connId),
                    manager, bus, svc.alerter(),
                    () -> {
                        if (openInstanceTabHandler != null) openInstanceTabHandler.accept(connId);
                    },
                    () -> {
                        MongoService mongo = manager.service(connId);
                        if (mongo == null) return;
                        MongoConnection c = connectionStore == null ? null : connectionStore.get(connId);
                        ClusterInfoDialog.show(getScene() == null ? null : getScene().getWindow(),
                                mongo, c != null ? c.name() : connId);
                    },
                    () -> {
                        MongoService mongo = manager.service(connId);
                        MongoConnection c = connectionStore == null ? null : connectionStore.get(connId);
                        ProfilingSettingsDialog.show(getScene() == null ? null : getScene().getWindow(),
                                svc, connId, c != null ? c.name() : connId, mongo);
                    }));
        }

        // Re-order children to follow profile order (cheap — reparents Nodes without touching subs).
        connectionsBox.getChildren().clear();
        if (cardsByConnId.isEmpty()) {
            if (emptyStateLabel == null) {
                emptyStateLabel = new Label(
                        "No monitored connections. Connect to a MongoDB instance to start sampling.");
                emptyStateLabel.setStyle("-fx-text-fill: #6b7280;");
            }
            connectionsBox.getChildren().add(emptyStateLabel);
            return;
        }
        for (String connId : wantedIds) {
            ConnectionCard c = cardsByConnId.get(connId);
            if (c != null) connectionsBox.getChildren().add(c);
        }
    }

    // ===== helpers ========================================================

    private static String humanDuration(long seconds) {
        long days = seconds / 86400;
        long hours = (seconds % 86400) / 3600;
        long mins = (seconds % 3600) / 60;
        if (days > 0) return days + "d " + hours + "h";
        if (hours > 0) return hours + "h " + mins + "m";
        return mins + "m";
    }

    private static String statusColour(ConnectionState.Status s) {
        return switch (s) {
            case CONNECTED    -> "#16a34a";
            case CONNECTING   -> "#d97706";
            case ERROR        -> "#b91c1c";
            case DISCONNECTED -> "#6b7280";
        };
    }
}

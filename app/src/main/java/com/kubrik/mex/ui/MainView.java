package com.kubrik.mex.ui;

import com.kubrik.mex.cluster.safety.KillSwitch;
import com.kubrik.mex.cluster.service.OpsExecutor;
import com.kubrik.mex.cluster.store.OpsAuditDao;
import com.kubrik.mex.core.ConnectionManager;
import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.migration.MigrationService;
import com.kubrik.mex.model.MongoConnection;
import com.kubrik.mex.monitoring.MonitoringService;
import com.kubrik.mex.store.ConnectionStore;
import com.kubrik.mex.store.HistoryStore;
import com.kubrik.mex.ui.cluster.BalancerPane;
import com.kubrik.mex.ui.cluster.ClusterTab;
import com.kubrik.mex.ui.cluster.CurrentOpPane;
import com.kubrik.mex.ui.cluster.KillSwitchPill;
import com.kubrik.mex.ui.cluster.TopologyPane;
import com.kubrik.mex.ui.cluster.ZonesPane;
import com.kubrik.mex.ui.migration.MigrationsTab;
import com.kubrik.mex.ui.migration.MigrationWizard;
import com.kubrik.mex.monitoring.model.MetricId;
import com.kubrik.mex.monitoring.store.ProfileSampleRecord;
import com.kubrik.mex.ui.monitoring.ExpandedMetricView;
import com.kubrik.mex.ui.monitoring.IndexDetailView;
import com.kubrik.mex.ui.monitoring.MaximizedSectionTab;
import com.kubrik.mex.ui.monitoring.MemberDetailView;
import com.kubrik.mex.ui.monitoring.MetricExpander;
import com.kubrik.mex.ui.monitoring.MonitoringTab;
import com.kubrik.mex.ui.monitoring.NamespaceDetailView;
import com.kubrik.mex.ui.monitoring.RowExpandOpener;
import com.kubrik.mex.ui.monitoring.SlowQueryDetailView;
import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.scene.control.Label;
import javafx.scene.control.Menu;
import javafx.scene.control.MenuBar;
import javafx.scene.control.MenuItem;
import javafx.scene.control.SeparatorMenuItem;
import javafx.scene.control.SplitPane;
import javafx.scene.control.Tab;
import javafx.scene.control.TabPane;
import javafx.scene.input.KeyCode;
import javafx.scene.input.KeyCodeCombination;
import javafx.scene.input.KeyCombination;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.HBox;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class MainView extends BorderPane {

    private final ConnectionManager manager;
    private final ConnectionStore connectionStore;
    private final HistoryStore historyStore;
    private final EventBus events;
    private final MigrationService migrationService;
    private final MonitoringService monitoringService;
    private final com.kubrik.mex.store.Database database;
    private final CurrentOpPane.KillOpHandler killOpHandler;
    private final TopologyPane.RsAdminHandler rsAdminHandler;
    private final OpsAuditDao opsAuditDao;
    private final OpsExecutor opsExecutor;
    private final BalancerPane.BalancerHandler balancerHandler;
    private final ZonesPane.ZonesHandler zonesHandler;
    private final KillSwitch killSwitch;

    private final ConnectionTree connTree;
    private final TabPane tabs = new TabPane();
    private final Map<String, Tab> openCollectionTabs = new HashMap<>();
    private Tab manageTab;
    private Tab historyTab;
    private Tab logsTab;
    private Tab migrationsTab;
    private Tab monitoringTab;
    private MigrationsTab migrationsView;
    private MonitoringTab monitoringView;
    private LogsView logsView;
    private WelcomeView welcomeView;

    private final Label statusServer = new Label("");
    private final Label statusConn = new Label("Ready");

    public MainView(ConnectionManager manager,
                    ConnectionStore connectionStore,
                    HistoryStore historyStore,
                    EventBus events,
                    MigrationService migrationService,
                    MonitoringService monitoringService,
                    com.kubrik.mex.store.Database database,
                    CurrentOpPane.KillOpHandler killOpHandler,
                    TopologyPane.RsAdminHandler rsAdminHandler,
                    OpsAuditDao opsAuditDao,
                    OpsExecutor opsExecutor,
                    BalancerPane.BalancerHandler balancerHandler,
                    ZonesPane.ZonesHandler zonesHandler,
                    KillSwitch killSwitch) {
        this.manager = manager;
        this.connectionStore = connectionStore;
        this.historyStore = historyStore;
        this.events = events;
        this.migrationService = migrationService;
        this.monitoringService = monitoringService;
        this.database = database;
        this.killOpHandler = killOpHandler;
        this.rsAdminHandler = rsAdminHandler;
        this.opsAuditDao = opsAuditDao;
        this.opsExecutor = opsExecutor;
        this.balancerHandler = balancerHandler;
        this.zonesHandler = zonesHandler;
        this.killSwitch = killSwitch;

        this.connTree = new ConnectionTree(manager, connectionStore, events);
        this.connTree.setOpenHandler(new ConnectionTree.OpenHandler() {
            @Override public void openCollection(String connectionId, String db, String coll) {
                openCollectionTab(connectionId, db, coll);
            }
            @Override public void openManageConnections() { openManageTab(); }
            @Override public void openConnectionEditor(MongoConnection existing) { openEditor(existing); }
            @Override public void openLogs() { openLogsTab(); }
            @Override public void openMigrate(String connectionId, String db, String coll) {
                MigrationWizard.openSeeded(getScene().getWindow(), migrationService,
                        connectionStore, manager, events, connectionId, db, coll);
            }
            @Override public void openMonitoring(String connectionId) {
                openMonitoringTab();
            }
            @Override public void openCluster(String connectionId) {
                openClusterTab(connectionId);
            }
        });

        // Shared LogsView so it captures everything from app start.
        this.logsView = new LogsView(events);

        SplitPane split = new SplitPane(connTree, tabs);
        split.setDividerPositions(0.22);

        // Welcome tab
        welcomeView = new WelcomeView(
                manager, connectionStore, events,
                () -> openEditor(null),
                this::openManageTab,
                this::openLogsTab,
                this::openMonitoringTab,
                c -> {
                    if (manager.state(c.id()).status() != com.kubrik.mex.model.ConnectionState.Status.CONNECTED) {
                        manager.connect(c.id());
                    }
                    connTree.reloadAll();
                },
                this::openEditor);
        Tab welcome = new Tab("Welcome", welcomeView);
        welcome.setClosable(false);
        tabs.getTabs().add(welcome);

        // Status bar (UX-13 pill anchored at the right, hidden when no live migration jobs)
        com.kubrik.mex.ui.migration.StatusBarRunningJobsPill runningJobsPill =
                new com.kubrik.mex.ui.migration.StatusBarRunningJobsPill(
                        migrationService, events,
                        id -> {
                            openMigrationsTab();
                            if (migrationsView != null) migrationsView.openJob(id);
                        });
        javafx.scene.layout.Region spacer = new javafx.scene.layout.Region();
        HBox.setHgrow(spacer, javafx.scene.layout.Priority.ALWAYS);
        KillSwitchPill killSwitchPill = new KillSwitchPill(killSwitch);
        HBox status = new HBox(12, statusConn, new Label("·"), statusServer, spacer,
                runningJobsPill, killSwitchPill);
        status.setPadding(new Insets(6, 12, 6, 12));
        status.setStyle("-fx-background-color: #f3f4f6; -fx-border-color: #e5e7eb; -fx-border-width: 1 0 0 0;");
        statusServer.setStyle("-fx-text-fill: #6b7280;");
        statusConn.setStyle("-fx-text-fill: #6b7280;");

        setTop(buildMenuBar());
        setCenter(split);
        setBottom(status);

        events.onState(s -> Platform.runLater(() -> {
            statusConn.setText(s.connectionId() + ": " + s.status().name()
                    + (s.lastError() != null ? " — " + s.lastError() : ""));
            statusServer.setText(s.serverVersion() != null ? "MongoDB " + s.serverVersion() : "");
            if (s.status() == com.kubrik.mex.model.ConnectionState.Status.ERROR) openLogsTab();
        }));
    }

    private MenuBar buildMenuBar() {
        MenuBar bar = new MenuBar();
        bar.setUseSystemMenuBar(true);

        // ----- File -----
        Menu file = new Menu("File");
        MenuItem newConn = item("New Connection…", new KeyCodeCombination(KeyCode.N, KeyCombination.SHORTCUT_DOWN),
                () -> openEditor(null));
        // NAV-1 claims Cmd/Ctrl+M for Monitoring; Manage Connections moves to shift-modifier.
        MenuItem manageConns = item("Manage Connections",
                new KeyCodeCombination(KeyCode.M, KeyCombination.SHORTCUT_DOWN, KeyCombination.SHIFT_DOWN),
                this::openManageTab);
        MenuItem closeTab = item("Close Tab", new KeyCodeCombination(KeyCode.W, KeyCombination.SHORTCUT_DOWN),
                this::closeCurrentTab);
        MenuItem quit = item("Quit", new KeyCodeCombination(KeyCode.Q, KeyCombination.SHORTCUT_DOWN),
                () -> javafx.application.Platform.exit());
        file.getItems().addAll(newConn, manageConns, new SeparatorMenuItem(), closeTab, new SeparatorMenuItem(), quit);

        // ----- Edit -----
        Menu edit = new Menu("Edit");
        edit.getItems().addAll(
                item("Cut", new KeyCodeCombination(KeyCode.X, KeyCombination.SHORTCUT_DOWN),
                        () -> fireFocusedTextAction("cut")),
                item("Copy", new KeyCodeCombination(KeyCode.C, KeyCombination.SHORTCUT_DOWN),
                        () -> fireFocusedTextAction("copy")),
                item("Paste", new KeyCodeCombination(KeyCode.V, KeyCombination.SHORTCUT_DOWN),
                        () -> fireFocusedTextAction("paste")),
                new SeparatorMenuItem(),
                item("Select All", new KeyCodeCombination(KeyCode.A, KeyCombination.SHORTCUT_DOWN),
                        () -> fireFocusedTextAction("selectAll")));

        // ----- View -----
        Menu view = new Menu("View");
        view.getItems().addAll(
                item("Reload Connection Tree", new KeyCodeCombination(KeyCode.R, KeyCombination.SHORTCUT_DOWN),
                        connTree::reloadAll),
                item("Connection Log", new KeyCodeCombination(KeyCode.L, KeyCombination.SHORTCUT_DOWN),
                        this::openLogsTab));

        // ----- Connection -----
        Menu connection = new Menu("Connection");
        connection.getItems().addAll(
                item("New Connection…", null, () -> openEditor(null)),
                item("Manage Connections", null, this::openManageTab),
                new SeparatorMenuItem(),
                item("Disconnect All", null, () -> {
                    for (var c : connectionStore.list()) manager.disconnect(c.id());
                    connTree.reloadAll();
                }));

        // ----- Tools -----
        Menu tools = new Menu("Tools");
        tools.getItems().addAll(
                item("Migrate…", null, this::openMigrationWizard),
                item("Migrations tab", null, this::openMigrationsTab),
                new SeparatorMenuItem(),
                // NAV-1 — Cmd/Ctrl+M opens Monitoring (creates or focuses the tab).
                item("Monitoring", new KeyCodeCombination(KeyCode.M, KeyCombination.SHORTCUT_DOWN),
                        this::openMonitoringTab),
                // v2.4 UI-OPS-8 — Cmd/Ctrl+Alt+C opens the Cluster tab for the
                // connection selected in the tree (falls back to the first
                // connected one).
                item("Cluster view",
                        new KeyCodeCombination(KeyCode.C,
                                KeyCombination.SHORTCUT_DOWN, KeyCombination.ALT_DOWN),
                        this::openClusterTabForSelectedConnection),
                // v2.4 UI-OPS-8 — Cmd/Ctrl+Alt+O opens Cluster ▸ Ops with
                // secs_running >= 10 pre-filtered (jump to long-running ops).
                item("Long-running ops",
                        new KeyCodeCombination(KeyCode.O,
                                KeyCombination.SHORTCUT_DOWN, KeyCombination.ALT_DOWN),
                        this::openLongRunningOpsView),
                // v2.4 UI-OPS-8 — Cmd/Ctrl+Alt+L opens the Logs tab and
                // filters to ops-audit rows.
                item("Logs (audit only)",
                        new KeyCodeCombination(KeyCode.L,
                                KeyCombination.SHORTCUT_DOWN, KeyCombination.ALT_DOWN),
                        this::openLogsFilteredToAudit));

        // ----- Help -----
        Menu help = new Menu("Help");
        help.getItems().add(item("About Mongo Explorer", null, () ->
                UiHelpers.info(getScene().getWindow(), "Mongo Explorer",
                        "A simple MongoDB explorer.\nBuilt with JavaFX + AtlantaFX.")));

        bar.getMenus().addAll(file, edit, view, connection, tools, help);
        return bar;
    }

    private void openMigrationWizard() {
        new MigrationWizard(getScene().getWindow(), migrationService,
                connectionStore, manager, events).show();
    }

    public void openMigrationsTabPublic() { openMigrationsTab(); }

    public void openMonitoringTabPublic() { openMonitoringTab(); }

    /** Open a maximized view of a single Monitoring section in its own tab, scoped
     *  to a specific connection id (v2.2.0 — ROUTE-4). */
    private void openMonitoringSectionTab(String sectionId, String connectionId) {
        MaximizedSectionTab body = new MaximizedSectionTab(sectionId, connectionId,
                events, monitoringService, manager, this::openExpandedMetric, rowExpandOpener);
        Tab t = new Tab(MaximizedSectionTab.tabTitle(sectionId, connectionId), body);
        t.setOnClosed(e -> body.close());
        tabs.getTabs().add(t);
        tabs.getSelectionModel().select(t);
    }

    /** Per-(metric, connection) expand views re-select rather than duplicate when re-opened. */
    private final java.util.Map<String, Tab> metricExpandTabs = new java.util.HashMap<>();
    private final java.util.Map<String, Tab> rowExpandTabs = new java.util.HashMap<>();

    /** ROW-EXPAND-* — open a row-detail view as a modal or as an app-level tab. */
    private final RowExpandOpener rowExpandOpener = new RowExpandOpener() {
        @Override public void openSlowQuery(ProfileSampleRecord r, String connectionId,
                                            MetricExpander.Mode mode) {
            String name = displayName(connectionId);
            hostRow("Slow query · " + r.ns() + " · " + name,
                    "slow|" + connectionId + "|" + r.tsMs() + "|" + r.queryHash(),
                    new SlowQueryDetailView(r, name), mode);
        }
        @Override public void openIndex(String db, String coll, String index,
                                        long sizeBytes, double opsPerSec, String flags,
                                        String connectionId, MetricExpander.Mode mode) {
            String name = displayName(connectionId);
            hostRow("Index · " + db + "." + coll + "." + index + " · " + name,
                    "idx|" + connectionId + "|" + db + "." + coll + "." + index,
                    new IndexDetailView(db, coll, index, sizeBytes, opsPerSec, flags,
                            connectionId, name, events),
                    mode);
        }
        @Override public void openNamespace(String db, String coll,
                                            double readMsPerSec, double writeMsPerSec, double totalMsPerSec,
                                            double readOpsPerSec, double writeOpsPerSec,
                                            String connectionId, MetricExpander.Mode mode) {
            String name = displayName(connectionId);
            hostRow("Namespace · " + db + "." + coll + " · " + name,
                    "ns|" + connectionId + "|" + db + "." + coll,
                    new NamespaceDetailView(db, coll, readMsPerSec, writeMsPerSec, totalMsPerSec,
                            readOpsPerSec, writeOpsPerSec, connectionId, name, events),
                    mode);
        }
        @Override public void openMember(String member, String state, String health, String uptime,
                                         String ping, String lag, String lastHeartbeat,
                                         String connectionId, MetricExpander.Mode mode) {
            String name = displayName(connectionId);
            hostRow("Member · " + member + " · " + name,
                    "mbr|" + connectionId + "|" + member,
                    new MemberDetailView(member, state, health, uptime, ping, lag, lastHeartbeat,
                            connectionId, name, events),
                    mode);
        }
    };

    private String displayName(String connectionId) {
        MongoConnection c = connectionStore.get(connectionId);
        return c != null ? c.name() : connectionId;
    }

    /** NAV-4 — if no per-instance / metric-expand / row-expand tab remains, focus the main Monitoring tab. */
    private void focusMonitoringTabIfLastExpand() {
        if (!instanceMonitoringTabs.isEmpty()) return;
        if (!metricExpandTabs.isEmpty()) return;
        if (!rowExpandTabs.isEmpty()) return;
        if (monitoringTab != null && tabs.getTabs().contains(monitoringTab)) {
            tabs.getSelectionModel().select(monitoringTab);
        }
    }

    private void hostRow(String title, String key, javafx.scene.Node body, MetricExpander.Mode mode) {
        if (mode == MetricExpander.Mode.TAB) {
            Tab existing = rowExpandTabs.get(key);
            if (existing != null && tabs.getTabs().contains(existing)) {
                tabs.getSelectionModel().select(existing); return;
            }
            Tab t = new Tab(title, body);
            t.setOnClosed(e -> {
                closeIfAutoCloseable(body);
                rowExpandTabs.remove(key);
                focusMonitoringTabIfLastExpand();
            });
            rowExpandTabs.put(key, t);
            tabs.getTabs().add(t);
            tabs.getSelectionModel().select(t);
        } else {
            javafx.stage.Stage s = new javafx.stage.Stage();
            s.setTitle(title);
            s.setScene(new javafx.scene.Scene(new javafx.scene.control.ScrollPane(body), 720, 520));
            if (getScene() != null && getScene().getWindow() != null) s.initOwner(getScene().getWindow());
            s.setOnHidden(e -> closeIfAutoCloseable(body));
            s.show();
        }
    }

    private static void closeIfAutoCloseable(Object body) {
        if (body instanceof AutoCloseable a) {
            try { a.close(); } catch (Throwable ignored) {}
        }
    }

    /** GRAPH-EXPAND-3/4: open the expand view in a modal stage or a dedicated tab. */
    public void openExpandedMetric(MetricId metric, String connectionId, MetricExpander.Mode mode) {
        if (metric == null || connectionId == null) return;
        MongoConnection conn = connectionStore.get(connectionId);
        String displayName = conn != null ? conn.name() : connectionId;
        if (mode == MetricExpander.Mode.TAB) {
            String key = connectionId + "|" + metric.name();
            Tab existing = metricExpandTabs.get(key);
            if (existing != null && tabs.getTabs().contains(existing)) {
                tabs.getSelectionModel().select(existing);
                return;
            }
            ExpandedMetricView body = new ExpandedMetricView(connectionId, metric, displayName,
                    events, monitoringService);
            Tab t = new Tab("Metric · " + metric.metricName() + " · " + displayName, body);
            t.setOnClosed(e -> {
                body.close();
                metricExpandTabs.remove(key);
                focusMonitoringTabIfLastExpand();
            });
            metricExpandTabs.put(key, t);
            tabs.getTabs().add(t);
            tabs.getSelectionModel().select(t);
        } else {
            ExpandedMetricView body = new ExpandedMetricView(connectionId, metric, displayName,
                    events, monitoringService);
            javafx.stage.Stage s = new javafx.stage.Stage();
            s.setTitle(metric.metricName() + " · " + displayName);
            s.setScene(new javafx.scene.Scene(new javafx.scene.control.ScrollPane(body), 960, 560));
            if (getScene() != null && getScene().getWindow() != null) s.initOwner(getScene().getWindow());
            s.setOnHidden(e -> body.close());
            s.show();
        }
    }

    /** Per-instance monitoring tab — one Tab per connection id, re-selected on duplicate open. */
    private final java.util.Map<String, Tab> instanceMonitoringTabs = new java.util.HashMap<>();

    private void openInstanceMonitoringTab(String connectionId) {
        Tab existing = instanceMonitoringTabs.get(connectionId);
        if (existing != null && tabs.getTabs().contains(existing)) {
            tabs.getSelectionModel().select(existing);
            return;
        }
        com.kubrik.mex.ui.monitoring.InstanceMonitoringTab body =
                new com.kubrik.mex.ui.monitoring.InstanceMonitoringTab(
                        connectionId, events, monitoringService, manager, connectionStore,
                        this::openMonitoringSectionTab, this::openExpandedMetric, rowExpandOpener);
        com.kubrik.mex.model.MongoConnection conn = connectionStore.get(connectionId);
        String title = "Monitoring · " + (conn != null ? conn.name() : connectionId);
        Tab t = new Tab(title, body);
        t.setOnClosed(e -> {
            body.close();
            instanceMonitoringTabs.remove(connectionId);
            focusMonitoringTabIfLastExpand();
        });
        instanceMonitoringTabs.put(connectionId, t);
        tabs.getTabs().add(t);
        tabs.getSelectionModel().select(t);
    }

    /** Per-connection Cluster tab (v2.4 UI-OPS-1) — one Tab per connection id,
     *  re-selected on duplicate open. Body is closed on tab dispose to release
     *  the bus subscriptions held by {@code TopologyPane} / {@code ClusterTab}. */
    private final java.util.Map<String, Tab> clusterTabs = new java.util.HashMap<>();

    /** Keyboard-accelerator entry point. Picks the tree's current connection
     *  context, falling back to the first connected one; if nothing is
     *  connected the status bar surfaces the reason instead of silently no-op'ing. */
    private void openClusterTabForSelectedConnection() {
        String selected = resolveClusterContext();
        if (selected == null) return;
        openClusterTab(selected);
    }

    /** v2.4 UI-OPS-8 — Cmd/Ctrl+Alt+O opens the Cluster tab for the
     *  connection in context and focuses the Ops sub-tab with a
     *  {@code secs_running >= 10} preset so a DBA can jump straight to
     *  long-running ops during an incident. */
    private void openLongRunningOpsView() {
        String selected = resolveClusterContext();
        if (selected == null) return;
        openClusterTab(selected);
        Tab t = clusterTabs.get(selected);
        if (t != null && t.getContent() instanceof com.kubrik.mex.ui.cluster.ClusterTab ct) {
            ct.focusOpsWithLongRunningPreset();
        }
    }

    /** v2.4 UI-OPS-8 — Cmd/Ctrl+Alt+L opens the Logs tab with the
     *  "Audit only" toggle engaged so destructive-action rows are isolated
     *  from connection noise. */
    private void openLogsFilteredToAudit() {
        openLogsTab();
        if (logsView != null) logsView.filterToAudit();
    }

    /** Shared accelerator helper — returns the target connection id (tree
     *  selection → first connected fallback) or {@code null} after pushing
     *  the reason into the status bar. */
    private String resolveClusterContext() {
        String selected = connTree.selectedConnectionId();
        if (selected == null) {
            for (var c : connectionStore.list()) {
                if (manager.state(c.id()).status()
                        == com.kubrik.mex.model.ConnectionState.Status.CONNECTED) {
                    return c.id();
                }
            }
            statusConn.setText("Cluster view: no connected cluster — connect one first.");
            return null;
        }
        return selected;
    }

    private void openClusterTab(String connectionId) {
        Tab existing = clusterTabs.get(connectionId);
        if (existing != null && tabs.getTabs().contains(existing)) {
            tabs.getSelectionModel().select(existing);
            return;
        }
        ClusterTab body = new ClusterTab(connectionId, events, manager, opsAuditDao,
                opsExecutor, balancerHandler, zonesHandler, killOpHandler, rsAdminHandler);
        MongoConnection conn = connectionStore.get(connectionId);
        String title = "Cluster · " + (conn != null ? conn.name() : connectionId);
        Tab t = new Tab(title, body);
        t.setOnClosed(e -> {
            body.close();
            clusterTabs.remove(connectionId);
        });
        clusterTabs.put(connectionId, t);
        tabs.getTabs().add(t);
        tabs.getSelectionModel().select(t);
    }

    private void openMonitoringTab() {
        if (monitoringTab != null && tabs.getTabs().contains(monitoringTab)) {
            tabs.getSelectionModel().select(monitoringTab);
            return;
        }
        if (monitoringView == null) {
            monitoringView = new MonitoringTab(events, monitoringService, manager, connectionStore,
                    database, this::openExpandedMetric, rowExpandOpener);
            monitoringView.onMaximize(this::openMonitoringSectionTab);
            monitoringView.onOpenInstanceTab(this::openInstanceMonitoringTab);
        }
        monitoringTab = new Tab("Monitoring", monitoringView);
        monitoringTab.setOnClosed(e -> monitoringTab = null);
        tabs.getTabs().add(monitoringTab);
        tabs.getSelectionModel().select(monitoringTab);
    }

    private void openMigrationsTab() {
        if (migrationsTab != null && tabs.getTabs().contains(migrationsTab)) {
            tabs.getSelectionModel().select(migrationsTab);
            return;
        }
        if (migrationsView == null) {
            migrationsView = new MigrationsTab(migrationService, connectionStore, manager, events);
        }
        migrationsTab = new Tab("Migrations", migrationsView);
        migrationsTab.setOnClosed(e -> migrationsTab = null);
        tabs.getTabs().add(migrationsTab);
        tabs.getSelectionModel().select(migrationsTab);
    }

    private static MenuItem item(String text, KeyCombination accel, Runnable action) {
        MenuItem mi = new MenuItem(text);
        if (accel != null) mi.setAccelerator(accel);
        mi.setOnAction(e -> action.run());
        return mi;
    }

    private void closeCurrentTab() {
        Tab t = tabs.getSelectionModel().getSelectedItem();
        if (t != null && t.isClosable()) {
            tabs.getTabs().remove(t);
            if (t.getOnClosed() != null) t.getOnClosed().handle(null);
        }
    }

    private void fireFocusedTextAction(String which) {
        if (getScene() == null) return;
        javafx.scene.Node f = getScene().getFocusOwner();
        if (f instanceof javafx.scene.control.TextInputControl tic) {
            switch (which) {
                case "cut" -> tic.cut();
                case "copy" -> tic.copy();
                case "paste" -> tic.paste();
                case "selectAll" -> tic.selectAll();
            }
        }
    }

    private void openLogsTab() {
        if (logsTab != null && tabs.getTabs().contains(logsTab)) {
            tabs.getSelectionModel().select(logsTab);
            return;
        }
        logsTab = new Tab("Logs", logsView);
        logsTab.setOnClosed(e -> logsTab = null);
        tabs.getTabs().add(logsTab);
        tabs.getSelectionModel().select(logsTab);
    }

    private void openCollectionTab(String connectionId, String db, String coll) {
        String key = connectionId + "|" + db + "|" + coll;
        Tab existing = openCollectionTabs.get(key);
        if (existing != null) {
            tabs.getSelectionModel().select(existing);
            return;
        }
        QueryView qv = new QueryView(manager, historyStore);
        qv.setFixedNamespace(connectionId, db, coll);
        Tab t = new Tab(db + "." + coll, qv);
        t.setOnClosed(e -> openCollectionTabs.remove(key));
        openCollectionTabs.put(key, t);
        tabs.getTabs().add(t);
        tabs.getSelectionModel().select(t);
    }

    private void openManageTab() {
        if (manageTab != null && tabs.getTabs().contains(manageTab)) {
            tabs.getSelectionModel().select(manageTab);
            return;
        }
        ConnectionsView cv = new ConnectionsView(manager, connectionStore);
        cv.setOnSelected(id -> {});
        manageTab = new Tab("Connections", cv);
        manageTab.setOnClosed(e -> manageTab = null);
        tabs.getTabs().add(manageTab);
        tabs.getSelectionModel().select(manageTab);
    }

    private void openEditor(MongoConnection existing) {
        ConnectionEditDialog d = new ConnectionEditDialog(manager, manager.crypto(), Optional.ofNullable(existing));
        d.initOwner(getScene().getWindow());
        d.showAndWait().ifPresent(c -> {
            connectionStore.upsert(c);
            connTree.reloadAll();
            if (welcomeView != null) welcomeView.refresh();
        });
    }
}

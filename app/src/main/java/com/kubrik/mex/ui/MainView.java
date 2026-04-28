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
import com.kubrik.mex.ui.backup.BackupsTab;
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

    private Tab backupsTab;
    private BackupsTab backupsView;
    private final com.kubrik.mex.backup.store.BackupPolicyDao backupPolicyDao;
    private final com.kubrik.mex.backup.store.BackupCatalogDao backupCatalogDao;
    private final com.kubrik.mex.backup.store.BackupFileDao backupFileDao;
    private final com.kubrik.mex.backup.store.SinkDao sinkDao;
    private final com.kubrik.mex.backup.verify.CatalogVerifier catalogVerifier;
    private final com.kubrik.mex.backup.runner.RestoreService restoreService;
    private final com.kubrik.mex.backup.pitr.PitrPlanner pitrPlanner;
    private final com.kubrik.mex.backup.rehearse.DrRehearsalReport rehearsalReport;
    private final String callerUser;
    private final String callerHost;

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
                    KillSwitch killSwitch,
                    com.kubrik.mex.backup.store.BackupPolicyDao backupPolicyDao,
                    com.kubrik.mex.backup.store.BackupCatalogDao backupCatalogDao,
                    com.kubrik.mex.backup.store.BackupFileDao backupFileDao,
                    com.kubrik.mex.backup.store.SinkDao sinkDao,
                    com.kubrik.mex.backup.verify.CatalogVerifier catalogVerifier,
                    com.kubrik.mex.backup.runner.RestoreService restoreService,
                    com.kubrik.mex.backup.pitr.PitrPlanner pitrPlanner,
                    com.kubrik.mex.backup.rehearse.DrRehearsalReport rehearsalReport,
                    String callerUser, String callerHost) {
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
        this.backupPolicyDao = backupPolicyDao;
        this.backupCatalogDao = backupCatalogDao;
        this.backupFileDao = backupFileDao;
        this.sinkDao = sinkDao;
        this.catalogVerifier = catalogVerifier;
        this.restoreService = restoreService;
        this.pitrPlanner = pitrPlanner;
        this.rehearsalReport = rehearsalReport;
        this.callerUser = callerUser;
        this.callerHost = callerHost;

        this.connTree = new ConnectionTree(manager, connectionStore, events);
        this.connTree.setLabBadgeProvider(this::resolveLabBadge);
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

        // Welcome tab — v2.6 chip provider folds SecuritySignals into each
        // card; DAOs are lazy-initialised so the welcome screen doesn't
        // pay startup cost until a connection actually has security data.
        com.kubrik.mex.security.cert.CertCacheDao certCacheDao =
                new com.kubrik.mex.security.cert.CertCacheDao(database);
        java.util.function.Function<String, com.kubrik.mex.security.SecuritySignals.Summary>
                signalProvider = cxId -> {
            ensureSecurityDaos();
            return com.kubrik.mex.security.SecuritySignals.compute(
                    cxId, securityBaselineDao, driftAckDao, certCacheDao,
                    System.currentTimeMillis());
        };

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
                this::openEditor,
                signalProvider);
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
                        this::openLogsFilteredToAudit),
                // v2.5 UI-BKP-1 — Backups top-level tab (policies + history).
                item("Backups",
                        new KeyCodeCombination(KeyCode.B,
                                KeyCombination.SHORTCUT_DOWN, KeyCombination.ALT_DOWN),
                        this::openBackupsTab),
                // v2.6 UI-SEC — Security tab for the connection in context.
                item("Security",
                        new KeyCodeCombination(KeyCode.S,
                                KeyCombination.SHORTCUT_DOWN, KeyCombination.ALT_DOWN),
                        this::openSecurityTabForContext),
                // v2.7 UI-MAINT — Maintenance & change-mgmt surfaces
                // (approvals, schema, reconfig, rolling index, compact,
                // parameters, upgrade, drift).
                item("Maintenance",
                        new KeyCodeCombination(KeyCode.M,
                                KeyCombination.SHORTCUT_DOWN, KeyCombination.ALT_DOWN),
                        this::openMaintenanceTabForContext),
                // v2.8.0 UI-LAB — Local Docker sandbox Labs tab.
                // App-singleton (not per-connection) because Labs are
                // created inside the Labs tab itself, not targeted at
                // an existing connection.
                item("Labs",
                        new KeyCodeCombination(KeyCode.L,
                                KeyCombination.SHORTCUT_DOWN, KeyCombination.ALT_DOWN),
                        this::openLabsTab),
                // v2.8.1 UI-K8S — Kubernetes Clusters tab. Gated by
                // the `k8s.enabled` system property so the Alpha gate
                // (milestone §6.1) stays off-by-default until RC.
                item("Clusters",
                        new KeyCodeCombination(KeyCode.K,
                                KeyCombination.SHORTCUT_DOWN, KeyCombination.ALT_DOWN),
                        this::openClustersTab),
                // v2.8.1 Q2.8-N — Local K8s Labs tab. Requires k8s.enabled
                // (reuses the production pipeline) + labs.k8s.enabled to
                // explicitly opt in to the distro-lifecycle flow.
                item("K8s Labs",
                        new KeyCodeCombination(KeyCode.J,
                                KeyCombination.SHORTCUT_DOWN, KeyCombination.ALT_DOWN),
                        this::openK8sLabsTab),
                // v2.8.4 Q2.8.4-A — Cloud credentials manager.
                item("Cloud Credentials",
                        new KeyCodeCombination(KeyCode.D,
                                KeyCombination.SHORTCUT_DOWN, KeyCombination.ALT_DOWN),
                        this::openCloudCredentialsTab),
                // v2.8.4 Q2.8.4-F — Cloud Operations history.
                item("Cloud Operations",
                        new KeyCodeCombination(KeyCode.O,
                                KeyCombination.SHORTCUT_DOWN, KeyCombination.SHIFT_DOWN),
                        this::openCloudOperationsTab));

        // ----- Help -----
        Menu help = new Menu("Help");
        help.getItems().add(item("About Mongo Explorer", null, () ->
                AboutDialog.show(getScene() == null ? null : getScene().getWindow())));

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
    /** Single, reusable placeholder tab shown when the Cluster view is opened
     *  but no connection is available. Cleared on close so the next invocation
     *  builds a fresh one (and so it doesn't linger after a connection lands). */
    private Tab clusterEmptyTab;

    // v2.6 Q2.6-UI — per-connection Security tabs + lazy-init shared DAOs.
    private final java.util.Map<String, Tab> securityTabs = new java.util.HashMap<>();

    // v2.7 UI-MAINT — per-connection Maintenance tab cache + lazy DAOs.
    private final java.util.Map<String, Tab> maintenanceTabs = new java.util.HashMap<>();
    private com.kubrik.mex.maint.approval.ApprovalDao approvalDao;
    private com.kubrik.mex.maint.approval.ApprovalService approvalService;
    private com.kubrik.mex.maint.drift.ConfigSnapshotDao configSnapshotDao;

    // v2.8.0 UI-LAB — Labs tab is a singleton (not per-connection);
    // the lifecycle service + DAOs are constructed on first open.
    private Tab labsTab;
    private com.kubrik.mex.labs.docker.DockerClient labsDocker;
    private com.kubrik.mex.labs.templates.LabTemplateRegistry labsRegistry;
    private com.kubrik.mex.labs.store.LabDeploymentDao labDeploymentDao;
    private com.kubrik.mex.labs.store.LabEventDao labEventDao;
    private com.kubrik.mex.labs.lifecycle.LabLifecycleService labLifecycle;

    // v2.8.1 UI-K8S — Clusters tab. Also a singleton. Lazy-wired on
    // first open because the kubernetes-client library is heavy to
    // initialise and most users won't open the pane every session.
    private Tab clustersTab;
    private com.kubrik.mex.k8s.ui.ClustersPane clustersPane;
    private com.kubrik.mex.k8s.client.KubeClientFactory kubeClientFactory;
    private com.kubrik.mex.k8s.cluster.KubeClusterDao kubeClusterDao;
    private com.kubrik.mex.k8s.cluster.KubeClusterService kubeClusterService;
    private com.kubrik.mex.k8s.cluster.ClusterProbeService kubeProbeService;
    private com.kubrik.mex.k8s.discovery.DiscoveryService kubeDiscoveryService;
    private com.kubrik.mex.k8s.secret.SecretPickupService kubeSecretService;
    private com.kubrik.mex.k8s.portforward.PortForwardAuditDao kubePortForwardAuditDao;
    private com.kubrik.mex.k8s.portforward.PortForwardService kubePortForwardService;
    private com.kubrik.mex.k8s.apply.ProvisioningRecordDao kubeProvisioningDao;
    private com.kubrik.mex.k8s.rollout.RolloutEventDao kubeRolloutEventDao;
    private com.kubrik.mex.k8s.provision.ProvisioningService kubeProvisioningService;
    private com.kubrik.mex.k8s.teardown.TearDownService kubeTearDownService;
    // v2.8.1 Q2.8-N — Local K8s Labs: distro-lifecycle + template
    // catalogue layer on top of the production provisioning path.
    private Tab k8sLabsTab;
    private com.kubrik.mex.labs.k8s.ui.LabK8sPane k8sLabsPane;
    private com.kubrik.mex.labs.k8s.distro.DistroDetector labK8sDetector;
    private com.kubrik.mex.labs.k8s.store.LabK8sClusterDao labK8sClusterDao;
    private com.kubrik.mex.labs.k8s.distro.LocalK8sDistroService labK8sDistroService;
    private com.kubrik.mex.labs.k8s.templates.LabK8sTemplateRegistry labK8sRegistry;
    private com.kubrik.mex.labs.k8s.lifecycle.LabK8sLifecycleService labK8sLifecycle;
    private com.kubrik.mex.security.baseline.SecurityBaselineDao securityBaselineDao;
    private com.kubrik.mex.security.drift.DriftAckDao driftAckDao;
    private com.kubrik.mex.security.cis.CisSuppressionsDao cisSuppressionsDao;
    private com.kubrik.mex.security.audit.AuditIndex auditIndex;
    private com.kubrik.mex.security.EvidenceSigner evidenceSigner;

    /** Keyboard-accelerator entry point. Picks the tree's current connection
     *  context, falling back to the first connected one; if nothing is
     *  connected we now open an explicit empty-state tab (icon + CTA to the
     *  Connections view) rather than silently updating the status bar. */
    private void openClusterTabForSelectedConnection() {
        String selected = resolveClusterContext();
        if (selected == null) {
            openClusterEmptyTab();
            return;
        }
        openClusterTab(selected);
    }

    /** v2.4 UI-OPS-8 — Cmd/Ctrl+Alt+O opens the Cluster tab for the
     *  connection in context and focuses the Ops sub-tab with a
     *  {@code secs_running >= 10} preset so a DBA can jump straight to
     *  long-running ops during an incident. */
    private void openLongRunningOpsView() {
        String selected = resolveClusterContext();
        if (selected == null) {
            openClusterEmptyTab();
            return;
        }
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

    /** Opens (or focuses) a single placeholder Cluster tab that explains there
     *  is no active connection and links the user to the Connections view.
     *  Reuses an existing empty tab rather than stacking duplicates. Also
     *  pushes the reason into the status bar as a secondary signal for
     *  users with a connection-strip visible. */
    private void openClusterEmptyTab() {
        statusConn.setText("Cluster view: no connected cluster — connect one first.");
        if (clusterEmptyTab != null && tabs.getTabs().contains(clusterEmptyTab)) {
            tabs.getSelectionModel().select(clusterEmptyTab);
            return;
        }
        com.kubrik.mex.ui.cluster.ClusterEmptyPane body =
                new com.kubrik.mex.ui.cluster.ClusterEmptyPane(
                        this::openManageTab,
                        () -> openEditor(null));
        Tab t = new Tab("Cluster", body);
        t.setOnClosed(e -> clusterEmptyTab = null);
        clusterEmptyTab = t;
        tabs.getTabs().add(t);
        tabs.getSelectionModel().select(t);
    }

    /**
     * v2.6 Q2.6-UI — Cmd/Ctrl+Alt+S opens a per-connection Security tab
     * with the full sub-tab set: Roles, Audit, Drift, Certificates,
     * Auth, Encryption, CIS. Lazy-initialised DAOs + EvidenceSigner are
     * shared across every Security tab in the session.
     */
    private void openSecurityTabForContext() {
        String selected = resolveClusterContext();
        if (selected == null) return;
        openSecurityTab(selected);
    }

    private void openSecurityTab(String connectionId) {
        Tab existing = securityTabs.get(connectionId);
        if (existing != null && tabs.getTabs().contains(existing)) {
            tabs.getSelectionModel().select(existing);
            return;
        }
        com.kubrik.mex.core.MongoService svc = manager.service(connectionId);
        if (svc == null) {
            statusConn.setText("Security tab: connect the cluster first.");
            return;
        }
        ensureSecurityDaos();

        // Q2.6-K wire-up — resolve the current topology's member list on
        // every probe, so encryption + cert sweeps expand across every
        // replset / shard / mongos host and pick up topology changes
        // without re-opening the tab. EventBus.latestTopology caches the
        // most recent snapshot per connection so the first Refresh sees
        // data without waiting for the next sampler tick.
        java.util.function.Supplier<java.util.List<String>> membersProvider = () -> {
            var snap = events.latestTopology(connectionId);
            return snap == null ? java.util.List.of() : snap.allHosts();
        };

        com.kubrik.mex.ui.security.SecurityTab body =
                new com.kubrik.mex.ui.security.SecurityTab(
                        connectionId, callerUser, svc, membersProvider,
                        securityBaselineDao, driftAckDao, cisSuppressionsDao,
                        auditIndex, evidenceSigner);
        MongoConnection conn = connectionStore.get(connectionId);
        String title = "Security · " + (conn != null ? conn.name() : connectionId);
        Tab t = new Tab(title, body);
        t.setOnClosed(e -> securityTabs.remove(connectionId));
        securityTabs.put(connectionId, t);
        tabs.getTabs().add(t);
        tabs.getSelectionModel().select(t);
    }

    /**
     * v2.7 UI-MAINT — opens a Maintenance tab for the given connection.
     * Lazy-initialises the shared maintenance DAOs + ApprovalService
     * on first use so an install that never touches Maintenance pays
     * nothing at startup.
     */
    private void openMaintenanceTabForContext() {
        String selected = resolveClusterContext();
        if (selected == null) {
            statusConn.setText("Maintenance tab: pick a connected cluster first.");
            return;
        }
        Tab existing = maintenanceTabs.get(selected);
        if (existing != null && tabs.getTabs().contains(existing)) {
            tabs.getSelectionModel().select(existing);
            return;
        }
        com.kubrik.mex.core.MongoService svc = manager.service(selected);
        if (svc == null) {
            statusConn.setText("Maintenance tab: connect the cluster first.");
            return;
        }

        ensureMaintenanceDaos();

        final String cxId = selected;
        com.kubrik.mex.maint.ui.MaintenanceTab body =
                new com.kubrik.mex.maint.ui.MaintenanceTab(
                        approvalService, configSnapshotDao,
                        // clientSupplier: hands the pane the live driver client
                        //                 for the currently-selected connection.
                        () -> {
                            com.kubrik.mex.core.MongoService s = manager.service(cxId);
                            return s == null ? null : s.client();
                        },
                        // connectionIdSupplier: the approvals queue scopes
                        //                      per-connection; snapshot captures
                        //                      stamp connectionId on every row.
                        () -> cxId,
                        // memberOpener: for the rolling-index + compact panes —
                        //               auth-aware direct-connection clients
                        //               reusing credentials + TLS from svc.
                        host -> {
                            com.kubrik.mex.core.MongoService s = manager.service(cxId);
                            if (s == null) throw new IllegalStateException(
                                    "connection " + cxId + " not open");
                            return s.openMemberClient(host, 15_000);
                        });

        MongoConnection conn = connectionStore.get(selected);
        String title = "Maintenance · "
                + (conn != null ? conn.name() : selected);
        Tab t = new Tab(title, body);
        t.setOnClosed(e -> maintenanceTabs.remove(cxId));
        maintenanceTabs.put(selected, t);
        tabs.getTabs().add(t);
        tabs.getSelectionModel().select(t);
    }

    private void ensureMaintenanceDaos() {
        if (approvalService != null) return;
        // The approval JWS piggybacks on the v2.6 evidence key; if
        // Main.java didn't inject it (test harness / early boot), fall
        // through ensureSecurityDaos to lazily construct one.
        ensureSecurityDaos();
        approvalDao = new com.kubrik.mex.maint.approval.ApprovalDao(database);
        com.kubrik.mex.maint.approval.JwsSigner jws =
                new com.kubrik.mex.maint.approval.JwsSigner(evidenceSigner);
        approvalService = new com.kubrik.mex.maint.approval.ApprovalService(
                approvalDao, jws);
        configSnapshotDao = new com.kubrik.mex.maint.drift.ConfigSnapshotDao(database);
    }

    /**
     * v2.8.0 UI-LAB — open the singleton Labs tab. Lazy-init the
     * DockerClient + Lab DAOs + lifecycle service on first use.
     * Also registers the JVM shutdown hook (once) + kicks off the
     * reconciler so rows match the live compose state.
     */
    private void openLabsTab() {
        if (labsTab != null && tabs.getTabs().contains(labsTab)) {
            tabs.getSelectionModel().select(labsTab);
            return;
        }
        ensureLabsWiring();
        com.kubrik.mex.labs.ui.LabsTab body =
                new com.kubrik.mex.labs.ui.LabsTab(labsDocker, labsRegistry,
                        labLifecycle, labDeploymentDao, events);
        labsTab = new Tab("Labs", body);
        labsTab.setOnClosed(e -> labsTab = null);
        tabs.getTabs().add(labsTab);
        tabs.getSelectionModel().select(labsTab);
    }

    private void ensureLabsWiring() {
        if (labLifecycle != null) return;
        labsDocker = new com.kubrik.mex.labs.docker.DockerClient();
        labsRegistry = new com.kubrik.mex.labs.templates.LabTemplateRegistry();
        labsRegistry.loadBuiltins();
        labDeploymentDao = new com.kubrik.mex.labs.store.LabDeploymentDao(database);
        labEventDao = new com.kubrik.mex.labs.store.LabEventDao(database);

        java.nio.file.Path labsDir = com.kubrik.mex.store.AppPaths.dataDir()
                .resolve("labs");
        com.kubrik.mex.labs.lifecycle.LabAutoConnectionWriter connWriter =
                new com.kubrik.mex.labs.lifecycle.LabAutoConnectionWriter(
                        connectionStore);
        labLifecycle = new com.kubrik.mex.labs.lifecycle.LabLifecycleService(
                labsDocker,
                new com.kubrik.mex.labs.templates.ComposeRenderer(),
                new com.kubrik.mex.labs.ports.EphemeralPortAllocator(),
                new com.kubrik.mex.labs.lifecycle.LabHealthWatcher(),
                connWriter,
                labDeploymentDao, labEventDao, events,
                labsDir, mexVersion());

        // Plug in the Q2.8.4-E seeder (cache dir is colocated with
        // labs data so docker compose down -v leaves it alone).
        labLifecycle.setSeedStep(new com.kubrik.mex.labs.seed.SeedRunner(
                new com.kubrik.mex.labs.seed.RemoteSeedFetcher(
                        labsDir.resolve("cache")),
                new com.kubrik.mex.labs.seed.SeedMarker(),
                labEventDao, labsDir, "docker"));

        // One-shot reconcile at first open so CREATING / RUNNING
        // rows that survived an app crash are reclassified.
        new com.kubrik.mex.labs.lifecycle.LabReconciler(
                labsDocker, labDeploymentDao, labEventDao, events,
                labsDir).reconcileAsync();

        // Shutdown hook — honours labs.on_exit (default "stop").
        new com.kubrik.mex.labs.lifecycle.LabAppExitHook(
                labLifecycle, labDeploymentDao,
                com.kubrik.mex.labs.lifecycle.LabAppExitHook.parsePolicy(
                        System.getProperty("labs.on_exit"))).register();

        invalidateLabBadges();
    }

    /**
     * v2.8.1 UI-K8S — open the singleton Clusters tab. Lazy-init the
     * KubeClientFactory + DAOs on first use. Feature-flagged on
     * {@code k8s.enabled}: when the flag is unset the menu item is
     * still visible but the tab renders a feature-disabled explanation
     * so users discover the pane during Alpha without accidentally
     * wiring it into production. (Milestone §6.1.)
     */
    private void openClustersTab() {
        if (clustersTab != null && tabs.getTabs().contains(clustersTab)) {
            tabs.getSelectionModel().select(clustersTab);
            return;
        }
        if (!isK8sEnabled()) {
            Label disabled = new Label(
                    "Kubernetes integration is gated behind the `k8s.enabled` "
                    + "system property during v2.8.1 Alpha. Launch with "
                    + "`-Dk8s.enabled=true` to attach clusters.");
            disabled.setWrapText(true);
            disabled.setPadding(new Insets(24));
            clustersTab = new Tab("Clusters", disabled);
            clustersTab.setOnClosed(e -> clustersTab = null);
            tabs.getTabs().add(clustersTab);
            tabs.getSelectionModel().select(clustersTab);
            return;
        }
        ensureK8sWiring();
        clustersPane = new com.kubrik.mex.k8s.ui.ClustersPane(
                kubeClusterService, events,
                kubeDiscoveryService, kubeSecretService,
                kubePortForwardService, connectionStore,
                kubeProvisioningService,
                kubeProvisioningDao, kubeTearDownService);
        if (cloudCredentialDao == null) {
            cloudCredentialDao =
                    new com.kubrik.mex.k8s.compute.managedpool.CloudCredentialDao(database);
        }
        clustersPane.setCloudCredentialDao(cloudCredentialDao);
        clustersTab = new Tab("Clusters", clustersPane);
        clustersTab.setOnClosed(e -> {
            if (clustersPane != null) clustersPane.close();
            clustersTab = null;
            clustersPane = null;
        });
        tabs.getTabs().add(clustersTab);
        tabs.getSelectionModel().select(clustersTab);
    }

    private void ensureK8sWiring() {
        if (kubeClusterService != null) return;
        kubeClientFactory = new com.kubrik.mex.k8s.client.KubeClientFactory();
        kubeClusterDao = new com.kubrik.mex.k8s.cluster.KubeClusterDao(database);
        kubeProbeService = new com.kubrik.mex.k8s.cluster.ClusterProbeService(kubeClientFactory);
        kubeClusterService = new com.kubrik.mex.k8s.cluster.KubeClusterService(
                kubeClusterDao, kubeClientFactory, kubeProbeService, events);
        kubeDiscoveryService = new com.kubrik.mex.k8s.discovery.DiscoveryService(
                kubeClientFactory, events);
        kubeSecretService = new com.kubrik.mex.k8s.secret.SecretPickupService(kubeClientFactory);
        kubePortForwardAuditDao = new com.kubrik.mex.k8s.portforward.PortForwardAuditDao(database);
        kubePortForwardService = new com.kubrik.mex.k8s.portforward.PortForwardService(
                kubeClientFactory, kubePortForwardAuditDao, events);
        kubeProvisioningDao = new com.kubrik.mex.k8s.apply.ProvisioningRecordDao(database);
        kubeRolloutEventDao = new com.kubrik.mex.k8s.rollout.RolloutEventDao(database);
        // v2.8.4 — managed-pool phase. Cloud-creds DAO is lazy-built;
        // the registry ships three stubs by default so all four
        // strategies execute through the same orchestrator path.
        if (cloudCredentialDao == null) {
            cloudCredentialDao =
                    new com.kubrik.mex.k8s.compute.managedpool.CloudCredentialDao(database);
        }
        com.kubrik.mex.k8s.compute.managedpool.ManagedPoolOperationDao mpOpDao =
                new com.kubrik.mex.k8s.compute.managedpool.ManagedPoolOperationDao(database);
        if (cloudSecretStore == null) {
            com.kubrik.mex.k8s.compute.managedpool.OsKeychainSecretStore os =
                    new com.kubrik.mex.k8s.compute.managedpool.OsKeychainSecretStore();
            cloudSecretStore = os.isAvailable() ? os
                    : new com.kubrik.mex.k8s.compute.managedpool.InMemorySecretStore();
        }
        com.kubrik.mex.k8s.compute.managedpool.ManagedPoolPhaseService mpPhase =
                new com.kubrik.mex.k8s.compute.managedpool.ManagedPoolPhaseService(
                        com.kubrik.mex.k8s.compute.managedpool.ManagedPoolAdapterRegistry
                                .defaultRegistry(cloudSecretStore),
                        cloudCredentialDao, mpOpDao);

        kubeProvisioningService = com.kubrik.mex.k8s.provision.ProvisioningService.wire(
                kubeClientFactory, kubeProvisioningDao, kubeRolloutEventDao, events,
                kubePortForwardService, connectionStore, mpPhase);
        // TearDownService sits parallel to the ProvisioningService — it
        // doesn't own an adapter, just reuses the LiveApplyOpener's
        // delete path. Constructing it here keeps the wiring explicit.
        kubeTearDownService = new com.kubrik.mex.k8s.teardown.TearDownService(
                kubeProvisioningDao, kubeClientFactory,
                new com.kubrik.mex.k8s.apply.ApplyOrchestrator.LiveDispatcher(),
                events);
        // Tear every live forward down on JVM exit so SQLite rows
        // get their closed_at stamp and no dangling listeners leak
        // past app shutdown.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (kubePortForwardService != null) kubePortForwardService.closeAll();
        }, "k8s-pfwd-shutdown"));
    }

    private static boolean isK8sEnabled() {
        // v2.8.4-alpha: K8s integration is on by default. Set
        // -Dk8s.enabled=false to opt out (e.g. minimal builds that
        // don't need the Clusters tab).
        String v = System.getProperty("k8s.enabled", "true");
        return v.equalsIgnoreCase("true") || v.equals("1")
                || v.equalsIgnoreCase("yes");
    }

    private static boolean isLabsK8sEnabled() {
        // v2.8.4-alpha: Local K8s Labs is on by default. Set
        // -Dlabs.k8s.enabled=false to opt out.
        String v = System.getProperty("labs.k8s.enabled", "true");
        return v.equalsIgnoreCase("true") || v.equals("1")
                || v.equalsIgnoreCase("yes");
    }

    /**
     * v2.8.1 Q2.8-N — Open the Local K8s Labs tab. Requires both
     * {@code k8s.enabled} (the production pipeline) and
     * {@code labs.k8s.enabled} (the distro-lifecycle opt-in); a
     * disabled flag renders an explanation pane instead so the menu
     * entry is discoverable without accidentally wiring the CLI
     * shell-outs into a production environment.
     */
    private void openK8sLabsTab() {
        if (k8sLabsTab != null && tabs.getTabs().contains(k8sLabsTab)) {
            tabs.getSelectionModel().select(k8sLabsTab);
            return;
        }
        if (!isK8sEnabled() || !isLabsK8sEnabled()) {
            Label disabled = new Label(
                    "Local K8s Labs requires BOTH -Dk8s.enabled=true "
                    + "(production pipeline) AND -Dlabs.k8s.enabled=true "
                    + "(distro-lifecycle opt-in). Launch with both "
                    + "system properties set to attach a local cluster.");
            disabled.setWrapText(true);
            disabled.setPadding(new Insets(24));
            k8sLabsTab = new Tab("K8s Labs", disabled);
            k8sLabsTab.setOnClosed(e -> k8sLabsTab = null);
            tabs.getTabs().add(k8sLabsTab);
            tabs.getSelectionModel().select(k8sLabsTab);
            return;
        }
        ensureK8sWiring();        // foundation + provisioning services
        ensureLabsK8sWiring();    // distro detector + registry + DAO + lifecycle
        k8sLabsPane = new com.kubrik.mex.labs.k8s.ui.LabK8sPane(
                labK8sDetector, labK8sRegistry, labK8sClusterDao, labK8sLifecycle);
        k8sLabsTab = new Tab("K8s Labs", k8sLabsPane);
        k8sLabsTab.setOnClosed(e -> {
            k8sLabsTab = null;
            k8sLabsPane = null;
        });
        tabs.getTabs().add(k8sLabsTab);
        tabs.getSelectionModel().select(k8sLabsTab);
    }

    private com.kubrik.mex.k8s.compute.managedpool.CloudCredentialDao cloudCredentialDao;
    private com.kubrik.mex.k8s.compute.managedpool.SecretStore cloudSecretStore;
    private Tab cloudCredentialsTab;

    /** v2.8.4 Q2.8.4-A — open the Cloud Credentials manager. */
    private void openCloudCredentialsTab() {
        if (cloudCredentialsTab != null && tabs.getTabs().contains(cloudCredentialsTab)) {
            tabs.getSelectionModel().select(cloudCredentialsTab);
            return;
        }
        if (cloudCredentialDao == null) {
            cloudCredentialDao =
                    new com.kubrik.mex.k8s.compute.managedpool.CloudCredentialDao(database);
        }
        if (cloudSecretStore == null) {
            com.kubrik.mex.k8s.compute.managedpool.OsKeychainSecretStore os =
                    new com.kubrik.mex.k8s.compute.managedpool.OsKeychainSecretStore();
            cloudSecretStore = os.isAvailable() ? os
                    : new com.kubrik.mex.k8s.compute.managedpool.InMemorySecretStore();
        }
        var pane = new com.kubrik.mex.k8s.compute.managedpool.ui.CloudCredentialsPane(
                cloudCredentialDao, cloudSecretStore);
        cloudCredentialsTab = new Tab("Cloud Credentials", pane);
        cloudCredentialsTab.setOnClosed(e -> cloudCredentialsTab = null);
        tabs.getTabs().add(cloudCredentialsTab);
        tabs.getSelectionModel().select(cloudCredentialsTab);
    }

    public com.kubrik.mex.k8s.compute.managedpool.CloudCredentialDao cloudCredentialDaoOrNull() {
        return cloudCredentialDao;
    }

    private com.kubrik.mex.k8s.compute.managedpool.ManagedPoolOperationDao cloudOpsDao;
    private com.kubrik.mex.k8s.compute.managedpool.ui.CloudOperationsPane cloudOpsPane;
    private Tab cloudOpsTab;

    /** v2.8.4 — open the Cloud Operations history pane (read-only
     *  view over managed_pool_operations). */
    private void openCloudOperationsTab() {
        if (cloudOpsTab != null && tabs.getTabs().contains(cloudOpsTab)) {
            tabs.getSelectionModel().select(cloudOpsTab);
            return;
        }
        if (cloudOpsDao == null) {
            cloudOpsDao = new com.kubrik.mex.k8s.compute.managedpool
                    .ManagedPoolOperationDao(database);
        }
        cloudOpsPane = new com.kubrik.mex.k8s.compute.managedpool.ui
                .CloudOperationsPane(cloudOpsDao);
        cloudOpsTab = new Tab("Cloud Operations", cloudOpsPane);
        cloudOpsTab.setOnClosed(e -> {
            if (cloudOpsPane != null) cloudOpsPane.close();
            cloudOpsTab = null;
            cloudOpsPane = null;
        });
        tabs.getTabs().add(cloudOpsTab);
        tabs.getSelectionModel().select(cloudOpsTab);
    }

    private void ensureLabsK8sWiring() {
        if (labK8sLifecycle != null) return;
        labK8sDetector = new com.kubrik.mex.labs.k8s.distro.DistroDetector();
        labK8sClusterDao = new com.kubrik.mex.labs.k8s.store.LabK8sClusterDao(database);
        labK8sRegistry = new com.kubrik.mex.labs.k8s.templates.LabK8sTemplateRegistry();
        labK8sDistroService = new com.kubrik.mex.labs.k8s.distro.LocalK8sDistroService(
                labK8sDetector, labK8sClusterDao, kubeClusterService);
        labK8sLifecycle = new com.kubrik.mex.labs.k8s.lifecycle.LabK8sLifecycleService(
                labK8sDistroService, kubeProvisioningService, kubeClusterService);
        // Orphan reconciliation: any lab_k8s_clusters row that says
        // RUNNING but the CLI can't find the cluster (box rebooted,
        // out-of-band minikube delete) flips to FAILED so the UI
        // stops showing a stale healthy status. Async on a virtual
        // thread so opening the tab isn't blocked by distro probes.
        final var reconciler = labK8sDistroService;
        Thread.ofVirtual().name("k8s-labs-reconcile").start(() -> {
            try {
                int flipped = reconciler.reconcile();
                if (flipped > 0) {
                    org.slf4j.LoggerFactory.getLogger(MainView.class)
                            .info("K8s Labs reconciler flipped {} stale row(s) to FAILED", flipped);
                }
            } catch (Throwable t) {
                org.slf4j.LoggerFactory.getLogger(MainView.class)
                        .warn("K8s Labs reconciler errored: {}", t.toString());
            }
            Platform.runLater(this::invalidateLabBadges);
        });
    }

    // Q2.8-N6 — ConnectionTree "Lab" / "Lab•K8s" chip provenance.
    // Both DAOs are lazy-wired; the resolver just returns null until
    // the matching pane is opened. Cached with a 5 s TTL so rendering
    // the tree doesn't hammer SQLite on every scroll event.
    private volatile long labBadgeCacheAt = 0L;
    private volatile java.util.Map<String, ConnectionTree.LabBadge> labBadgeCache =
            java.util.Map.of();
    private static final long LAB_BADGE_TTL_MS = 5_000L;

    private ConnectionTree.LabBadge resolveLabBadge(String connectionId) {
        if (connectionId == null) return null;
        long now = System.currentTimeMillis();
        if (now - labBadgeCacheAt > LAB_BADGE_TTL_MS) {
            // Stamp the timestamp BEFORE the rebuild so a concurrent
            // burst of cell-render calls (the JavaFX TableView fires
            // updateItem N times per scroll) doesn't all queue up
            // through the rebuild's synchronized lock — only the
            // first wins.
            labBadgeCacheAt = now;
            rebuildLabBadgeCache();
        }
        return labBadgeCache.get(connectionId);
    }

    private synchronized void rebuildLabBadgeCache() {
        java.util.Map<String, ConnectionTree.LabBadge> m = new java.util.HashMap<>();
        if (labDeploymentDao != null) {
            try {
                for (com.kubrik.mex.labs.model.LabDeployment d : labDeploymentDao.listLive()) {
                    d.connectionId().ifPresent(cid ->
                            m.put(cid, new ConnectionTree.LabBadge(
                                    ConnectionTree.LabBadge.Kind.DOCKER_LAB,
                                    "Docker Lab: " + d.displayName())));
                }
            } catch (Throwable ignored) { /* DAO unavailable — fine */ }
        }
        if (labK8sClusterDao != null) {
            try {
                for (var e : labK8sClusterDao.connectionIdToCluster().entrySet()) {
                    var lk = e.getValue();
                    // K8s wins over Docker if somehow both match.
                    m.put(e.getKey(), new ConnectionTree.LabBadge(
                            ConnectionTree.LabBadge.Kind.K8S_LAB,
                            "Lab K8s: " + lk.distro().cliName() + " / "
                                    + lk.identifier()));
                }
            } catch (Throwable ignored) { /* DAO unavailable — fine */ }
        }
        this.labBadgeCache = java.util.Map.copyOf(m);
    }

    /** Invalidate the Lab-badge cache and repaint the tree. Called
     *  after a Lab is created / destroyed so the chip appears or
     *  disappears immediately instead of waiting out the TTL. */
    public void invalidateLabBadges() {
        labBadgeCacheAt = 0L;
        connTree.refreshLabBadges();
    }

    private static String mexVersion() {
        // app/build.gradle.kts sets project.version; at runtime we
        // don't have a clean accessor, so read the manifest or
        // return a build-time placeholder. Good enough for the
        // template_version audit field.
        String pkg = MainView.class.getPackage().getImplementationVersion();
        return pkg == null ? "dev" : pkg;
    }

    /** v2.6 — Main.java shares its pre-constructed security DAOs with
     *  MainView so the app holds a single instance per DAO. Previously
     *  MainView also lazy-built AuditIndex, which meant two DAOs wrote
     *  to {@code audit_native_fts} and Main's CertExpiryScheduler +
     *  MainView's AuditPane shared the SQLite write-lock unnecessarily. */
    public void injectSecurityDaos(
            com.kubrik.mex.security.baseline.SecurityBaselineDao baselineDao,
            com.kubrik.mex.security.drift.DriftAckDao ackDao,
            com.kubrik.mex.security.cis.CisSuppressionsDao suppressionsDao,
            com.kubrik.mex.security.audit.AuditIndex audit,
            com.kubrik.mex.security.EvidenceSigner signer) {
        this.securityBaselineDao = baselineDao;
        this.driftAckDao = ackDao;
        this.cisSuppressionsDao = suppressionsDao;
        this.auditIndex = audit;
        this.evidenceSigner = signer;
    }

    private void ensureSecurityDaos() {
        if (securityBaselineDao != null) return;
        // Fallback path when the injector hasn't been called — keeps
        // tests + early-boot paths compiling. Production wiring goes
        // through injectSecurityDaos from Main.java.
        securityBaselineDao = new com.kubrik.mex.security.baseline.SecurityBaselineDao(database);
        driftAckDao = new com.kubrik.mex.security.drift.DriftAckDao(database);
        cisSuppressionsDao = new com.kubrik.mex.security.cis.CisSuppressionsDao(database);
        auditIndex = new com.kubrik.mex.security.audit.AuditIndex(database);
        evidenceSigner = new com.kubrik.mex.security.EvidenceSigner(
                database, new com.kubrik.mex.core.Crypto());
    }

    private void openBackupsTab() {
        if (backupsTab != null && tabs.getTabs().contains(backupsTab)) {
            tabs.getSelectionModel().select(backupsTab);
            return;
        }
        if (backupsView == null) {
            backupsView = new BackupsTab(backupPolicyDao, backupCatalogDao, backupFileDao,
                    sinkDao, catalogVerifier, restoreService, pitrPlanner, rehearsalReport,
                    callerUser, callerHost, events, manager, connectionStore);
        }
        backupsTab = new Tab("Backups", backupsView);
        backupsTab.setOnClosed(e -> {
            if (backupsView != null) backupsView.close();
            backupsTab = null;
        });
        tabs.getTabs().add(backupsTab);
        tabs.getSelectionModel().select(backupsTab);
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

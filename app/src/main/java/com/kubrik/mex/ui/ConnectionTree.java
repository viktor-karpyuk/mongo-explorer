package com.kubrik.mex.ui;

import com.kubrik.mex.core.ConnectionManager;
import com.kubrik.mex.core.MongoService;
import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.migration.JobId;
import com.kubrik.mex.migration.events.CollectionProgress;
import com.kubrik.mex.migration.events.JobEvent;
import com.kubrik.mex.model.ConnectionState;
import com.kubrik.mex.model.MongoConnection;
import com.kubrik.mex.store.ConnectionStore;
import javafx.application.Platform;
import javafx.geometry.Pos;
import javafx.scene.control.ContextMenu;
import javafx.scene.control.Label;
import javafx.scene.control.MenuItem;
import javafx.scene.control.SeparatorMenuItem;
import javafx.scene.control.TextInputDialog;
import javafx.scene.control.Tooltip;
import javafx.scene.control.TreeCell;
import javafx.scene.control.TreeItem;
import javafx.scene.control.TreeView;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import org.kordamp.ikonli.javafx.FontIcon;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Studio-3T-style left sidebar tree:
 *   Connections
 *     ├ Connection A   ●
 *     │   ├ database1
 *     │   │   ├ collA
 *     │   │   └ collB
 *     │   └ database2
 *     └ Connection B   ●
 */
public class ConnectionTree extends VBox {

    public interface OpenHandler {
        void openCollection(String connectionId, String db, String coll);
        void openManageConnections();
        void openConnectionEditor(MongoConnection existing);
        void openLogs();
        /** Opens the migration wizard, pre-filled with the clicked connection/db/coll (any
         *  may be null depending on where the context menu was invoked). */
        default void openMigrate(String connectionId, String db, String coll) {}
        /** Opens the Monitoring tab. v2.1.0 — see docs/v2/v2.1/functional-spec.md §3.2. */
        default void openMonitoring(String connectionId) {}
        /** Opens the Cluster tab for the selected connection. v2.4.0 UI-OPS-1. */
        default void openCluster(String connectionId) {}
    }

    public record Node(String type, String connectionId, String db, String coll, String label) {
        public static Node connection(String id, String label) { return new Node("conn", id, null, null, label); }
        public static Node db(String id, String d)             { return new Node("db",   id, d,    null, d); }
        public static Node coll(String id, String d, String c) { return new Node("coll", id, d,    c,    c); }
        public static Node loading()                            { return new Node("loading", null, null, null, "loading…"); }
        @Override public String toString() { return label; }
    }

    /** v2.8.1 Q2.8-N6 — Composite Lab/K8s provenance chip shown next
     *  to a connection node. {@link Kind#DOCKER_LAB} means the
     *  connection was created by a v2.8.0 Docker Lab; {@link Kind#K8S_LAB}
     *  means a v2.8.1 Local K8s Lab provisioned a Mongo deployment
     *  that this connection points to. */
    public record LabBadge(Kind kind, String tooltip) {
        public enum Kind { DOCKER_LAB, K8S_LAB }
    }

    /** Resolves a {@link LabBadge} for a given connection id, or
     *  returns {@code null} if the connection has no Lab provenance.
     *  Called on the FX thread during cell rendering — implementations
     *  must be fast (cache in front of any DB lookups). */
    public interface LabBadgeProvider {
        LabBadge forConnection(String connectionId);
    }

    public enum SortOrder {
        AZ("A → Z", String.CASE_INSENSITIVE_ORDER),
        ZA("Z → A", String.CASE_INSENSITIVE_ORDER.reversed());

        final String label;
        final Comparator<String> comparator;
        SortOrder(String label, Comparator<String> comparator) {
            this.label = label;
            this.comparator = comparator;
        }
        SortOrder next() { return values()[(ordinal() + 1) % values().length]; }
    }

    private final ConnectionManager manager;
    private final ConnectionStore store;
    private final EventBus events;
    private final TreeView<Node> tree = new TreeView<>();
    private OpenHandler openHandler;
    private volatile LabBadgeProvider labBadgeProvider;
    private final Map<String, TreeItem<Node>> connectionItems = new HashMap<>();
    private SortOrder sortOrder = SortOrder.AZ;

    // OBS-4 — live migration-progress badges. Populated from JobEvent.Progress, cleared on
    // Completed / Failed / Cancelled. Keyed by the *target* namespace — that's where the writes
    // (and the progress the user cares about) land.
    private record BadgeKey(String connectionId, String db, String coll) {}
    private record BadgeState(JobId jobId, String status, long docsCopied, long docsTotal) {}
    private final Map<BadgeKey, BadgeState> liveBadges = new ConcurrentHashMap<>();
    private final Map<JobId, String> jobTargetConn = new ConcurrentHashMap<>();
    private final AtomicBoolean refreshScheduled = new AtomicBoolean(false);

    public ConnectionTree(ConnectionManager manager, ConnectionStore store, EventBus events) {
        this.manager = manager;
        this.store = store;
        this.events = events;

        Label title = new Label("Connections");
        title.setStyle("-fx-font-size: 13px; -fx-font-weight: bold; -fx-padding: 8 12 6 12;");

        FontIcon plus = new FontIcon("fth-plus");
        plus.setIconSize(14);
        javafx.scene.control.Button add = new javafx.scene.control.Button();
        add.setGraphic(plus);
        add.setTooltip(new javafx.scene.control.Tooltip("New connection"));
        add.setStyle("-fx-background-color: transparent; -fx-padding: 4 8 4 8;");
        add.setOnAction(e -> { if (openHandler != null) openHandler.openConnectionEditor(null); });

        FontIcon mgr = new FontIcon("fth-list");
        mgr.setIconSize(14);
        javafx.scene.control.Button manage = new javafx.scene.control.Button();
        manage.setGraphic(mgr);
        manage.setTooltip(new javafx.scene.control.Tooltip("Manage connections"));
        manage.setStyle("-fx-background-color: transparent; -fx-padding: 4 8 4 8;");
        manage.setOnAction(e -> { if (openHandler != null) openHandler.openManageConnections(); });

        FontIcon logIcon = new FontIcon("fth-file-text");
        logIcon.setIconSize(14);
        javafx.scene.control.Button logs = new javafx.scene.control.Button();
        logs.setGraphic(logIcon);
        logs.setTooltip(new javafx.scene.control.Tooltip("Connection log"));
        logs.setStyle("-fx-background-color: transparent; -fx-padding: 4 8 4 8;");
        logs.setOnAction(e -> { if (openHandler != null) openHandler.openLogs(); });

        FontIcon sortIcon = new FontIcon("fth-bar-chart-2");
        sortIcon.setIconSize(14);
        javafx.scene.control.Button sortBtn = new javafx.scene.control.Button();
        sortBtn.setGraphic(sortIcon);
        sortBtn.setTooltip(new javafx.scene.control.Tooltip("Sort: " + sortOrder.label));
        sortBtn.setStyle("-fx-background-color: transparent; -fx-padding: 4 8 4 8;");
        sortBtn.setOnAction(e -> {
            sortOrder = sortOrder.next();
            sortBtn.setTooltip(new javafx.scene.control.Tooltip("Sort: " + sortOrder.label));
            resortAllCollections();
        });

        FontIcon ref = new FontIcon("fth-refresh-cw");
        ref.setIconSize(14);
        javafx.scene.control.Button refresh = new javafx.scene.control.Button();
        refresh.setGraphic(ref);
        refresh.setTooltip(new javafx.scene.control.Tooltip("Reload"));
        refresh.setStyle("-fx-background-color: transparent; -fx-padding: 4 8 4 8;");
        refresh.setOnAction(e -> reloadAll());

        for (javafx.scene.control.Button b : new javafx.scene.control.Button[]{add, manage, sortBtn, logs, refresh}) {
            applyToolbarHover(b);
        }

        Region sp = new Region();
        HBox.setHgrow(sp, Priority.ALWAYS);
        HBox toolbar = new HBox(2, title, sp, add, manage, sortBtn, logs, refresh);
        toolbar.setAlignment(Pos.CENTER_LEFT);
        toolbar.setStyle("-fx-background-color: #f9fafb; -fx-border-color: #e5e7eb; -fx-border-width: 0 0 1 0;");

        tree.setShowRoot(false);
        tree.setCellFactory(tv -> new ConnCell());
        ContextMenu ctx = buildContextMenu();
        tree.setContextMenu(ctx);
        // Select the row under the pointer AND rebuild the menu items before showing.
        tree.addEventFilter(javafx.scene.input.ContextMenuEvent.CONTEXT_MENU_REQUESTED, e -> {
            javafx.scene.Node n = e.getPickResult().getIntersectedNode();
            while (n != null && !(n instanceof TreeCell)) n = n.getParent();
            if (n instanceof TreeCell<?> cell && cell.getTreeItem() != null) {
                @SuppressWarnings("unchecked")
                TreeItem<Node> ti = (TreeItem<Node>) cell.getTreeItem();
                tree.getSelectionModel().select(ti);
            }
            rebuildContextMenuItems(ctx);
        });
        tree.setOnMouseClicked(e -> {
            if (e.getClickCount() == 2) activateSelected();
        });
        // Enter on the selected row mirrors double-click: connect to the
        // selected cluster (or toggle expand if already connected), and open
        // the collection tab when the selection is a collection node.
        tree.addEventFilter(javafx.scene.input.KeyEvent.KEY_PRESSED, e -> {
            if (e.getCode() == javafx.scene.input.KeyCode.ENTER) {
                activateSelected();
                e.consume();
            }
        });

        VBox.setVgrow(tree, Priority.ALWAYS);
        getChildren().addAll(toolbar, tree);
        setPrefWidth(280);

        events.onState(s -> Platform.runLater(() -> {
            TreeItem<Node> item = connectionItems.get(s.connectionId());
            if (item != null) {
                tree.refresh();
                if (s.status() == ConnectionState.Status.CONNECTED && item.getChildren().isEmpty()) {
                    loadDatabases(item, s.connectionId());
                    item.setExpanded(true);
                } else if (s.status() == ConnectionState.Status.DISCONNECTED) {
                    item.getChildren().clear();
                }
            }
        }));

        events.onJob(this::onJobEvent);

        reloadAll();
    }

    /** OBS-4 — translate migration events into per-collection badge state and schedule a cell
     *  refresh. Runs on the bus thread; state is mutated via concurrent maps so we can repaint
     *  safely from the FX thread. */
    private void onJobEvent(JobEvent e) {
        switch (e) {
            case JobEvent.Started started -> {
                String targetConn = started.spec().target().connectionId();
                if (targetConn != null) jobTargetConn.put(started.jobId(), targetConn);
            }
            case JobEvent.Progress prog -> {
                String targetConn = jobTargetConn.get(prog.jobId());
                if (targetConn == null) return;
                for (CollectionProgress cp : prog.snapshot().perCollection()) {
                    String ns = cp.target();
                    if (ns == null) continue;
                    int dot = ns.indexOf('.');
                    if (dot <= 0 || dot == ns.length() - 1) continue;
                    BadgeKey key = new BadgeKey(targetConn, ns.substring(0, dot), ns.substring(dot + 1));
                    liveBadges.put(key, new BadgeState(
                            prog.jobId(), cp.status(), cp.docsCopied(), cp.docsTotal()));
                }
                scheduleBadgeRefresh();
            }
            case JobEvent.Completed c -> clearJobBadges(c.jobId());
            case JobEvent.Failed f    -> clearJobBadges(f.jobId());
            case JobEvent.Cancelled x -> clearJobBadges(x.jobId());
            default -> {} // LogLine / StatusChanged don't affect badges
        }
    }

    private void clearJobBadges(JobId jobId) {
        liveBadges.entrySet().removeIf(entry -> jobId.equals(entry.getValue().jobId()));
        jobTargetConn.remove(jobId);
        scheduleBadgeRefresh();
    }

    private void scheduleBadgeRefresh() {
        if (refreshScheduled.compareAndSet(false, true)) {
            Platform.runLater(() -> {
                refreshScheduled.set(false);
                tree.refresh();
            });
        }
    }

    private static void applyToolbarHover(javafx.scene.control.Button btn) {
        btn.setOnMouseEntered(e -> btn.setStyle("-fx-background-color: #e5e7eb; -fx-padding: 4 8 4 8; -fx-background-radius: 4; -fx-cursor: hand;"));
        btn.setOnMouseExited(e -> btn.setStyle("-fx-background-color: transparent; -fx-padding: 4 8 4 8;"));
    }

    public void setOpenHandler(OpenHandler h) { this.openHandler = h; }

    /** Inject a Lab provenance resolver; repaints the tree so the new
     *  chips appear immediately. Passing {@code null} hides all chips. */
    public void setLabBadgeProvider(LabBadgeProvider p) {
        this.labBadgeProvider = p;
        refreshLabBadges();
    }

    /** Force-repaint connection rows after the badge provider's backing
     *  state has mutated (e.g. a Lab was created / destroyed). Safe to
     *  call from any thread. */
    public void refreshLabBadges() {
        Platform.runLater(tree::refresh);
    }

    /** Returns the connection id currently selected in the tree (either a
     *  connection row or any of its descendants), or {@code null} if no
     *  connection context is selected. */
    public String selectedConnectionId() {
        TreeItem<Node> sel = tree.getSelectionModel().getSelectedItem();
        if (sel == null) return null;
        Node n = sel.getValue();
        return n == null ? null : n.connectionId;
    }

    public void reloadAll() {
        TreeItem<Node> root = new TreeItem<>(Node.connection("root", "root"));
        connectionItems.clear();
        for (MongoConnection c : store.list()) {
            TreeItem<Node> ci = new TreeItem<>(Node.connection(c.id(), c.name()));
            connectionItems.put(c.id(), ci);
            if (manager.state(c.id()).status() == ConnectionState.Status.CONNECTED) {
                loadDatabases(ci, c.id());
            }
            root.getChildren().add(ci);
        }
        tree.setRoot(root);
    }

    private void loadDatabases(TreeItem<Node> connItem, String connectionId) {
        MongoService svc = manager.service(connectionId);
        if (svc == null) return;
        connItem.getChildren().setAll(new TreeItem<>(Node.loading()));
        Thread.startVirtualThread(() -> {
            try {
                List<String> dbs = svc.listDatabaseNames();
                dbs.sort(String.CASE_INSENSITIVE_ORDER);
                Platform.runLater(() -> {
                    connItem.getChildren().clear();
                    for (String d : dbs) {
                        TreeItem<Node> dbItem = new TreeItem<>(Node.db(connectionId, d));
                        dbItem.getChildren().add(new TreeItem<>(Node.loading()));
                        dbItem.expandedProperty().addListener((o, a, b) -> {
                            if (b && dbItem.getChildren().size() == 1
                                    && "loading".equals(dbItem.getChildren().get(0).getValue().type)) {
                                loadCollections(dbItem, connectionId, d);
                            }
                        });
                        connItem.getChildren().add(dbItem);
                    }
                });
            } catch (Exception ex) {
                Platform.runLater(() -> connItem.getChildren().clear());
            }
        });
    }

    private void loadCollections(TreeItem<Node> dbItem, String connectionId, String db) {
        MongoService svc = manager.service(connectionId);
        if (svc == null) return;
        Thread.startVirtualThread(() -> {
            try {
                List<String> colls = svc.listCollectionNames(db);
                colls.sort(sortOrder.comparator);
                Platform.runLater(() -> {
                    dbItem.getChildren().clear();
                    for (String c : colls) dbItem.getChildren().add(new TreeItem<>(Node.coll(connectionId, db, c)));
                });
            } catch (Exception ex) {
                Platform.runLater(() -> dbItem.getChildren().clear());
            }
        });
    }

    /** Re-sort collection nodes in-place under every expanded database node. */
    private void resortAllCollections() {
        TreeItem<Node> root = tree.getRoot();
        if (root == null) return;
        for (TreeItem<Node> connItem : root.getChildren()) {
            for (TreeItem<Node> dbItem : connItem.getChildren()) {
                if (!dbItem.isExpanded()) continue;
                List<TreeItem<Node>> collItems = new java.util.ArrayList<>(dbItem.getChildren());
                if (collItems.isEmpty() || !"coll".equals(collItems.get(0).getValue().type)) continue;
                collItems.sort(Comparator.comparing(ti -> ti.getValue().label, sortOrder.comparator));
                dbItem.getChildren().setAll(collItems);
            }
        }
    }

    // Cached menu items — created once, wired once, reused across context menu invocations.
    private final MenuItem miConnect = new MenuItem("Connect");
    private final MenuItem miDisconnect = new MenuItem("Disconnect");
    private final MenuItem miDisconnectAll = new MenuItem("Disconnect all");
    private final MenuItem miEdit = new MenuItem("Edit connection…");
    private final MenuItem miDuplicate = new MenuItem("Duplicate connection");
    private final MenuItem miDelete = new MenuItem("Delete connection");
    private final MenuItem miNewConn = new MenuItem("New connection…");
    private final MenuItem miRefresh = new MenuItem("Reload tree");
    private final MenuItem miReloadDbs = new MenuItem("Reload databases");
    private final MenuItem miCreateDb = new MenuItem("Create database…");
    private final MenuItem miServerInfo = new MenuItem("Server status…");
    private final MenuItem miCopyUri = new MenuItem("Copy connection URI");
    private final MenuItem miNewColl = new MenuItem("New collection…");
    private final MenuItem miDropDb = new MenuItem("Drop database");
    private final MenuItem miRunCmd = new MenuItem("Run command…");
    private final MenuItem miUsers = new MenuItem("Users…");
    private final MenuItem miOpenColl = new MenuItem("Open");
    private final MenuItem miRenameColl = new MenuItem("Rename collection…");
    private final MenuItem miDropColl = new MenuItem("Drop collection");
    private final MenuItem miIndexes = new MenuItem("Indexes…");
    private final MenuItem miMigrate = new MenuItem("Migrate…");
    private final MenuItem miMonitor = new MenuItem("Monitor this connection");
    private final MenuItem miCluster = new MenuItem("Open cluster view…");

    private void rebuildContextMenuItems(ContextMenu m) {
        TreeItem<Node> sel = tree.getSelectionModel().getSelectedItem();
        String t = sel == null ? "" : sel.getValue().type;
        boolean isConn = "conn".equals(t);
        boolean isDb = "db".equals(t);
        boolean isColl = "coll".equals(t);
        boolean connected = isConn && manager.state(sel.getValue().connectionId).status() == ConnectionState.Status.CONNECTED;

        java.util.List<MenuItem> items = new java.util.ArrayList<>();
        if (isConn) {
            items.add(connected ? miDisconnect : miConnect);
            items.add(miDisconnectAll);
            if (connected) {
                items.add(new SeparatorMenuItem());
                items.add(miReloadDbs);
                items.add(miCreateDb);
                items.add(miServerInfo);
                items.add(miMigrate);
                items.add(miMonitor);
                items.add(miCluster);
            }
            items.add(new SeparatorMenuItem());
            items.add(miEdit);
            items.add(miDuplicate);
            items.add(miCopyUri);
            items.add(miDelete);
        } else if (isDb) {
            items.add(miNewColl);
            items.add(miUsers);
            items.add(miMigrate);
            items.add(miDropDb);
            items.add(miRunCmd);
        } else if (isColl) {
            items.add(miOpenColl);
            items.add(miRenameColl);
            items.add(miIndexes);
            items.add(miMigrate);
            items.add(miDropColl);
        }
        if (!items.isEmpty()) items.add(new SeparatorMenuItem());
        items.add(miNewConn);
        items.add(miRefresh);
        m.getItems().setAll(items);
    }

    private ContextMenu buildContextMenu() {
        ContextMenu m = new ContextMenu();

        miConnect.setOnAction(e -> withSel(n -> manager.connect(n.connectionId)));
        miDisconnect.setOnAction(e -> withSel(n -> { manager.disconnect(n.connectionId); reloadAll(); }));
        miEdit.setOnAction(e -> withSel(n -> {
            if (openHandler != null) openHandler.openConnectionEditor(store.get(n.connectionId));
        }));
        miDisconnectAll.setOnAction(e -> {
            for (MongoConnection c : store.list()) manager.disconnect(c.id());
            reloadAll();
        });
        miDuplicate.setOnAction(e -> withSel(n -> {
            MongoConnection orig = store.get(n.connectionId);
            if (orig == null) return;
            MongoConnection copy = new MongoConnection(
                    null, orig.name() + " (copy)", orig.mode(), orig.uri(),
                    orig.connectionType(), orig.hosts(), orig.srvHost(),
                    orig.authMode(), orig.username(), orig.encPassword(), orig.authDb(),
                    orig.gssapiServiceName(), orig.awsSessionToken(),
                    orig.tlsEnabled(), orig.tlsCaFile(), orig.tlsClientCertFile(),
                    orig.encTlsClientCertPassword(), orig.tlsAllowInvalidHostnames(), orig.tlsAllowInvalidCertificates(),
                    orig.sshEnabled(), orig.sshHost(), orig.sshPort(), orig.sshUser(), orig.sshAuthMode(),
                    orig.encSshPassword(), orig.sshKeyFile(), orig.encSshKeyPassphrase(),
                    orig.proxyType(), orig.proxyHost(), orig.proxyPort(), orig.proxyUser(), orig.encProxyPassword(),
                    orig.replicaSetName(), orig.readPreference(), orig.defaultDb(), orig.appName(), orig.manualUriOptions(),
                    0L, 0L);
            store.upsert(copy);
            reloadAll();
        }));
        miReloadDbs.setOnAction(e -> withSel(n -> {
            TreeItem<Node> ci = connectionItems.get(n.connectionId);
            if (ci != null) loadDatabases(ci, n.connectionId);
        }));
        miCreateDb.setOnAction(e -> withSel(n -> {
            javafx.scene.control.Dialog<String[]> d = new javafx.scene.control.Dialog<>();
            d.initOwner(getScene().getWindow());
            d.setTitle("Create database");
            javafx.scene.control.TextField dbName = new javafx.scene.control.TextField();
            javafx.scene.control.TextField cName = new javafx.scene.control.TextField();
            javafx.scene.layout.GridPane g = new javafx.scene.layout.GridPane();
            g.setHgap(8); g.setVgap(6); g.setPadding(new javafx.geometry.Insets(12));
            g.addRow(0, new javafx.scene.control.Label("Database"), dbName);
            g.addRow(1, new javafx.scene.control.Label("First collection"), cName);
            d.getDialogPane().setContent(g);
            d.getDialogPane().getButtonTypes().addAll(javafx.scene.control.ButtonType.OK, javafx.scene.control.ButtonType.CANCEL);
            d.setResultConverter(bt -> bt == javafx.scene.control.ButtonType.OK
                    ? new String[]{dbName.getText(), cName.getText()} : null);
            d.showAndWait().ifPresent(arr -> {
                if (arr[0].isBlank() || arr[1].isBlank()) return;
                String connId = n.connectionId;
                Thread.startVirtualThread(() -> {
                    try {
                        manager.service(connId).createCollection(arr[0], arr[1]);
                        Platform.runLater(() -> reloadDbsFor(connId));
                    } catch (Exception ex) {
                        Platform.runLater(() -> UiHelpers.error(getScene().getWindow(), ex.getMessage()));
                    }
                });
            });
        }));
        miServerInfo.setOnAction(e -> withSel(n -> {
            var svc = manager.service(n.connectionId);
            if (svc == null) {
                UiHelpers.error(getScene().getWindow(), "Connect to " + n.label + " first.");
                return;
            }
            ClusterInfoDialog.show(getScene().getWindow(), svc, n.label);
        }));
        miCopyUri.setOnAction(e -> withSel(n -> {
            MongoConnection c = store.get(n.connectionId);
            if (c == null) return;
            String uri = com.kubrik.mex.core.ConnectionUriBuilder.build(c, manager.crypto());
            javafx.scene.input.ClipboardContent cc = new javafx.scene.input.ClipboardContent();
            cc.putString(uri);
            javafx.scene.input.Clipboard.getSystemClipboard().setContent(cc);
        }));
        miDelete.setOnAction(e -> withSel(n -> {
            MongoConnection c = store.get(n.connectionId);
            if (c == null) return;
            if (UiHelpers.confirm(getScene().getWindow(), "Delete connection \"" + c.name() + "\"?")) {
                manager.disconnect(c.id());
                store.delete(c.id());
                reloadAll();
            }
        }));
        miNewConn.setOnAction(e -> { if (openHandler != null) openHandler.openConnectionEditor(null); });
        miRefresh.setOnAction(e -> reloadAll());

        miNewColl.setOnAction(e -> {
            TreeItem<Node> sel = tree.getSelectionModel().getSelectedItem();
            if (sel == null) return;
            Node n = sel.getValue();
            TreeItem<Node> dbItem = "db".equals(n.type) ? sel : sel.getParent();
            String db = n.db;
            String connId = n.connectionId;
            UiHelpers.styledInput(getScene().getWindow(),
                    "New Collection",
                    "Create a new collection in " + db,
                    "Collection name", "").ifPresent(name -> {
                Thread.startVirtualThread(() -> {
                    try {
                        manager.service(connId).createCollection(db, name);
                        Platform.runLater(() -> {
                            dbItem.getChildren().add(new TreeItem<>(Node.coll(connId, db, name)));
                            dbItem.setExpanded(true);
                        });
                    } catch (Exception ex) {
                        Platform.runLater(() -> UiHelpers.error(getScene().getWindow(), ex.getMessage()));
                    }
                });
            });
        });
        miDropDb.setOnAction(e -> withSel(n -> {
            if (UiHelpers.confirmTyped(getScene().getWindow(), n.db)) {
                String connId = n.connectionId, db = n.db;
                Thread.startVirtualThread(() -> {
                    try {
                        manager.service(connId).dropDatabase(db);
                        Platform.runLater(this::reloadAll);
                    } catch (Exception ex) {
                        Platform.runLater(() -> UiHelpers.error(getScene().getWindow(), ex.getMessage()));
                    }
                });
            }
        }));
        miUsers.setOnAction(e -> withSel(n -> {
            UserManagementDialog d = new UserManagementDialog(manager.service(n.connectionId), n.db);
            d.initOwner(getScene().getWindow());
            d.showAndWait();
        }));
        miRunCmd.setOnAction(e -> withSel(n -> {
            TextInputDialog d = new TextInputDialog("{ \"ping\": 1 }");
            d.initOwner(getScene().getWindow());
            d.setTitle("Run command on " + n.db); d.setHeaderText("Command JSON");
            d.showAndWait().ifPresent(cmd -> {
                String connId = n.connectionId, db = n.db;
                Thread.startVirtualThread(() -> {
                    try {
                        var res = manager.service(connId).runCommand(db, cmd);
                        String json = res.toJson(MongoService.JSON_RELAXED);
                        Platform.runLater(() -> UiHelpers.info(getScene().getWindow(), "Result", json));
                    } catch (Exception ex) {
                        Platform.runLater(() -> UiHelpers.error(getScene().getWindow(), ex.getMessage()));
                    }
                });
            });
        }));

        miOpenColl.setOnAction(e -> withSel(n -> {
            if (openHandler != null) openHandler.openCollection(n.connectionId, n.db, n.coll);
        }));
        miMigrate.setOnAction(e -> withSel(n -> {
            if (openHandler != null) openHandler.openMigrate(n.connectionId, n.db, n.coll);
        }));
        miMonitor.setOnAction(e -> withSel(n -> {
            if (openHandler != null) openHandler.openMonitoring(n.connectionId);
        }));
        miCluster.setOnAction(e -> withSel(n -> {
            if (openHandler != null) openHandler.openCluster(n.connectionId);
        }));
        miRenameColl.setOnAction(e -> withSel(n -> {
            TextInputDialog d = new TextInputDialog(n.coll);
            d.initOwner(getScene().getWindow());
            d.setTitle("Rename collection"); d.setHeaderText("New name");
            d.showAndWait().ifPresent(name -> {
                String connId = n.connectionId, db = n.db, old = n.coll;
                Thread.startVirtualThread(() -> {
                    try {
                        manager.service(connId).renameCollection(db, old, name);
                        Platform.runLater(this::reloadAll);
                    } catch (Exception ex) {
                        Platform.runLater(() -> UiHelpers.error(getScene().getWindow(), ex.getMessage()));
                    }
                });
            });
        }));
        miDropColl.setOnAction(e -> {
            TreeItem<Node> sel = tree.getSelectionModel().getSelectedItem();
            if (sel == null) return;
            Node n = sel.getValue();
            if (UiHelpers.confirmTyped(getScene().getWindow(), n.coll)) {
                String connId = n.connectionId, db = n.db, coll = n.coll;
                Thread.startVirtualThread(() -> {
                    try {
                        manager.service(connId).dropCollection(db, coll);
                        Platform.runLater(() -> sel.getParent().getChildren().remove(sel));
                    } catch (Exception ex) {
                        Platform.runLater(() -> UiHelpers.error(getScene().getWindow(), ex.getMessage()));
                    }
                });
            }
        });
        miIndexes.setOnAction(e -> withSel(n -> {
            IndexDialog d = new IndexDialog(manager.service(n.connectionId), n.db, n.coll);
            d.initOwner(getScene().getWindow());
            d.showAndWait();
        }));

        return m;
    }

    private void reloadDbsFor(String connectionId) {
        TreeItem<Node> ci = connectionItems.get(connectionId);
        if (ci != null) loadDatabases(ci, connectionId);
    }

    private void withSel(java.util.function.Consumer<Node> f) {
        TreeItem<Node> sel = tree.getSelectionModel().getSelectedItem();
        if (sel != null) f.accept(sel.getValue());
    }

    /**
     * Default activation for the currently selected tree row, shared by
     * double-click and the Enter shortcut: connect a disconnected cluster,
     * toggle expand on a connected cluster, or open a collection tab.
     */
    private void activateSelected() {
        TreeItem<Node> sel = tree.getSelectionModel().getSelectedItem();
        if (sel == null) return;
        Node n = sel.getValue();
        if ("coll".equals(n.type) && openHandler != null) {
            openHandler.openCollection(n.connectionId, n.db, n.coll);
        } else if ("conn".equals(n.type)) {
            if (manager.state(n.connectionId).status() != ConnectionState.Status.CONNECTED) {
                beginConnectWithFeedback(n.connectionId, n.label);
            } else {
                sel.setExpanded(!sel.isExpanded());
            }
        } else if (sel.getValue() != null && !sel.isLeaf()) {
            sel.setExpanded(!sel.isExpanded());
        }
    }

    /**
     * Kick off a connect with non-blocking feedback: the tree row's status
     * dot already turns amber for CONNECTING and green for CONNECTED, so we
     * don't gate the rest of the app behind a modal. We just show a
     * one-shot toast on success or an error dialog on failure. The
     * subscription is registered before the connect starts and detaches
     * itself the first time a terminal state for this id arrives; a safety
     * timeout detaches it if the connect never publishes one (e.g., a
     * synchronous throw inside ConnectionUriBuilder).
     */
    /** Public entry-point so WelcomeView (and any other connect surface)
     *  can reuse the same modal + Cancel feedback path. */
    public void connectWithFeedback(String connId, String label) {
        beginConnectWithFeedback(connId, label);
    }

    private void beginConnectWithFeedback(String connId, String label) {
        javafx.stage.Window owner = getScene() == null ? null : getScene().getWindow();
        UiHelpers.ConnectingHandle dlg = UiHelpers.connectingDialog(owner, label,
                () -> manager.cancelConnect(connId));
        EventBus.Subscription[] holder = new EventBus.Subscription[1];
        AtomicBoolean done = new AtomicBoolean(false);
        Runnable detach = () -> {
            if (done.compareAndSet(false, true)) {
                try { if (holder[0] != null) holder[0].close(); } catch (Exception ignored) {}
            }
        };
        holder[0] = events.onState(s -> {
            if (!connId.equals(s.connectionId())) return;
            if (s.status() == ConnectionState.Status.CONNECTING) return;
            if (done.get()) return;
            Platform.runLater(() -> {
                if (done.get()) return;
                detach.run();
                dlg.close();
                if (s.status() == ConnectionState.Status.CONNECTED) {
                    UiHelpers.toast(owner,
                            "Connected to " + label
                                    + (s.serverVersion() == null ? "" : "  ·  mongo " + s.serverVersion()),
                            2500);
                }
                // ERROR / DISCONNECTED: MainView's global listener
                // surfaces the modal (DISCONNECTED is the cancel path —
                // no error needed).
            });
        });
        dlg.show();
        manager.connect(connId);
        // Safety net: if no terminal state arrives within the connect
        // timeout window, drop the listener AND the dialog so neither
        // lingers forever. Slightly longer than the 35 s connect
        // watchdog in ConnectionManager so a real timeout still
        // surfaces through the listener first.
        javafx.animation.PauseTransition guard =
                new javafx.animation.PauseTransition(javafx.util.Duration.seconds(45));
        guard.setOnFinished(e -> { detach.run(); dlg.close(); });
        guard.play();
    }

    /** Cell with a leading icon / status dot. */
    private class ConnCell extends TreeCell<Node> {
        @Override
        protected void updateItem(Node item, boolean empty) {
            super.updateItem(item, empty);
            if (empty || item == null) {
                setText(null); setGraphic(null); return;
            }
            String iconLit;
            javafx.scene.paint.Color color = javafx.scene.paint.Color.web("#374151");
            boolean showStatusDot = false;
            switch (item.type) {
                case "conn" -> {
                    ConnectionState s = manager.state(item.connectionId);
                    iconLit = "fth-server";
                    color = javafx.scene.paint.Color.web(UiHelpers.colorFor(s.status()));
                    showStatusDot = s.status() == ConnectionState.Status.CONNECTED;
                }
                case "db" -> iconLit = "fth-database";
                case "coll" -> iconLit = "fth-grid";
                case "loading" -> iconLit = "fth-clock";
                default -> iconLit = "fth-circle";
            }
            FontIcon icon = new FontIcon(iconLit);
            icon.setIconSize(13);
            icon.setIconColor(color);
            javafx.scene.Node graphic;
            if (showStatusDot) {
                javafx.scene.shape.Circle dot = new javafx.scene.shape.Circle(4);
                dot.setFill(javafx.scene.paint.Color.web("#16a34a"));
                javafx.scene.layout.StackPane iconWithDot = new javafx.scene.layout.StackPane(icon, dot);
                javafx.scene.layout.StackPane.setAlignment(dot, Pos.BOTTOM_RIGHT);
                iconWithDot.setPrefSize(16, 16);
                graphic = iconWithDot;
            } else {
                graphic = icon;
            }
            Label lbl = new Label(item.label);
            lbl.setMinWidth(Region.USE_PREF_SIZE);
            HBox row = new HBox(6, graphic, lbl);
            row.setAlignment(Pos.CENTER_LEFT);
            row.setMinWidth(Region.USE_PREF_SIZE);

            // Q2.8-N6 — Lab provenance chip for connection rows.
            if ("conn".equals(item.type)) {
                LabBadgeProvider p = labBadgeProvider;
                LabBadge lb = p == null ? null : p.forConnection(item.connectionId);
                if (lb != null) row.getChildren().add(buildLabBadge(lb));
            }

            // OBS-4 — migration-progress badge for target-side collection nodes.
            if ("coll".equals(item.type)) {
                BadgeState bs = liveBadges.get(new BadgeKey(item.connectionId, item.db, item.coll));
                if (bs != null) row.getChildren().add(buildMigrationBadge(bs));
            }

            setGraphic(row);
            setText(null);
            setMinWidth(Region.USE_PREF_SIZE);
            setPrefWidth(Region.USE_COMPUTED_SIZE);
        }

        private Label buildMigrationBadge(BadgeState bs) {
            String text;
            if (bs.docsTotal() > 0) {
                int pct = (int) Math.min(100, (bs.docsCopied() * 100L) / bs.docsTotal());
                text = "▶ " + pct + "%";
            } else if (bs.docsCopied() > 0) {
                text = "▶ " + compactDocs(bs.docsCopied());
            } else {
                text = "▶";
            }
            Label badge = new Label(text);
            badge.setStyle(
                    "-fx-background-color: #dbeafe;"
                  + "-fx-text-fill: #1d4ed8;"
                  + "-fx-font-size: 10px;"
                  + "-fx-font-weight: bold;"
                  + "-fx-background-radius: 8;"
                  + "-fx-padding: 0 6 0 6;");
            badge.setTooltip(new Tooltip(
                    "Migration " + (bs.status() == null ? "running" : bs.status().toLowerCase())
                  + ": " + bs.docsCopied()
                  + (bs.docsTotal() > 0 ? " / " + bs.docsTotal() : "")
                  + " documents"));
            return badge;
        }

        private Label buildLabBadge(LabBadge lb) {
            String text = lb.kind() == LabBadge.Kind.K8S_LAB ? "Lab•K8s" : "Lab";
            String bg, fg;
            if (lb.kind() == LabBadge.Kind.K8S_LAB) {
                // violet — matches the K8s pane's accent
                bg = "#ede9fe"; fg = "#6d28d9";
            } else {
                // amber — matches the Docker Labs pane's accent
                bg = "#fef3c7"; fg = "#92400e";
            }
            Label badge = new Label(text);
            badge.setStyle(
                    "-fx-background-color: " + bg + ";"
                  + "-fx-text-fill: " + fg + ";"
                  + "-fx-font-size: 10px;"
                  + "-fx-font-weight: bold;"
                  + "-fx-background-radius: 8;"
                  + "-fx-padding: 0 6 0 6;");
            if (lb.tooltip() != null && !lb.tooltip().isBlank()) {
                badge.setTooltip(new Tooltip(lb.tooltip()));
            }
            return badge;
        }

        private String compactDocs(long n) {
            if (n < 1_000) return Long.toString(n);
            if (n < 1_000_000) return (n / 1_000) + "k";
            if (n < 1_000_000_000L) return (n / 1_000_000) + "M";
            return (n / 1_000_000_000L) + "B";
        }
    }
}

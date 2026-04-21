package com.kubrik.mex.ui;

import com.kubrik.mex.core.ConnectionManager;
import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.model.MongoConnection;
import com.kubrik.mex.store.ConnectionStore;
import com.kubrik.mex.store.HistoryStore;
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

    private final ConnectionTree connTree;
    private final TabPane tabs = new TabPane();
    private final Map<String, Tab> openCollectionTabs = new HashMap<>();
    private Tab manageTab;
    private Tab historyTab;
    private Tab logsTab;
    private LogsView logsView;
    private WelcomeView welcomeView;

    private final Label statusServer = new Label("");
    private final Label statusConn = new Label("Ready");

    public MainView(ConnectionManager manager,
                    ConnectionStore connectionStore,
                    HistoryStore historyStore,
                    EventBus events) {
        this.manager = manager;
        this.connectionStore = connectionStore;
        this.historyStore = historyStore;
        this.events = events;

        this.connTree = new ConnectionTree(manager, connectionStore, events);
        this.connTree.setOpenHandler(new ConnectionTree.OpenHandler() {
            @Override public void openCollection(String connectionId, String db, String coll) {
                openCollectionTab(connectionId, db, coll);
            }
            @Override public void openManageConnections() { openManageTab(); }
            @Override public void openConnectionEditor(MongoConnection existing) { openEditor(existing); }
            @Override public void openLogs() { openLogsTab(); }
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

        // Status bar
        HBox status = new HBox(12, statusConn, new Label("·"), statusServer);
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
        MenuItem manageConns = item("Manage Connections", new KeyCodeCombination(KeyCode.M, KeyCombination.SHORTCUT_DOWN),
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

        // ----- Help -----
        Menu help = new Menu("Help");
        help.getItems().add(item("About Mongo Explorer", null, () ->
                UiHelpers.info(getScene().getWindow(), "Mongo Explorer",
                        "A simple MongoDB explorer.\nBuilt with JavaFX + AtlantaFX.")));

        bar.getMenus().addAll(file, edit, view, connection, help);
        return bar;
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

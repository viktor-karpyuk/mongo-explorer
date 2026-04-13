package com.kubrik.mex.ui;

import com.kubrik.mex.core.ConnectionManager;
import com.kubrik.mex.core.MongoService;
import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Button;
import javafx.scene.control.ContextMenu;
import javafx.scene.control.Label;
import javafx.scene.control.MenuItem;
import javafx.scene.control.SplitPane;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextInputDialog;
import javafx.scene.control.TreeItem;
import javafx.scene.control.TreeView;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import org.bson.Document;

import java.util.function.BiConsumer;

public class ExplorerView extends VBox {

    public record Node(String type, String dbName, String collName) {
        public static Node root() { return new Node("root", null, null); }
        public static Node db(String d) { return new Node("db", d, null); }
        public static Node coll(String d, String c) { return new Node("coll", d, c); }
        @Override public String toString() {
            return switch (type) {
                case "root" -> "(connection)";
                case "db" -> dbName;
                case "coll" -> collName;
                default -> "?";
            };
        }
    }

    private final ConnectionManager manager;
    private String connectionId;
    private final TreeView<Node> tree = new TreeView<>();
    private final TextArea details = new TextArea();
    private BiConsumer<String, String> onQuery = (a, b) -> {};

    public ExplorerView(ConnectionManager manager) {
        this.manager = manager;
        setPadding(new Insets(16));
        setSpacing(12);

        Label title = new Label("Explorer");
        title.setStyle("-fx-font-size: 20px; -fx-font-weight: bold;");

        Button refresh = UiHelpers.iconButton("fth-refresh-cw", "Refresh");
        refresh.setOnAction(e -> refresh());
        Region sp = new Region(); HBox.setHgrow(sp, Priority.ALWAYS);
        HBox toolbar = new HBox(8, title, sp, refresh);
        toolbar.setAlignment(Pos.CENTER_LEFT);

        details.setEditable(false);
        details.setStyle("-fx-font-family: 'Menlo','Monaco',monospace; -fx-font-size: 12px;");

        SplitPane split = new SplitPane(tree, details);
        split.setDividerPositions(0.35);
        VBox.setVgrow(split, Priority.ALWAYS);

        tree.setShowRoot(false);
        tree.getSelectionModel().selectedItemProperty().addListener((o, a, b) -> showDetails(b));
        tree.setContextMenu(buildContextMenu());

        getChildren().addAll(toolbar, split);
    }

    public void setOnQuery(BiConsumer<String, String> c) { this.onQuery = c; }
    public String currentConnection() { return connectionId; }

    public void setConnection(String id) {
        this.connectionId = id;
        refresh();
    }

    public void refresh() {
        if (connectionId == null) { tree.setRoot(null); details.clear(); return; }
        MongoService svc = manager.service(connectionId);
        if (svc == null) { tree.setRoot(null); details.setText("Not connected."); return; }
        TreeItem<Node> root = new TreeItem<>(Node.root());
        root.setExpanded(true);
        Thread.startVirtualThread(() -> {
            try {
                var dbs = svc.listDatabaseNames();
                dbs.sort(String.CASE_INSENSITIVE_ORDER);
                Platform.runLater(() -> {
                    for (String d : dbs) {
                        TreeItem<Node> dbItem = new TreeItem<>(Node.db(d));
                        dbItem.getChildren().add(new TreeItem<>(new Node("loading", d, null)));
                        dbItem.expandedProperty().addListener((o, a, b) -> {
                            if (b && dbItem.getChildren().size() == 1
                                    && "loading".equals(dbItem.getChildren().get(0).getValue().type())) {
                                loadCollections(svc, dbItem, d);
                            }
                        });
                        root.getChildren().add(dbItem);
                    }
                    tree.setRoot(root);
                });
            } catch (Exception e) {
                Platform.runLater(() -> details.setText("Error: " + e.getMessage()));
            }
        });
    }

    private void loadCollections(MongoService svc, TreeItem<Node> dbItem, String db) {
        Thread.startVirtualThread(() -> {
            try {
                var colls = svc.listCollectionNames(db);
                colls.sort(String.CASE_INSENSITIVE_ORDER);
                Platform.runLater(() -> {
                    dbItem.getChildren().clear();
                    for (String c : colls) {
                        dbItem.getChildren().add(new TreeItem<>(Node.coll(db, c)));
                    }
                });
            } catch (Exception e) {
                Platform.runLater(() -> details.setText("Error: " + e.getMessage()));
            }
        });
    }

    private void showDetails(TreeItem<Node> item) {
        if (item == null) { details.clear(); return; }
        Node n = item.getValue();
        MongoService svc = manager.service(connectionId);
        if (svc == null) return;
        Thread.startVirtualThread(() -> {
            try {
                String text;
                if ("db".equals(n.type)) {
                    Document s = svc.dbStats(n.dbName);
                    text = "Database: " + n.dbName + "\n\n" + s.toJson(MongoService.JSON_RELAXED);
                } else if ("coll".equals(n.type)) {
                    Document s = svc.collStats(n.dbName, n.collName);
                    text = "Collection: " + n.dbName + "." + n.collName + "\n\n" + s.toJson(MongoService.JSON_RELAXED);
                } else return;
                Platform.runLater(() -> details.setText(text));
            } catch (Exception e) {
                Platform.runLater(() -> details.setText("Error: " + e.getMessage()));
            }
        });
    }

    private ContextMenu buildContextMenu() {
        ContextMenu m = new ContextMenu();
        MenuItem query = new MenuItem("Query…");
        MenuItem newColl = new MenuItem("New collection…");
        MenuItem renameColl = new MenuItem("Rename collection…");
        MenuItem dropColl = new MenuItem("Drop collection");
        MenuItem dropDb = new MenuItem("Drop database");
        MenuItem indexes = new MenuItem("Indexes…");
        MenuItem runCmd = new MenuItem("Run command…");

        m.setOnShowing(e -> {
            TreeItem<Node> sel = tree.getSelectionModel().getSelectedItem();
            boolean isDb = sel != null && "db".equals(sel.getValue().type);
            boolean isColl = sel != null && "coll".equals(sel.getValue().type);
            query.setVisible(isColl);
            newColl.setVisible(isDb);
            renameColl.setVisible(isColl);
            dropColl.setVisible(isColl);
            indexes.setVisible(isColl);
            dropDb.setVisible(isDb);
            runCmd.setVisible(isDb);
        });

        query.setOnAction(e -> {
            TreeItem<Node> s = tree.getSelectionModel().getSelectedItem();
            if (s != null) onQuery.accept(s.getValue().dbName, s.getValue().collName);
        });
        newColl.setOnAction(e -> {
            TreeItem<Node> s = tree.getSelectionModel().getSelectedItem();
            String db = s.getValue().dbName;
            UiHelpers.styledInput(getScene().getWindow(),
                    "New Collection",
                    "Create a new collection in " + db,
                    "Collection name", "").ifPresent(name -> {
                Thread.startVirtualThread(() -> {
                    try {
                        manager.service(connectionId).createCollection(db, name);
                        Platform.runLater(() -> {
                            s.getChildren().add(new TreeItem<>(Node.coll(db, name)));
                            s.setExpanded(true);
                        });
                    } catch (Exception ex) {
                        Platform.runLater(() -> UiHelpers.error(getScene().getWindow(), ex.getMessage()));
                    }
                });
            });
        });
        renameColl.setOnAction(e -> {
            TreeItem<Node> s = tree.getSelectionModel().getSelectedItem();
            TextInputDialog d = new TextInputDialog(s.getValue().collName);
            d.setTitle("Rename collection"); d.setHeaderText("New name");
            d.initOwner(getScene().getWindow());
            d.showAndWait().ifPresent(name -> {
                String dbName = s.getValue().dbName, oldName = s.getValue().collName;
                Thread.startVirtualThread(() -> {
                    try {
                        manager.service(connectionId).renameCollection(dbName, oldName, name);
                        Platform.runLater(this::refresh);
                    } catch (Exception ex) {
                        Platform.runLater(() -> UiHelpers.error(getScene().getWindow(), ex.getMessage()));
                    }
                });
            });
        });
        dropColl.setOnAction(e -> {
            TreeItem<Node> s = tree.getSelectionModel().getSelectedItem();
            if (UiHelpers.confirmTyped(getScene().getWindow(), s.getValue().collName)) {
                String dbName = s.getValue().dbName, collName = s.getValue().collName;
                Thread.startVirtualThread(() -> {
                    try {
                        manager.service(connectionId).dropCollection(dbName, collName);
                        Platform.runLater(() -> s.getParent().getChildren().remove(s));
                    } catch (Exception ex) {
                        Platform.runLater(() -> UiHelpers.error(getScene().getWindow(), ex.getMessage()));
                    }
                });
            }
        });
        dropDb.setOnAction(e -> {
            TreeItem<Node> s = tree.getSelectionModel().getSelectedItem();
            if (UiHelpers.confirmTyped(getScene().getWindow(), s.getValue().dbName)) {
                String dbName = s.getValue().dbName;
                Thread.startVirtualThread(() -> {
                    try {
                        manager.service(connectionId).dropDatabase(dbName);
                        Platform.runLater(this::refresh);
                    } catch (Exception ex) {
                        Platform.runLater(() -> UiHelpers.error(getScene().getWindow(), ex.getMessage()));
                    }
                });
            }
        });
        indexes.setOnAction(e -> {
            TreeItem<Node> s = tree.getSelectionModel().getSelectedItem();
            IndexDialog d = new IndexDialog(manager.service(connectionId), s.getValue().dbName, s.getValue().collName);
            d.initOwner(getScene().getWindow());
            d.showAndWait();
        });
        runCmd.setOnAction(e -> {
            TreeItem<Node> s = tree.getSelectionModel().getSelectedItem();
            TextInputDialog d = new TextInputDialog("{ \"ping\": 1 }");
            d.setTitle("Run command on " + s.getValue().dbName);
            d.setHeaderText("Command JSON");
            d.initOwner(getScene().getWindow());
            d.showAndWait().ifPresent(cmd -> {
                String dbName = s.getValue().dbName;
                Thread.startVirtualThread(() -> {
                    try {
                        Document res = manager.service(connectionId).runCommand(dbName, cmd);
                        Platform.runLater(() -> UiHelpers.info(getScene().getWindow(), "Result",
                                res.toJson(MongoService.JSON_RELAXED)));
                    } catch (Exception ex) {
                        Platform.runLater(() -> UiHelpers.error(getScene().getWindow(), ex.getMessage()));
                    }
                });
            });
        });

        m.getItems().addAll(query, newColl, renameColl, indexes, dropColl, dropDb, runCmd);
        return m;
    }
}

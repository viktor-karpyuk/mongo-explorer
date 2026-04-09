package com.kubrik.mex.ui;

import com.kubrik.mex.core.ConnectionManager;
import com.kubrik.mex.model.ConnectionState;
import com.kubrik.mex.model.MongoConnection;
import com.kubrik.mex.store.ConnectionStore;
import javafx.application.Platform;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;

import java.util.Optional;
import java.util.function.Consumer;

public class ConnectionsView extends VBox {

    private final ConnectionManager manager;
    private final ConnectionStore store;
    private final TableView<MongoConnection> table = new TableView<>();
    private Consumer<String> onSelected = id -> {};

    public ConnectionsView(ConnectionManager manager, ConnectionStore store) {
        this.manager = manager;
        this.store = store;
        setPadding(new Insets(16));
        setSpacing(12);

        Label title = new Label("Connections");
        title.getStyleClass().add("title-2");
        title.setStyle("-fx-font-size: 20px; -fx-font-weight: bold;");

        Button add = new Button("Add connection");
        Button edit = UiHelpers.iconButton("fth-edit-2", "Edit");
        Button del = UiHelpers.iconButton("fth-trash-2", "Delete");
        Button connect = UiHelpers.iconButton("fth-play", "Connect");
        Button disconnect = UiHelpers.iconButton("fth-square", "Disconnect");
        Button open = UiHelpers.iconButton("fth-arrow-right", "Open in Explorer");

        Region spacer = new Region();
        HBox.setHgrow(spacer, Priority.ALWAYS);
        HBox toolbar = new HBox(8, add, spacer, connect, disconnect, open, edit, del);
        toolbar.setAlignment(Pos.CENTER_LEFT);

        TableColumn<MongoConnection, String> statusDotCol = new TableColumn<>("");
        statusDotCol.setCellValueFactory(c -> {
            ConnectionState s = manager.state(c.getValue().id());
            return new SimpleStringProperty("●");
        });
        statusDotCol.setCellFactory(col -> new javafx.scene.control.TableCell<>() {
            @Override protected void updateItem(String item, boolean empty) {
                super.updateItem(item, empty);
                if (empty || item == null) { setText(null); setGraphic(null); return; }
                MongoConnection mc = getTableView().getItems().get(getIndex());
                ConnectionState s = manager.state(mc.id());
                setText("●");
                setStyle("-fx-text-fill: " + UiHelpers.colorFor(s.status()) + "; -fx-font-size: 16px; -fx-alignment: center;");
            }
        });
        statusDotCol.setPrefWidth(36);
        statusDotCol.setMaxWidth(36);
        statusDotCol.setSortable(false);

        TableColumn<MongoConnection, String> nameCol = new TableColumn<>("Name");
        nameCol.setCellValueFactory(c -> new SimpleStringProperty(c.getValue().name()));
        nameCol.setPrefWidth(220);

        TableColumn<MongoConnection, String> uriCol = new TableColumn<>("Target");
        uriCol.setCellValueFactory(c -> {
            MongoConnection v = c.getValue();
            if ("URI".equals(v.mode())) return new SimpleStringProperty(redact(v.uri()));
            String hosts = "DNS_SRV".equals(v.connectionType()) ? v.srvHost() : v.hosts();
            String user = v.username() == null || v.username().isBlank() ? "" : v.username() + "@";
            return new SimpleStringProperty(user + (hosts == null ? "" : hosts));
        });
        uriCol.setPrefWidth(420);

        TableColumn<MongoConnection, String> statusCol = new TableColumn<>("Status");
        statusCol.setCellValueFactory(c -> {
            ConnectionState s = manager.state(c.getValue().id());
            return new SimpleStringProperty(s.status().name() +
                    (s.serverVersion() != null ? " · " + s.serverVersion() : "") +
                    (s.lastError() != null ? " · " + s.lastError() : ""));
        });
        statusCol.setPrefWidth(280);

        table.getColumns().addAll(statusDotCol, nameCol, uriCol, statusCol);
        // Horizontal scroll: don't auto-shrink columns to fit table width.
        table.setColumnResizePolicy(TableView.UNCONSTRAINED_RESIZE_POLICY);
        table.setItems(FXCollections.observableArrayList(store.list()));
        table.setRowFactory(tv -> {
            javafx.scene.control.TableRow<MongoConnection> row = new javafx.scene.control.TableRow<>();
            row.setOnMouseClicked(e -> {
                if (e.getClickCount() == 2 && !row.isEmpty()) {
                    MongoConnection c = row.getItem();
                    if (manager.state(c.id()).status() != ConnectionState.Status.CONNECTED) {
                        manager.connect(c.id());
                    }
                    onSelected.accept(c.id());
                }
            });
            return row;
        });
        VBox.setVgrow(table, Priority.ALWAYS);

        add.setOnAction(e -> openEdit(null));
        edit.setOnAction(e -> selected().ifPresent(this::openEdit));
        del.setOnAction(e -> selected().ifPresent(c -> {
            if (UiHelpers.confirm(getScene().getWindow(), "Delete connection \"" + c.name() + "\"?")) {
                manager.disconnect(c.id());
                store.delete(c.id());
                refresh();
            }
        }));
        connect.setOnAction(e -> selected().ifPresent(c -> manager.connect(c.id())));
        disconnect.setOnAction(e -> selected().ifPresent(c -> manager.disconnect(c.id())));
        open.setOnAction(e -> selected().ifPresent(c -> {
            if (manager.state(c.id()).status() != ConnectionState.Status.CONNECTED) manager.connect(c.id());
            onSelected.accept(c.id());
        }));

        getChildren().addAll(title, toolbar, table);
    }

    public void setOnSelected(Consumer<String> c) { this.onSelected = c; }

    public void refresh() {
        Platform.runLater(() -> {
            MongoConnection sel = table.getSelectionModel().getSelectedItem();
            table.setItems(FXCollections.observableArrayList(store.list()));
            if (sel != null) {
                for (MongoConnection c : table.getItems()) {
                    if (c.id().equals(sel.id())) { table.getSelectionModel().select(c); break; }
                }
            }
            table.refresh();
        });
    }

    private Optional<MongoConnection> selected() {
        return Optional.ofNullable(table.getSelectionModel().getSelectedItem());
    }

    private void openEdit(MongoConnection existing) {
        ConnectionEditDialog d = new ConnectionEditDialog(manager, manager.crypto(), Optional.ofNullable(existing));
        d.initOwner(getScene().getWindow());
        d.showAndWait().ifPresent(c -> {
            store.upsert(c);
            refresh();
        });
    }

    private static String redact(String uri) {
        return uri.replaceAll("(://[^:]+:)([^@]+)(@)", "$1****$3");
    }
}

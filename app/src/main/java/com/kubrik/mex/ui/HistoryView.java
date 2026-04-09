package com.kubrik.mex.ui;

import com.kubrik.mex.store.HistoryStore;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.geometry.Insets;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.VBox;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class HistoryView extends VBox {

    private final HistoryStore store;
    private final QueryView queryView;
    private String connectionId;
    private final TableView<HistoryStore.Entry> table = new TableView<>();
    private static final DateTimeFormatter FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());

    public HistoryView(HistoryStore store, QueryView queryView) {
        this.store = store;
        this.queryView = queryView;
        setPadding(new Insets(16));
        setSpacing(12);

        Label title = new Label("Query history");
        title.setStyle("-fx-font-size: 20px; -fx-font-weight: bold;");

        Button reload = new Button("Refresh");
        Button reuse = new Button("Load into Query");
        HBox toolbar = new HBox(8, title, new javafx.scene.layout.Region(), reload, reuse);

        TableColumn<HistoryStore.Entry, String> when = new TableColumn<>("When");
        when.setCellValueFactory(c -> new SimpleStringProperty(FMT.format(Instant.ofEpochMilli(c.getValue().createdAt()))));
        when.setPrefWidth(160);
        TableColumn<HistoryStore.Entry, String> kind = new TableColumn<>("Kind");
        kind.setCellValueFactory(c -> new SimpleStringProperty(c.getValue().kind()));
        kind.setPrefWidth(80);
        TableColumn<HistoryStore.Entry, String> ns = new TableColumn<>("Namespace");
        ns.setCellValueFactory(c -> new SimpleStringProperty(c.getValue().dbName() + "." + c.getValue().collName()));
        ns.setPrefWidth(220);
        TableColumn<HistoryStore.Entry, String> body = new TableColumn<>("Body");
        body.setCellValueFactory(c -> new SimpleStringProperty(c.getValue().body()));
        body.setPrefWidth(500);
        table.getColumns().addAll(when, kind, ns, body);

        VBox.setVgrow(table, Priority.ALWAYS);
        getChildren().addAll(toolbar, table);

        reload.setOnAction(e -> refresh());
        reuse.setOnAction(e -> {
            HistoryStore.Entry sel = table.getSelectionModel().getSelectedItem();
            if (sel != null) queryView.prefill(sel.dbName(), sel.collName(), sel.body());
        });
    }

    public void setConnection(String id) {
        this.connectionId = id;
        refresh();
    }

    public void refresh() {
        if (connectionId == null) { table.setItems(FXCollections.observableArrayList()); return; }
        table.setItems(FXCollections.observableArrayList(store.recent(connectionId, 200)));
    }
}

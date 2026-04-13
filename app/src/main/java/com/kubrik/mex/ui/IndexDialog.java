package com.kubrik.mex.ui;

import com.kubrik.mex.core.MongoService;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Button;
import javafx.scene.control.ButtonType;
import javafx.scene.control.CheckBox;
import javafx.scene.control.Dialog;
import javafx.scene.control.Label;
import javafx.scene.control.Separator;
import javafx.scene.control.Spinner;
import javafx.scene.control.SpinnerValueFactory;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import org.bson.Document;

/**
 * Studio-3T-style index manager: list existing indexes with all properties,
 * create with full options (unique, sparse, TTL, partial filter, name, background).
 */
public class IndexDialog extends Dialog<Void> {

    private final MongoService svc;
    private final String db;
    private final String coll;
    private final TableView<Document> table = new TableView<>();

    public IndexDialog(MongoService svc, String db, String coll) {
        this.svc = svc;
        this.db = db;
        this.coll = coll;
        setTitle("Index Manager · " + db + "." + coll);
        setResizable(true);

        /* ========== Existing indexes table ========== */
        Label listTitle = new Label("Existing indexes");
        listTitle.setStyle("-fx-font-size: 14px; -fx-font-weight: bold;");

        TableColumn<Document, String> nameCol = new TableColumn<>("Name");
        nameCol.setCellValueFactory(c -> new SimpleStringProperty(str(c.getValue(), "name")));
        nameCol.setPrefWidth(180);

        TableColumn<Document, String> keyCol = new TableColumn<>("Key");
        keyCol.setCellValueFactory(c -> new SimpleStringProperty(str(c.getValue(), "key")));
        keyCol.setPrefWidth(200);

        TableColumn<Document, String> uniqCol = new TableColumn<>("Unique");
        uniqCol.setCellValueFactory(c -> new SimpleStringProperty(str(c.getValue(), "unique")));
        uniqCol.setPrefWidth(60);

        TableColumn<Document, String> sparseCol = new TableColumn<>("Sparse");
        sparseCol.setCellValueFactory(c -> new SimpleStringProperty(str(c.getValue(), "sparse")));
        sparseCol.setPrefWidth(60);

        TableColumn<Document, String> ttlCol = new TableColumn<>("TTL (s)");
        ttlCol.setCellValueFactory(c -> new SimpleStringProperty(str(c.getValue(), "expireAfterSeconds")));
        ttlCol.setPrefWidth(70);

        TableColumn<Document, String> partialCol = new TableColumn<>("Partial filter");
        partialCol.setCellValueFactory(c -> new SimpleStringProperty(str(c.getValue(), "partialFilterExpression")));
        partialCol.setPrefWidth(180);

        TableColumn<Document, String> bgCol = new TableColumn<>("Background");
        bgCol.setCellValueFactory(c -> new SimpleStringProperty(str(c.getValue(), "background")));
        bgCol.setPrefWidth(80);

        table.getColumns().addAll(nameCol, keyCol, uniqCol, sparseCol, ttlCol, partialCol, bgCol);
        table.setColumnResizePolicy(TableView.UNCONSTRAINED_RESIZE_POLICY);
        VBox.setVgrow(table, Priority.ALWAYS);

        // Details pane on selection
        TextArea detailArea = new TextArea();
        detailArea.setEditable(false);
        detailArea.setPrefRowCount(5);
        detailArea.setStyle("-fx-font-family: 'Menlo','Monaco',monospace; -fx-font-size: 11px;");
        table.getSelectionModel().selectedItemProperty().addListener((o, a, b) -> {
            detailArea.setText(b == null ? "" : b.toJson(MongoService.JSON_RELAXED));
        });

        Button dropBtn = new Button("Drop selected index");
        dropBtn.setStyle("-fx-text-fill: #dc2626;");
        dropBtn.setOnAction(e -> dropSelected());

        VBox listSection = new VBox(8, listTitle, table, detailArea, dropBtn);

        /* ========== Create new index form ========== */
        Label createTitle = new Label("Create new index");
        createTitle.setStyle("-fx-font-size: 14px; -fx-font-weight: bold;");

        TextField keysField = new TextField("{ \"field\": 1 }");
        keysField.setPromptText("{ \"field\": 1, \"other\": -1 }");
        keysField.setStyle("-fx-font-family: 'Menlo','Monaco',monospace;");

        TextField nameField = new TextField();
        nameField.setPromptText("(auto-generated if blank)");

        CheckBox uniqueCb = new CheckBox("Unique");
        CheckBox sparseCb = new CheckBox("Sparse");
        CheckBox bgCb = new CheckBox("Background build");

        Label ttlLabel = new Label("TTL (seconds)");
        Spinner<Integer> ttlSpinner = new Spinner<>(new SpinnerValueFactory.IntegerSpinnerValueFactory(0, Integer.MAX_VALUE, 0));
        ttlSpinner.setEditable(true);
        ttlSpinner.setPrefWidth(120);
        Label ttlHint = new Label("0 = no expiration. Only works on date fields.");
        ttlHint.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 10px;");

        TextField partialField = new TextField();
        partialField.setPromptText("{ \"status\": \"active\" }");
        partialField.setStyle("-fx-font-family: 'Menlo','Monaco',monospace;");
        Label partialHint = new Label("Only index documents matching this filter expression.");
        partialHint.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 10px;");

        GridPane createGrid = new GridPane();
        createGrid.setHgap(10); createGrid.setVgap(6);
        javafx.scene.layout.ColumnConstraints cl = new javafx.scene.layout.ColumnConstraints();
        cl.setMinWidth(120); cl.setHalignment(javafx.geometry.HPos.RIGHT);
        javafx.scene.layout.ColumnConstraints cr = new javafx.scene.layout.ColumnConstraints();
        cr.setHgrow(Priority.ALWAYS); cr.setFillWidth(true);
        createGrid.getColumnConstraints().addAll(cl, cr);

        int r = 0;
        createGrid.addRow(r++, new Label("Keys (JSON)"), keysField);
        createGrid.addRow(r++, new Label("Name"), nameField);
        HBox flags = new HBox(16, uniqueCb, sparseCb, bgCb);
        flags.setAlignment(Pos.CENTER_LEFT);
        createGrid.addRow(r++, new Label("Options"), flags);
        createGrid.addRow(r++, ttlLabel, new HBox(8, ttlSpinner, ttlHint));
        createGrid.addRow(r++, new Label("Partial filter"), partialField);
        createGrid.addRow(r++, new Label(""), partialHint);

        Button createBtn = new Button("Create index");
        createBtn.setStyle("-fx-background-color: #16a34a; -fx-text-fill: white; -fx-font-weight: bold;");
        createBtn.setOnAction(e -> {
            String keys = keysField.getText();
            boolean unique = uniqueCb.isSelected(), sparse = sparseCb.isSelected(), bg = bgCb.isSelected();
            int ttl = ttlSpinner.getValue();
            String idxName = nameField.getText(), partial = partialField.getText();
            createBtn.setDisable(true);
            Thread.startVirtualThread(() -> {
                try {
                    svc.createIndex(db, coll, keys, unique, sparse, ttl, idxName, partial, bg);
                    javafx.application.Platform.runLater(() -> { createBtn.setDisable(false); refresh(); });
                } catch (Exception ex) {
                    javafx.application.Platform.runLater(() -> {
                        createBtn.setDisable(false);
                        UiHelpers.error(getDialogPane().getScene().getWindow(), ex.getMessage());
                    });
                }
            });
        });

        VBox createSection = new VBox(8, createTitle, createGrid, createBtn);

        /* ========== Layout ========== */
        VBox content = new VBox(12, listSection, new Separator(), createSection);
        content.setPadding(new Insets(16));
        content.setPrefSize(860, 620);

        getDialogPane().setContent(content);
        getDialogPane().getButtonTypes().setAll(ButtonType.CLOSE);
        refresh();
    }

    private void dropSelected() {
        Document sel = table.getSelectionModel().getSelectedItem();
        if (sel == null) return;
        String name = String.valueOf(sel.get("name"));
        if ("_id_".equals(name)) {
            UiHelpers.error(getDialogPane().getScene().getWindow(), "Cannot drop the _id index.");
            return;
        }
        if (UiHelpers.confirmTyped(getDialogPane().getScene().getWindow(), name)) {
            Thread.startVirtualThread(() -> {
                try {
                    svc.dropIndex(db, coll, name);
                    javafx.application.Platform.runLater(this::refresh);
                } catch (Exception ex) {
                    javafx.application.Platform.runLater(() -> UiHelpers.error(getDialogPane().getScene().getWindow(), ex.getMessage()));
                }
            });
        }
    }

    private void refresh() {
        Thread.startVirtualThread(() -> {
            try {
                var indexes = svc.listIndexes(db, coll);
                javafx.application.Platform.runLater(() -> table.setItems(FXCollections.observableArrayList(indexes)));
            } catch (Exception e) {
                javafx.application.Platform.runLater(() -> UiHelpers.error(getDialogPane().getScene().getWindow(), e.getMessage()));
            }
        });
    }

    private static String str(Document d, String key) {
        Object v = d.get(key);
        return v == null ? "" : String.valueOf(v);
    }
}

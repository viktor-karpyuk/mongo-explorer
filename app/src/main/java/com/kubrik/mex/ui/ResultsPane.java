package com.kubrik.mex.ui;

import com.kubrik.mex.core.MongoService;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.scene.control.Tab;
import javafx.scene.control.TabPane;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextArea;
import javafx.scene.control.TreeItem;
import javafx.scene.control.TreeView;
import org.fxmisc.flowless.VirtualizedScrollPane;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Three-tab document viewer: Table (sampled top-level fields), Tree (expandable),
 * and JSON (extended-JSON list). Selection in any view fires the shared callback.
 */
public class ResultsPane extends TabPane {

    public record DocRef(int index, String label) {
        @Override public String toString() { return label; }
    }

    private List<Document> docs = List.of();
    private final TableView<Document> table = new TableView<>();
    private final TreeView<Object> tree = new TreeView<>();
    private final JsonCodeArea jsonArea = new JsonCodeArea("");
    private final TextArea errorArea = new TextArea();
    private final Tab errorTab;
    private Consumer<Document> onSelect = d -> {};

    public ResultsPane() {
        jsonArea.setEditable(false);
        VirtualizedScrollPane<JsonCodeArea> jsonScroll = new VirtualizedScrollPane<>(jsonArea);

        // Error tab — pgAdmin-style, populated when a query/parse error occurs.
        // The TextArea is read-only but selectable so the user can Cmd/Ctrl+C
        // the message verbatim. Tab is removed from the TabPane while there
        // is no error so it doesn't clutter the UI.
        errorArea.setEditable(false);
        errorArea.setWrapText(true);
        errorArea.setStyle(
                "-fx-font-family: 'Menlo','Monaco',monospace; -fx-font-size: 12px; "
                + "-fx-text-fill: #b91c1c; -fx-control-inner-background: #fef2f2;");
        errorTab = new Tab("Error", errorArea);
        errorTab.setClosable(false);
        errorTab.setStyle("-fx-text-base-color: #b91c1c;");

        Tab t1 = new Tab("Table", table);
        Tab t2 = new Tab("Tree", tree);
        Tab t3 = new Tab("JSON", jsonScroll);
        for (Tab t : List.of(t1, t2, t3)) t.setClosable(false);
        getTabs().addAll(t1, t2, t3);

        // Load stylesheet for JSON highlighting
        try {
            String css = getClass().getResource("/json-editor.css").toExternalForm();
            jsonArea.getStylesheets().add(css);
        } catch (Exception ignored) {}

        table.setColumnResizePolicy(TableView.UNCONSTRAINED_RESIZE_POLICY);
        table.setPlaceholder(new javafx.scene.control.Label("No results"));
        tree.setShowRoot(false);

        table.getSelectionModel().selectedItemProperty().addListener((o, a, b) -> {
            if (b != null) onSelect.accept(b);
        });
        tree.getSelectionModel().selectedItemProperty().addListener((o, a, b) -> {
            if (b == null) return;
            TreeItem<Object> cur = b;
            while (cur != null && !(cur.getValue() instanceof DocRef)) cur = cur.getParent();
            if (cur != null && cur.getValue() instanceof DocRef ref && ref.index() < docs.size()) {
                onSelect.accept(docs.get(ref.index()));
            }
        });
    }

    public void setOnSelect(Consumer<Document> c) { this.onSelect = c; }

    public List<Document> currentDocuments() { return docs; }

    public Document selected() {
        Document d = table.getSelectionModel().getSelectedItem();
        if (d != null) return d;
        TreeItem<Object> sel = tree.getSelectionModel().getSelectedItem();
        if (sel != null) {
            TreeItem<Object> cur = sel;
            while (cur != null && !(cur.getValue() instanceof DocRef)) cur = cur.getParent();
            if (cur != null && cur.getValue() instanceof DocRef ref && ref.index() < docs.size()) {
                return docs.get(ref.index());
            }
        }
        return null;
    }

    public void setDocuments(List<Document> documents) {
        this.docs = documents == null ? List.of() : documents;
        rebuildTable();
        rebuildTree();
        rebuildJson();
    }

    /**
     * Show an error in a dedicated, copyable Error tab and bring it to focus.
     * The message text is selectable for Cmd/Ctrl+C. Pass {@code null} or
     * blank to clear/hide the tab.
     */
    public void setError(String message) {
        if (message == null || message.isBlank()) { clearError(); return; }
        errorArea.setText(message);
        if (!getTabs().contains(errorTab)) getTabs().add(0, errorTab);
        getSelectionModel().select(errorTab);
    }

    public void clearError() {
        getTabs().remove(errorTab);
        errorArea.clear();
    }

    private void rebuildTable() {
        table.getColumns().clear();
        LinkedHashSet<String> fields = new LinkedHashSet<>();
        for (Document d : docs) fields.addAll(d.keySet());
        for (String f : fields) {
            TableColumn<Document, String> col = new TableColumn<>(f);
            col.setCellValueFactory(c -> new SimpleStringProperty(formatValue(c.getValue().get(f))));
            col.setPrefWidth(Math.min(280, Math.max(90, f.length() * 11 + 30)));
            table.getColumns().add(col);
        }
        table.setItems(FXCollections.observableArrayList(docs));
    }

    private void rebuildTree() {
        TreeItem<Object> root = new TreeItem<>("results");
        for (int i = 0; i < docs.size(); i++) {
            Document d = docs.get(i);
            TreeItem<Object> docItem = new TreeItem<>(new DocRef(i, "[" + i + "] " + summarize(d)));
            populateChildren(docItem, d);
            root.getChildren().add(docItem);
        }
        tree.setRoot(root);
    }

    private void rebuildJson() {
        StringBuilder sb = new StringBuilder("[\n");
        for (int i = 0; i < docs.size(); i++) {
            sb.append("  ");
            sb.append(docs.get(i).toJson(MongoService.JSON_RELAXED));
            if (i < docs.size() - 1) sb.append(",");
            sb.append("\n");
        }
        sb.append("]");
        jsonArea.replaceText(sb.toString());
        jsonArea.refreshHighlight();
        jsonArea.moveTo(0);
    }

    private static void populateChildren(TreeItem<Object> parent, Object value) {
        if (value instanceof Document doc) {
            for (Map.Entry<String, Object> e : doc.entrySet()) {
                Object v = e.getValue();
                TreeItem<Object> child = new TreeItem<>(e.getKey() + ": " + formatValue(v));
                if (v instanceof Document || v instanceof List) populateChildren(child, v);
                parent.getChildren().add(child);
            }
        } else if (value instanceof List<?> list) {
            for (int i = 0; i < list.size(); i++) {
                Object v = list.get(i);
                TreeItem<Object> child = new TreeItem<>("[" + i + "]: " + formatValue(v));
                if (v instanceof Document || v instanceof List) populateChildren(child, v);
                parent.getChildren().add(child);
            }
        }
    }

    private static String formatValue(Object v) {
        if (v == null) return "null";
        if (v instanceof ObjectId oid) return "ObjectId(\"" + oid.toHexString() + "\")";
        if (v instanceof Date dt) return dt.toInstant().toString();
        if (v instanceof Document d) return "{ " + d.size() + " fields }";
        if (v instanceof List<?> l) return "[ " + l.size() + " items ]";
        if (v instanceof Decimal128 dec) return dec.toString();
        if (v instanceof Binary) return "Binary(...)";
        if (v instanceof String s) return s.length() > 120 ? s.substring(0, 117) + "…" : s;
        return String.valueOf(v);
    }

    private static String summarize(Document d) {
        Object id = d.get("_id");
        return id != null ? "_id=" + formatValue(id) : "{ " + d.size() + " fields }";
    }
}

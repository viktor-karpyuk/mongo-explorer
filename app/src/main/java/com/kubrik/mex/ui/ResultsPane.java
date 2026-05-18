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
import java.util.concurrent.atomic.AtomicLong;
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
    private final Tab jsonTab;
    private Consumer<Document> onSelect = d -> {};
    /** Bumped on every {@link #setDocuments} so async JSON / Tree-children
     *  builds know when their snapshot is stale and skip the apply. */
    private final AtomicLong epoch = new AtomicLong();
    /** True once the JSON tab content is current for the latest docs. */
    private boolean jsonReady = false;

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
        jsonTab = new Tab("JSON", jsonScroll);
        for (Tab t : List.of(t1, t2, jsonTab)) t.setClosable(false);
        getTabs().addAll(t1, t2, jsonTab);

        // Lazy JSON: only serialize+highlight when the user actually looks at
        // it. Cheap result sets (most clicks) never pay for it.
        jsonTab.selectedProperty().addListener((o, was, now) -> {
            if (Boolean.TRUE.equals(now) && !jsonReady) rebuildJsonAsync();
        });

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
        epoch.incrementAndGet();
        // Synchronous + cheap: table columns + top-level tree rows only.
        rebuildTable();
        rebuildTree();
        // JSON is heavy (toJson per doc + regex highlight over the whole
        // string) — defer until the JSON tab is actually selected. Clear
        // any stale text so the user doesn't see last query's output.
        jsonReady = false;
        if (!jsonArea.getText().isEmpty()) jsonArea.replaceText("");
        if (jsonTab.isSelected()) rebuildJsonAsync();
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

    /** Top-level rows only. Each row gets a placeholder child so the
     *  disclosure arrow is shown; the real children are materialised the
     *  first time the user expands the row. For a 100-doc result this
     *  takes ~1 % of the FX-thread time the eager version did. */
    private void rebuildTree() {
        TreeItem<Object> root = new TreeItem<>("results");
        for (int i = 0; i < docs.size(); i++) {
            final Document d = docs.get(i);
            final int idx = i;
            TreeItem<Object> docItem = new TreeItem<>(new DocRef(idx, "[" + idx + "] " + summarize(d)));
            // Placeholder so the arrow appears; replaced on first expand.
            docItem.getChildren().add(new TreeItem<>(LAZY_PLACEHOLDER));
            docItem.expandedProperty().addListener((o, was, now) -> {
                if (Boolean.TRUE.equals(now)
                        && docItem.getChildren().size() == 1
                        && LAZY_PLACEHOLDER.equals(docItem.getChildren().get(0).getValue())) {
                    docItem.getChildren().clear();
                    populateChildren(docItem, d);
                }
            });
            root.getChildren().add(docItem);
        }
        tree.setRoot(root);
    }

    private static final String LAZY_PLACEHOLDER = "LAZY";

    /** Builds the JSON text on a virtual thread (toJson per doc + the
     *  StringBuilder concat are both CPU-heavy for big result sets) and
     *  applies it on the FX thread. Stale snapshots (newer setDocuments
     *  in flight) are dropped via the epoch counter. The textProperty
     *  listener on JsonCodeArea re-tokenises automatically — we used to
     *  also call refreshHighlight() right after, paying for the regex
     *  twice. Don't. */
    private void rebuildJsonAsync() {
        final long mine = epoch.get();
        final List<Document> snap = docs;
        Thread.startVirtualThread(() -> {
            String text;
            if (snap.isEmpty()) {
                text = "[]";
            } else {
                StringBuilder sb = new StringBuilder(Math.min(64, snap.size()) * 256);
                sb.append("[\n");
                for (int i = 0; i < snap.size(); i++) {
                    sb.append("  ").append(snap.get(i).toJson(MongoService.JSON_RELAXED));
                    if (i < snap.size() - 1) sb.append(",");
                    sb.append("\n");
                }
                sb.append("]");
                text = sb.toString();
            }
            javafx.application.Platform.runLater(() -> {
                if (epoch.get() != mine) return;          // superseded
                jsonArea.replaceText(text);
                jsonArea.moveTo(0);
                jsonReady = true;
            });
        });
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

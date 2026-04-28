package com.kubrik.mex.ui.monitoring;

import javafx.scene.control.ContextMenu;
import javafx.scene.control.MenuItem;
import javafx.scene.control.TableRow;
import javafx.scene.control.TableView;
import javafx.scene.input.Clipboard;
import javafx.scene.input.ClipboardContent;
import javafx.scene.input.MouseButton;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * ROW-EXPAND-1: attaches "Open details" / "Open in tab" / "Copy row as JSON" to
 * every non-empty row in a {@link TableView}, plus a double-click handler that
 * opens the detail modal. The row-open callback is resolved per-row so the
 * caller can drop the action when the row's connection id is unknown.
 */
public final class TableRowActions {

    private TableRowActions() {}

    /**
     * @param table       the table to decorate.
     * @param openerFor   returns a {@code Consumer<MetricExpander.Mode>} for a row, or null to
     *                    suppress the Open-details / Open-in-tab actions for that row.
     * @param jsonFor     returns a JSON representation of the row for Copy row as JSON.
     */
    public static <T> void install(TableView<T> table,
                                   Function<T, Consumer<MetricExpander.Mode>> openerFor,
                                   Function<T, String> jsonFor) {
        table.setRowFactory(tv -> {
            TableRow<T> row = new TableRow<>();
            ContextMenu menu = new ContextMenu();
            MenuItem open = new MenuItem("Open details");
            MenuItem openTab = new MenuItem("Open in tab");
            MenuItem copy = new MenuItem("Copy row as JSON");
            menu.getItems().addAll(open, openTab, copy);

            open.setOnAction(e -> {
                if (row.getItem() == null) return;
                Consumer<MetricExpander.Mode> o = openerFor.apply(row.getItem());
                if (o != null) o.accept(MetricExpander.Mode.MODAL);
            });
            openTab.setOnAction(e -> {
                if (row.getItem() == null) return;
                Consumer<MetricExpander.Mode> o = openerFor.apply(row.getItem());
                if (o != null) o.accept(MetricExpander.Mode.TAB);
            });
            copy.setOnAction(e -> {
                if (row.getItem() == null) return;
                ClipboardContent cc = new ClipboardContent();
                cc.putString(jsonFor.apply(row.getItem()));
                Clipboard.getSystemClipboard().setContent(cc);
            });

            row.contextMenuProperty().bind(
                    javafx.beans.binding.Bindings.when(row.emptyProperty())
                            .then((ContextMenu) null).otherwise(menu));

            row.setOnMouseClicked(e -> {
                if (row.isEmpty()) return;
                if (e.getButton() == MouseButton.PRIMARY && e.getClickCount() == 2) {
                    Consumer<MetricExpander.Mode> o = openerFor.apply(row.getItem());
                    if (o != null) o.accept(MetricExpander.Mode.MODAL);
                }
            });
            return row;
        });
    }

    /** Null-safe, minimal JSON string quoting for Copy row as JSON payloads. */
    public static String jsonStr(String s) {
        if (s == null) return "null";
        StringBuilder sb = new StringBuilder(s.length() + 2).append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"'  -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default   -> sb.append(c);
            }
        }
        return sb.append('"').toString();
    }
}

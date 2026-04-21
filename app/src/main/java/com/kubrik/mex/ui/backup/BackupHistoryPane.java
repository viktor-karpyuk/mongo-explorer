package com.kubrik.mex.ui.backup;

import com.kubrik.mex.backup.store.BackupCatalogDao;
import com.kubrik.mex.events.EventBus;
import javafx.beans.property.SimpleObjectProperty;
import javafx.geometry.Insets;
import javafx.scene.control.Label;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.VBox;

/**
 * v2.5 UI-BKP-4 — backup history / catalog pane (stub).
 *
 * <p>Full implementation lands with Q2.5-C part 3: paginated table with
 * status pills, filters (status / policy / sink / search), live-prepend via
 * {@link EventBus#onBackup}, and per-row drawer (run log + timeline +
 * audit-row links). For now the pane tracks the connection context so the
 * shared selection in {@link BackupsTab} doesn't need stubs on the other
 * side of the listener.</p>
 */
public final class BackupHistoryPane extends BorderPane implements AutoCloseable {

    private final BackupCatalogDao catalog;
    private final EventBus bus;
    private final SimpleObjectProperty<String> connection = new SimpleObjectProperty<>();

    public BackupHistoryPane(BackupCatalogDao catalog, EventBus bus) {
        this.catalog = catalog;
        this.bus = bus;
        setStyle("-fx-background-color: white;");
        setPadding(new Insets(18));
        Label title = new Label("Backup history");
        title.setStyle("-fx-font-size: 16px; -fx-font-weight: 700;");
        Label sub = new Label("Paginated catalog + filters land with Q2.5-C part 3.");
        sub.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 12px;");
        setCenter(new VBox(4, title, sub));
    }

    public SimpleObjectProperty<String> connectionProperty() { return connection; }

    @Override
    public void close() { /* Part 3 closes EventBus subs here. */ }
}

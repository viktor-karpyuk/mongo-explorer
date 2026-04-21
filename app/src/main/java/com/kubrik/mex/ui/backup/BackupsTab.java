package com.kubrik.mex.ui.backup;

import com.kubrik.mex.backup.store.BackupCatalogDao;
import com.kubrik.mex.backup.store.BackupFileDao;
import com.kubrik.mex.backup.store.BackupPolicyDao;
import com.kubrik.mex.backup.store.SinkDao;
import com.kubrik.mex.backup.verify.CatalogVerifier;
import com.kubrik.mex.core.ConnectionManager;
import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.store.ConnectionStore;
import javafx.geometry.Insets;
import javafx.scene.control.Label;
import javafx.scene.control.Tab;
import javafx.scene.control.TabPane;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.VBox;

/**
 * v2.5 UI-BKP-1..2 — top-level Backups tab. Hosts two sub-tabs in v2.5.0:
 * Policies (editor + list) and History (paginated catalog). Further sub-tabs
 * — Catalog, Restore, Rehearsals — land with Q2.5-D..G.
 */
public final class BackupsTab extends BorderPane implements AutoCloseable {

    private final PolicyEditorPane policyPane;
    private final BackupHistoryPane historyPane;

    public BackupsTab(BackupPolicyDao policyDao, BackupCatalogDao catalogDao,
                      BackupFileDao fileDao, SinkDao sinkDao,
                      CatalogVerifier verifier, EventBus bus,
                      ConnectionManager connManager, ConnectionStore connectionStore) {
        this.policyPane = new PolicyEditorPane(policyDao, sinkDao, connManager, connectionStore);
        this.historyPane = new BackupHistoryPane(catalogDao, fileDao, verifier, bus);
        // Share the connection selection across both sub-tabs.
        policyPane.connectionProperty().addListener((obs, o, n) ->
                historyPane.connectionProperty().set(n));

        TabPane tabs = new TabPane(
                closeable("Policies", policyPane),
                closeable("History", historyPane));
        tabs.setTabClosingPolicy(TabPane.TabClosingPolicy.UNAVAILABLE);
        setCenter(tabs);
        setStyle("-fx-background-color: #f9fafb;");
    }

    @Override
    public void close() {
        try { historyPane.close(); } catch (Exception ignored) {}
    }

    private static Tab closeable(String name, javafx.scene.Node content) {
        Tab t = new Tab(name, content);
        t.setClosable(false);
        return t;
    }

    /** Placeholder until Q2.5-C.3 lands the real history surface. */
    static VBox placeholder(String message) {
        Label l = new Label(message);
        l.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 12px;");
        VBox v = new VBox(l);
        v.setPadding(new Insets(40));
        return v;
    }
}

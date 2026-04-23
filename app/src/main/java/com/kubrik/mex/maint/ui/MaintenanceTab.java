package com.kubrik.mex.maint.ui;

import com.kubrik.mex.maint.approval.ApprovalService;
import com.kubrik.mex.maint.drift.ConfigSnapshotDao;
import com.mongodb.client.MongoClient;
import javafx.scene.control.Tab;
import javafx.scene.control.TabPane;
import javafx.scene.layout.BorderPane;

import java.util.function.Function;
import java.util.function.Supplier;

/**
 * v2.7 UI-MAINT-* — Host for the maintenance sub-tabs. Each tab
 * is a real wizard pane; all dependencies arrive by constructor so
 * the host pane stays stateless.
 *
 * <p>Gating lives with the caller: {@code MainView} only instantiates
 * this pane when {@code maintenance.enabled} is true.</p>
 */
public final class MaintenanceTab extends BorderPane {

    /**
     * Fully-wired constructor.
     *
     * @param memberOpener given a {@code host:port}, returns an
     *                     auth-aware direct-connection MongoClient
     *                     reusing credentials + TLS settings from the
     *                     active service. Production wiring passes
     *                     {@code mongoService::openMemberClient}.
     */
    public MaintenanceTab(ApprovalService approvalService,
                          ConfigSnapshotDao configSnapshotDao,
                          Supplier<MongoClient> clientSupplier,
                          Supplier<String> connectionIdSupplier,
                          Function<String, MongoClient> memberOpener) {
        setStyle("-fx-background-color: white;");
        setPadding(new javafx.geometry.Insets(14, 16, 14, 16));

        TabPane tabs = new TabPane(
                wrap("Approvals",
                        new ApprovalsPane(approvalService, connectionIdSupplier)),
                wrap("Schema validator",
                        new SchemaValidatorPane(clientSupplier)),
                wrap("Reconfig",
                        new ReconfigWizardPane(clientSupplier)),
                wrap("Rolling index",
                        new RollingIndexPane(clientSupplier, memberOpener)),
                wrap("Compact / Resync",
                        new CompactResyncPane(clientSupplier, memberOpener)),
                wrap("Parameters",
                        new ParameterTuningPane(clientSupplier)),
                wrap("Upgrade",
                        new UpgradePlannerPane()),
                wrap("Config drift",
                        new ConfigDriftPane(configSnapshotDao, clientSupplier,
                                connectionIdSupplier))
        );
        tabs.setTabClosingPolicy(TabPane.TabClosingPolicy.UNAVAILABLE);
        setCenter(tabs);
    }

    private static Tab wrap(String name, javafx.scene.Node content) {
        Tab t = new Tab(name, content);
        t.setClosable(false);
        return t;
    }
}

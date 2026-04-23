package com.kubrik.mex.maint.ui;

import com.kubrik.mex.maint.approval.ApprovalService;
import com.kubrik.mex.maint.drift.ConfigSnapshotDao;
import com.mongodb.client.MongoClient;
import javafx.scene.control.Tab;
import javafx.scene.control.TabPane;
import javafx.scene.layout.BorderPane;

import java.util.function.Supplier;

/**
 * v2.7 UI-MAINT-* — Host for the maintenance sub-tabs. Each tab
 * is a real wizard pane; all dependencies arrive by constructor so
 * the host pane stays stateless.
 *
 * <p>Gating lives with the caller: {@code MainView} only instantiates
 * this pane when {@code maintenance.enabled} is true. The placeholder
 * constructor that shipped in the v2.7.0-alpha stub is retained for
 * callers that want a purely visual preview.</p>
 */
public final class MaintenanceTab extends BorderPane {

    /** Fully-wired constructor — every sub-tab is a real pane. */
    public MaintenanceTab(ApprovalService approvalService,
                          ConfigSnapshotDao configSnapshotDao,
                          Supplier<MongoClient> clientSupplier,
                          Supplier<String> connectionIdSupplier) {
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
                        new RollingIndexPane(clientSupplier)),
                wrap("Compact / Resync",
                        new CompactResyncPane(clientSupplier)),
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

    /** Placeholder-only constructor — retained so a consumer can preview
     *  the pane shell without wiring every dependency. The v2.7.0-alpha
     *  wiring used this shape; real installs now call the 4-arg form. */
    public MaintenanceTab() {
        setStyle("-fx-background-color: white;");
        setPadding(new javafx.geometry.Insets(14, 16, 14, 16));
        TabPane tabs = new TabPane();
        tabs.setTabClosingPolicy(TabPane.TabClosingPolicy.UNAVAILABLE);
        for (String name : new String[] {
                "Approvals", "Schema validator", "Reconfig", "Rolling index",
                "Compact / Resync", "Parameters", "Upgrade", "Config drift" }) {
            Tab t = new Tab(name);
            t.setClosable(false);
            javafx.scene.control.Label l = new javafx.scene.control.Label(
                    name + " — wire with the 4-arg MaintenanceTab ctor.");
            l.setStyle("-fx-text-fill: #9ca3af; -fx-font-size: 11px; "
                    + "-fx-font-style: italic;");
            javafx.scene.layout.VBox v = new javafx.scene.layout.VBox(l);
            v.setPadding(new javafx.geometry.Insets(16));
            t.setContent(v);
            tabs.getTabs().add(t);
        }
        setCenter(tabs);
    }

    private static Tab wrap(String name, javafx.scene.Node content) {
        Tab t = new Tab(name, content);
        t.setClosable(false);
        return t;
    }
}

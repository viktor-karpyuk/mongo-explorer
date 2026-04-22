package com.kubrik.mex.maint.ui;

import javafx.geometry.Insets;
import javafx.scene.control.Label;
import javafx.scene.control.Tab;
import javafx.scene.control.TabPane;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.VBox;

/**
 * v2.7 UI-MAINT-* — Host for the maintenance sub-tabs. Full wizard
 * UIs land incrementally; this pane exposes stable empty-state
 * placeholders that name each workstream so the feature-flag can flip
 * safely while implementation continues.
 *
 * <p>Gating: the caller (MainView) only instantiates this when the
 * {@code maintenance.enabled} setting is true, so the pane itself
 * doesn't need to re-check.</p>
 */
public final class MaintenanceTab extends BorderPane {

    public MaintenanceTab() {
        setPadding(new Insets(14, 16, 14, 16));
        setStyle("-fx-background-color: white;");

        TabPane tabs = new TabPane(
                placeholder("Approvals",
                        "Two-person approvals queue. Pending actions land here "
                                + "for a reviewer to approve, reject, or export "
                                + "as a signed token."),
                placeholder("Schema validator",
                        "Edit per-collection $jsonSchema validators with "
                                + "live preview — how many existing docs would "
                                + "fail under the proposed schema?"),
                placeholder("Rolling index",
                        "Build an index across replica-set members one at a "
                                + "time. Secondaries first, step-down, primary last."),
                placeholder("Reconfig",
                        "rs.reconfig wizard with full preflight arithmetic "
                                + "(quorum / votes / priority) and a rollback plan."),
                placeholder("Compact / Resync",
                        "Run compact on a chosen secondary; trigger resync "
                                + "on lagging members."),
                placeholder("Parameters",
                        "Curated setParameter catalogue with rationale + "
                                + "recommended values per cluster shape."),
                placeholder("Upgrade",
                        "Scan the cluster for upgrade blockers and emit a "
                                + "rolling-restart runbook (Markdown + HTML)."),
                placeholder("Config drift",
                        "Daily snapshot of cluster parameters + FCV + "
                                + "cmdline + sharding, diffed via the v2.6 engine.")
        );
        tabs.setTabClosingPolicy(TabPane.TabClosingPolicy.UNAVAILABLE);
        setCenter(tabs);
    }

    /** Placeholder sub-tab. v2.7 ships every workstream's headless
     *  kernel + unit tests; the FX-bound wizards fill these in as
     *  each UI story lands post-alpha. */
    private static Tab placeholder(String name, String summary) {
        Tab t = new Tab(name);
        Label title = new Label(name);
        title.setStyle("-fx-font-size: 14px; -fx-font-weight: 700;");
        Label body = new Label(summary);
        body.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px;");
        body.setWrapText(true);
        Label todo = new Label("UI story pending. Headless runner + tests "
                + "already available on the branch.");
        todo.setStyle("-fx-text-fill: #9ca3af; -fx-font-size: 11px; "
                + "-fx-font-style: italic;");
        VBox v = new VBox(8, title, body, todo);
        v.setPadding(new Insets(16));
        t.setContent(v);
        t.setClosable(false);
        return t;
    }
}

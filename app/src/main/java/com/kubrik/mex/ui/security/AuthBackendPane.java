package com.kubrik.mex.ui.security;

import com.kubrik.mex.security.authn.AuthBackend;
import com.kubrik.mex.security.authn.AuthBackendProbe;
import javafx.application.Platform;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * v2.6 Q2.6-F2 — Auth-backend sub-tab. Lists every SASL mechanism the
 * server advertises (enabled / disabled) and shows the config fields
 * associated with the selected one. Read-only — changing mechanisms
 * stays in the MongoDB config file, not in this surface.
 */
public final class AuthBackendPane extends BorderPane {

    private final ObservableList<AuthBackend> rows = FXCollections.observableArrayList();
    private final TableView<AuthBackend> table = new TableView<>(rows);
    private final VBox detail = new VBox(8);
    private final Button refreshBtn = SecurityPaneHelpers.refreshButton(
            "Reads authenticationMechanisms + getCmdLineOpts.security. "
            + "Requires clusterMonitor or similar read-side role.");
    private final Label footer = SecurityPaneHelpers.footer("—");

    private Supplier<AuthBackendProbe.Snapshot> loader = () -> null;

    public AuthBackendPane() {
        setStyle("-fx-background-color: -color-bg-default;");
        setPadding(new Insets(14, 16, 14, 16));

        setTop(buildTopBar());
        setCenter(buildSplit());
        HBox foot = new HBox(footer);
        foot.setPadding(new Insets(8, 0, 0, 0));
        setBottom(foot);

        table.getSelectionModel().selectedItemProperty().addListener((o, old, b) -> {
            if (b == null) detail.getChildren().setAll(SecurityPaneHelpers.small(
                    "Select a mechanism above to see its config."));
            else renderDetail(b);
        });
    }

    public void setLoader(Supplier<AuthBackendProbe.Snapshot> loader) {
        this.loader = loader == null ? () -> null : loader;
    }

    public void refresh() { refreshBtn.fire(); }

    /* =========================== top bar =========================== */

    private Region buildTopBar() {
        refreshBtn.setOnAction(e -> doRefresh());
        return SecurityPaneHelpers.topBar(
                SecurityPaneHelpers.paneTitle("Authentication backends"), refreshBtn);
    }

    /* ============================ split ============================ */

    private Region buildSplit() {
        table.setPlaceholder(SecurityPaneHelpers.emptyState(
                "No probe data yet",
                "Click Refresh to read the server's authenticationMechanisms "
                + "parameter and fold in any LDAP / Kerberos / TLS config that "
                + "getCmdLineOpts exposes."));
        table.getColumns().setAll(
                col("Mechanism", 160, b -> b.mechanism().wire()),
                col("Status", 90, b -> b.enabled() ? "enabled" : "disabled"),
                col("Config fields", 110,
                        b -> b.details().isEmpty() ? "(none)" : b.details().size() + " field(s)"));
        SecurityPaneHelpers.describe(table,
                "Authentication mechanisms advertised by the server. "
                + "Click a row to inspect the config fields in the side pane.");

        detail.setPadding(new Insets(12));
        detail.setStyle("-fx-background-color: -color-bg-subtle;");
        detail.getChildren().setAll(SecurityPaneHelpers.small(
                "Select a mechanism above to see its config."));

        javafx.scene.control.SplitPane split = new javafx.scene.control.SplitPane(table, detail);
        split.setDividerPositions(0.45);
        VBox.setVgrow(split, Priority.ALWAYS);
        return split;
    }

    /* ============================ data ============================ */

    private void doRefresh() {
        refreshBtn.setDisable(true);
        footer.setText("Loading authentication backends…");
        Thread.startVirtualThread(() -> {
            AuthBackendProbe.Snapshot snap = loader.get();
            Platform.runLater(() -> {
                refreshBtn.setDisable(false);
                if (snap == null) {
                    rows.clear();
                    footer.setText("Probe failed — check the connection.");
                    return;
                }
                rows.setAll(snap.backends());
                long enabled = snap.backends().stream().filter(AuthBackend::enabled).count();
                footer.setText(enabled + " of " + snap.backends().size() + " mechanisms enabled");
            });
        });
    }

    private void renderDetail(AuthBackend b) {
        detail.getChildren().clear();
        Label head = new Label(b.mechanism().wire() + (b.enabled() ? "  · enabled" : "  · disabled"));
        head.setStyle("-fx-font-size: 13px; -fx-font-weight: 700;");
        detail.getChildren().add(head);

        if (b.details().isEmpty()) {
            detail.getChildren().add(SecurityPaneHelpers.small(b.enabled()
                    ? "No explicit config keys — built-in defaults apply."
                    : "Not advertised. Enable via authenticationMechanisms in mongod.conf."));
            return;
        }
        for (Map.Entry<String, String> e : b.details().entrySet()) {
            Label row = new Label(e.getKey() + "  →  " + e.getValue());
            row.setStyle("-fx-font-family: 'JetBrains Mono','Menlo',monospace; -fx-font-size: 12px;");
            row.setWrapText(true);
            detail.getChildren().add(row);
        }
        Label redacted = SecurityPaneHelpers.small(
                "Secret-bearing fields (passwords / key passphrases) are filtered out at the probe layer.");
        redacted.setWrapText(true);
        detail.getChildren().add(redacted);
    }

    private static <T> TableColumn<T, String> col(String title, int width,
                                                   java.util.function.Function<T, String> getter) {
        TableColumn<T, String> c = new TableColumn<>(title);
        c.setPrefWidth(width);
        c.setCellValueFactory(cd -> new SimpleStringProperty(getter.apply(cd.getValue())));
        return c;
    }

    // suppress unused warning for List import during future evolution
    @SuppressWarnings("unused")
    private static List<String> noOp() { return List.of(); }
}

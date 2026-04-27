package com.kubrik.mex.ui.cluster;

import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.layout.StackPane;
import javafx.scene.layout.VBox;
import javafx.scene.paint.Color;
import org.kordamp.ikonli.javafx.FontIcon;

/**
 * Empty-state pane for the Cluster tab, shown when the user triggers the
 * Cluster view while no connection is selected (and none is connected to
 * fall back to). Before this pane existed the action silently updated the
 * status bar and opened nothing, which read as "the button is broken".
 *
 * <p>Layout is a single centred column: server-off icon → headline → body →
 * primary CTA ("Open Connections") → secondary link ("Add a connection…").
 * The CTAs are injected by {@code MainView} so this pane stays free of
 * dependencies on the app shell.</p>
 */
public final class ClusterEmptyPane extends StackPane {

    public ClusterEmptyPane(Runnable onOpenConnections, Runnable onAddConnection) {
        setStyle("-fx-background-color: #f9fafb;");
        setPadding(new Insets(48, 32, 48, 32));

        // `fth-database` is thematically right for a DB client with no active
        // connection. The Feather pack does not ship a `fth-server-off`; the
        // only "-off" suffixes in the pack are cloud-off and wifi-off.
        FontIcon icon = new FontIcon("fth-database");
        icon.setIconSize(72);
        icon.setIconColor(Color.web("#9ca3af"));

        Label headline = new Label("No active connection");
        headline.setStyle("-fx-font-size: 18px; -fx-font-weight: 700; -fx-text-fill: #111827;");

        Label body = new Label(
                "Connect to a MongoDB instance to view its topology, "
                        + "ops, balancer, oplog and audit trail.");
        body.setWrapText(true);
        body.setMaxWidth(420);
        body.setStyle("-fx-font-size: 13px; -fx-text-fill: #6b7280; -fx-text-alignment: center;");
        body.setAlignment(Pos.CENTER);

        Button openConns = new Button("Open Connections");
        openConns.setDefaultButton(true);
        openConns.setStyle(
                "-fx-background-color: #2563eb; -fx-text-fill: white;"
                        + "-fx-font-weight: 600; -fx-padding: 8 18 8 18;"
                        + "-fx-background-radius: 6;");
        if (onOpenConnections != null) openConns.setOnAction(e -> onOpenConnections.run());

        Button addConn = new Button("Add a connection…");
        addConn.setStyle(
                "-fx-background-color: transparent; -fx-text-fill: #2563eb;"
                        + "-fx-font-size: 12px; -fx-border-color: transparent;"
                        + "-fx-padding: 6 12 6 12;");
        if (onAddConnection != null) addConn.setOnAction(e -> onAddConnection.run());

        Label tipTitle = new Label("What you'll see once connected");
        tipTitle.setStyle(
                "-fx-font-size: 11px; -fx-text-fill: #6b7280;"
                        + "-fx-font-weight: 600; -fx-padding: 24 0 4 0;");
        Label tips = new Label(
                "· Replica-set topology with primary / secondary / arbiter states\n"
                        + "· Live $currentOp with killOp and lock analytics\n"
                        + "· Balancer controls + chunk distribution (sharded)\n"
                        + "· Oplog viewer and the v2.4 ops audit trail");
        tips.setStyle("-fx-font-size: 12px; -fx-text-fill: #6b7280;");
        tips.setMaxWidth(420);

        VBox col = new VBox(12,
                icon,
                headline,
                body,
                new VBox(8, openConns, addConn) {{
                    setAlignment(Pos.CENTER);
                }},
                tipTitle,
                tips);
        col.setAlignment(Pos.CENTER);
        col.setMaxWidth(480);

        getChildren().add(col);
    }
}

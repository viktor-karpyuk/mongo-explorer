package com.kubrik.mex.ui;

import com.kubrik.mex.core.ConnectionManager;
import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.model.ConnectionState;
import com.kubrik.mex.model.MongoConnection;
import com.kubrik.mex.store.ConnectionStore;
import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.ScrollPane;
import javafx.scene.layout.FlowPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import org.kordamp.ikonli.javafx.FontIcon;

import java.util.function.Consumer;

/**
 * Welcome / "home" tab — quick actions plus a card grid of saved connections.
 */
public class WelcomeView extends VBox {

    private final ConnectionManager manager;
    private final ConnectionStore store;
    private final FlowPane cards = new FlowPane(16, 16);
    private final Consumer<MongoConnection> onConnectAndOpen;

    public WelcomeView(ConnectionManager manager,
                       ConnectionStore store,
                       EventBus events,
                       Runnable onNewConnection,
                       Runnable onManageConnections,
                       Runnable onOpenLogs,
                       Consumer<MongoConnection> onConnectAndOpen,
                       Consumer<MongoConnection> onEditConnection) {
        this.manager = manager;
        this.store = store;
        this.onConnectAndOpen = onConnectAndOpen;

        setSpacing(20);
        setPadding(new Insets(32, 40, 32, 40));
        setStyle("-fx-background-color: white;");

        /* ----- header ----- */
        FontIcon hicon = new FontIcon("fth-database");
        hicon.setIconSize(48);
        hicon.setIconColor(javafx.scene.paint.Color.web("#2563eb"));
        Label title = new Label("Mongo Explorer");
        title.setStyle("-fx-font-size: 28px; -fx-font-weight: bold;");
        Label subtitle = new Label("Browse, query, and manage your MongoDB databases.");
        subtitle.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 13px;");
        VBox titles = new VBox(2, title, subtitle);
        HBox header = new HBox(20, hicon, titles);
        header.setAlignment(Pos.CENTER_LEFT);

        /* ----- quick actions ----- */
        Button newConn = primaryButton("fth-plus", "New Connection");
        newConn.setOnAction(e -> onNewConnection.run());
        Button manage = secondaryButton("fth-list", "Manage Connections");
        manage.setOnAction(e -> onManageConnections.run());
        Button logs = secondaryButton("fth-file-text", "Connection Log");
        logs.setOnAction(e -> onOpenLogs.run());
        HBox actions = new HBox(12, newConn, manage, logs);
        actions.setAlignment(Pos.CENTER_LEFT);

        /* ----- saved connections section ----- */
        Label section = new Label("Your connections");
        section.setStyle("-fx-font-size: 16px; -fx-font-weight: bold; -fx-padding: 16 0 0 0;");

        cards.setPadding(new Insets(8, 0, 0, 0));
        cards.setPrefWrapLength(900);

        ScrollPane scroll = new ScrollPane(cards);
        scroll.setFitToWidth(true);
        scroll.setStyle("-fx-background: white; -fx-background-color: white; -fx-border-color: transparent;");
        VBox.setVgrow(scroll, Priority.ALWAYS);

        getChildren().addAll(header, actions, section, scroll);

        // Each card is rebuilt on state changes so the status dot stays fresh.
        events.onState(s -> Platform.runLater(this::refresh));

        // Capture editing handler for cards
        this.onEditConnection = onEditConnection;
        refresh();
    }

    private final Consumer<MongoConnection> onEditConnection;

    public void refresh() {
        cards.getChildren().clear();
        java.util.List<MongoConnection> list = store.list();
        if (list.isEmpty()) {
            Label empty = new Label("No connections yet. Click \"New Connection\" to add one.");
            empty.setStyle("-fx-text-fill: #6b7280; -fx-font-style: italic; -fx-padding: 16;");
            cards.getChildren().add(empty);
            return;
        }
        for (MongoConnection c : list) cards.getChildren().add(buildCard(c));
    }

    private VBox buildCard(MongoConnection c) {
        ConnectionState state = manager.state(c.id());
        String dotColor = UiHelpers.colorFor(state.status());

        FontIcon icon = new FontIcon("fth-server");
        icon.setIconSize(20);
        icon.setIconColor(javafx.scene.paint.Color.web("#374151"));
        Label dot = new Label("●");
        dot.setStyle("-fx-text-fill: " + dotColor + "; -fx-font-size: 14px;");
        Region sp = new Region();
        HBox.setHgrow(sp, Priority.ALWAYS);
        HBox top = new HBox(8, icon, sp, dot);
        top.setAlignment(Pos.CENTER_LEFT);

        Label name = new Label(c.name());
        name.setStyle("-fx-font-size: 14px; -fx-font-weight: bold;");

        String target;
        if ("URI".equals(c.mode())) target = redact(c.uri());
        else if ("DNS_SRV".equals(c.connectionType())) target = "mongodb+srv://" + (c.srvHost() == null ? "" : c.srvHost());
        else target = c.hosts() == null ? "" : c.hosts();
        Label tgt = new Label(target);
        tgt.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px; -fx-font-family: 'Menlo','Monaco',monospace;");
        tgt.setWrapText(true);
        tgt.setMaxWidth(220);

        Label statusLine = new Label(state.status().name()
                + (state.serverVersion() != null ? " · MongoDB " + state.serverVersion() : "")
                + (state.lastError() != null ? " · " + truncate(state.lastError(), 60) : ""));
        statusLine.setStyle("-fx-text-fill: " + dotColor + "; -fx-font-size: 11px;");
        statusLine.setWrapText(true);
        statusLine.setMaxWidth(240);

        Button connect = new Button(state.status() == ConnectionState.Status.CONNECTED ? "Open" : "Connect");
        String connectIdle = "-fx-background-color: #16a34a; -fx-text-fill: white; -fx-font-weight: bold; -fx-padding: 6 14 6 14; -fx-background-radius: 4;";
        String connectHover = "-fx-background-color: #15803d; -fx-text-fill: white; -fx-font-weight: bold; -fx-padding: 6 14 6 14; -fx-background-radius: 4;";
        connect.setStyle(connectIdle);
        connect.setOnMouseEntered(ev -> connect.setStyle(connectHover));
        connect.setOnMouseExited(ev -> connect.setStyle(connectIdle));
        connect.setOnAction(e -> onConnectAndOpen.accept(c));
        Button edit = UiHelpers.iconButton("fth-edit-2", "Edit");
        edit.setOnAction(e -> onEditConnection.accept(c));
        HBox btns = new HBox(8, connect, edit);

        VBox card = new VBox(8, top, name, tgt, statusLine, new Region() {{ setPrefHeight(4); }}, btns);
        card.setPadding(new Insets(14));
        card.setPrefWidth(260);
        card.setMinHeight(170);
        card.setStyle(
                "-fx-background-color: white; "
                        + "-fx-border-color: #e5e7eb; "
                        + "-fx-border-radius: 8; "
                        + "-fx-background-radius: 8; "
                        + "-fx-effect: dropshadow(gaussian, rgba(0,0,0,0.04), 6, 0, 0, 1);");
        card.setOnMouseEntered(e -> card.setStyle(card.getStyle()
                + "-fx-border-color: #2563eb;"));
        card.setOnMouseExited(e -> card.setStyle(card.getStyle()
                .replace("-fx-border-color: #2563eb;", "")));
        card.setOnMouseClicked(e -> {
            if (e.getClickCount() == 2) onConnectAndOpen.accept(c);
        });
        return card;
    }

    private static Button primaryButton(String iconLit, String text) {
        FontIcon ic = new FontIcon(iconLit);
        ic.setIconSize(14);
        ic.setIconColor(javafx.scene.paint.Color.WHITE);
        Button b = new Button(text);
        b.setGraphic(ic);
        b.setStyle("-fx-background-color: #2563eb; -fx-text-fill: white; -fx-font-weight: bold; -fx-padding: 8 16 8 16; -fx-background-radius: 6;");
        return b;
    }

    private static Button secondaryButton(String iconLit, String text) {
        FontIcon ic = new FontIcon(iconLit);
        ic.setIconSize(14);
        ic.setIconColor(javafx.scene.paint.Color.web("#374151"));
        Button b = new Button(text);
        b.setGraphic(ic);
        b.setStyle("-fx-background-color: #f3f4f6; -fx-text-fill: #374151; -fx-padding: 8 16 8 16; -fx-background-radius: 6; -fx-border-color: #e5e7eb; -fx-border-radius: 6;");
        return b;
    }

    private static String redact(String uri) {
        return uri == null ? "" : uri.replaceAll("(://[^:]+:)([^@]+)(@)", "$1****$3");
    }
    private static String truncate(String s, int n) { return s.length() > n ? s.substring(0, n - 1) + "…" : s; }
}

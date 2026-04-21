package com.kubrik.mex.ui.security;

import com.kubrik.mex.security.audit.AuditEvent;
import com.kubrik.mex.security.audit.AuditIndex;
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
import javafx.scene.control.TextField;
import javafx.scene.control.Tooltip;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.util.Duration;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * v2.6 Q2.6-C4 — Audit sub-tab. Reads the FTS5 index built by the
 * Q2.6-C3 {@link AuditIndex} (populated by a live {@link
 * com.kubrik.mex.security.audit.AuditLogTailer} elsewhere in the app).
 * The pane is a search + browse surface: empty query lists the N most
 * recent events; a non-empty query runs against FTS5's match grammar.
 */
public final class AuditPane extends BorderPane {

    private static final DateTimeFormatter TS_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());

    private final TextField searchField = new TextField();
    private final Button refreshBtn = new Button("Refresh");
    private final ObservableList<AuditEvent> rows = FXCollections.observableArrayList();
    private final TableView<AuditEvent> table = new TableView<>(rows);
    private final Label footer = new Label("—");

    private AuditIndex index;
    private String connectionId;

    public AuditPane() {
        setStyle("-fx-background-color: white;");
        setPadding(new Insets(14, 16, 14, 16));

        setTop(buildTopBar());
        setCenter(buildTable());
        HBox foot = new HBox(footer);
        foot.setPadding(new Insets(8, 0, 0, 0));
        footer.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px;");
        setBottom(foot);
    }

    public void configure(String connectionId, AuditIndex index) {
        this.connectionId = connectionId;
        this.index = index;
    }

    public void refresh() { refreshBtn.fire(); }

    /* ============================ top bar ============================ */

    private Region buildTopBar() {
        Label title = new Label("Native MongoDB audit log");
        title.setStyle("-fx-font-size: 14px; -fx-font-weight: 700;");

        searchField.setPromptText("FTS5 query (e.g. authenticate who:dba)");
        searchField.setPrefWidth(320);
        searchField.setTooltip(tooltip(
                "SQLite FTS5 match grammar. Examples:\n"
                + "   authenticate\n"
                + "   atype:createUser\n"
                + "   who:dba AND atype:auth*\n"
                + "Leave blank for the most recent events."));
        searchField.setOnAction(e -> doSearch());

        refreshBtn.setOnAction(e -> doSearch());

        Region grow = new Region();
        HBox.setHgrow(grow, Priority.ALWAYS);
        HBox row = new HBox(10, title, grow, searchField, refreshBtn);
        row.setAlignment(Pos.CENTER_LEFT);
        row.setPadding(new Insets(0, 0, 10, 0));
        return row;
    }

    private Region buildTable() {
        table.setPlaceholder(new Label(
                "No audited events yet — make sure the connection's auditLog.destination = file "
                + "and the app is pointed at the log path."));
        table.getColumns().setAll(
                col("When", 160,
                        e -> TS_FMT.format(Instant.ofEpochMilli(e.tsMs()))),
                col("Atype", 160, AuditEvent::atype),
                col("Who", 180,
                        e -> e.whoDb().isBlank() ? e.who() : e.who() + "@" + e.whoDb()),
                col("From", 180, AuditEvent::fromHost),
                col("Raw", 520, AuditEvent::rawJson));
        return table;
    }

    /* ============================= actions ============================= */

    private void doSearch() {
        if (index == null || connectionId == null) {
            footer.setText("No audit index configured.");
            return;
        }
        String query = searchField.getText();
        refreshBtn.setDisable(true);
        Thread.startVirtualThread(() -> {
            List<AuditEvent> hits = index.search(connectionId, query, 500);
            Platform.runLater(() -> {
                refreshBtn.setDisable(false);
                rows.setAll(hits);
                footer.setText(hits.size() + " event(s)"
                        + (query == null || query.isBlank() ? "  ·  recent"
                                : "  ·  query: " + query));
            });
        });
    }

    /* ============================= helpers ============================= */

    private static Tooltip tooltip(String body) {
        Tooltip t = new Tooltip(body);
        t.setShowDelay(Duration.millis(250));
        t.setShowDuration(Duration.seconds(30));
        t.setWrapText(true);
        t.setMaxWidth(340);
        return t;
    }

    private static <T> TableColumn<T, String> col(String title, int width,
                                                   java.util.function.Function<T, String> getter) {
        TableColumn<T, String> c = new TableColumn<>(title);
        c.setPrefWidth(width);
        c.setCellValueFactory(cd -> new SimpleStringProperty(getter.apply(cd.getValue())));
        return c;
    }
}

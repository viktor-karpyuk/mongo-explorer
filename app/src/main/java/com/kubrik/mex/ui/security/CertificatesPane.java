package com.kubrik.mex.ui.security;

import com.kubrik.mex.security.cert.CertRecord;
import javafx.application.Platform;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TableCell;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.function.Supplier;

/**
 * v2.6 Q2.6-E2 — Certificates sub-tab. Shows one row per observed
 * cluster-member cert with the expiry band rendered as a coloured
 * pill (green / amber / red / expired). The "Days to expiry" column
 * surfaces the raw number the band is derived from.
 */
public final class CertificatesPane extends BorderPane {

    private static final DateTimeFormatter TS_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.systemDefault());

    private final ObservableList<CertRecord> rows = FXCollections.observableArrayList();
    private final TableView<CertRecord> table = new TableView<>(rows);
    private final Button refreshBtn = SecurityPaneHelpers.refreshButton(
            "Opens a TLS handshake against every cluster member and "
            + "captures the peer cert chain. Trust-all manager — we "
            + "inspect what the server presents, never authenticate "
            + "traffic against it.");
    private final Label footer = SecurityPaneHelpers.footer("—");
    private final Clock clock;

    private Supplier<List<CertRecord>> loader = List::of;

    public CertificatesPane() { this(Clock.systemUTC()); }

    /** Visible for tests: injectable clock so band rendering is
     *  deterministic when the pane is smoke-tested against a fixture. */
    CertificatesPane(Clock clock) {
        this.clock = clock == null ? Clock.systemUTC() : clock;
        setStyle("-fx-background-color: white;");
        setPadding(new Insets(14, 16, 14, 16));

        setTop(buildTopBar());
        setCenter(buildTable());
        HBox foot = new HBox(footer);
        foot.setPadding(new Insets(8, 0, 0, 0));
        setBottom(foot);
    }

    public void setLoader(Supplier<List<CertRecord>> loader) {
        this.loader = loader == null ? List::of : loader;
    }

    public void refresh() { refreshBtn.fire(); }

    /* ============================== UI ============================== */

    private Region buildTopBar() {
        refreshBtn.setOnAction(e -> doRefresh());
        return SecurityPaneHelpers.topBar(
                SecurityPaneHelpers.paneTitle("TLS certificate inventory"), refreshBtn);
    }

    private Region buildTable() {
        table.setPlaceholder(SecurityPaneHelpers.emptyState(
                "No certs captured yet",
                "Click Refresh to open a TLS handshake against every "
                + "cluster member and record the subject / issuer / SANs / "
                + "expiry date. Bands warn at the 30-day, 7-day, and "
                + "already-expired thresholds."));
        table.getColumns().setAll(
                col("Host",        190, CertRecord::host),
                col("Subject",     180, CertRecord::subjectCn),
                col("Issuer",      180, CertRecord::issuerCn),
                col("Not after",   110,
                        c -> TS_FMT.format(Instant.ofEpochMilli(c.notAfter()))),
                daysCol(),
                bandCol(),
                col("SANs", 220, c -> String.join(", ", c.sans())));
        SecurityPaneHelpers.describe(table,
                "TLS certificate inventory. Band column colours rows by expiry "
                + "proximity: green > 30 days, amber ≤ 30 days, red ≤ 7 days, "
                + "white-on-red EXPIRED.");
        return table;
    }

    private TableColumn<CertRecord, String> daysCol() {
        TableColumn<CertRecord, String> c = new TableColumn<>("Days to expiry");
        c.setPrefWidth(120);
        c.setCellValueFactory(cd -> new SimpleStringProperty(
                Long.toString(
                        Math.floorDiv(cd.getValue().msUntilExpiry(clock.millis()),
                                86_400_000L))));
        return c;
    }

    private TableColumn<CertRecord, CertRecord.ExpiryBand> bandCol() {
        TableColumn<CertRecord, CertRecord.ExpiryBand> c = new TableColumn<>("Band");
        c.setPrefWidth(100);
        c.setCellValueFactory(cd -> new javafx.beans.property.SimpleObjectProperty<>(
                cd.getValue().expiryBand(clock.millis())));
        c.setCellFactory(col -> new TableCell<>() {
            @Override protected void updateItem(CertRecord.ExpiryBand b, boolean empty) {
                super.updateItem(b, empty);
                if (empty || b == null) { setText(""); setStyle(""); return; }
                setText(b.name());
                setStyle(bandStyle(b));
            }
        });
        return c;
    }

    private static String bandStyle(CertRecord.ExpiryBand b) {
        return switch (b) {
            case GREEN   -> "-fx-text-fill: #065f46; -fx-font-weight: 700;";
            case AMBER   -> "-fx-text-fill: #92400e; -fx-font-weight: 700;";
            case RED     -> "-fx-text-fill: #b91c1c; -fx-font-weight: 700;";
            case EXPIRED -> "-fx-text-fill: #ffffff; -fx-background-color: #991b1b; "
                    + "-fx-font-weight: 700;";
        };
    }

    /* ============================= data ============================= */

    private void doRefresh() {
        refreshBtn.setDisable(true);
        footer.setText("Probing TLS handshakes…");
        Thread.startVirtualThread(() -> {
            List<CertRecord> snap = loader.get();
            Platform.runLater(() -> {
                refreshBtn.setDisable(false);
                rows.setAll(snap == null ? List.of() : snap);
                long nowMs = clock.millis();
                long bad = rows.stream()
                        .map(c -> c.expiryBand(nowMs))
                        .filter(b -> b != CertRecord.ExpiryBand.GREEN).count();
                if (snap == null) footer.setText("Probe failed.");
                else if (bad == 0) footer.setText(rows.size() + " cert(s) · all > 30 days");
                else footer.setText(rows.size() + " cert(s) · " + bad + " within 30 days");
            });
        });
    }

    private static <T> TableColumn<T, String> col(String title, int width,
                                                   java.util.function.Function<T, String> getter) {
        TableColumn<T, String> c = new TableColumn<>(title);
        c.setPrefWidth(width);
        c.setCellValueFactory(cd -> new SimpleStringProperty(getter.apply(cd.getValue())));
        return c;
    }
}

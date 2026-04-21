package com.kubrik.mex.ui.security;

import com.kubrik.mex.security.encryption.EncryptionStatus;
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

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.function.Supplier;

/**
 * v2.6 Q2.6-G2 — Encryption-at-rest sub-tab. One row per host probed,
 * showing engine + keystore + cipher + last rotation. Cell renderer
 * flags unencrypted rows red so the operator sees them without reading
 * the column.
 */
public final class EncryptionPane extends BorderPane {

    private static final DateTimeFormatter TS_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());

    private final ObservableList<EncryptionStatus> rows = FXCollections.observableArrayList();
    private final TableView<EncryptionStatus> table = new TableView<>(rows);
    private final Button refreshBtn = new Button("Refresh");
    private final Label footer = new Label("—");

    private Supplier<List<EncryptionStatus>> loader = List::of;

    public EncryptionPane() {
        setStyle("-fx-background-color: white;");
        setPadding(new Insets(14, 16, 14, 16));

        setTop(buildTopBar());
        setCenter(buildTable());
        HBox foot = new HBox(footer);
        foot.setPadding(new Insets(8, 0, 0, 0));
        footer.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px;");
        setBottom(foot);
    }

    public void setLoader(Supplier<List<EncryptionStatus>> loader) {
        this.loader = loader == null ? List::of : loader;
    }

    public void refresh() { refreshBtn.fire(); }

    /* =========================== top bar =========================== */

    private Region buildTopBar() {
        Label title = new Label("Encryption at rest");
        title.setStyle("-fx-font-size: 14px; -fx-font-weight: 700;");
        refreshBtn.setOnAction(e -> doRefresh());
        Region grow = new Region();
        HBox.setHgrow(grow, Priority.ALWAYS);
        HBox row = new HBox(10, title, grow, refreshBtn);
        row.setAlignment(Pos.CENTER_LEFT);
        row.setPadding(new Insets(0, 0, 10, 0));
        return row;
    }

    private Region buildTable() {
        table.setPlaceholder(new Label(
                "Click Refresh to probe serverStatus + getCmdLineOpts on every member."));
        table.getColumns().setAll(
                col("Host", 220, EncryptionStatus::host),
                statusCol(),
                col("Engine", 110, EncryptionStatus::engine),
                col("Keystore", 110, s -> s.keystore().name()),
                col("Cipher", 110, EncryptionStatus::cipher),
                col("Rotated", 160,
                        s -> s.rotatedAtMs() == null ? "—"
                                : TS_FMT.format(Instant.ofEpochMilli(s.rotatedAtMs()))));
        return table;
    }

    private static TableColumn<EncryptionStatus, String> statusCol() {
        TableColumn<EncryptionStatus, String> c = new TableColumn<>("Status");
        c.setPrefWidth(100);
        c.setCellValueFactory(cd -> new SimpleStringProperty(
                cd.getValue().enabled() ? "enabled" : "DISABLED"));
        c.setCellFactory(col -> new TableCell<>() {
            @Override protected void updateItem(String s, boolean empty) {
                super.updateItem(s, empty);
                if (empty || s == null) { setText(""); setStyle(""); return; }
                setText(s);
                setStyle(s.equals("enabled")
                        ? "-fx-text-fill: #166534; -fx-font-weight: 700;"
                        : "-fx-text-fill: #991b1b; -fx-font-weight: 700;");
            }
        });
        return c;
    }

    private void doRefresh() {
        refreshBtn.setDisable(true);
        footer.setText("Probing encryption-at-rest…");
        Thread.startVirtualThread(() -> {
            List<EncryptionStatus> snap = loader.get();
            Platform.runLater(() -> {
                refreshBtn.setDisable(false);
                rows.setAll(snap == null ? List.of() : snap);
                if (snap == null || snap.isEmpty()) {
                    footer.setText("No members probed.");
                    return;
                }
                long enabled = snap.stream().filter(EncryptionStatus::enabled).count();
                footer.setText(enabled + " of " + snap.size() + " nodes have encryption enabled");
            });
        });
    }

    private static TableColumn<EncryptionStatus, String> col(String title, int width,
                                                              java.util.function.Function<EncryptionStatus, String> getter) {
        TableColumn<EncryptionStatus, String> c = new TableColumn<>(title);
        c.setPrefWidth(width);
        c.setCellValueFactory(cd -> new SimpleStringProperty(getter.apply(cd.getValue())));
        return c;
    }
}

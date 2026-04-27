package com.kubrik.mex.k8s.compute.managedpool.ui;

import com.kubrik.mex.k8s.compute.managedpool.ManagedPoolOperationDao;
import javafx.application.Platform;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.scene.control.Button;
import javafx.scene.control.CheckBox;
import javafx.scene.control.Label;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextArea;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.VBox;

import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.function.Function;

/**
 * v2.8.4 — Cloud Operations history pane.
 *
 * <p>Read-only view over {@code managed_pool_operations} so an
 * operator can correlate Mongo Explorer-driven cloud calls with
 * CloudTrail / GCP Audit Logs / Azure Activity Logs via the
 * {@code cloud_call_id} column. Mirrors the PortForwardAudit
 * drawer's pattern: paged-on-demand by a Reload button + a
 * "Tail latest" auto-refresh toggle.</p>
 */
public final class CloudOperationsPane extends BorderPane {

    private static final DateTimeFormatter TS = DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm:ss")
            .withZone(ZoneId.systemDefault());

    private final ManagedPoolOperationDao dao;
    private final ObservableList<ManagedPoolOperationDao.Row> rows =
            FXCollections.observableArrayList();
    private final TableView<ManagedPoolOperationDao.Row> table = new TableView<>(rows);
    private final TextArea detailArea = new TextArea();
    private final Label statusLabel = new Label("No rows yet — apply a cloud-backed deployment to populate.");
    private final CheckBox tailLatest = new CheckBox("Tail latest (auto-refresh every 5 s)");
    private volatile Thread tailThread;
    /** Off-FX-thread loop flag. The CheckBox's isSelected() is FX-
     *  thread-only by contract; this volatile mirror lets the worker
     *  thread observe stop signals deterministically. */
    private final java.util.concurrent.atomic.AtomicBoolean tailRunning =
            new java.util.concurrent.atomic.AtomicBoolean(false);

    public CloudOperationsPane(ManagedPoolOperationDao dao) {
        this.dao = Objects.requireNonNull(dao, "dao");

        setStyle("-fx-background-color: -color-bg-default;");
        setPadding(new Insets(14, 16, 14, 16));

        Label title = new Label("Cloud Operations");
        title.setStyle("-fx-font-size: 14px; -fx-font-weight: 700;");
        Label hint = new Label(
                "Every managed-pool cloud API call routes through "
              + "managed_pool_operations. Cross-reference cloud_call_id "
              + "with CloudTrail / Audit Logs / Activity Logs to close "
              + "the audit loop.");
        hint.setStyle("-fx-text-fill: -color-fg-muted; -fx-font-size: 11px;");
        hint.setWrapText(true);

        table.setPlaceholder(new Label("No cloud operations recorded yet."));
        table.getColumns().setAll(
                col("Provider", 80, r -> r.provider().name()),
                col("Action", 160, r -> r.action().wireValue()),
                col("Status", 90, r -> r.status().name()),
                col("Region", 110, r -> r.region().orElse("")),
                col("Account", 140, r -> r.accountId().orElse("")),
                col("Pool", 140, r -> r.poolName().orElse("")),
                col("Cloud call id", 220, r -> r.cloudCallId().orElse("")),
                col("Started", 150, r -> TS.format(Instant.ofEpochMilli(r.startedAt()))),
                col("Ended", 150, r -> r.endedAt().map(t ->
                        TS.format(Instant.ofEpochMilli(t))).orElse("")));
        table.getSelectionModel().selectedItemProperty().addListener(
                (o, a, b) -> renderDetail(b));

        detailArea.setEditable(false);
        detailArea.setPrefRowCount(6);
        detailArea.setWrapText(true);
        detailArea.setStyle(
                "-fx-font-family: 'Menlo','Monaco',monospace; -fx-font-size: 11px;");

        Button reload = new Button("Reload");
        reload.setOnAction(e -> reload());
        Button copyCallId = new Button("Copy call id");
        copyCallId.setOnAction(e -> copySelectedCallId());
        copyCallId.setTooltip(new javafx.scene.control.Tooltip(
                "Copy the selected row's cloud_call_id to the "
              + "clipboard — paste into a CloudTrail / Audit Logs / "
              + "Activity Logs search to find the matching cloud-side "
              + "record."));
        tailLatest.selectedProperty().addListener((o, a, b) -> {
            if (b) startTail();
            else stopTail();
        });

        HBox actions = new HBox(8, reload, copyCallId, tailLatest);
        actions.setPadding(new Insets(8, 0, 0, 0));

        statusLabel.setStyle("-fx-text-fill: -color-fg-muted; -fx-font-size: 11px;");
        statusLabel.setWrapText(true);

        VBox header = new VBox(4, title, hint);
        header.setPadding(new Insets(0, 0, 8, 0));

        VBox center = new VBox(6, header, table, new Label("Detail"),
                detailArea, actions, statusLabel);
        VBox.setVgrow(table, Priority.ALWAYS);
        setCenter(center);
        reload();
    }

    public void close() { stopTail(); }

    /* ============================ helpers ============================ */

    private void reload() {
        try {
            // Preserve the user's selection across the rebuild — a
            // 5 s tail-latest poll otherwise jumps the highlighted
            // row off-screen every tick, making it impossible to
            // keep an eye on a single operation.
            ManagedPoolOperationDao.Row prevSel = table.getSelectionModel()
                    .getSelectedItem();
            long prevSelId = prevSel == null ? -1L : prevSel.id();
            rows.setAll(dao.listAll());
            if (prevSelId > 0) {
                for (int i = 0; i < rows.size(); i++) {
                    if (rows.get(i).id() == prevSelId) {
                        table.getSelectionModel().select(i);
                        break;
                    }
                }
            }
            statusLabel.setText("Showing " + rows.size() + " operation(s).");
        } catch (SQLException sqle) {
            statusLabel.setText("Reload failed: " + sqle.getMessage());
        }
    }

    private void renderDetail(ManagedPoolOperationDao.Row r) {
        if (r == null) { detailArea.clear(); return; }
        StringBuilder sb = new StringBuilder();
        sb.append("id: ").append(r.id()).append('\n');
        r.provisioningRecordId().ifPresent(p ->
                sb.append("provisioning_record_id: ").append(p).append('\n'));
        sb.append("provider: ").append(r.provider()).append('\n');
        sb.append("action: ").append(r.action().wireValue()).append('\n');
        r.region().ifPresent(v -> sb.append("region: ").append(v).append('\n'));
        r.accountId().ifPresent(v -> sb.append("account_id: ").append(v).append('\n'));
        r.poolName().ifPresent(v -> sb.append("pool_name: ").append(v).append('\n'));
        r.cloudCallId().ifPresent(v -> sb.append("cloud_call_id: ").append(v).append('\n'));
        sb.append("started_at: ").append(TS.format(Instant.ofEpochMilli(r.startedAt()))).append('\n');
        r.endedAt().ifPresent(t -> sb.append("ended_at: ")
                .append(TS.format(Instant.ofEpochMilli(t))).append('\n'));
        sb.append("status: ").append(r.status()).append('\n');
        r.errorMessage().ifPresent(v -> sb.append("error_message: ").append(v).append('\n'));
        detailArea.setText(sb.toString());
    }

    private void copySelectedCallId() {
        var sel = table.getSelectionModel().getSelectedItem();
        if (sel == null) { statusLabel.setText("Pick a row first."); return; }
        String id = sel.cloudCallId().orElse("");
        if (id.isEmpty()) { statusLabel.setText("Selected row has no cloud_call_id."); return; }
        var cc = new javafx.scene.input.ClipboardContent();
        cc.putString(id);
        javafx.scene.input.Clipboard.getSystemClipboard().setContent(cc);
        statusLabel.setText("Copied " + id + " to clipboard.");
    }

    private void startTail() {
        stopTail();
        tailRunning.set(true);
        tailThread = Thread.ofVirtual().name("cloud-ops-tail").start(() -> {
            // tailRunning is FX-thread-safe; isSelected() is technically
            // FX-thread-only and reading it from a virtual thread is
            // a property-binding race. The atomic mirror makes shutdown
            // deterministic AND survives the close() path where the
            // CheckBox may already be detached.
            while (tailRunning.get()) {
                try { Thread.sleep(5_000); }
                catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
                if (!tailRunning.get()) return;
                Platform.runLater(this::reload);
            }
        });
    }

    private void stopTail() {
        tailRunning.set(false);
        Thread t = tailThread;
        if (t != null) t.interrupt();
        tailThread = null;
    }

    private static <T> TableColumn<T, String> col(String title, int width,
                                                    Function<T, String> getter) {
        TableColumn<T, String> c = new TableColumn<>(title);
        c.setPrefWidth(width);
        c.setCellValueFactory(cd -> new SimpleStringProperty(getter.apply(cd.getValue())));
        return c;
    }
}

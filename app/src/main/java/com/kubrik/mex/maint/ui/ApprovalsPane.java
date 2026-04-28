package com.kubrik.mex.maint.ui;

import com.kubrik.mex.maint.approval.ApprovalService;
import com.kubrik.mex.maint.model.Approval;
import javafx.application.Platform;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.ButtonType;
import javafx.scene.control.Label;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextInputDialog;
import javafx.scene.control.Tooltip;
import javafx.scene.input.Clipboard;
import javafx.scene.input.ClipboardContent;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import javafx.util.Duration;

import java.time.Instant;
import java.util.List;

/**
 * v2.7 APPR-* UI — Pending-approvals queue for a given connection.
 *
 * <p>Two actions per row: <b>Approve</b> (opens a "who are you?" dialog
 * and flips the row to APPROVED via {@link ApprovalService#approveTwoPerson}),
 * <b>Reject</b>, and <b>Export token</b> (reviewer-mode — produces the
 * JWS the executor's install pastes into a TOKEN-mode request).</p>
 */
public final class ApprovalsPane extends BorderPane {

    private final ApprovalService service;
    private final java.util.function.Supplier<String> connectionIdSupplier;
    private final ObservableList<Approval.Row> rows =
            FXCollections.observableArrayList();
    private final TableView<Approval.Row> table = new TableView<>(rows);
    private final Label statusLabel = new Label("—");
    private final TextArea detailArea = new TextArea();

    public ApprovalsPane(ApprovalService service,
                         java.util.function.Supplier<String> connectionIdSupplier) {
        this.service = service;
        this.connectionIdSupplier = connectionIdSupplier;
        setStyle("-fx-background-color: -color-bg-default;");
        setPadding(new Insets(14, 16, 14, 16));
        setAccessibleText("Maintenance approvals queue");
        setAccessibleHelp(
                "List of pending two-person approvals for destructive "
                + "maintenance actions. Select a row and choose Approve, "
                + "Reject, or Export token.");

        setTop(buildHeader());
        setCenter(buildCenter());
        setBottom(buildActions());
        refresh();
    }

    /** Minimum gap between sweeps. Every refresh click + every
     *  post-approve reload previously took the SQLite write lock
     *  via sweepExpired — a button-masher could pile up UI-blocking
     *  UPDATEs. 30s is the tightest window that still catches newly-
     *  expired rows before the next human interaction matters. */
    private static final long SWEEP_THROTTLE_MS = 30_000L;
    private volatile long lastSweepAt = 0L;

    public void refresh() {
        String connId = connectionIdSupplier.get();
        if (connId == null) {
            rows.clear();
            statusLabel.setText("Select a connection to see its approval queue.");
            return;
        }
        // Throttle sweepExpired so a rapid refresh / approve cycle
        // doesn't fire an UPDATE on every click.
        long now = System.currentTimeMillis();
        int expired = 0;
        if (now - lastSweepAt > SWEEP_THROTTLE_MS) {
            expired = service.sweepExpired();
            lastSweepAt = now;
        }
        List<Approval.Row> pending = service.listPending(connId);
        rows.setAll(pending);
        statusLabel.setText(expired == 0
                ? (pending.size() + " pending approval" + (pending.size() == 1 ? "" : "s"))
                : pending.size() + " pending, " + expired + " expired this sweep");
    }

    /* =============================== layout =============================== */

    private Region buildHeader() {
        Label title = new Label("Approvals");
        title.setStyle("-fx-font-size: 14px; -fx-font-weight: 700;");
        Label hint = new Label(
                "Two-person approvals for destructive maintenance actions. "
                + "Reviewers approve or reject; executors consume the "
                + "APPROVED row when the action fires.");
        hint.setStyle("-fx-text-fill: -color-fg-muted; -fx-font-size: 11px;");
        hint.setWrapText(true);
        VBox v = new VBox(4, title, hint);
        v.setPadding(new Insets(0, 0, 10, 0));
        return v;
    }

    private Region buildCenter() {
        table.setPlaceholder(new Label("No pending approvals."));
        table.getColumns().setAll(
                col("Action", 160, r -> r.actionName()),
                col("Requested by", 140, Approval.Row::requestedBy),
                col("Requested at", 170, r -> Instant.ofEpochMilli(
                        r.requestedAt()).toString()),
                col("Mode", 110, r -> r.mode().name()),
                col("Expires", 170, r -> r.expiresAt() == null ? "—"
                        : Instant.ofEpochMilli(r.expiresAt()).toString()));
        table.setPrefHeight(220);
        table.getSelectionModel().selectedItemProperty().addListener((o, a, r) -> {
            if (r == null) {
                detailArea.clear();
            } else {
                detailArea.setText(
                        "action_uuid: " + r.actionUuid()
                        + "\npayload_hash: " + r.payloadHash()
                        + "\n\npayload:\n" + r.payloadJson());
            }
        });

        table.setAccessibleText("Pending approvals table");
        detailArea.setEditable(false);
        detailArea.setWrapText(true);
        detailArea.setPrefRowCount(6);
        detailArea.setAccessibleText("Action detail for the selected approval");
        Label detailLabel = new Label("Action detail");
        detailLabel.setStyle("-fx-text-fill: -color-fg-default; -fx-font-size: 11px; -fx-font-weight: 600;");
        VBox v = new VBox(6, table, detailLabel, detailArea);
        VBox.setVgrow(detailArea, Priority.ALWAYS);
        return v;
    }

    private Region buildActions() {
        Button approveBtn = new Button("Approve");
        approveBtn.setTooltip(tip(
                "Open a reviewer-name prompt; the approved row is ready "
                + "for the executor to consume."));
        approveBtn.setOnAction(e -> onApprove());

        Button rejectBtn = new Button("Reject");
        rejectBtn.setOnAction(e -> onReject());

        Button exportTokenBtn = new Button("Export token…");
        exportTokenBtn.setTooltip(tip(
                "Reviewer mode: produce a signed JWS the executor pastes "
                + "into a TOKEN-mode request in their own install."));
        exportTokenBtn.setOnAction(e -> onExportToken());

        Button refreshBtn = new Button("Refresh");
        refreshBtn.setOnAction(e -> refresh());

        Region grow = new Region();
        HBox.setHgrow(grow, Priority.ALWAYS);
        HBox actions = new HBox(8, approveBtn, rejectBtn, exportTokenBtn,
                grow, refreshBtn);
        actions.setPadding(new Insets(10, 0, 0, 0));

        statusLabel.setStyle("-fx-text-fill: -color-fg-muted; -fx-font-size: 11px;");
        statusLabel.setWrapText(true);
        VBox v = new VBox(6, actions, statusLabel);
        return v;
    }

    /* =============================== actions =============================== */

    private void onApprove() {
        Approval.Row sel = table.getSelectionModel().getSelectedItem();
        if (sel == null) { statusLabel.setText("Pick a row first."); return; }
        TextInputDialog dlg = new TextInputDialog();
        dlg.setHeaderText("Approve " + sel.actionName() + "?");
        dlg.setContentText("Your name (not the requester's):");
        dlg.showAndWait().ifPresent(name -> {
            String reviewer = name.trim();
            if (reviewer.isEmpty()) {
                statusLabel.setText("Cancelled — blank reviewer name.");
                return;
            }
            statusLabel.setText("Approving…");
            // Off the FX thread — approveTwoPerson takes the SQLite
            // write lock and synchronously waits for it. On a slow
            // disk or during another DB writer the FX thread would
            // otherwise freeze the whole app.
            Thread.startVirtualThread(() -> {
                boolean ok;
                try {
                    ok = service.approveTwoPerson(sel.actionUuid(), reviewer);
                } catch (Throwable t) {
                    Platform.runLater(() -> statusLabel.setText(
                            "Approve failed: " + t.getClass().getSimpleName()
                                    + ": " + t.getMessage()));
                    return;
                }
                Platform.runLater(() -> {
                    if (ok) statusLabel.setText("Approved by " + reviewer + ".");
                    else statusLabel.setText(
                            "Approval refused — self-approval is not allowed; "
                            + "only PENDING non-expired rows can be approved.");
                    refresh();
                });
            });
        });
    }

    private void onReject() {
        Approval.Row sel = table.getSelectionModel().getSelectedItem();
        if (sel == null) { statusLabel.setText("Pick a row first."); return; }
        Alert confirm = new Alert(Alert.AlertType.CONFIRMATION,
                "Reject approval for " + sel.actionName() + "?",
                ButtonType.OK, ButtonType.CANCEL);
        confirm.showAndWait().ifPresent(b -> {
            if (b != ButtonType.OK) return;
            statusLabel.setText("Rejecting…");
            Thread.startVirtualThread(() -> {
                try { service.reject(sel.actionUuid()); }
                catch (Throwable t) {
                    Platform.runLater(() -> statusLabel.setText(
                            "Reject failed: " + t.getMessage()));
                    return;
                }
                Platform.runLater(this::refresh);
            });
        });
    }

    private void onExportToken() {
        Approval.Row sel = table.getSelectionModel().getSelectedItem();
        if (sel == null) { statusLabel.setText("Pick a row first."); return; }
        TextInputDialog dlg = new TextInputDialog();
        dlg.setHeaderText("Reviewer name for the token");
        dlg.setContentText("Your name:");
        dlg.showAndWait().ifPresent(name -> {
            String jws = service.signDescriptor(toRequest(sel), name.trim());
            ClipboardContent c = new ClipboardContent();
            c.putString(jws);
            Clipboard.getSystemClipboard().setContent(c);
            statusLabel.setText(
                    "Token (" + jws.length() + " chars) copied to clipboard. "
                    + "Paste into the executor's TOKEN-mode dialog.");
        });
    }

    private static Approval.Request toRequest(Approval.Row r) {
        return new Approval.Request(r.actionUuid(), r.connectionId(),
                r.actionName(), r.payloadJson(), r.payloadHash(),
                r.requestedBy(), r.requestedAt(), r.mode());
    }

    /* =============================== helpers =============================== */

    private static Tooltip tip(String body) {
        Tooltip t = new Tooltip(body);
        t.setShowDelay(Duration.millis(250));
        t.setShowDuration(Duration.seconds(20));
        t.setWrapText(true);
        t.setMaxWidth(360);
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

package com.kubrik.mex.ui.security;

import com.kubrik.mex.security.access.UsersRolesFetcher;
import com.kubrik.mex.security.baseline.SecurityBaselineCaptureService;
import com.kubrik.mex.security.baseline.SecurityBaselineDao;
import com.kubrik.mex.security.drift.DriftAck;
import com.kubrik.mex.security.drift.DriftAckDao;
import com.kubrik.mex.security.drift.DriftDiffEngine;
import com.kubrik.mex.security.drift.DriftFinding;
import javafx.application.Platform;
import javafx.beans.property.SimpleObjectProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.ButtonType;
import javafx.scene.control.ChoiceBox;
import javafx.scene.control.ContextMenu;
import javafx.scene.control.Dialog;
import javafx.scene.control.DialogPane;
import javafx.scene.control.Label;
import javafx.scene.control.MenuItem;
import javafx.scene.control.TableCell;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextArea;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * v2.6 Q2.6-D2 — Drift sub-tab. Lets the operator pick a prior
 * {@code sec_baselines} row, diff it against either the latest
 * persisted baseline or a freshly-captured snapshot, and ack / mute
 * individual findings. The diff engine + ack DAO land in D1 and D3;
 * this pane is the view layer over both.
 */
public final class DriftPane extends BorderPane {

    private static final DateTimeFormatter TS_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm").withZone(ZoneId.systemDefault());

    private final ChoiceBox<SecurityBaselineDao.Row> baselinePicker = new ChoiceBox<>();
    private final Button refreshBtn = SecurityPaneHelpers.withTip(
            new Button("Refresh baselines"),
            "Reloads the list of captured sec_baselines rows for this connection.");
    private final Button recaptureBtn = SecurityPaneHelpers.withTip(
            new Button("Capture new baseline"),
            "Takes a fresh users + roles snapshot and diffs it against the picked baseline. "
            + "Kick this when you want 'what's drifted since Monday?'.");
    private final Button diffBtn = SecurityPaneHelpers.withTip(
            new Button("Diff against latest"),
            "Diffs the picked baseline against the most recent one captured for this connection.");
    private final ObservableList<DriftFinding> rows = FXCollections.observableArrayList();
    private final TableView<DriftFinding> table = new TableView<>(rows);
    private final Label footer = SecurityPaneHelpers.footer("—");

    private final AtomicReference<List<DriftFinding>> lastFindings = new AtomicReference<>(List.of());
    private final AtomicReference<Long> lastBaselineId = new AtomicReference<>(0L);

    private String connectionId;
    private String capturedBy = "";
    private SecurityBaselineDao baselineDao;
    private DriftAckDao ackDao;
    private SecurityBaselineCaptureService captureService;
    private Supplier<UsersRolesFetcher.Snapshot> liveSnapshot = () -> null;

    public DriftPane() {
        setStyle("-fx-background-color: white;");
        setPadding(new Insets(14, 16, 14, 16));

        setTop(buildTopBar());
        setCenter(buildTable());
        HBox foot = new HBox(footer);
        foot.setPadding(new Insets(8, 0, 0, 0));
        setBottom(foot);
    }

    public void configure(String connectionId, String capturedBy,
                           SecurityBaselineDao baselineDao, DriftAckDao ackDao,
                           SecurityBaselineCaptureService captureService,
                           Supplier<UsersRolesFetcher.Snapshot> liveSnapshot) {
        this.connectionId = connectionId;
        this.capturedBy = capturedBy == null ? "" : capturedBy;
        this.baselineDao = baselineDao;
        this.ackDao = ackDao;
        this.captureService = captureService;
        this.liveSnapshot = liveSnapshot == null ? () -> null : liveSnapshot;
    }

    public void refresh() { refreshBtn.fire(); }

    /* =========================== top bar =========================== */

    private Region buildTopBar() {
        baselinePicker.setConverter(new javafx.util.StringConverter<>() {
            @Override public String toString(SecurityBaselineDao.Row r) {
                return r == null ? "" : "#" + r.id() + "  · "
                        + TS_FMT.format(Instant.ofEpochMilli(r.capturedAt()))
                        + (r.notes().isBlank() ? "" : "  · " + r.notes());
            }
            @Override public SecurityBaselineDao.Row fromString(String s) { return null; }
        });
        baselinePicker.setPrefWidth(320);
        baselinePicker.setTooltip(SecurityPaneHelpers.tip(
                "Pick the 'before' baseline for the diff. Most-recent rows appear first."));

        refreshBtn.setOnAction(e -> reloadBaselines());
        recaptureBtn.setOnAction(e -> recaptureAndDiff());
        diffBtn.setOnAction(e -> diffSelected());

        return SecurityPaneHelpers.topBar(
                SecurityPaneHelpers.paneTitle("Security drift"),
                baselinePicker, refreshBtn, diffBtn, recaptureBtn);
    }

    private Region buildTable() {
        table.setPlaceholder(SecurityPaneHelpers.emptyState(
                "No drift findings",
                "Pick a baseline above and click Diff, or click Capture new "
                + "baseline to snapshot the current state and diff against "
                + "the selection. Per-row Ack/Mute actions write to "
                + "sec_drift_acks."));
        table.getColumns().setAll(
                col("Section", 100, DriftFinding::section),
                kindCol(),
                col("Path", 440, DriftFinding::path),
                col("Before", 200, f -> f.before() == null ? "—" : trim(f.before())),
                col("After", 200, f -> f.after() == null ? "—" : trim(f.after())));

        table.setRowFactory(tv -> {
            var r = new javafx.scene.control.TableRow<DriftFinding>();
            ContextMenu menu = new ContextMenu();
            MenuItem ack = new MenuItem("Ack this diff (this baseline only)");
            MenuItem mute = new MenuItem("Mute path (hide across every future diff)");
            ack.setOnAction(e -> {
                if (!r.isEmpty()) openAckDialog(r.getItem(), DriftAck.Mode.ACK);
            });
            mute.setOnAction(e -> {
                if (!r.isEmpty()) openAckDialog(r.getItem(), DriftAck.Mode.MUTE);
            });
            menu.getItems().addAll(ack, mute);
            r.setContextMenu(menu);
            return r;
        });
        return table;
    }

    private static TableColumn<DriftFinding, DriftFinding.Kind> kindCol() {
        TableColumn<DriftFinding, DriftFinding.Kind> c = new TableColumn<>("Change");
        c.setPrefWidth(100);
        c.setCellValueFactory(cd -> new SimpleObjectProperty<>(cd.getValue().kind()));
        c.setCellFactory(col -> new TableCell<>() {
            @Override protected void updateItem(DriftFinding.Kind k, boolean empty) {
                super.updateItem(k, empty);
                if (empty || k == null) { setText(""); setStyle(""); return; }
                setText(k.name());
                setStyle(switch (k) {
                    case ADDED   -> "-fx-text-fill: #166534; -fx-font-weight: 700;";
                    case REMOVED -> "-fx-text-fill: #991b1b; -fx-font-weight: 700;";
                    case CHANGED -> "-fx-text-fill: #b45309; -fx-font-weight: 700;";
                });
            }
        });
        return c;
    }

    /* ============================ actions ============================ */

    private void reloadBaselines() {
        if (baselineDao == null || connectionId == null) return;
        refreshBtn.setDisable(true);
        Thread.startVirtualThread(() -> {
            List<SecurityBaselineDao.Row> list = baselineDao.listForConnection(connectionId, 100);
            Platform.runLater(() -> {
                baselinePicker.getItems().setAll(list);
                if (!list.isEmpty()) baselinePicker.setValue(list.get(0));
                refreshBtn.setDisable(false);
                footer.setText(list.size() + " baseline(s) available");
            });
        });
    }

    private void diffSelected() {
        SecurityBaselineDao.Row picked = baselinePicker.getValue();
        if (picked == null) {
            alert("Pick a baseline first.");
            return;
        }
        // Diff against the most-recent baseline captured for this connection.
        // If the picked baseline is already the latest, diff against a live
        // capture so the operator sees current drift.
        SecurityBaselineDao.Row latest = baselineDao.latestForConnection(connectionId).orElse(null);
        if (latest == null) {
            alert("No baselines available.");
            return;
        }
        if (latest.id() == picked.id()) {
            recaptureAndDiff();
            return;
        }
        renderDiff(picked, latest);
    }

    private void recaptureAndDiff() {
        if (captureService == null || connectionId == null) return;
        SecurityBaselineDao.Row picked = baselinePicker.getValue();
        if (picked == null) {
            alert("Pick a baseline to diff against first.");
            return;
        }
        diffBtn.setDisable(true);
        recaptureBtn.setDisable(true);
        footer.setText("Capturing a fresh baseline…");
        Thread.startVirtualThread(() -> {
            UsersRolesFetcher.Snapshot snap = liveSnapshot.get();
            SecurityBaselineCaptureService.Result capture = captureService.persist(
                    connectionId, capturedBy, "auto-captured from drift diff", snap);
            SecurityBaselineDao.Row fresh = baselineDao.byId(capture.baselineId()).orElseThrow();
            Platform.runLater(() -> {
                recaptureBtn.setDisable(false);
                diffBtn.setDisable(false);
                // Refresh the picker so the new baseline shows up selectable.
                reloadBaselines();
                renderDiff(picked, fresh);
            });
        });
    }

    private void renderDiff(SecurityBaselineDao.Row before, SecurityBaselineDao.Row after) {
        java.util.Map<String, Object> beforeTree = parseJsonPayload(before.snapshotJson());
        java.util.Map<String, Object> afterTree = parseJsonPayload(after.snapshotJson());
        List<DriftFinding> all = DriftDiffEngine.diff(
                (java.util.Map<String, Object>) beforeTree.getOrDefault("payload", java.util.Map.of()),
                (java.util.Map<String, Object>) afterTree.getOrDefault("payload", java.util.Map.of()));
        List<DriftAck> acks = ackDao.listForConnection(connectionId);
        List<DriftFinding> visible = DriftAck.hideAcked(all, after.id(), acks);

        lastFindings.set(visible);
        lastBaselineId.set(after.id());
        rows.setAll(visible);

        int hidden = all.size() - visible.size();
        footer.setText(
                "Diff #" + before.id() + " → #" + after.id()
                + "  ·  " + visible.size() + " visible"
                + (hidden > 0 ? "  ·  " + hidden + " hidden by ack/mute" : ""));
    }

    private void openAckDialog(DriftFinding f, DriftAck.Mode mode) {
        if (ackDao == null) return;
        Dialog<ButtonType> d = new Dialog<>();
        if (getScene() != null) d.initOwner(getScene().getWindow());
        d.setTitle((mode == DriftAck.Mode.ACK ? "Ack" : "Mute") + " — " + f.path());

        Label head = new Label(f.kind() + "  " + f.path());
        head.setStyle("-fx-font-family: 'JetBrains Mono','Menlo',monospace; -fx-font-weight: 700;");
        TextArea note = new TextArea();
        note.setPromptText(mode == DriftAck.Mode.ACK
                ? "Why is this change expected on this baseline?"
                : "Why is this path safe to hide across every future diff?");
        note.setPrefRowCount(3);

        GridPane g = new GridPane();
        g.setHgap(10); g.setVgap(8); g.setPadding(new Insets(14));
        g.add(head, 0, 0, 2, 1);
        g.add(SecurityPaneHelpers.small("Note"), 0, 1); g.add(note, 1, 1);
        DialogPane pane = d.getDialogPane();
        pane.setContent(g);
        pane.getButtonTypes().setAll(ButtonType.CANCEL, ButtonType.OK);

        Optional<ButtonType> choice = d.showAndWait();
        if (choice.isEmpty() || choice.get() != ButtonType.OK) return;
        if (note.getText() == null || note.getText().isBlank()) {
            alert("Note is required.");
            return;
        }
        ackDao.insert(new DriftAck(-1, connectionId, lastBaselineId.get(),
                f.path(), System.currentTimeMillis(), capturedBy,
                mode, note.getText().trim()));
        // Re-apply the ack filter against the current findings so the
        // row disappears immediately without another diff round-trip.
        List<DriftAck> acks = ackDao.listForConnection(connectionId);
        List<DriftFinding> filtered = DriftAck.hideAcked(
                lastFindings.get(), lastBaselineId.get(), acks);
        rows.setAll(filtered);
    }

    /* ============================ parsing ============================ */

    /**
     * Tiny JSON-to-Map parser tuned for what
     * {@link SecurityBaselineCaptureService#toPayload} emits. A proper
     * parser would be overkill given the shape is known and canonical;
     * we route the stored JSON through the BSON {@code Document.parse}
     * which already handles the escape rules our writer uses.
     */
    @SuppressWarnings("unchecked")
    private static java.util.Map<String, Object> parseJsonPayload(String json) {
        if (json == null || json.isBlank()) return java.util.Map.of();
        try {
            // Document.parse emits a nested BSON tree whose scalars / lists
            // satisfy the diff engine's contract (Map / List / Number /
            // String / Boolean). Good enough for the baseline JSON shape.
            org.bson.Document d = org.bson.Document.parse(json);
            return new java.util.LinkedHashMap<>(d);
        } catch (Exception e) {
            return java.util.Map.of();
        }
    }

    /* ============================ helpers ============================ */

    private static final Pattern LONG_VALUE = Pattern.compile(".{60,}");

    private static String trim(String s) {
        if (s == null) return "";
        Matcher m = LONG_VALUE.matcher(s);
        if (m.find()) return s.substring(0, 57) + "…";
        return s;
    }


    private void alert(String msg) {
        Alert a = new Alert(Alert.AlertType.INFORMATION);
        if (getScene() != null) a.initOwner(getScene().getWindow());
        a.setHeaderText(null);
        a.setContentText(msg);
        a.showAndWait();
    }

    private static <T> TableColumn<T, String> col(String title, int width,
                                                   java.util.function.Function<T, String> getter) {
        TableColumn<T, String> c = new TableColumn<>(title);
        c.setPrefWidth(width);
        c.setCellValueFactory(cd -> new SimpleStringProperty(getter.apply(cd.getValue())));
        return c;
    }

    @SuppressWarnings("unused")
    private void suppressIOWarning(IOException ioe) {} // reserved for future export hook
}

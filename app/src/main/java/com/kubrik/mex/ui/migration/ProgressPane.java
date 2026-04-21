package com.kubrik.mex.ui.migration;

import com.kubrik.mex.migration.JobId;
import com.kubrik.mex.migration.events.CollectionProgress;
import com.kubrik.mex.migration.events.JobEvent;
import com.kubrik.mex.migration.events.JobStatus;
import com.kubrik.mex.migration.events.ProgressSnapshot;
import javafx.application.Platform;
import javafx.beans.property.SimpleObjectProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.ProgressBar;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.input.KeyCode;
import javafx.scene.input.KeyCodeCombination;
import javafx.scene.input.KeyCombination;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.atomic.AtomicReference;

/** Live progress view — the content of wizard step 6 and also the detail pane for a
 *  historical job in the Migrations tab. Bound to {@link JobEvent} on the bus. */
public final class ProgressPane extends VBox {

    private final Label header = new Label("—");
    private final Label metrics = new Label("—");
    private final ProgressBar bar = new ProgressBar(-1);
    private final TableView<Row> table = new TableView<>();
    private final ObservableList<Row> rows = FXCollections.observableArrayList();
    private final HBox controls = new HBox(8);
    private final Button pauseBtn = new Button("Pause");
    private final Button cancelBtn = new Button("Cancel");
    private final SimpleObjectProperty<JobId> jobId = new SimpleObjectProperty<>();
    private final SimpleObjectProperty<JobStatus> status = new SimpleObjectProperty<>(JobStatus.PENDING);

    // OBS-6 debug overlay (Ctrl+Shift+D). Off by default; diagnostic only — not user-facing.
    private final Label debugOverlay = new Label();
    private final Deque<Long> recentSnapshotNanos = new ArrayDeque<>();
    private volatile long lastSnapshotArrivedNanos = 0L;
    private long lastSnapshotAppliedNanos = 0L;
    private long lastFxLagMs = 0L;

    /** OBS-6 P2.6 frame coalescer. Progress events arrive faster than the FX thread can
     *  render. The latest snapshot wins: writers on the bus thread atomically publish into
     *  this ref, and the FX {@link AnimationTimer} drains it once per frame. Status events
     *  (lower frequency) still go through {@code Platform.runLater}. */
    private final AtomicReference<ProgressSnapshot> pendingSnapshot = new AtomicReference<>();

    public ProgressPane() {
        setSpacing(10);
        setPadding(new Insets(12));
        header.setStyle("-fx-font-weight: bold; -fx-font-size: 14px;");
        bar.setMaxWidth(Double.MAX_VALUE);

        TableColumn<Row, String> nsCol = new TableColumn<>("Collection");
        nsCol.setCellValueFactory(new PropertyValueFactory<>("sourceNs"));
        TableColumn<Row, String> tgtCol = new TableColumn<>("Target");
        tgtCol.setCellValueFactory(new PropertyValueFactory<>("targetNs"));
        TableColumn<Row, Long> docsCol = new TableColumn<>("Docs");
        docsCol.setCellValueFactory(new PropertyValueFactory<>("docs"));
        TableColumn<Row, String> statusCol = new TableColumn<>("Status");
        statusCol.setCellValueFactory(new PropertyValueFactory<>("status"));
        table.getColumns().setAll(java.util.List.of(nsCol, tgtCol, docsCol, statusCol));
        table.setItems(rows);
        table.setPlaceholder(new Label("Waiting for progress…"));
        table.setPrefHeight(200);

        controls.getChildren().setAll(pauseBtn, cancelBtn);
        getChildren().setAll(header, metrics, bar, table, controls);

        debugOverlay.setStyle(
                "-fx-font-family: 'Menlo', 'Monaco', monospace; -fx-font-size: 11px; "
                        + "-fx-background-color: rgba(0,0,0,0.78); -fx-text-fill: #8effc1; "
                        + "-fx-padding: 6 10; -fx-background-radius: 4;");
        debugOverlay.setManaged(false);
        debugOverlay.setVisible(false);
        debugOverlay.setMouseTransparent(true);
        getChildren().add(debugOverlay);
        debugOverlay.layoutXProperty().bind(
                widthProperty().subtract(debugOverlay.widthProperty()).subtract(18));
        debugOverlay.setLayoutY(12);

        sceneProperty().addListener((obs, oldScene, newScene) -> {
            if (newScene != null) {
                newScene.getAccelerators().put(
                        new KeyCodeCombination(KeyCode.D,
                                KeyCombination.SHORTCUT_DOWN, KeyCombination.SHIFT_DOWN),
                        this::toggleDebugOverlay);
            }
        });

    }

    private void toggleDebugOverlay() {
        boolean now = !debugOverlay.isVisible();
        debugOverlay.setVisible(now);
        if (now) refreshDebugOverlay();
    }

    private void recordSnapshotForDebug() {
        long now = System.nanoTime();
        lastFxLagMs = (now - lastSnapshotArrivedNanos) / 1_000_000L;
        lastSnapshotAppliedNanos = now;
        recentSnapshotNanos.addLast(now);
        long cutoff = now - 1_000_000_000L; // 1 s window
        while (!recentSnapshotNanos.isEmpty() && recentSnapshotNanos.peekFirst() < cutoff) {
            recentSnapshotNanos.removeFirst();
        }
        if (debugOverlay.isVisible()) refreshDebugOverlay();
    }

    private void refreshDebugOverlay() {
        long ageMs = lastSnapshotAppliedNanos == 0L ? -1L
                : (System.nanoTime() - lastSnapshotAppliedNanos) / 1_000_000L;
        debugOverlay.setText(String.format(
                "OBS-6 debug%n  last snapshot: %s%n  snapshots/sec: %d%n  fx-lag: %d ms",
                ageMs < 0 ? "—" : ageMs + " ms",
                recentSnapshotNanos.size(),
                lastFxLagMs));
    }

    public SimpleObjectProperty<JobStatus> statusProperty() { return status; }
    public Button pauseButton() { return pauseBtn; }
    public Button cancelButton() { return cancelBtn; }
    public void setJobId(JobId id) { jobId.set(id); header.setText("Job " + id.value()); }

    /** OBS-5 §3.3: the "Docs" column and headline metric bind to {@code docsProcessed} in
     *  dry-run mode (no writes, so {@code docsCopied} stays 0) and to {@code docsCopied}
     *  otherwise. */
    private volatile boolean dryRun = false;
    public void setDryRun(boolean dryRun) { this.dryRun = dryRun; }

    public void onEvent(JobEvent ev) {
        if (jobId.get() == null || !jobId.get().equals(ev.jobId())) return;
        if (ev instanceof JobEvent.Progress p) {
            // Latest-wins coalescing: if a runLater drain hasn't run yet we overwrite. 200 ms
            // publish cadence gives us ≈5 ev/sec — well within what the FX thread handles.
            lastSnapshotArrivedNanos = System.nanoTime();
            pendingSnapshot.set(p.snapshot());
            Platform.runLater(() -> {
                ProgressSnapshot s = pendingSnapshot.getAndSet(null);
                if (s != null) { renderSnapshot(s); recordSnapshotForDebug(); }
            });
            return;
        }
        Platform.runLater(() -> apply(ev));
    }

    private void apply(JobEvent ev) {
        switch (ev) {
            // Progress events bypass this path — they go through pendingSnapshot + the
            // FX pulse drain (see AnimationTimer in the constructor).
            case JobEvent.StatusChanged sc -> {
                status.set(sc.newStatus());
                header.setText("Job " + ev.jobId().value() + "  ·  " + sc.newStatus());
            }
            case JobEvent.Failed f -> {
                status.set(JobStatus.FAILED);
                metrics.setText("Failed: " + f.error());
            }
            case JobEvent.Completed c -> {
                status.set(JobStatus.COMPLETED);
                bar.setProgress(1.0);
            }
            case JobEvent.Cancelled c -> status.set(JobStatus.CANCELLED);
            default -> {}
        }
    }

    private void renderSnapshot(ProgressSnapshot s) {
        long headline = dryRun ? s.docsProcessed() : s.docsCopied();
        metrics.setText(String.format(
                "%,d docs · %.1f docs/s · %.2f MB/s · %d errors · elapsed %ds",
                headline, s.docsPerSecRolling(), s.mbPerSecRolling(),
                s.errors(), s.elapsed().toSeconds()));
        if (s.docsTotal() > 0) bar.setProgress(headline / (double) s.docsTotal());
        rows.clear();
        for (CollectionProgress cp : s.perCollection()) {
            long cellDocs = dryRun ? cp.docsProcessed() : cp.docsCopied();
            rows.add(new Row(cp.source(), cp.target(), cellDocs, cp.status()));
        }
    }

    /** Table row — public only so JavaFX reflection via {@link PropertyValueFactory} works. */
    public static final class Row {
        private final String sourceNs, targetNs, status;
        private final long docs;
        public Row(String sourceNs, String targetNs, long docs, String status) {
            this.sourceNs = sourceNs; this.targetNs = targetNs; this.docs = docs; this.status = status;
        }
        public String getSourceNs() { return sourceNs; }
        public String getTargetNs() { return targetNs; }
        public long getDocs() { return docs; }
        public String getStatus() { return status; }
    }

}

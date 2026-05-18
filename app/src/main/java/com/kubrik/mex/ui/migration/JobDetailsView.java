package com.kubrik.mex.ui.migration;

import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.migration.JobId;
import com.kubrik.mex.migration.MigrationJobRecord;
import com.kubrik.mex.migration.MigrationService;
import com.kubrik.mex.migration.events.JobEvent;
import com.kubrik.mex.migration.events.JobStatus;
import com.kubrik.mex.ui.util.DurationFormat;
import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TextArea;
import javafx.scene.control.TitledPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;

/** UX-12 — read-only / live details view for a single migration job.
 *  <ul>
 *    <li>Header: id, kind, status, source/target, started/ended, duration.</li>
 *    <li>Spec summary: the persisted {@link com.kubrik.mex.migration.spec.MigrationSpec}.</li>
 *    <li>Per-collection progress: reuses {@link ProgressPane} in replay (terminal) or live mode.</li>
 *    <li>Log tail: last redacted N lines of {@code artifactDir/job.log}.</li>
 *    <li>Resume button when {@code resumePath != null} and the job is terminal-failed.</li>
 *  </ul> */
public final class JobDetailsView extends VBox {

    private static final DateTimeFormatter FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());
    private static final int LOG_TAIL_LINES = 500;

    private final MigrationService service;
    private final EventBus bus;
    private final MigrationJobRecord record;
    private final ProgressPane progress = new ProgressPane();
    private final Label headerLine = new Label();
    private final Label timingsLine = new Label();

    public JobDetailsView(MigrationService service, EventBus bus, MigrationJobRecord record) {
        this.service = service;
        this.bus = bus;
        this.record = record;

        setSpacing(12);
        setPadding(new Insets(14));

        // Header — condensed metadata.
        headerLine.setStyle("-fx-font-size: 13px; -fx-font-weight: bold;");
        timingsLine.setStyle("-fx-font-size: 11px; -fx-text-fill: #475569;");
        refreshHeader();

        // Spec summary — rendered as a text area so users can copy it.
        TextArea specText = new TextArea();
        specText.setEditable(false);
        specText.setWrapText(true);
        specText.setPrefRowCount(10);
        try {
            specText.setText(service.codec().toYaml(record.spec()));
        } catch (Exception e) {
            specText.setText("(failed to render spec: " + e.getMessage() + ")");
        }
        TitledPane specPane = new TitledPane("Spec", specText);
        specPane.setCollapsible(true);
        specPane.setExpanded(false);

        // Progress pane — live mode subscribes; terminal rows render the persisted snapshot.
        progress.setJobId(record.id());
        if (record.status().isActive()) {
            bus.onJob(progress::onEvent);
        } else {
            // Terminal-row "replay": synthesise a minimal Progress event from the final record
            // so the counters and per-collection table show up without a live subscription.
            Platform.runLater(() -> progress.onEvent(new JobEvent.Progress(
                    record.id(), record.endedAt() == null ? record.updatedAt() : record.endedAt(),
                    replaySnapshot(record))));
        }

        TitledPane logPane = new TitledPane("Log tail", logTailArea());
        logPane.setCollapsible(true);
        logPane.setExpanded(false);

        HBox footer = new HBox(8);
        footer.setPadding(new Insets(4, 0, 0, 0));
        if (record.status() == JobStatus.FAILED && record.resumePath() != null) {
            Button resume = new Button("Resume");
            resume.setOnAction(e -> {
                try { service.resume(record.id()); }
                catch (Exception ex) { showError(ex.getMessage()); }
            });
            footer.getChildren().add(resume);
        }

        VBox.setVgrow(progress, Priority.ALWAYS);
        getChildren().addAll(headerLine, timingsLine, specPane, progress, logPane, footer);
    }

    private com.kubrik.mex.migration.events.ProgressSnapshot replaySnapshot(MigrationJobRecord r) {
        // A persisted record doesn't carry per-collection breakdown beyond the timings table —
        // the replay view surfaces the aggregate counters and leaves per-coll rows empty.
        // (Richer per-coll replay is a future refinement — for now the numbers match what the
        // history row shows.)
        Duration elapsed = r.startedAt() == null || r.endedAt() == null
                ? Duration.ZERO
                : Duration.between(r.startedAt(), r.endedAt());
        return new com.kubrik.mex.migration.events.ProgressSnapshot(
                r.docsCopied(), r.docsProcessed(), -1L, r.bytesCopied(),
                0.0, 0.0, elapsed, Duration.ZERO, List.of(), r.errors());
    }

    private Region logTailArea() {
        TextArea area = new TextArea();
        area.setEditable(false);
        area.setStyle("-fx-font-family: 'Menlo', 'Monaco', monospace; -fx-font-size: 11px;");
        area.setPrefRowCount(10);
        area.setText(readLogTail(record.artifactDir()));
        return area;
    }

    private String readLogTail(Path artifactDir) {
        if (artifactDir == null) return "(no artifact directory)";
        Path log = artifactDir.resolve("job.log");
        if (!Files.exists(log)) return "(no job.log yet)";
        try {
            List<String> all = Files.readAllLines(log);
            int start = Math.max(0, all.size() - LOG_TAIL_LINES);
            return String.join("\n", all.subList(start, all.size()));
        } catch (IOException e) {
            return "(failed to read job.log: " + e.getMessage() + ")";
        }
    }

    private void refreshHeader() {
        String kind = record.kind().name();
        String src = record.sourceConnectionId() == null ? "—" : record.sourceConnectionId();
        String tgt = record.targetConnectionId() == null ? "—" : record.targetConnectionId();
        headerLine.setText(kind + "  ·  " + record.id().value()
                + "  ·  " + src + " → " + tgt + "  ·  " + record.status().name());

        String started = record.startedAt() == null ? "—" : FMT.format(record.startedAt());
        String ended = record.endedAt() == null ? "—" : FMT.format(record.endedAt());
        Duration wall = record.startedAt() == null ? Duration.ZERO
                : Duration.between(record.startedAt(),
                        record.endedAt() == null ? java.time.Instant.now() : record.endedAt());
        String duration = DurationFormat.format(wall);
        String active = DurationFormat.format(Duration.ofMillis(record.activeMillis()));
        timingsLine.setText("started " + started + "  ·  ended " + ended
                + "  ·  wall " + duration + "  ·  active " + active);
    }

    private void showError(String msg) {
        javafx.scene.control.Alert a = new javafx.scene.control.Alert(
                javafx.scene.control.Alert.AlertType.ERROR, msg);
        a.initOwner(getScene().getWindow());
        a.showAndWait();
    }
}

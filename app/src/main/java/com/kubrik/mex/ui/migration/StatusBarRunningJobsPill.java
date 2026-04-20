package com.kubrik.mex.ui.migration;

import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.migration.JobHistoryQuery;
import com.kubrik.mex.migration.JobId;
import com.kubrik.mex.migration.MigrationJobRecord;
import com.kubrik.mex.migration.MigrationService;
import com.kubrik.mex.migration.events.JobEvent;
import com.kubrik.mex.migration.events.JobStatus;
import javafx.application.Platform;
import javafx.scene.control.Label;
import javafx.scene.layout.HBox;

import java.util.List;
import java.util.function.Consumer;

/** UX-13 — a small always-visible pill placed in the main window's status bar. Visible iff
 *  at least one migration job is in a non-terminal state; clicking it opens the job's
 *  details view via the supplied callback. */
public final class StatusBarRunningJobsPill extends HBox {

    private final MigrationService service;
    private final Label label = new Label();
    private final Consumer<JobId> onOpen;

    public StatusBarRunningJobsPill(MigrationService service,
                                     EventBus bus,
                                     Consumer<JobId> onOpen) {
        this.service = service;
        this.onOpen = onOpen;

        setSpacing(6);
        setPadding(new javafx.geometry.Insets(2, 10, 2, 10));
        setStyle("-fx-background-radius: 10px; -fx-background-color: #dbeafe; "
                + "-fx-border-radius: 10px; -fx-border-color: #93c5fd; -fx-border-width: 1;");
        label.setStyle("-fx-text-fill: #1d4ed8; -fx-font-size: 11px; -fx-font-weight: bold;");
        getChildren().add(label);

        setVisible(false);
        setManaged(false);
        setOnMouseClicked(e -> openMostRecent());

        bus.onJob(ev -> {
            if (!(ev instanceof JobEvent.StatusChanged) && !(ev instanceof JobEvent.Started)
                    && !(ev instanceof JobEvent.Completed) && !(ev instanceof JobEvent.Failed)
                    && !(ev instanceof JobEvent.Cancelled)) {
                return;
            }
            Platform.runLater(this::refresh);
        });
        refresh();
    }

    private void refresh() {
        List<MigrationJobRecord> active = service.list(JobHistoryQuery.all()).stream()
                .filter(r -> !r.status().isTerminal())
                .toList();
        if (active.isEmpty()) {
            setVisible(false);
            setManaged(false);
            return;
        }
        label.setText("▶ Migration running — " + active.size() + (active.size() == 1 ? " job" : " jobs"));
        setVisible(true);
        setManaged(true);
    }

    private void openMostRecent() {
        List<MigrationJobRecord> active = service.list(JobHistoryQuery.all()).stream()
                .filter(r -> !r.status().isTerminal())
                .toList();
        if (active.isEmpty()) return;
        MigrationJobRecord most = active.get(0); // list is ordered by started_at DESC
        onOpen.accept(most.id());
    }

    /** Suppress unused-warnings when the enum is widened by later phases. */
    @SuppressWarnings("unused")
    private static boolean isLive(JobStatus s) { return s != null && !s.isTerminal(); }
}

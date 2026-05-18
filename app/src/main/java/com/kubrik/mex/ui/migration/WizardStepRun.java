package com.kubrik.mex.ui.migration;

import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.migration.JobId;
import com.kubrik.mex.migration.MigrationService;
import com.kubrik.mex.migration.events.JobStatus;
import javafx.beans.binding.Bindings;
import javafx.beans.binding.BooleanBinding;
import javafx.scene.layout.Region;

/** Step 6: renders live progress for the running job. Pause/Cancel buttons wire into
 *  {@link MigrationService}. */
public final class WizardStepRun implements WizardStep {

    private final MigrationService service;
    private final ProgressPane pane = new ProgressPane();

    public WizardStepRun(MigrationService service, EventBus bus) {
        this.service = service;
        bus.onJob(pane::onEvent);

        pane.pauseButton().setOnAction(e -> {
            JobId id = currentJob();
            if (id == null) return;
            JobStatus st = pane.statusProperty().get();
            if (st == JobStatus.PAUSED) {
                service.resumeLive(id);
            } else if (st == JobStatus.RUNNING) {
                service.pause(id);
            }
        });
        // Flip label between Pause/Resume as status changes.
        pane.statusProperty().addListener((o, a, b) ->
                pane.pauseButton().setText(b == JobStatus.PAUSED ? "Resume" : "Pause"));
        pane.cancelButton().setOnAction(e -> {
            JobId id = currentJob();
            if (id != null) service.cancel(id);
        });
    }

    private JobId currentJob() {
        return runningJobId;
    }

    private JobId runningJobId;

    public void bindJob(JobId jobId, boolean dryRun) {
        runningJobId = jobId;
        pane.setJobId(jobId);
        pane.setDryRun(dryRun);
    }

    @Override public String title() { return "6. Run"; }
    @Override public Region view() { return pane; }

    @Override public BooleanBinding validProperty() {
        return Bindings.createBooleanBinding(() -> true);
    }
}

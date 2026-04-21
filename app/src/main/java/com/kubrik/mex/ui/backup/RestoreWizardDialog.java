package com.kubrik.mex.ui.backup;

import com.kubrik.mex.backup.runner.RestoreService;
import com.kubrik.mex.backup.store.BackupCatalogRow;
import com.kubrik.mex.cluster.audit.Outcome;
import com.kubrik.mex.cluster.dryrun.CommandJson;
import com.kubrik.mex.cluster.dryrun.DryRunRenderer;
import com.kubrik.mex.cluster.safety.DryRunResult;
import com.kubrik.mex.cluster.safety.TypedConfirmDialog;
import com.kubrik.mex.cluster.safety.TypedConfirmModel;
import com.kubrik.mex.migration.log.Redactor;
import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.ButtonType;
import javafx.scene.control.CheckBox;
import javafx.scene.control.Label;
import javafx.scene.control.ProgressBar;
import javafx.scene.control.RadioButton;
import javafx.scene.control.TextField;
import javafx.scene.control.ToggleGroup;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;
import javafx.stage.Window;
import org.bson.Document;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * v2.5 Q2.5-E — multi-step restore wizard fronting {@link RestoreService}.
 *
 * <p>Flow:
 * <ol>
 *   <li>Form: target URI, mode radio (Rehearse / Execute), rehearse prefix,
 *       drop-before-restore (Execute only), oplog-replay.</li>
 *   <li>Execute-mode confirm: typed confirm on the catalog's sinkPath plus
 *       an info banner with the three-gate reminder.</li>
 *   <li>Live progress: indeterminate progress bar + verdict label + Cancel
 *       button while the service runs on a virtual thread. On end, a
 *       result alert summarises outcome + failures + duration.</li>
 * </ol>
 */
public final class RestoreWizardDialog {

    private RestoreWizardDialog() {}

    public static void show(Window owner, BackupCatalogRow row, RestoreService service,
                             String callerUser, String callerHost) {
        Stage stage = new Stage();
        if (owner != null) stage.initOwner(owner);
        stage.setTitle("Restore backup #" + row.id());

        TextField uriField = new TextField("mongodb://");
        ToggleGroup modeGroup = new ToggleGroup();
        RadioButton rehearse = new RadioButton("Rehearse (dry-run into sandbox)");
        RadioButton execute = new RadioButton("Execute (write to target)");
        rehearse.setToggleGroup(modeGroup);
        execute.setToggleGroup(modeGroup);
        rehearse.setSelected(true);

        TextField rehearsePrefix = new TextField("rehearse_");
        CheckBox dropBox = new CheckBox("Drop target namespaces first");
        dropBox.setDisable(true);
        CheckBox oplogBox = new CheckBox("Replay oplog slice");
        oplogBox.setSelected(true);

        modeGroup.selectedToggleProperty().addListener((o, old, n) -> {
            boolean isExec = execute.isSelected();
            dropBox.setDisable(!isExec);
            rehearsePrefix.setDisable(isExec);
        });

        Label banner = new Label();
        banner.setWrapText(true);
        banner.setStyle("-fx-text-fill: #92400e; -fx-font-size: 11px;");
        banner.setText("Rehearse runs with --dryRun; no namespaces are modified. "
                + "Execute requires a typed confirmation and respects the "
                + "kill-switch.");

        ProgressBar progress = new ProgressBar(0);
        progress.setPrefWidth(Double.MAX_VALUE);
        progress.setVisible(false);
        progress.managedProperty().bind(progress.visibleProperty());
        Label status = new Label("");
        status.setStyle("-fx-text-fill: #374151; -fx-font-size: 12px;");

        Button startBtn = new Button("Start");
        Button closeBtn = new Button("Close");
        closeBtn.setOnAction(e -> stage.close());

        startBtn.setOnAction(e -> {
            String uri = uriField.getText().trim();
            if (uri.isEmpty() || !uri.startsWith("mongodb")) {
                status.setText("Target URI must start with mongodb:// or mongodb+srv://");
                status.setStyle("-fx-text-fill: #b91c1c; -fx-font-size: 12px;");
                return;
            }
            RestoreService.Mode mode = execute.isSelected()
                    ? RestoreService.Mode.EXECUTE : RestoreService.Mode.REHEARSE;
            if (mode == RestoreService.Mode.EXECUTE
                    && !confirmExecute(stage, row, uri, dropBox.isSelected(), oplogBox.isSelected())) {
                status.setText("Cancelled.");
                status.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 12px;");
                return;
            }
            startBtn.setDisable(true);
            closeBtn.setDisable(true);
            progress.setVisible(true);
            progress.setProgress(ProgressBar.INDETERMINATE_PROGRESS);
            status.setText("Running mongorestore…");
            status.setStyle("-fx-text-fill: #2563eb; -fx-font-size: 12px; -fx-font-weight: 700;");

            String prefix = rehearsePrefix.getText();
            boolean drop = dropBox.isSelected();
            boolean oplog = oplogBox.isSelected();
            Thread.startVirtualThread(() -> {
                RestoreService.RestoreResult r = service.execute(row.id(), uri,
                        mode, prefix, drop, oplog, callerUser, callerHost);
                Platform.runLater(() -> {
                    progress.setVisible(false);
                    startBtn.setDisable(false);
                    closeBtn.setDisable(false);
                    status.setText(r.outcome() + "  —  " + r.durationMs() + " ms"
                            + (r.failures() > 0 ? "  ·  " + r.failures() + " failures" : ""));
                    status.setStyle(resultStyle(r.outcome()));
                    if (r.outcome() == Outcome.OK) {
                        info(stage, "Restore complete",
                                "Restore finished successfully." + (r.failures() > 0
                                        ? "\n(" + r.failures() + " per-namespace failures)" : ""));
                    } else {
                        info(stage, "Restore " + r.outcome(), r.message());
                    }
                });
            });
        });

        /* ============================ layout ============================ */

        GridPane g = new GridPane();
        g.setHgap(10);
        g.setVgap(8);
        g.setPadding(new Insets(14));
        int row0 = 0;
        g.add(small("target URI"), 0, row0); g.add(uriField, 1, row0++, 2, 1);
        g.add(small("mode"), 0, row0);
        g.add(new VBox(4, rehearse, execute), 1, row0++, 2, 1);
        g.add(small("rehearse prefix"), 0, row0); g.add(rehearsePrefix, 1, row0++, 2, 1);
        g.add(small("options"), 0, row0);
        g.add(new VBox(4, dropBox, oplogBox), 1, row0++, 2, 1);
        g.add(banner, 0, row0++, 3, 1);

        Region grow = new Region();
        HBox.setHgrow(grow, Priority.ALWAYS);
        HBox actions = new HBox(8, startBtn, grow, closeBtn);
        actions.setAlignment(Pos.CENTER_RIGHT);
        actions.setPadding(new Insets(6, 14, 12, 14));

        VBox bottom = new VBox(8, progress, status, actions);
        bottom.setPadding(new Insets(6, 14, 0, 14));

        BorderPane root = new BorderPane(g);
        Label title = new Label("Restore backup #" + row.id() + "  ·  " + row.sinkPath());
        title.setStyle("-fx-font-size: 14px; -fx-font-weight: 700; -fx-padding: 12 14 0 14;");
        root.setTop(title);
        root.setBottom(bottom);
        root.setStyle("-fx-background-color: white;");

        Scene scene = new Scene(root, 640, 460);
        stage.setScene(scene);
        stage.show();
    }

    /* =========================== confirm step =========================== */

    /** Reuses the v2.4 three-gate surface: typed-confirm dialog + preview
     *  JSON + hash footer. Expected string is the catalog row's sink path
     *  so operators have to look at which backup they're about to restore
     *  from before they can enable Execute. */
    private static boolean confirmExecute(Window owner, BackupCatalogRow row,
                                          String targetUri, boolean drop, boolean oplog) {
        // Never show the raw URI in the preview — it may carry credentials
        // (mongodb://user:pass@host). The redactor swaps the password slot
        // for "***" so screen-capture and session-recording tools never see
        // the secret. Audit rows only store the sinkPath anyway.
        String redactedUri = Redactor.defaultInstance().redact(targetUri);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("mongorestore", row.sinkPath());
        body.put("target", redactedUri);
        body.put("drop", drop);
        body.put("oplogReplay", oplog);
        String json = CommandJson.render(body);
        String hash = DryRunRenderer.sha256(json);
        String summary = "Execute restore from " + row.sinkPath() + " → " + redactedUri;
        String predicted = "mongorestore reads the backup tree and writes into the target cluster. "
                + "Kill-switch and role gate still apply at dispatch. "
                + (drop ? "Existing target namespaces are dropped first. "
                        : "Existing documents are upserted; no drop. ")
                + (oplog ? "Oplog slice is replayed after restore for point-in-time consistency."
                        : "Oplog replay skipped.");
        DryRunResult preview = new DryRunResult("mongorestore",
                new Document(body), json, summary, predicted, hash);
        TypedConfirmModel model = new TypedConfirmModel(row.sinkPath(), preview);
        Optional<TypedConfirmModel.Outcome> outcome =
                TypedConfirmDialog.showAndWait(owner, model);
        return outcome.isPresent() && outcome.get() == TypedConfirmModel.Outcome.CONFIRMED;
    }

    private static void info(Window owner, String title, String body) {
        Alert a = new Alert(Alert.AlertType.INFORMATION);
        a.initOwner(owner);
        a.setTitle(title);
        a.setHeaderText(null);
        a.setContentText(body);
        a.getButtonTypes().setAll(ButtonType.OK);
        a.showAndWait();
    }

    /* ============================== helpers ============================= */

    private static Label small(String s) {
        Label l = new Label(s);
        l.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px;");
        return l;
    }

    private static String resultStyle(Outcome outcome) {
        String fg = switch (outcome) {
            case OK -> "#166534";
            case CANCELLED -> "#92400e";
            case FAIL -> "#b91c1c";
            case PENDING -> "#374151";
        };
        return "-fx-text-fill: " + fg + "; -fx-font-size: 12px; -fx-font-weight: 700;";
    }
}

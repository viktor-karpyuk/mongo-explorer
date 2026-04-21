package com.kubrik.mex.ui.backup;

import com.kubrik.mex.backup.pitr.PitrPlanner;
import com.kubrik.mex.backup.pitr.RestorePlan;
import com.kubrik.mex.backup.runner.RestoreService;
import com.kubrik.mex.backup.store.BackupCatalogRow;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.DatePicker;
import javafx.scene.control.Label;
import javafx.scene.control.TextField;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;
import javafx.stage.Window;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * v2.5 Q2.5-F.3 — point-in-time recovery picker.
 *
 * <p>User picks a target date + HH:MM (UTC); the dialog runs
 * {@link PitrPlanner#plan} and shows either a feasible
 * {@link RestorePlan} (source backup + oplog replay limit) or the
 * refusal reason the planner produced. On a feasible plan, a
 * <em>Restore to this point</em> button hands off to
 * {@link RestoreWizardDialog} pre-populated with the selected backup.</p>
 */
public final class PitrPickerDialog {

    private static final DateTimeFormatter TS_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss 'UTC'")
                    .withZone(ZoneId.of("UTC"));

    private PitrPickerDialog() {}

    public static void show(Window owner, String connectionId, PitrPlanner planner,
                             RestoreService restoreService,
                             String callerUser, String callerHost) {
        Stage stage = new Stage();
        if (owner != null) stage.initOwner(owner);
        stage.setTitle("PITR picker · " + connectionId);

        DatePicker datePicker = new DatePicker(LocalDate.now(ZoneId.of("UTC")));
        TextField timeField = new TextField("00:00");
        timeField.setPromptText("HH:MM");
        timeField.setPrefWidth(80);

        Label plannerVerdict = new Label("Pick a target time and press \"Plan\".");
        plannerVerdict.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 12px;");
        plannerVerdict.setWrapText(true);

        Button planBtn = new Button("Plan");
        Button restoreBtn = new Button("Restore to this point…");
        restoreBtn.setDisable(true);
        Button closeBtn = new Button("Close");
        closeBtn.setOnAction(e -> stage.close());

        java.util.concurrent.atomic.AtomicReference<RestorePlan> lastPlan =
                new java.util.concurrent.atomic.AtomicReference<>();

        planBtn.setOnAction(e -> {
            LocalDate date = datePicker.getValue();
            LocalTime time;
            try {
                String raw = timeField.getText() == null ? "" : timeField.getText().trim();
                time = LocalTime.parse(raw.isEmpty() ? "00:00" : raw);
            } catch (Exception bad) {
                plannerVerdict.setText("Invalid time: " + bad.getMessage());
                plannerVerdict.setStyle("-fx-text-fill: #b91c1c; -fx-font-size: 12px;");
                restoreBtn.setDisable(true);
                return;
            }
            Instant target = LocalDateTime.of(date, time).atZone(ZoneId.of("UTC")).toInstant();
            RestorePlan plan = planner.plan(connectionId, target.getEpochSecond());
            lastPlan.set(plan);
            if (plan.feasible()) {
                BackupCatalogRow src = plan.source().orElseThrow();
                plannerVerdict.setText("Plan: restore from backup #" + src.id()
                        + "  (" + src.sinkPath() + ")\n"
                        + "Oplog replay up to " + TS_FMT.format(
                                Instant.ofEpochSecond(plan.oplogLimitTs())));
                plannerVerdict.setStyle("-fx-text-fill: #166534; -fx-font-size: 12px; "
                        + "-fx-font-weight: 700;");
                restoreBtn.setDisable(restoreService == null);
            } else {
                plannerVerdict.setText("No feasible plan: " + plan.refusal());
                plannerVerdict.setStyle("-fx-text-fill: #b45309; -fx-font-size: 12px;");
                restoreBtn.setDisable(true);
            }
        });

        restoreBtn.setOnAction(e -> {
            RestorePlan plan = lastPlan.get();
            if (plan == null || !plan.feasible() || restoreService == null) return;
            RestoreWizardDialog.show(stage, plan.source().orElseThrow(),
                    restoreService, callerUser, callerHost);
        });

        GridPane g = new GridPane();
        g.setHgap(10);
        g.setVgap(8);
        g.setPadding(new Insets(14));
        g.add(small("date (UTC)"), 0, 0); g.add(datePicker, 1, 0);
        g.add(small("time HH:MM"), 0, 1); g.add(timeField, 1, 1);
        g.add(planBtn, 1, 2);
        g.add(plannerVerdict, 0, 3, 3, 1);

        Region grow = new Region();
        HBox.setHgrow(grow, Priority.ALWAYS);
        HBox footer = new HBox(8, grow, restoreBtn, closeBtn);
        footer.setAlignment(Pos.CENTER_RIGHT);
        footer.setPadding(new Insets(10, 14, 12, 14));

        Label title = new Label("Point-in-time recovery");
        title.setStyle("-fx-font-size: 14px; -fx-font-weight: 700; -fx-padding: 12 14 4 14;");
        Label sub = new Label("Pick a UTC target; the planner finds the most-recent "
                + "backup whose oplog window covers it.");
        sub.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px; -fx-padding: 0 14 8 14;");
        BorderPane root = new BorderPane(g);
        root.setTop(new VBox(title, sub));
        root.setBottom(footer);
        root.setStyle("-fx-background-color: white;");

        Scene scene = new Scene(root, 560, 360);
        stage.setScene(scene);
        stage.show();
    }

    private static Label small(String s) {
        Label l = new Label(s);
        l.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px;");
        return l;
    }
}

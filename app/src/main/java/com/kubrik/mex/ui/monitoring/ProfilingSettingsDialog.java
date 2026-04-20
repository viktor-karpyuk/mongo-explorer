package com.kubrik.mex.ui.monitoring;

import com.kubrik.mex.core.MongoService;
import com.kubrik.mex.monitoring.MonitoringProfile;
import com.kubrik.mex.monitoring.MonitoringService;
import com.kubrik.mex.monitoring.ProfilingController;
import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.ButtonType;
import javafx.scene.control.CheckBox;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Dialog;
import javafx.scene.control.DialogPane;
import javafx.scene.control.Label;
import javafx.scene.control.Spinner;
import javafx.scene.layout.GridPane;
import javafx.stage.Window;

import java.sql.SQLException;
import java.time.Duration;

/**
 * Configure server-side slow-query profiling for one monitored connection.
 * Applies {@code db.setProfilingLevel} across every non-system database,
 * persists the resulting {@link MonitoringProfile}, and starts/stops the
 * tailing {@link com.kubrik.mex.monitoring.sampler.ProfilerSampler}.
 *
 * <p>Auto-disable window protects against leaving profiling on overnight on a
 * prod cluster — a one-shot timer flips it back off when the window elapses.
 */
public final class ProfilingSettingsDialog extends Dialog<Boolean> {

    private static final java.util.List<AutoDisableOption> AUTO_DISABLE_OPTIONS = java.util.List.of(
            new AutoDisableOption("Never (leave on until disabled manually)", Duration.ZERO),
            new AutoDisableOption("15 minutes",  Duration.ofMinutes(15)),
            new AutoDisableOption("1 hour",      Duration.ofHours(1)),
            new AutoDisableOption("4 hours",     Duration.ofHours(4)),
            new AutoDisableOption("24 hours",    Duration.ofHours(24)));

    public ProfilingSettingsDialog(Window owner,
                                   MonitoringService svc,
                                   String connectionId,
                                   String connectionDisplayName,
                                   MongoService mongo) {
        setTitle("Slow-query profiling · " + connectionDisplayName);
        if (owner != null) initOwner(owner);

        MonitoringProfile current = svc.profile(connectionId).orElse(null);
        boolean initialEnabled = current != null && current.profilerEnabled();
        int initialSlowMs = current != null ? current.profilerSlowMs() : 100;
        Duration initialAuto = current != null ? current.profilerAutoDisableAfter() : Duration.ZERO;

        CheckBox enable = new CheckBox("Enable slow-query profiling");
        enable.setSelected(initialEnabled);

        Spinner<Integer> slowMs = new Spinner<>(10, 60_000, Math.max(10, initialSlowMs), 10);
        slowMs.setEditable(true);
        slowMs.setPrefWidth(140);

        ComboBox<AutoDisableOption> autoDisable = new ComboBox<>();
        autoDisable.getItems().setAll(AUTO_DISABLE_OPTIONS);
        autoDisable.getSelectionModel().select(bestMatch(initialAuto));
        autoDisable.setPrefWidth(280);

        Label warn = new Label(
                "Profiling level 1 is set on every user database (system DBs are skipped). "
              + "Each slow op becomes one insert into system.profile — keep slowms high "
              + "on busy clusters.");
        warn.setWrapText(true);
        warn.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px;");
        warn.setMaxWidth(420);

        Label result = new Label("");
        result.setStyle("-fx-font-size: 11px;");
        result.setWrapText(true);
        result.setMaxWidth(420);

        GridPane grid = new GridPane();
        grid.setHgap(10); grid.setVgap(8);
        grid.setPadding(new Insets(8, 8, 0, 8));
        grid.addRow(0, new Label("Enabled:"),        enable);
        grid.addRow(1, new Label("slowms:"),         slowMs);
        grid.addRow(2, new Label("Auto-disable:"),   autoDisable);
        grid.add(warn,   1, 3);
        grid.add(result, 1, 4);

        slowMs.disableProperty().bind(enable.selectedProperty().not());
        autoDisable.disableProperty().bind(enable.selectedProperty().not());

        DialogPane pane = getDialogPane();
        pane.setContent(grid);
        pane.getButtonTypes().setAll(ButtonType.APPLY, ButtonType.CANCEL);
        Button apply = (Button) pane.lookupButton(ButtonType.APPLY);
        apply.setText("Apply");
        apply.addEventFilter(javafx.event.ActionEvent.ACTION, ev -> {
            ev.consume();
            apply.setDisable(true);
            result.setText("Applying…");
            boolean wantEnabled = enable.isSelected();
            int ms = Math.max(1, slowMs.getValue() == null ? 100 : slowMs.getValue());
            Duration auto = autoDisable.getValue() != null ? autoDisable.getValue().duration() : Duration.ZERO;
            Thread.startVirtualThread(() -> {
                try {
                    ProfilingController.Result r = svc.setProfilingEnabled(
                            connectionId, mongo, wantEnabled, ms, auto);
                    Platform.runLater(() -> {
                        result.setText(describe(r, wantEnabled));
                        setResult(Boolean.TRUE);
                        close();
                    });
                } catch (SQLException | RuntimeException ex) {
                    Platform.runLater(() -> {
                        apply.setDisable(false);
                        result.setStyle("-fx-font-size: 11px; -fx-text-fill: #b91c1c;");
                        result.setText("Failed: " + ex.getMessage());
                    });
                }
            });
        });
    }

    /** Convenience: show the dialog against an owner window. */
    public static void show(Window owner, MonitoringService svc, String connectionId,
                            String connectionDisplayName, MongoService mongo) {
        if (mongo == null) {
            Alert a = new Alert(Alert.AlertType.WARNING,
                    "Connect to " + connectionDisplayName + " before configuring profiling.");
            a.initOwner(owner);
            a.showAndWait();
            return;
        }
        new ProfilingSettingsDialog(owner, svc, connectionId, connectionDisplayName, mongo).showAndWait();
    }

    private static AutoDisableOption bestMatch(Duration d) {
        if (d == null) return AUTO_DISABLE_OPTIONS.get(0);
        for (AutoDisableOption o : AUTO_DISABLE_OPTIONS) {
            if (o.duration().equals(d)) return o;
        }
        // Non-standard value (e.g. 60min from MonitoringProfile.defaults) — pick nearest.
        AutoDisableOption best = AUTO_DISABLE_OPTIONS.get(0);
        long diff = Long.MAX_VALUE;
        for (AutoDisableOption o : AUTO_DISABLE_OPTIONS) {
            long dd = Math.abs(o.duration().toMillis() - d.toMillis());
            if (dd < diff) { diff = dd; best = o; }
        }
        return best;
    }

    private static String describe(ProfilingController.Result r, boolean enabled) {
        String verb = enabled ? "Enabled" : "Disabled";
        if (r.failedCount() == 0) return verb + " on " + r.changedCount() + " database(s).";
        return verb + " on " + r.changedCount() + " database(s); "
                + r.failedCount() + " refused (likely admin/local/system).";
    }

    private record AutoDisableOption(String label, Duration duration) {
        @Override public String toString() { return label; }
    }
}

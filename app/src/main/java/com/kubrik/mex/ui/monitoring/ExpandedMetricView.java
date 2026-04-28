package com.kubrik.mex.ui.monitoring;

import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.monitoring.MonitoringService;
import com.kubrik.mex.monitoring.model.MetricId;
import com.kubrik.mex.monitoring.model.MetricSample;
import com.kubrik.mex.monitoring.model.Unit;
import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Button;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Label;
import javafx.scene.control.TextArea;
import javafx.scene.input.Clipboard;
import javafx.scene.input.ClipboardContent;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;

/**
 * GRAPH-EXPAND-2: full-size view of a single metric, scoped to one
 * {@code (connectionId, metric)} pair. Seeds the chart from
 * {@link MonitoringService#queryRaw(String, MetricId, long, long)} over the
 * chosen range (15m / 1h / 6h / 24h) and streams forward via
 * {@link EventBus#onMetrics}. Readouts (current / min / max / avg / p95 / p99)
 * are computed over the visible window. The diagnostic command block comes from
 * {@link DiagnosticCommands#forMetric(MetricId)}.
 *
 * <p>No unsubscribe: the bus does not support it. Practically this leaks one
 * metric filter per open view, bounded by how often the user opens the modal;
 * each filter is a cheap {@code equals} branch per sample.
 */
public final class ExpandedMetricView extends VBox implements AutoCloseable {

    public enum Range {
        M15("15 min",  15L * 60_000L),
        H1 ("1 hour",       3_600_000L),
        H6 ("6 hours",  6L * 3_600_000L),
        H24("24 hours",24L * 3_600_000L);
        final String label;
        final long   durationMs;
        Range(String label, long durationMs) { this.label = label; this.durationMs = durationMs; }
        @Override public String toString() { return label; }
    }

    private final String connectionId;
    private final MetricId metric;
    private final MonitoringService svc;

    private final Sparkline chart;
    private final Label current = readout("Current", "—");
    private final Label min     = readout("Min",     "—");
    private final Label max     = readout("Max",     "—");
    private final Label avg     = readout("Avg",     "—");
    private final Label p95     = readout("p95",     "—");
    private final Label p99     = readout("p99",     "—");
    private final ComboBox<Range> rangePicker = new ComboBox<>();
    private final Deque<MetricSample> window = new ArrayDeque<>();
    private final EventBus.Subscription sub;

    public ExpandedMetricView(String connectionId, MetricId metric,
                              String connectionDisplayName,
                              EventBus bus, MonitoringService svc) {
        this.connectionId = connectionId;
        this.metric = metric;
        this.svc = svc;

        setPadding(new Insets(16));
        setSpacing(12);

        Label header = new Label(metric.metricName() + "  ·  " + connectionDisplayName);
        header.setStyle("-fx-font-size: 18px; -fx-font-weight: bold;");
        Label subhead = new Label(metric.name() + "   [" + metric.unit() + "]");
        subhead.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 12px;");

        rangePicker.getItems().setAll(Range.values());
        rangePicker.setValue(Range.M15);
        rangePicker.valueProperty().addListener((o, a, b) -> seedHistorical(b));

        Button copyCmd = new Button("Copy diagnostic command");
        copyCmd.setOnAction(e -> {
            Clipboard cb = Clipboard.getSystemClipboard();
            ClipboardContent cc = new ClipboardContent();
            cc.putString(DiagnosticCommands.forMetric(metric));
            cb.setContent(cc);
        });

        Region spacer = new Region();
        HBox.setHgrow(spacer, Priority.ALWAYS);
        HBox controls = new HBox(8, new Label("Range:"), rangePicker, spacer, copyCmd);
        controls.setAlignment(Pos.CENTER_LEFT);

        this.chart = new Sparkline(900, 360, true);

        GridPane readouts = new GridPane();
        readouts.setHgap(24);
        readouts.setVgap(2);
        readouts.addRow(0, current, min, max, avg, p95, p99);

        TextArea cmdArea = new TextArea(DiagnosticCommands.forMetric(metric));
        cmdArea.setEditable(false);
        cmdArea.setWrapText(true);
        cmdArea.setPrefRowCount(4);
        cmdArea.setStyle("-fx-font-family: 'Menlo', 'Monaco', monospace; -fx-font-size: 12px;");

        Label cmdHeader = new Label("Diagnostic command");
        cmdHeader.setStyle("-fx-font-weight: bold; -fx-font-size: 12px;");

        getChildren().addAll(header, subhead, controls, chart, readouts, cmdHeader, cmdArea);

        this.sub = bus.onMetrics(this::onSamples);
        seedHistorical(Range.M15);
    }

    public String connectionId() { return connectionId; }
    public MetricId metric() { return metric; }

    @Override
    public void close() { try { sub.close(); } catch (Throwable ignored) {} }

    // ---- sample intake --------------------------------------------------

    private void onSamples(List<MetricSample> batch) {
        java.util.List<MetricSample> matched = null;
        for (MetricSample s : batch) {
            if (!connectionId.equals(s.connectionId())) continue;
            if (s.metric() != metric) continue;
            if (matched == null) matched = new java.util.ArrayList<>();
            matched.add(s);
            window.addLast(s);
        }
        trimWindow();
        if (matched == null) return;
        chart.push(matched);
        recomputeReadouts();
    }

    private void seedHistorical(Range r) {
        long now = System.currentTimeMillis();
        long from = now - r.durationMs;
        Thread.startVirtualThread(() -> {
            List<MetricSample> rows;
            try { rows = svc.queryRaw(connectionId, metric, from, now); }
            catch (Exception ex) { rows = List.of(); }
            List<MetricSample> seed = rows;
            Platform.runLater(() -> {
                window.clear();
                window.addAll(seed);
                chart.reset();
                if (!seed.isEmpty()) chart.push(seed);
                recomputeReadouts();
            });
        });
    }

    private void trimWindow() {
        Range r = rangePicker.getValue();
        long cutoff = System.currentTimeMillis() - (r == null ? Range.M15.durationMs : r.durationMs);
        while (!window.isEmpty() && window.peekFirst().tsMs() < cutoff) window.pollFirst();
    }

    private void recomputeReadouts() {
        if (window.isEmpty()) return;
        double[] vals = new double[window.size()];
        int i = 0; double sum = 0, mn = Double.POSITIVE_INFINITY, mx = Double.NEGATIVE_INFINITY;
        MetricSample last = null;
        for (MetricSample s : window) {
            vals[i] = s.value();
            if (s.value() < mn) mn = s.value();
            if (s.value() > mx) mx = s.value();
            sum += s.value();
            last = s;
            i++;
        }
        double[] sorted = vals.clone();
        Arrays.sort(sorted);
        double p95v = sorted[Math.min(sorted.length - 1, (int) Math.ceil(sorted.length * 0.95) - 1)];
        double p99v = sorted[Math.min(sorted.length - 1, (int) Math.ceil(sorted.length * 0.99) - 1)];
        final double avgV = sum / vals.length;
        final double mnF = mn, mxF = mx, p95F = p95v, p99F = p99v;
        final MetricSample lastF = last;
        Platform.runLater(() -> {
            setValue(current, "Current", fmt(lastF == null ? 0 : lastF.value(), metric.unit()));
            setValue(min,     "Min",     fmt(mnF, metric.unit()));
            setValue(max,     "Max",     fmt(mxF, metric.unit()));
            setValue(avg,     "Avg",     fmt(avgV, metric.unit()));
            setValue(p95,     "p95",     fmt(p95F, metric.unit()));
            setValue(p99,     "p99",     fmt(p99F, metric.unit()));
        });
    }

    // ---- UI helpers -----------------------------------------------------

    private static Label readout(String title, String value) {
        Label l = new Label(title + "\n" + value);
        l.setStyle("-fx-font-size: 12px; -fx-text-fill: #111827;");
        return l;
    }

    private static void setValue(Label l, String title, String value) {
        l.setText(title + "\n" + value);
    }

    private static String fmt(double v, Unit unit) {
        return switch (unit) {
            case BYTES, BYTES_PER_SECOND -> humanBytes(v) + (unit == Unit.BYTES_PER_SECOND ? "/s" : "");
            case RATIO                   -> String.format("%.3f", v);
            case OPS_PER_SECOND          -> String.format("%.1f ops/s", v);
            case MICROSECONDS            -> String.format("%.0f µs", v);
            case MILLISECONDS            -> String.format("%.1f ms", v);
            case BOOL                    -> v == 0 ? "false" : "true";
            default                      -> String.format("%.0f", v);
        };
    }

    private static String humanBytes(double v) {
        String[] u = { "B", "KB", "MB", "GB", "TB" };
        int i = 0;
        while (v >= 1024 && i < u.length - 1) { v /= 1024; i++; }
        return String.format("%.1f %s", v, u[i]);
    }
}

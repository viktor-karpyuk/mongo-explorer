package com.kubrik.mex.ui.monitoring;

import com.kubrik.mex.monitoring.model.MetricId;
import com.kubrik.mex.monitoring.model.MetricSample;
import com.kubrik.mex.monitoring.model.Unit;
import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Cursor;
import javafx.scene.control.ContextMenu;
import javafx.scene.control.Label;
import javafx.scene.control.MenuItem;
import javafx.scene.input.Clipboard;
import javafx.scene.input.ClipboardContent;
import javafx.scene.input.KeyCode;
import javafx.scene.input.MouseButton;
import javafx.scene.layout.VBox;

import java.util.List;

/**
 * One cell of the metric grid: title, current value + unit, sparkline,
 * status dot. Two preset sizes — {@link Size#NORMAL} for the standard dashboard
 * grid and {@link Size#LARGE} for a maximized-section view.
 */
public final class MetricCell extends VBox {

    public enum Size {
        NORMAL(200, 140, 176, 56, 18, true),
        LARGE (340, 240, 316, 140, 28, true);

        final double cellW, cellH, chartW, chartH;
        final int valueFontPx;
        final boolean showAxes;
        Size(double cellW, double cellH, double chartW, double chartH, int valueFontPx, boolean showAxes) {
            this.cellW = cellW; this.cellH = cellH;
            this.chartW = chartW; this.chartH = chartH;
            this.valueFontPx = valueFontPx;
            this.showAxes = showAxes;
        }
    }

    private final MetricId metric;
    private final Label title = new Label();
    private final Label valueLabel = new Label();
    private final Sparkline chart;
    private final Label status = new Label("● collecting…");
    private final Size size;
    private MetricExpander expander;

    public MetricCell(MetricId metric, String titleText) {
        this(metric, titleText, Size.NORMAL);
    }

    public MetricCell(MetricId metric, String titleText, Size size) {
        this(metric, titleText, size, null);
    }

    public MetricCell(MetricId metric, String titleText, Size size, MetricExpander expander) {
        this.metric = metric;
        this.size = size;
        this.expander = expander;
        getStyleClass().add("mex-metric-cell");
        setSpacing(6);
        setAlignment(Pos.TOP_LEFT);
        setPrefSize(size.cellW, size.cellH);
        setMinSize(size.cellW, size.cellH);
        setPadding(new Insets(10, 12, 10, 12));
        setStyle(
                "-fx-background-color: white;"
              + "-fx-border-color: #e5e7eb;"
              + "-fx-border-width: 1;"
              + "-fx-background-radius: 6;"
              + "-fx-border-radius: 6;");
        title.setText(titleText);
        title.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px; -fx-font-weight: bold;");
        valueLabel.setText("—");
        valueLabel.setStyle("-fx-font-size: " + size.valueFontPx + "px; -fx-font-weight: bold;");
        this.chart = new Sparkline(size.chartW, size.chartH, size.showAxes);
        status.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 10px;");
        getChildren().addAll(title, valueLabel, chart, status);

        // GRAPH-EXPAND-1 / NAV-3 — expand affordances.
        setFocusTraversable(true);
        setCursor(Cursor.HAND);
        setOnMouseClicked(e -> {
            if (e.getButton() == MouseButton.PRIMARY && e.getClickCount() == 2) openExpand(MetricExpander.Mode.MODAL);
        });
        setOnKeyPressed(e -> {
            if (e.getCode() == KeyCode.ENTER) openExpand(MetricExpander.Mode.MODAL);
        });

        ContextMenu menu = new ContextMenu();
        MenuItem openModal = new MenuItem("Open in modal");
        openModal.setOnAction(e -> openExpand(MetricExpander.Mode.MODAL));
        MenuItem openTab = new MenuItem("Open in tab");
        openTab.setOnAction(e -> openExpand(MetricExpander.Mode.TAB));
        MenuItem copyCmd = new MenuItem("Copy diagnostic command");
        copyCmd.setOnAction(e -> {
            Clipboard cb = Clipboard.getSystemClipboard();
            ClipboardContent cc = new ClipboardContent();
            cc.putString(DiagnosticCommands.forMetric(metric));
            cb.setContent(cc);
        });
        menu.getItems().addAll(openModal, openTab, copyCmd);
        setOnContextMenuRequested(e -> menu.show(this, e.getScreenX(), e.getScreenY()));
    }

    /** Section can re-wire the expander on rebind (same cell, new connection id context). */
    public void setExpander(MetricExpander expander) { this.expander = expander; }

    private void openExpand(MetricExpander.Mode mode) {
        MetricExpander ex = this.expander;
        if (ex != null) ex.open(metric, mode);
    }

    /** Clear the chart window + reset the value label. Used on section re-bind. */
    public void reset() {
        chart.reset();
        Platform.runLater(() -> {
            valueLabel.setText("—");
            status.setText("● collecting…");
            status.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 10px;");
        });
    }

    /** Push new samples (called off the event-bus thread). */
    public void onSamples(List<MetricSample> samples) {
        if (samples.isEmpty()) return;
        MetricSample last = samples.get(samples.size() - 1);
        chart.push(samples);
        Platform.runLater(() -> {
            valueLabel.setText(format(last.value(), metric.unit()));
            status.setText("● live");
            status.setStyle("-fx-text-fill: #16a34a; -fx-font-size: 10px;");
        });
    }

    public MetricId metric() { return metric; }

    private static String format(double v, Unit unit) {
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

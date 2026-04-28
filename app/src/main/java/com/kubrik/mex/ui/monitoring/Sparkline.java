package com.kubrik.mex.ui.monitoring;

import com.kubrik.mex.monitoring.model.MetricSample;
import javafx.application.Platform;
import javafx.scene.canvas.Canvas;
import javafx.scene.canvas.GraphicsContext;
import javafx.scene.paint.Color;
import javafx.scene.paint.CycleMethod;
import javafx.scene.paint.LinearGradient;
import javafx.scene.paint.Stop;
import javafx.scene.text.Font;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Rolling-window line chart on a Canvas with a gradient area fill, faint min/max
 * guides, and corner labels for the observed range. Repaint is coalesced to one
 * {@link Platform#runLater} per animation frame regardless of how many samples
 * land in that frame.
 */
public final class Sparkline extends Canvas {

    public static final int MAX_POINTS = 400;

    private static final Color LINE_COLOR = Color.web("#2563eb");
    private static final Color GUIDE_COLOR = Color.web("#e5e7eb");
    private static final Color LABEL_COLOR = Color.web("#6b7280");
    private static final LinearGradient AREA_GRADIENT = new LinearGradient(
            0, 0, 0, 1, true, CycleMethod.NO_CYCLE,
            new Stop(0, Color.web("#2563eb", 0.28)),
            new Stop(1, Color.web("#2563eb", 0.02)));

    private final Deque<MetricSample> window = new ArrayDeque<>();
    private final AtomicReference<List<MetricSample>> pending = new AtomicReference<>();
    private volatile boolean flushScheduled;

    /** Draw value axis labels and guides. Defaults off for very small cells. */
    private boolean showAxes;

    public Sparkline(double width, double height) {
        this(width, height, width >= 180);
    }

    public Sparkline(double width, double height, boolean showAxes) {
        super(width, height);
        this.showAxes = showAxes;
        clear();
    }

    /** Drop the entire window and re-paint empty. Used on section re-bind. */
    public void reset() {
        window.clear();
        pending.set(null);
        Platform.runLater(() -> {
            GraphicsContext g = getGraphicsContext2D();
            g.clearRect(0, 0, getWidth(), getHeight());
        });
    }

    public void push(List<MetricSample> samples) {
        pending.accumulateAndGet(samples, (cur, add) -> {
            if (cur == null) return List.copyOf(add);
            java.util.ArrayList<MetricSample> combined =
                    new java.util.ArrayList<>(cur.size() + add.size());
            combined.addAll(cur);
            combined.addAll(add);
            return combined;
        });
        if (!flushScheduled) {
            flushScheduled = true;
            Platform.runLater(this::flush);
        }
    }

    private void flush() {
        flushScheduled = false;
        List<MetricSample> batch = pending.getAndSet(null);
        if (batch == null) return;
        for (MetricSample s : batch) {
            window.addLast(s);
            while (window.size() > MAX_POINTS) window.pollFirst();
        }
        redraw();
    }

    private void redraw() {
        double w = getWidth(), h = getHeight();
        GraphicsContext g = getGraphicsContext2D();
        g.clearRect(0, 0, w, h);
        if (window.size() < 2) {
            if (showAxes) {
                g.setFill(LABEL_COLOR);
                g.setFont(Font.font(10));
                g.fillText("collecting…", 4, h / 2 + 3);
            }
            return;
        }
        double min = Double.POSITIVE_INFINITY, max = Double.NEGATIVE_INFINITY;
        for (MetricSample s : window) {
            if (s.value() < min) min = s.value();
            if (s.value() > max) max = s.value();
        }
        // Guard against a flat line: if min==max, pad a small range so the line
        // renders in the middle rather than off-canvas.
        if (max == min) { double pad = Math.abs(max) * 0.02 + 1; min -= pad; max += pad; }

        double plotTop = showAxes ? 12 : 0;
        double plotBottom = showAxes ? h - 12 : h;
        double plotH = Math.max(1, plotBottom - plotTop);
        double xStep = w / Math.max(1, window.size() - 1);

        // Faint min/max guides.
        if (showAxes) {
            g.setStroke(GUIDE_COLOR);
            g.setLineWidth(1);
            g.strokeLine(0, plotTop + 0.5, w, plotTop + 0.5);
            g.strokeLine(0, plotBottom - 0.5, w, plotBottom - 0.5);
        }

        // Build the polyline points.
        int n = window.size();
        double[] xs = new double[n];
        double[] ys = new double[n];
        int i = 0;
        for (MetricSample s : window) {
            xs[i] = i * xStep;
            ys[i] = plotBottom - ((s.value() - min) / (max - min)) * plotH;
            i++;
        }

        // Area fill: line points + bottom corners.
        double[] fillXs = new double[n + 2];
        double[] fillYs = new double[n + 2];
        System.arraycopy(xs, 0, fillXs, 0, n);
        System.arraycopy(ys, 0, fillYs, 0, n);
        fillXs[n]     = xs[n - 1];
        fillYs[n]     = plotBottom;
        fillXs[n + 1] = xs[0];
        fillYs[n + 1] = plotBottom;
        g.setFill(AREA_GRADIENT);
        g.fillPolygon(fillXs, fillYs, n + 2);

        // Line.
        g.setStroke(LINE_COLOR);
        g.setLineWidth(1.6);
        g.strokePolyline(xs, ys, n);

        // Corner value labels.
        if (showAxes) {
            g.setFill(LABEL_COLOR);
            g.setFont(Font.font(9));
            g.fillText(fmt(max), 2, plotTop - 1);
            g.fillText(fmt(min), 2, h - 1);
        }
    }

    private static String fmt(double v) {
        double abs = Math.abs(v);
        if (abs == 0) return "0";
        if (abs >= 1_000_000) return String.format("%.1fM", v / 1_000_000);
        if (abs >= 1_000) return String.format("%.1fk", v / 1_000);
        if (abs >= 10) return String.format("%.0f", v);
        if (abs >= 1) return String.format("%.2f", v);
        return String.format("%.3f", v);
    }

    private void clear() {
        getGraphicsContext2D().clearRect(0, 0, getWidth(), getHeight());
    }
}

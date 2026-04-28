package com.kubrik.mex.ui.monitoring;

import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.monitoring.model.MetricSample;
import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.scene.control.Label;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.VBox;

import java.util.List;
import java.util.Map;

/**
 * ROW-EXPAND-4 — live namespace (db.coll) top-metric detail. Subscribes to
 * {@code TOP_*} samples matching {@code (connectionId, db, coll)} and updates
 * the rate + average-latency readouts in place. Closing detaches.
 */
public final class NamespaceDetailView extends VBox implements AutoCloseable {

    private final String connectionId, db, coll;

    private double readMsPerSec, writeMsPerSec, totalMsPerSec;
    private double readOpsPerSec, writeOpsPerSec;

    private final Label readMs   = valueLabel();
    private final Label writeMs  = valueLabel();
    private final Label totalMs  = valueLabel();
    private final Label readOps  = valueLabel();
    private final Label writeOps = valueLabel();
    private final Label avgRead  = valueLabel();
    private final Label avgWrite = valueLabel();

    private final EventBus.Subscription sub;

    public NamespaceDetailView(String db, String coll,
                               double readMsPerSec, double writeMsPerSec, double totalMsPerSec,
                               double readOpsPerSec, double writeOpsPerSec,
                               String connectionId, String connectionDisplayName,
                               EventBus bus) {
        this.connectionId = connectionId;
        this.db = db; this.coll = coll;
        this.readMsPerSec = readMsPerSec;
        this.writeMsPerSec = writeMsPerSec;
        this.totalMsPerSec = totalMsPerSec;
        this.readOpsPerSec = readOpsPerSec;
        this.writeOpsPerSec = writeOpsPerSec;

        setPadding(new Insets(16));
        setSpacing(12);

        Label header = new Label("Namespace · " + db + "." + coll);
        header.setStyle("-fx-font-size: 18px; -fx-font-weight: bold;");
        Label sub1 = new Label(connectionDisplayName);
        sub1.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 12px;");

        GridPane g = new GridPane();
        g.setHgap(16); g.setVgap(6);
        int row = 0;
        g.add(keyLabel("Read ms/s"),  0, row); g.add(readMs,   1, row++);
        g.add(keyLabel("Write ms/s"), 0, row); g.add(writeMs,  1, row++);
        g.add(keyLabel("Total ms/s"), 0, row); g.add(totalMs,  1, row++);
        g.add(keyLabel("Read ops/s"), 0, row); g.add(readOps,  1, row++);
        g.add(keyLabel("Write ops/s"),0, row); g.add(writeOps, 1, row++);
        g.add(keyLabel("Avg read latency"),  0, row); g.add(avgRead,  1, row++);
        g.add(keyLabel("Avg write latency"), 0, row); g.add(avgWrite, 1, row);

        getChildren().addAll(header, sub1, g);
        applyLabels();
        this.sub = bus.onMetrics(this::onSamples);
    }

    @Override public void close() { try { sub.close(); } catch (Throwable ignored) {} }

    private void onSamples(List<MetricSample> batch) {
        boolean changed = false;
        for (MetricSample s : batch) {
            if (!connectionId.equals(s.connectionId())) continue;
            Map<String, String> l = s.labels().labels();
            if (!db.equals(l.get("db")) || !coll.equals(l.get("coll"))) continue;
            switch (s.metric()) {
                case TOP_1 -> { readMsPerSec  = s.value(); changed = true; }
                case TOP_2 -> { writeMsPerSec = s.value(); changed = true; }
                case TOP_3 -> { totalMsPerSec = s.value(); changed = true; }
                case TOP_4 -> { readOpsPerSec = s.value(); changed = true; }
                case TOP_5 -> { writeOpsPerSec = s.value(); changed = true; }
                default -> {}
            }
        }
        if (changed) Platform.runLater(this::applyLabels);
    }

    private void applyLabels() {
        double avgReadLat  = readOpsPerSec  > 0 ? readMsPerSec  / readOpsPerSec  : 0;
        double avgWriteLat = writeOpsPerSec > 0 ? writeMsPerSec / writeOpsPerSec : 0;
        readMs.setText(String.format("%.2f", readMsPerSec));
        writeMs.setText(String.format("%.2f", writeMsPerSec));
        totalMs.setText(String.format("%.2f", totalMsPerSec));
        readOps.setText(String.format("%.2f", readOpsPerSec));
        writeOps.setText(String.format("%.2f", writeOpsPerSec));
        avgRead.setText(String.format("%.2f ms", avgReadLat));
        avgWrite.setText(String.format("%.2f ms", avgWriteLat));
    }

    private static Label keyLabel(String k) {
        Label l = new Label(k);
        l.setStyle("-fx-text-fill: #6b7280;");
        return l;
    }

    private static Label valueLabel() { return new Label("—"); }
}

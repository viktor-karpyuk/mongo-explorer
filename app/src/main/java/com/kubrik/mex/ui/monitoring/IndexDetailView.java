package com.kubrik.mex.ui.monitoring;

import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.monitoring.model.MetricId;
import com.kubrik.mex.monitoring.model.MetricSample;
import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.scene.control.Label;
import javafx.scene.control.TextArea;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.VBox;

import java.util.List;
import java.util.Map;

/**
 * ROW-EXPAND-3 — index row detail. Live: subscribes to {@code IDX_FOOT_*} and
 * {@code IDX_USE_*} samples matching {@code (connectionId, db, coll, index)} and
 * updates the size / ops-per-sec / flags fields in place. Closing detaches the
 * subscription.
 */
public final class IndexDetailView extends VBox implements AutoCloseable {

    private final String connectionId, db, coll, index;

    private long sizeBytes;
    private double opsPerSec;
    private boolean unique, sparse, partial, hidden, candidateUnused;
    private Long ttlSeconds;

    private final Label sizeLabel  = valueLabel();
    private final Label opsLabel   = valueLabel();
    private final Label flagsLabel = valueLabel();

    private final EventBus.Subscription sub;

    public IndexDetailView(String db, String coll, String index,
                           long sizeBytes, double opsPerSec, String initialFlags,
                           String connectionId, String connectionDisplayName,
                           EventBus bus) {
        this.connectionId = connectionId;
        this.db = db; this.coll = coll; this.index = index;
        this.sizeBytes = sizeBytes;
        this.opsPerSec = opsPerSec;
        // seed the flag booleans from the comma-separated summary where possible
        if (initialFlags != null) {
            unique  = initialFlags.contains("unique");
            sparse  = initialFlags.contains("sparse");
            partial = initialFlags.contains("partial");
            hidden  = initialFlags.contains("hidden");
            candidateUnused = initialFlags.contains("unused");
        }

        setPadding(new Insets(16));
        setSpacing(12);

        Label header = new Label("Index · " + db + "." + coll + "." + index);
        header.setStyle("-fx-font-size: 18px; -fx-font-weight: bold;");
        Label sub1 = new Label(connectionDisplayName);
        sub1.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 12px;");

        GridPane grid = new GridPane();
        grid.setHgap(16); grid.setVgap(6);
        int row = 0;
        row = keyValueRow(grid, row, "Database",   db);
        row = keyValueRow(grid, row, "Collection", coll);
        row = keyValueRow(grid, row, "Index",      index);
        grid.add(keyLabel("Size"),    0, row); grid.add(sizeLabel,  1, row++);
        grid.add(keyLabel("Ops/sec"), 0, row); grid.add(opsLabel,   1, row++);
        grid.add(keyLabel("Flags"),   0, row); grid.add(flagsLabel, 1, row);

        Label snipTitle = new Label("Advisory drop snippet (read-only)");
        snipTitle.setStyle("-fx-font-weight: bold; -fx-font-size: 12px;");
        TextArea snip = new TextArea(
                "// Review the usage trend before running:\n"
              + "db.getSiblingDB('" + db + "').getCollection('" + coll + "').dropIndex('" + index + "');");
        snip.setEditable(false);
        snip.setWrapText(true);
        snip.setPrefRowCount(3);
        snip.setStyle("-fx-font-family: 'Menlo', 'Monaco', monospace; -fx-font-size: 12px;");

        getChildren().addAll(header, sub1, grid, snipTitle, snip);

        applyLabels();
        this.sub = bus.onMetrics(this::onSamples);
    }

    @Override public void close() { try { sub.close(); } catch (Throwable ignored) {} }

    private void onSamples(List<MetricSample> batch) {
        boolean changed = false;
        for (MetricSample s : batch) {
            if (!connectionId.equals(s.connectionId())) continue;
            Map<String, String> l = s.labels().labels();
            if (!db.equals(l.get("db")) || !coll.equals(l.get("coll")) || !index.equals(l.get("index"))) continue;
            switch (s.metric()) {
                case IDX_FOOT_1 -> { sizeBytes = (long) s.value(); changed = true; }
                case IDX_FOOT_3 -> { unique  = s.value() != 0; changed = true; }
                case IDX_FOOT_4 -> { sparse  = s.value() != 0; changed = true; }
                case IDX_FOOT_5 -> { ttlSeconds = s.value() > 0 ? (long) s.value() : null; changed = true; }
                case IDX_FOOT_6 -> { partial = s.value() != 0; changed = true; }
                case IDX_FOOT_7 -> { hidden  = s.value() != 0; changed = true; }
                case IDX_USE_3  -> { opsPerSec = s.value(); changed = true; }
                case IDX_USE_4  -> { candidateUnused = s.value() != 0; changed = true; }
                default -> {}
            }
        }
        if (changed) Platform.runLater(this::applyLabels);
    }

    private void applyLabels() {
        sizeLabel.setText(humanBytes(sizeBytes));
        opsLabel.setText(opsPerSec < 0 ? "—" : String.format("%.2f", opsPerSec));
        flagsLabel.setText(flagsText());
    }

    private String flagsText() {
        StringBuilder sb = new StringBuilder();
        if (unique)  append(sb, "unique");
        if (sparse)  append(sb, "sparse");
        if (partial) append(sb, "partial");
        if (hidden)  append(sb, "hidden");
        if (ttlSeconds != null) append(sb, "ttl " + ttlSeconds + "s");
        if (candidateUnused) append(sb, "● unused?");
        return sb.length() == 0 ? "—" : sb.toString();
    }

    private static void append(StringBuilder sb, String s) {
        if (sb.length() > 0) sb.append(", ");
        sb.append(s);
    }

    private static int keyValueRow(GridPane g, int row, String k, String v) {
        g.add(keyLabel(k), 0, row);
        g.add(new Label(v), 1, row);
        return row + 1;
    }

    private static Label keyLabel(String k) {
        Label l = new Label(k);
        l.setStyle("-fx-text-fill: #6b7280;");
        return l;
    }

    private static Label valueLabel() {
        return new Label("—");
    }

    private static String humanBytes(long n) {
        double v = n;
        String[] u = { "B", "KB", "MB", "GB", "TB" };
        int i = 0;
        while (v >= 1024 && i < u.length - 1) { v /= 1024; i++; }
        return String.format("%.1f %s", v, u[i]);
    }
}

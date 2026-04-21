package com.kubrik.mex.ui.monitoring;

import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.monitoring.model.MetricId;
import com.kubrik.mex.monitoring.model.MetricSample;
import javafx.geometry.Insets;
import javafx.scene.Node;
import javafx.scene.layout.FlowPane;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Instance-level panel (INST-*, LAT-*, WT-*, LOCK-*, NET-*, CUR-*, TXN-*).
 * Rendered as a grid of {@link MetricCell}s sized per {@link MetricCell.Size}.
 *
 * <p>v2.2.0 `ROUTE-*`: the section is bound to a single {@code connectionId} at
 * construction time; samples for any other connection are ignored. Use
 * {@link #setConnectionId(String)} to re-bind — cell windows are cleared so
 * there's no visible carry-over from the previous instance.
 */
public final class InstanceSection implements AutoCloseable {

    public static final String ID = "instance";
    public static final String TITLE = "Instance";

    private final Map<MetricId, MetricCell> cells = new EnumMap<>(MetricId.class);
    private final FlowPane grid = new FlowPane(12, 12);
    private volatile String connectionId;
    private final EventBus.Subscription sub;

    public InstanceSection(EventBus bus, MetricCell.Size size, String connectionId) {
        this(bus, size, connectionId, null);
    }

    public InstanceSection(EventBus bus, MetricCell.Size size, String connectionId,
                           MetricExpander expander) {
        this.connectionId = connectionId;
        grid.setPadding(new Insets(12));
        for (Entry e : ENTRIES) {
            MetricCell c = new MetricCell(e.id, e.label, size, expander);
            cells.put(e.id, c);
            grid.getChildren().add(c);
        }
        this.sub = bus.onMetrics(this::route);
    }

    @Override public void close() { try { sub.close(); } catch (Throwable ignored) {} }

    public Node view() { return grid; }

    public String connectionId() { return connectionId; }

    /** Re-bind to a different connection id. Clears every cell's window. */
    public void setConnectionId(String newConnectionId) {
        this.connectionId = newConnectionId;
        cells.values().forEach(MetricCell::reset);
    }

    private void route(List<MetricSample> batch) {
        String bound = this.connectionId;
        if (bound == null) return;
        Map<MetricId, List<MetricSample>> grouped = new HashMap<>();
        for (MetricSample s : batch) {
            if (!bound.equals(s.connectionId())) continue;
            if (!cells.containsKey(s.metric())) continue;
            grouped.computeIfAbsent(s.metric(), k -> new ArrayList<>()).add(s);
        }
        for (var e : grouped.entrySet()) cells.get(e.getKey()).onSamples(e.getValue());
    }

    private record Entry(MetricId id, String label) {}

    private static final List<Entry> ENTRIES = List.of(
            new Entry(MetricId.INST_OP_1,   "Insert ops"),
            new Entry(MetricId.INST_OP_2,   "Query ops"),
            new Entry(MetricId.INST_OP_3,   "Update ops"),
            new Entry(MetricId.INST_OP_4,   "Delete ops"),
            new Entry(MetricId.INST_OP_5,   "GetMore ops"),
            new Entry(MetricId.INST_OP_6,   "Command ops"),
            new Entry(MetricId.INST_CONN_1, "Connections"),
            new Entry(MetricId.INST_CONN_5, "Conn saturation"),
            new Entry(MetricId.LAT_1,       "Reads latency"),
            new Entry(MetricId.LAT_2,       "Writes latency"),
            new Entry(MetricId.LAT_3,       "Cmd latency"),
            new Entry(MetricId.WT_3,        "WT cache fill"),
            new Entry(MetricId.WT_5,        "WT dirty ratio"),
            new Entry(MetricId.WT_10,       "WT app-thread reads"),
            new Entry(MetricId.WT_TKT_4,    "Read tkt saturation"),
            new Entry(MetricId.WT_TKT_8,    "Write tkt saturation"),
            new Entry(MetricId.LOCK_3,      "Global queue"),
            new Entry(MetricId.NET_1,       "Net bytes in"),
            new Entry(MetricId.NET_2,       "Net bytes out"),
            new Entry(MetricId.CUR_4,       "Cursors timed-out"),
            new Entry(MetricId.TXN_6,       "Txn abort rate")
    );
}

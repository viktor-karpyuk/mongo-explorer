package com.kubrik.mex.ui.monitoring;

import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.monitoring.MonitoringService;
import com.kubrik.mex.monitoring.model.MetricId;
import com.kubrik.mex.monitoring.model.MetricSample;
import com.kubrik.mex.monitoring.store.ProfileSampleRecord;
import javafx.application.Platform;
import javafx.beans.property.ReadOnlyObjectWrapper;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.scene.Node;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.layout.FlowPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;

import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Workload panel (M-4) — three regions:
 *
 * <ul>
 *   <li><b>Current ops</b> — OP-* scalars + a per-op-type mini breakdown from
 *       {@code OP-5} ({@code op_type} label).</li>
 *   <li><b>Top namespaces</b> — {@code TOP-*} per (db, coll) tallies with read /
 *       write time and rates.</li>
 *   <li><b>Slow queries</b> — profiler samples loaded on demand from the store.
 *       The opt-in flow is not wired here (M-10 work); the table reads whatever
 *       the profiler has captured so far.</li>
 * </ul>
 */
public final class WorkloadSection implements AutoCloseable {

    public static final String ID = "workload";
    public static final String TITLE = "Workload";

    private com.kubrik.mex.events.EventBus.Subscription sub;

    private final Map<MetricId, MetricCell> opCells;
    private final ObservableList<OpTypeRow> opTypeRows = FXCollections.observableArrayList();
    private final ConcurrentMap<String, OpTypeRow> opTypesByKind = new ConcurrentHashMap<>();

    private final ConcurrentMap<String, NsRow> nsRows = new ConcurrentHashMap<>();
    private final ObservableList<NsRow> nsList = FXCollections.observableArrayList();

    private final ObservableList<ProfileSampleRecord> slowRows = FXCollections.observableArrayList();

    private final VBox root = new VBox(12);
    private final MonitoringService svc;
    private volatile String connectionId;
    private Label slowPlaceholder;

    public WorkloadSection(EventBus bus, MonitoringService svc, MetricCell.Size size, String connectionId) {
        this(bus, svc, size, connectionId, null, null);
    }

    public WorkloadSection(EventBus bus, MonitoringService svc, MetricCell.Size size,
                           String connectionId, MetricExpander expander) {
        this(bus, svc, size, connectionId, expander, null);
    }

    public WorkloadSection(EventBus bus, MonitoringService svc, MetricCell.Size size,
                           String connectionId, MetricExpander expander,
                           RowExpandOpener rowOpener) {
        this.svc = svc;
        this.connectionId = connectionId;
        root.setPadding(new Insets(12));

        // --- Current ops cards --------------------------------------------
        FlowPane opGrid = new FlowPane(12, 12);
        opCells = Map.of(
                MetricId.OP_1, new MetricCell(MetricId.OP_1, "Active ops",          size, expander),
                MetricId.OP_2, new MetricCell(MetricId.OP_2, "Longest running (s)", size, expander),
                MetricId.OP_3, new MetricCell(MetricId.OP_3, "Waiting for lock",    size, expander),
                MetricId.OP_4, new MetricCell(MetricId.OP_4, "Prepare conflict",    size, expander)
        );
        opCells.values().forEach(opGrid.getChildren()::add);

        TableView<OpTypeRow> opTypeTable = new TableView<>(opTypeRows);
        opTypeTable.setPrefHeight(size == MetricCell.Size.LARGE ? 160 : 120);
        opTypeTable.setPlaceholder(new Label("Waiting for $currentOp samples."));
        opTypeTable.getColumns().addAll(List.of(
                col("Op type", 160, (OpTypeRow r) -> r.opType),
                col("Active",  100, (OpTypeRow r) -> Long.toString(r.count))));

        // --- Top namespaces -----------------------------------------------
        TableView<NsRow> nsTable = new TableView<>(nsList);
        nsTable.setPrefHeight(size == MetricCell.Size.LARGE ? 280 : 200);
        nsTable.setPlaceholder(new Label("No per-namespace top samples yet."));
        nsTable.getColumns().addAll(List.of(
                col("Database",       140, (NsRow r) -> r.db),
                col("Collection",     200, (NsRow r) -> r.coll),
                col("Read ms/s",      110, (NsRow r) -> fmtMs(r.readMsPerSec)),
                col("Write ms/s",     110, (NsRow r) -> fmtMs(r.writeMsPerSec)),
                col("Total ms/s",     110, (NsRow r) -> fmtMs(r.totalMsPerSec)),
                col("Read ops/s",     110, (NsRow r) -> fmtRate(r.readOpsPerSec)),
                col("Write ops/s",    110, (NsRow r) -> fmtRate(r.writeOpsPerSec))));
        TableRowActions.install(nsTable, r -> rowOpener == null ? null : (mode -> rowOpener.openNamespace(
                r.db, r.coll, r.readMsPerSec, r.writeMsPerSec, r.totalMsPerSec,
                r.readOpsPerSec, r.writeOpsPerSec, this.connectionId, mode)),
                r -> String.format("{\"db\":\"%s\",\"coll\":\"%s\",\"readMsPerSec\":%.3f,"
                        + "\"writeMsPerSec\":%.3f,\"totalMsPerSec\":%.3f,"
                        + "\"readOpsPerSec\":%.3f,\"writeOpsPerSec\":%.3f}",
                        r.db, r.coll, r.readMsPerSec, r.writeMsPerSec, r.totalMsPerSec,
                        r.readOpsPerSec, r.writeOpsPerSec));

        // --- Slow queries -------------------------------------------------
        TableView<ProfileSampleRecord> slowTable = new TableView<>(slowRows);
        slowTable.setPrefHeight(size == MetricCell.Size.LARGE ? 280 : 200);
        // Placeholder text reflects the bound connection's current profilerEnabled
        // state — refreshed on load and whenever the profile is upserted via the
        // Profiling… dialog (see refreshSlowPlaceholder()).
        this.slowPlaceholder = new Label("");
        this.slowPlaceholder.setWrapText(true);
        this.slowPlaceholder.setMaxWidth(640);
        this.slowPlaceholder.setStyle("-fx-text-fill: #6b7280;");
        slowTable.setPlaceholder(this.slowPlaceholder);
        slowTable.getColumns().addAll(List.of(
                col("Namespace",      220, (ProfileSampleRecord r) -> r.ns()),
                col("Op",              80, (ProfileSampleRecord r) -> r.op()),
                col("Millis",         100, (ProfileSampleRecord r) -> Long.toString(r.millis())),
                col("Plan",           120, (ProfileSampleRecord r) -> r.planSummary() == null ? "—" : r.planSummary()),
                col("Examined",       100, (ProfileSampleRecord r) -> String.valueOf(r.docsExamined())),
                col("Returned",       100, (ProfileSampleRecord r) -> String.valueOf(r.docsReturned())),
                col("Query hash",     120, (ProfileSampleRecord r) -> r.queryHash() == null ? "—" : r.queryHash())));
        TableRowActions.install(slowTable,
                r -> rowOpener == null ? null : (mode -> rowOpener.openSlowQuery(r, this.connectionId, mode)),
                r -> String.format("{\"ns\":\"%s\",\"op\":\"%s\",\"millis\":%d,\"plan\":%s,\"hash\":%s,"
                        + "\"docsExamined\":%s,\"docsReturned\":%s,\"keysExamined\":%s}",
                        r.ns(), r.op(), r.millis(),
                        TableRowActions.jsonStr(r.planSummary()), TableRowActions.jsonStr(r.queryHash()),
                        r.docsExamined(), r.docsReturned(), r.keysExamined()));

        Button loadSlow = new Button("Load recent slow queries");
        loadSlow.setOnAction(e -> { refreshSlowPlaceholder(); loadSlowQueries(svc); });
        Region slowSpacer = new Region();
        HBox.setHgrow(slowSpacer, javafx.scene.layout.Priority.ALWAYS);
        HBox slowHeader = new HBox(8, header("Slow queries"), slowSpacer, loadSlow);
        refreshSlowPlaceholder();

        root.getChildren().addAll(
                header("Current ops"), opGrid, opTypeTable,
                header("Top namespaces"), nsTable,
                slowHeader, slowTable);

        this.sub = bus.onMetrics(this::route);
    }

    @Override public void close() { try { if (sub != null) sub.close(); } catch (Throwable ignored) {} }

    public Node view() { return root; }

    public String connectionId() { return connectionId; }

    /** Re-bind to a different connection id. Clears every row-keyed map + op-type cells; reloads slow queries. */
    public void setConnectionId(String newConnectionId) {
        this.connectionId = newConnectionId;
        nsRows.clear();
        opTypesByKind.clear();
        opCells.values().forEach(MetricCell::reset);
        Platform.runLater(() -> {
            nsList.clear();
            opTypeRows.clear();
            slowRows.clear();
            refreshSlowPlaceholder();
        });
        loadSlowQueries(svc);
    }

    private void refreshSlowPlaceholder() {
        if (slowPlaceholder == null) return;
        String bound = this.connectionId;
        String text;
        if (bound == null) {
            text = "Select a monitored connection to view slow queries.";
        } else {
            com.kubrik.mex.monitoring.MonitoringProfile p = svc.profile(bound).orElse(null);
            if (p == null || !p.profilerEnabled()) {
                text = "Slow-query profiling is OFF for this connection. "
                     + "Open the \"Profiling…\" dialog from the connection card to enable it.";
            } else {
                text = "Profiling is ON (slowms = " + p.profilerSlowMs()
                     + "). Click \"Load recent slow queries\" to fetch the last hour.";
            }
        }
        final String f = text;
        Platform.runLater(() -> slowPlaceholder.setText(f));
    }

    // ---------- routing --------------------------------------------------

    private void route(List<MetricSample> batch) {
        String bound = this.connectionId;
        if (bound == null) return;
        boolean opTypeChanged = false, nsChanged = false;
        for (MetricSample s : batch) {
            if (!bound.equals(s.connectionId())) continue;
            MetricId id = s.metric();
            Map<String, String> labels = s.labels().labels();
            if (id == MetricId.OP_5) {
                String opType = labels.get("op_type");
                if (opType != null) {
                    opTypesByKind.computeIfAbsent(opType, OpTypeRow::new).count = (long) s.value();
                    opTypeChanged = true;
                }
            } else if (opCells.containsKey(id)) {
                opCells.get(id).onSamples(List.of(s));
            } else if (id.name().startsWith("TOP_")) {
                String db = labels.get("db");
                String coll = labels.get("coll");
                if (db == null || coll == null) continue;
                nsRows.computeIfAbsent(db + "." + coll, k -> new NsRow(db, coll)).update(id, s);
                nsChanged = true;
            }
        }
        final boolean oc = opTypeChanged, nc = nsChanged;
        if (oc || nc) {
            Platform.runLater(() -> {
                if (oc) {
                    List<OpTypeRow> sorted = new java.util.ArrayList<>(opTypesByKind.values());
                    sorted.sort(Comparator.comparingLong((OpTypeRow r) -> -r.count));
                    opTypeRows.setAll(sorted);
                }
                if (nc) {
                    List<NsRow> sorted = new java.util.ArrayList<>(nsRows.values());
                    sorted.sort(Comparator.comparingDouble((NsRow r) -> -r.totalMsPerSec));
                    nsList.setAll(sorted);
                }
            });
        }
    }

    private void loadSlowQueries(MonitoringService svc) {
        String bound = this.connectionId;
        if (bound == null) return;
        Thread.startVirtualThread(() -> {
            // Per SECTION-WORKLOAD-4: only load samples for the active connection.
            // v2.1.0 concatenated across every enabled profile — that was the bug.
            long nowMs = System.currentTimeMillis();
            java.util.List<ProfileSampleRecord> rows =
                    new java.util.ArrayList<>(svc.loadSlowSamples(bound, nowMs - 3_600_000L, nowMs));
            rows.sort(Comparator.comparingLong((ProfileSampleRecord r) -> -r.tsMs()));
            Platform.runLater(() -> slowRows.setAll(rows));
        });
    }

    // ---------- row state ------------------------------------------------

    static final class OpTypeRow {
        final String opType;
        volatile long count;
        OpTypeRow(String opType) { this.opType = opType; }
        public String getOpType() { return opType; }
    }

    static final class NsRow {
        final String db, coll;
        double readMsPerSec, writeMsPerSec, totalMsPerSec;
        double readOpsPerSec, writeOpsPerSec;

        NsRow(String db, String coll) { this.db = db; this.coll = coll; }

        void update(MetricId id, MetricSample s) {
            switch (id) {
                case TOP_1 -> readMsPerSec = s.value();
                case TOP_2 -> writeMsPerSec = s.value();
                case TOP_3 -> totalMsPerSec = s.value();
                case TOP_4 -> readOpsPerSec = s.value();
                case TOP_5 -> writeOpsPerSec = s.value();
                default -> {}
            }
        }
    }

    // ---------- helpers --------------------------------------------------

    private static <R> TableColumn<R, String> col(String title, double width,
                                                  java.util.function.Function<R, String> extractor) {
        TableColumn<R, String> c = new TableColumn<>(title);
        c.setCellValueFactory(cd -> new ReadOnlyObjectWrapper<>(extractor.apply(cd.getValue())));
        c.setPrefWidth(width);
        return c;
    }

    private static Label header(String text) {
        Label l = new Label(text);
        l.setStyle("-fx-font-weight: bold; -fx-font-size: 13px;");
        return l;
    }

    private static String fmtMs(double v) {
        if (v == 0) return "0";
        if (v < 10) return String.format(Locale.ROOT, "%.2f", v);
        return String.format(Locale.ROOT, "%.1f", v);
    }

    private static String fmtRate(double v) {
        if (v == 0) return "0";
        if (v < 10) return String.format(Locale.ROOT, "%.2f", v);
        return String.format(Locale.ROOT, "%.1f", v);
    }
}

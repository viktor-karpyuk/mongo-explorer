package com.kubrik.mex.ui.monitoring;

import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.monitoring.model.MetricId;
import com.kubrik.mex.monitoring.model.MetricSample;
import javafx.application.Platform;
import javafx.beans.property.ReadOnlyObjectWrapper;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.scene.Node;
import javafx.scene.control.Label;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.layout.VBox;

import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Storage panel (M-3) — three tables:
 *
 * <ul>
 *   <li><b>Databases</b> — from {@code DBSTAT-*} per-DB samples (collections, views,
 *       objects, data / storage / index size, fragmentation ratio).</li>
 *   <li><b>Collections</b> — {@code COLLSTAT-*} per-(db, coll) with the top-N + an
 *       aggregated {@code _other_} row built server-side by the sampler.</li>
 *   <li><b>Indexes</b> — {@code IDX-FOOT-*} + {@code IDX-USE-*}, with a candidate-unused
 *       flag derived from {@code IDX-USE-4}.</li>
 * </ul>
 *
 * <p>Rows are keyed off sample labels; each incoming sample updates an in-memory
 * row and the TableView is refreshed per batch.
 */
public final class StorageSection implements AutoCloseable {

    public static final String ID = "storage";
    public static final String TITLE = "Storage";

    private EventBus.Subscription sub;

    private final ConcurrentMap<String, DbRow> dbRows = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, CollRow> collRows = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, IndexRow> idxRows = new ConcurrentHashMap<>();
    private final ObservableList<DbRow> dbList = FXCollections.observableArrayList();
    private final ObservableList<CollRow> collList = FXCollections.observableArrayList();
    private final ObservableList<IndexRow> idxList = FXCollections.observableArrayList();

    private final VBox root = new VBox(12);
    private volatile String connectionId;

    public StorageSection(EventBus bus, MetricCell.Size size, String connectionId) {
        this(bus, size, connectionId, null);
    }

    public StorageSection(EventBus bus, MetricCell.Size size, String connectionId,
                          RowExpandOpener rowOpener) {
        this.connectionId = connectionId;
        root.setPadding(new Insets(12));

        TableView<DbRow> dbTable = buildDbTable(size);
        dbTable.setItems(dbList);

        TableView<CollRow> collTable = buildCollTable(size);
        collTable.setItems(collList);

        TableView<IndexRow> idxTable = buildIndexTable(size);
        idxTable.setItems(idxList);

        // ROW-EXPAND-3: index rows → IndexDetailView.
        TableRowActions.install(idxTable,
                r -> rowOpener == null ? null : (mode -> rowOpener.openIndex(
                        r.db, r.coll, r.index, r.size, r.opsPerSec, flags(r),
                        this.connectionId, mode)),
                r -> String.format("{\"db\":\"%s\",\"coll\":\"%s\",\"index\":\"%s\","
                                + "\"size\":%d,\"opsPerSec\":%.3f,\"flags\":%s}",
                        r.db, r.coll, r.index, r.size, r.opsPerSec,
                        TableRowActions.jsonStr(flags(r))));

        root.getChildren().addAll(
                header("Databases"), dbTable,
                header("Collections"), collTable,
                header("Indexes"), idxTable);

        this.sub = bus.onMetrics(this::route);
    }

    @Override public void close() { try { if (sub != null) sub.close(); } catch (Throwable ignored) {} }

    public Node view() { return root; }

    public String connectionId() { return connectionId; }

    /** Re-bind to a different connection id. Clears every row-keyed map + TableView. */
    public void setConnectionId(String newConnectionId) {
        this.connectionId = newConnectionId;
        dbRows.clear();
        collRows.clear();
        idxRows.clear();
        Platform.runLater(() -> {
            dbList.clear();
            collList.clear();
            idxList.clear();
        });
    }

    // ---------- routing --------------------------------------------------

    private void route(List<MetricSample> batch) {
        String bound = this.connectionId;
        if (bound == null) return;
        boolean dbChanged = false, collChanged = false, idxChanged = false;
        for (MetricSample s : batch) {
            if (!bound.equals(s.connectionId())) continue;
            MetricId id = s.metric();
            Map<String, String> labels = s.labels().labels();
            String db = labels.get("db");
            String coll = labels.get("coll");
            String idx = labels.get("index");
            if (id.name().startsWith("DBSTAT_") && db != null) {
                dbRows.computeIfAbsent(db, DbRow::new).update(id, s);
                dbChanged = true;
            } else if (id.name().startsWith("COLLSTAT_") && db != null && coll != null) {
                collRows.computeIfAbsent(db + "." + coll, k -> new CollRow(db, coll)).update(id, s);
                collChanged = true;
            } else if (idx != null && db != null && coll != null) {
                if (id.name().startsWith("IDX_FOOT_") || id.name().startsWith("IDX_USE_")) {
                    String key = db + "." + coll + "/" + idx;
                    idxRows.computeIfAbsent(key, k -> new IndexRow(db, coll, idx)).update(id, s);
                    idxChanged = true;
                }
            }
        }
        final boolean dbC = dbChanged, coC = collChanged, ixC = idxChanged;
        Platform.runLater(() -> {
            if (dbC) dbList.setAll(new TreeMap<>(dbRows).values());
            if (coC) {
                List<CollRow> sorted = new java.util.ArrayList<>(collRows.values());
                sorted.sort(Comparator
                        .comparing((CollRow r) -> r.db)
                        .thenComparing((CollRow r) -> -r.storageSize));
                collList.setAll(sorted);
            }
            if (ixC) {
                List<IndexRow> sorted = new java.util.ArrayList<>(idxRows.values());
                sorted.sort(Comparator
                        .comparing((IndexRow r) -> r.db)
                        .thenComparing((IndexRow r) -> r.coll)
                        .thenComparing((IndexRow r) -> -r.size));
                idxList.setAll(sorted);
            }
        });
    }

    // ---------- tables ---------------------------------------------------

    private static TableView<DbRow> buildDbTable(MetricCell.Size size) {
        TableView<DbRow> t = new TableView<>();
        t.setPrefHeight(size == MetricCell.Size.LARGE ? 280 : 180);
        t.setPlaceholder(new Label("No database samples yet."));
        t.getColumns().addAll(List.of(
                col("Database",      200, (DbRow r) -> r.db),
                col("Collections",   100, (DbRow r) -> fmtCount(r.collections)),
                col("Views",          80, (DbRow r) -> fmtCount(r.views)),
                col("Objects",       120, (DbRow r) -> fmtCount(r.objects)),
                col("Data size",     120, (DbRow r) -> fmtBytes(r.dataSize)),
                col("Storage size",  120, (DbRow r) -> fmtBytes(r.storageSize)),
                col("Index size",    120, (DbRow r) -> fmtBytes(r.indexSize)),
                col("Free storage",  120, (DbRow r) -> fmtBytes(r.freeStorage)),
                col("Fragmentation", 110, (DbRow r) -> fmtRatio(r.fragRatio))));
        return t;
    }

    private static TableView<CollRow> buildCollTable(MetricCell.Size size) {
        TableView<CollRow> t = new TableView<>();
        t.setPrefHeight(size == MetricCell.Size.LARGE ? 280 : 200);
        t.setPlaceholder(new Label("No collection samples yet."));
        t.getColumns().addAll(List.of(
                col("Database",   140, (CollRow r) -> r.db),
                col("Collection", 200, (CollRow r) -> r.coll),
                col("Count",      110, (CollRow r) -> fmtCount(r.count)),
                col("Size",       110, (CollRow r) -> fmtBytes(r.size)),
                col("Storage",    110, (CollRow r) -> fmtBytes(r.storageSize)),
                col("Indexes",     80, (CollRow r) -> fmtCount(r.nIndexes)),
                col("Idx size",   110, (CollRow r) -> fmtBytes(r.idxSize)),
                col("Capped",      70, (CollRow r) -> r.capped ? "yes" : "")));
        return t;
    }

    private static TableView<IndexRow> buildIndexTable(MetricCell.Size size) {
        TableView<IndexRow> t = new TableView<>();
        t.setPrefHeight(size == MetricCell.Size.LARGE ? 280 : 180);
        t.setPlaceholder(new Label("No index samples yet."));
        t.getColumns().addAll(List.of(
                col("Database",     140, (IndexRow r) -> r.db),
                col("Collection",   170, (IndexRow r) -> r.coll),
                col("Index",        210, (IndexRow r) -> r.index),
                col("Size",         110, (IndexRow r) -> fmtBytes(r.size)),
                col("Ops/sec",      100, (IndexRow r) -> r.opsPerSec < 0 ? "—" : String.format("%.1f", r.opsPerSec)),
                col("Flags",        150, (IndexRow r) -> flags(r))));
        return t;
    }

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

    // ---------- rows -----------------------------------------------------

    static final class DbRow {
        final String db;
        long collections, views, objects, dataSize, storageSize, indexSize, freeStorage;
        double fragRatio = Double.NaN;

        DbRow(String db) { this.db = db; }

        void update(MetricId id, MetricSample s) {
            switch (id) {
                case DBSTAT_1 -> collections = (long) s.value();
                case DBSTAT_2 -> views = (long) s.value();
                case DBSTAT_3 -> objects = (long) s.value();
                case DBSTAT_4 -> dataSize = (long) s.value();
                case DBSTAT_5 -> storageSize = (long) s.value();
                case DBSTAT_6 -> indexSize = (long) s.value();
                case DBSTAT_8 -> freeStorage = (long) s.value();
                case DBSTAT_9 -> fragRatio = s.value();
                default -> {}
            }
        }
    }

    static final class CollRow {
        final String db, coll;
        long count, size, storageSize, idxSize, nIndexes;
        boolean capped;

        CollRow(String db, String coll) { this.db = db; this.coll = coll; }

        void update(MetricId id, MetricSample s) {
            switch (id) {
                case COLLSTAT_1 -> count = (long) s.value();
                case COLLSTAT_2 -> size = (long) s.value();
                case COLLSTAT_4 -> storageSize = (long) s.value();
                case COLLSTAT_6 -> idxSize = (long) s.value();
                case COLLSTAT_7 -> nIndexes = (long) s.value();
                case COLLSTAT_8 -> capped = s.value() != 0;
                default -> {}
            }
        }
    }

    static final class IndexRow {
        final String db, coll, index;
        long size;
        double opsPerSec = -1;
        boolean unique, sparse, partial, hidden;
        boolean candidateUnused;
        Long ttlSeconds;

        IndexRow(String db, String coll, String index) {
            this.db = db; this.coll = coll; this.index = index;
        }

        void update(MetricId id, MetricSample s) {
            switch (id) {
                case IDX_FOOT_1 -> size = (long) s.value();
                case IDX_FOOT_3 -> unique = s.value() != 0;
                case IDX_FOOT_4 -> sparse = s.value() != 0;
                case IDX_FOOT_5 -> ttlSeconds = s.value() > 0 ? (long) s.value() : null;
                case IDX_FOOT_6 -> partial = s.value() != 0;
                case IDX_FOOT_7 -> hidden = s.value() != 0;
                case IDX_USE_3  -> opsPerSec = s.value();
                case IDX_USE_4  -> candidateUnused = s.value() != 0;
                default -> {}
            }
        }
    }

    // ---------- formatters ----------------------------------------------

    private static String fmtCount(long n) {
        if (n == 0) return "0";
        if (n < 1_000) return Long.toString(n);
        if (n < 1_000_000) return String.format(Locale.ROOT, "%.1fk", n / 1_000.0);
        if (n < 1_000_000_000L) return String.format(Locale.ROOT, "%.1fM", n / 1_000_000.0);
        return String.format(Locale.ROOT, "%.1fB", n / 1_000_000_000.0);
    }

    private static String fmtBytes(long n) {
        double v = n;
        String[] u = { "B", "KB", "MB", "GB", "TB" };
        int i = 0;
        while (v >= 1024 && i < u.length - 1) { v /= 1024; i++; }
        return String.format(Locale.ROOT, "%.1f %s", v, u[i]);
    }

    private static String fmtRatio(double r) {
        if (Double.isNaN(r)) return "—";
        return String.format(Locale.ROOT, "%.1f%%", r * 100);
    }

    private static String flags(IndexRow r) {
        StringBuilder sb = new StringBuilder();
        if (r.unique)  append(sb, "unique");
        if (r.sparse)  append(sb, "sparse");
        if (r.partial) append(sb, "partial");
        if (r.hidden)  append(sb, "hidden");
        if (r.ttlSeconds != null) append(sb, "ttl " + r.ttlSeconds + "s");
        if (r.candidateUnused) append(sb, "● unused?");
        return sb.length() == 0 ? "" : sb.toString();
    }

    private static void append(StringBuilder sb, String s) {
        if (sb.length() > 0) sb.append(", ");
        sb.append(s);
    }
}

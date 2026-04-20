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
import javafx.scene.control.TableCell;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.layout.FlowPane;
import javafx.scene.layout.VBox;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Replication panel (M-1) — renders per-member state derived from
 * {@code REPL-NODE-*} sample labels keyed by {@code member}, plus the oplog
 * ({@code REPL-OPLOG-*}) and elections ({@code REPL-ELEC-*}) summary cards.
 *
 * <p>Subscribes to the shared {@link EventBus} and routes each sample into
 * either the member table's row state or the top-level oplog / elections cells.
 */
public final class ReplicationSection implements AutoCloseable {

    public static final String ID = "replication";
    public static final String TITLE = "Replication";

    private final ObservableList<MemberRow> rows = FXCollections.observableArrayList();
    private final ConcurrentMap<String, MemberRow> rowsByMember = new ConcurrentHashMap<>();

    private final Map<MetricId, MetricCell> oplogCells;
    private final Map<MetricId, MetricCell> electionCells;
    private final VBox root = new VBox(12);
    private volatile String connectionId;
    private EventBus.Subscription sub;

    public ReplicationSection(EventBus bus, MetricCell.Size size, String connectionId) {
        this(bus, size, connectionId, null);
    }

    public ReplicationSection(EventBus bus, MetricCell.Size size, String connectionId,
                              MetricExpander expander) {
        this(bus, size, connectionId, expander, null);
    }

    public ReplicationSection(EventBus bus, MetricCell.Size size, String connectionId,
                              MetricExpander expander, RowExpandOpener rowOpener) {
        this.connectionId = connectionId;
        root.setPadding(new Insets(12));

        TableView<MemberRow> table = new TableView<>(rows);
        table.setPrefHeight(size == MetricCell.Size.LARGE ? 320 : 220);
        table.setPlaceholder(new Label("No replica-set samples yet. "
                + "Members appear here once replSetGetStatus reports."));

        TableColumn<MemberRow, String> nameCol = new TableColumn<>("Member");
        nameCol.setCellValueFactory(c -> new ReadOnlyObjectWrapper<>(c.getValue().name));
        nameCol.setPrefWidth(220);

        TableColumn<MemberRow, MemberRow> stateCol = new TableColumn<>("State");
        stateCol.setCellValueFactory(c -> new ReadOnlyObjectWrapper<>(c.getValue()));
        stateCol.setCellFactory(col -> stateCell());
        stateCol.setPrefWidth(130);

        TableColumn<MemberRow, String> healthCol = new TableColumn<>("Health");
        healthCol.setCellValueFactory(c -> new ReadOnlyObjectWrapper<>(c.getValue().getHealth()));
        healthCol.setPrefWidth(80);

        TableColumn<MemberRow, String> uptimeCol = new TableColumn<>("Uptime");
        uptimeCol.setCellValueFactory(c -> new ReadOnlyObjectWrapper<>(c.getValue().getUptime()));
        uptimeCol.setPrefWidth(120);

        TableColumn<MemberRow, String> pingCol = new TableColumn<>("Ping");
        pingCol.setCellValueFactory(c -> new ReadOnlyObjectWrapper<>(c.getValue().getPing()));
        pingCol.setPrefWidth(80);

        TableColumn<MemberRow, String> lagCol = new TableColumn<>("Lag");
        lagCol.setCellValueFactory(c -> new ReadOnlyObjectWrapper<>(c.getValue().getLag()));
        lagCol.setPrefWidth(90);

        TableColumn<MemberRow, String> hbCol = new TableColumn<>("Last heartbeat");
        hbCol.setCellValueFactory(c -> new ReadOnlyObjectWrapper<>(c.getValue().getLastHeartbeat()));
        hbCol.setPrefWidth(140);

        table.getColumns().addAll(List.of(nameCol, stateCol, healthCol, uptimeCol, pingCol, lagCol, hbCol));

        // ROW-EXPAND-5: member rows → MemberDetailView.
        TableRowActions.install(table,
                r -> rowOpener == null ? null : (mode -> rowOpener.openMember(
                        r.getName(), r.stateText(), r.getHealth(), r.getUptime(),
                        r.getPing(), r.getLag(), r.getLastHeartbeat(),
                        this.connectionId, mode)),
                r -> String.format("{\"member\":%s,\"state\":%s,\"health\":%s,\"uptime\":%s,"
                                + "\"ping\":%s,\"lag\":%s,\"lastHeartbeat\":%s}",
                        TableRowActions.jsonStr(r.getName()), TableRowActions.jsonStr(r.stateText()),
                        TableRowActions.jsonStr(r.getHealth()), TableRowActions.jsonStr(r.getUptime()),
                        TableRowActions.jsonStr(r.getPing()), TableRowActions.jsonStr(r.getLag()),
                        TableRowActions.jsonStr(r.getLastHeartbeat())));

        Label membersH = sectionHeader("Members");
        Label oplogH   = sectionHeader("Oplog");
        Label elecH    = sectionHeader("Elections");

        FlowPane oplogGrid = new FlowPane(12, 12);
        oplogCells = Map.of(
                MetricId.REPL_OPLOG_1, new MetricCell(MetricId.REPL_OPLOG_1, "Size",        size, expander),
                MetricId.REPL_OPLOG_2, new MetricCell(MetricId.REPL_OPLOG_2, "Used",        size, expander),
                MetricId.REPL_OPLOG_3, new MetricCell(MetricId.REPL_OPLOG_3, "Window (hours)", size, expander),
                MetricId.REPL_OPLOG_4, new MetricCell(MetricId.REPL_OPLOG_4, "Insert rate", size, expander),
                MetricId.REPL_OPLOG_5, new MetricCell(MetricId.REPL_OPLOG_5, "Insert bytes", size, expander)
        );
        oplogCells.values().forEach(oplogGrid.getChildren()::add);

        FlowPane elecGrid = new FlowPane(12, 12);
        electionCells = Map.of(
                MetricId.REPL_ELEC_1, new MetricCell(MetricId.REPL_ELEC_1, "Last election",   size, expander),
                MetricId.REPL_ELEC_2, new MetricCell(MetricId.REPL_ELEC_2, "Count",           size, expander),
                MetricId.REPL_ELEC_3, new MetricCell(MetricId.REPL_ELEC_3, "Priority takeovers", size, expander),
                MetricId.REPL_ELEC_4, new MetricCell(MetricId.REPL_ELEC_4, "Step-downs",      size, expander)
        );
        electionCells.values().forEach(elecGrid.getChildren()::add);

        root.getChildren().addAll(membersH, table, oplogH, oplogGrid, elecH, elecGrid);
        this.sub = bus.onMetrics(this::route);
    }

    @Override public void close() { try { if (sub != null) sub.close(); } catch (Throwable ignored) {} }

    public Node view() { return root; }

    public String connectionId() { return connectionId; }

    /** Re-bind to a different connection id. Clears members table + cell windows. */
    public void setConnectionId(String newConnectionId) {
        this.connectionId = newConnectionId;
        rowsByMember.clear();
        Platform.runLater(rows::clear);
        oplogCells.values().forEach(MetricCell::reset);
        electionCells.values().forEach(MetricCell::reset);
    }

    private void route(List<MetricSample> batch) {
        String bound = this.connectionId;
        if (bound == null) return;
        boolean memberChanged = false;
        for (MetricSample s : batch) {
            if (!bound.equals(s.connectionId())) continue;
            MetricId id = s.metric();
            if (id.name().startsWith("REPL_NODE_")) {
                String member = s.labels().labels().get("member");
                if (member == null) continue;
                MemberRow row = rowsByMember.computeIfAbsent(member, MemberRow::new);
                row.update(id, s);
                memberChanged = true;
            } else {
                MetricCell cell = oplogCells.get(id);
                if (cell == null) cell = electionCells.get(id);
                if (cell != null) cell.onSamples(List.of(s));
            }
        }
        if (memberChanged) {
            Platform.runLater(this::publishRows);
        }
    }

    private void publishRows() {
        // Publish a sorted, stable snapshot to the observable list so TableView redraws once.
        Map<String, MemberRow> sorted = new TreeMap<>(rowsByMember);
        rows.setAll(sorted.values());
    }

    /** Cell factory that renders the state as a coloured pill. */
    private static TableCell<MemberRow, MemberRow> stateCell() {
        return new TableCell<>() {
            @Override protected void updateItem(MemberRow r, boolean empty) {
                super.updateItem(r, empty);
                if (empty || r == null) { setText(null); setGraphic(null); return; }
                Label pill = new Label(r.stateText());
                pill.setStyle(pillStyle(r.stateText()));
                setGraphic(pill); setText(null);
            }
        };
    }

    private static String pillStyle(String state) {
        String bg = switch (state == null ? "" : state) {
            case "PRIMARY"    -> "#dcfce7";
            case "SECONDARY"  -> "#dbeafe";
            case "ARBITER"    -> "#ede9fe";
            case "RECOVERING", "STARTUP", "STARTUP2" -> "#fef3c7";
            case "DOWN", "REMOVED", "ROLLBACK" -> "#fee2e2";
            default           -> "#f3f4f6";
        };
        String fg = switch (state == null ? "" : state) {
            case "PRIMARY"    -> "#166534";
            case "SECONDARY"  -> "#1e40af";
            case "ARBITER"    -> "#5b21b6";
            case "RECOVERING", "STARTUP", "STARTUP2" -> "#92400e";
            case "DOWN", "REMOVED", "ROLLBACK" -> "#991b1b";
            default           -> "#4b5563";
        };
        return "-fx-background-color: " + bg
                + "; -fx-text-fill: " + fg
                + "; -fx-font-weight: bold; -fx-font-size: 10px;"
                + " -fx-padding: 2 8 2 8; -fx-background-radius: 10;";
    }

    private static Label sectionHeader(String text) {
        Label l = new Label(text);
        l.setStyle("-fx-font-weight: bold; -fx-font-size: 13px;");
        return l;
    }

    /** Mutable row state updated in-place from incoming samples; TableView is refreshed per-batch. */
    public static final class MemberRow {
        final String name;
        volatile double stateCode = 0;
        volatile double healthCode = -1;
        volatile double uptimeSeconds = -1;
        volatile double pingMs = -1;
        volatile Double lagSeconds = null;
        volatile Double heartbeatAgo = null;

        MemberRow(String name) { this.name = name; }

        void update(MetricId id, MetricSample s) {
            switch (id) {
                case REPL_NODE_1 -> stateCode = s.value();
                case REPL_NODE_2 -> healthCode = s.value();
                case REPL_NODE_3 -> uptimeSeconds = s.value();
                case REPL_NODE_4 -> pingMs = s.value();
                case REPL_NODE_5 -> lagSeconds = s.value();
                case REPL_NODE_6 -> heartbeatAgo = s.value();
                default -> {}
            }
        }

        String stateText() {
            return switch ((int) stateCode) {
                case 1  -> "PRIMARY";
                case 2  -> "SECONDARY";
                case 3  -> "RECOVERING";
                case 4  -> "STARTUP";
                case 5  -> "STARTUP2";
                case 7  -> "ARBITER";
                case 8  -> "DOWN";
                case 9  -> "ROLLBACK";
                case 10 -> "REMOVED";
                default -> "UNKNOWN";
            };
        }

        // Getter methods used by CellValueFactory lambdas above — public to make the
        // PropertyValueFactory-style lookups simple. TableView pulls values on refresh.
        public String getName() { return name; }
        public String getHealth() { return healthCode < 0 ? "—" : (healthCode > 0 ? "OK" : "DOWN"); }
        public String getUptime() {
            return uptimeSeconds < 0 ? "—" : humanSeconds((long) uptimeSeconds);
        }
        public String getPing() { return pingMs < 0 ? "—" : ((long) pingMs) + " ms"; }
        public String getLag() { return lagSeconds == null ? "—" : lagSeconds.longValue() + " s"; }
        public String getLastHeartbeat() {
            return heartbeatAgo == null ? "—" : heartbeatAgo.longValue() + " s ago";
        }

        private static String humanSeconds(long s) {
            long d = s / 86400, h = (s % 86400) / 3600, m = (s % 3600) / 60;
            if (d > 0) return d + "d " + h + "h";
            if (h > 0) return h + "h " + m + "m";
            return m + "m " + (s % 60) + "s";
        }
    }
}

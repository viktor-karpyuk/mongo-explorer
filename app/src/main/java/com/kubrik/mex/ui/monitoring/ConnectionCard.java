package com.kubrik.mex.ui.monitoring;

import com.kubrik.mex.cluster.model.HealthScore;
import com.kubrik.mex.cluster.model.TopologySnapshot;
import com.kubrik.mex.cluster.service.HealthScorer;
import com.kubrik.mex.core.ConnectionManager;
import com.kubrik.mex.core.MongoService;
import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.model.ConnectionState;
import com.kubrik.mex.model.MongoConnection;
import com.kubrik.mex.monitoring.alerting.Alerter;
import com.kubrik.mex.monitoring.model.MetricSample;
import com.kubrik.mex.monitoring.model.Severity;
import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Cursor;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.Tooltip;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

/**
 * One row in the Monitored-connections list. Cards are long-lived — they
 * self-update on {@code onState} events rather than being rebuilt on every
 * state flap. Callers must invoke {@link #close()} on removal so the card's
 * three event-bus subscriptions (state / metrics / alerts) are detached; the
 * parent list diffs against the profile set and closes cards for removed ids.
 */
public final class ConnectionCard extends HBox implements AutoCloseable {

    private final String connectionId;
    private final ConnectionManager manager;
    private final Alerter alerter;
    private final Runnable onOpenMonitoring;
    private final Runnable onOpenClusterInfo;
    private final Runnable onOpenProfiling;

    private final Label insertPill = pillLabel();
    private final Label queryPill  = pillLabel();
    private final Label connPill   = pillLabel();
    private final Label cachePill  = pillLabel();
    private final Sparkline insertSpark = new Sparkline(140, 36, false);
    private final Label alertDot = new Label("●");
    private final Label statusDot = new Label("●");
    private final Label statusText = new Label("—");
    private final Label summary = new Label("Probing…");
    private final Label healthPill = new Label();
    private final Button openBtn;
    private final Button clusterBtn;
    private final Button profilingBtn;

    private final List<EventBus.Subscription> subs = new ArrayList<>();
    private volatile ConnectionState.Status lastStatus;

    public ConnectionCard(String connectionId,
                          MongoConnection conn,
                          ConnectionManager manager,
                          EventBus bus,
                          Alerter alerter,
                          Runnable onOpenMonitoring,
                          Runnable onOpenClusterInfo,
                          Runnable onOpenProfiling) {
        this.connectionId = connectionId;
        this.manager = manager;
        this.alerter = alerter;
        this.onOpenMonitoring = onOpenMonitoring;
        this.onOpenClusterInfo = onOpenClusterInfo;
        this.onOpenProfiling = onOpenProfiling;

        String displayName = conn != null ? conn.name() : connectionId;
        Label name = new Label(displayName);
        name.setStyle("-fx-font-weight: bold; -fx-font-size: 14px;");

        summary.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 12px;");
        statusText.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 12px;");
        alertDot.setStyle("-fx-text-fill: #16a34a; -fx-font-size: 14px;");
        Tooltip.install(alertDot, new Tooltip("No active alerts"));
        healthPill.setVisible(false);
        healthPill.managedProperty().bind(healthPill.visibleProperty());
        HBox statusRow = new HBox(6, statusDot, statusText,
                new Label("·"), new Label("alerts"), alertDot,
                new Label("·"), healthPill);
        statusRow.setAlignment(Pos.CENTER_LEFT);

        insertPill.setText("— /s");
        queryPill.setText("— /s");
        connPill.setText("—");
        cachePill.setText("—");
        HBox pills = new HBox(16,
                labelledPill("inserts", insertPill),
                labelledPill("queries", queryPill),
                labelledPill("conns",   connPill),
                labelledPill("WT fill", cachePill));

        VBox left = new VBox(4, name, summary, statusRow, pills);
        VBox sparkBox = new VBox(2, smallLabel("inserts/s (2 min)"), insertSpark);
        sparkBox.setAlignment(Pos.CENTER_LEFT);

        Region spacer = new Region();
        HBox.setHgrow(spacer, Priority.ALWAYS);

        openBtn = new Button("Open monitoring");
        openBtn.setOnAction(e -> onOpenMonitoring.run());
        clusterBtn = new Button("Cluster info…");
        clusterBtn.setOnAction(e -> onOpenClusterInfo.run());
        profilingBtn = new Button("Profiling…");
        profilingBtn.setOnAction(e -> { if (onOpenProfiling != null) onOpenProfiling.run(); });

        VBox buttons = new VBox(6, openBtn, clusterBtn, profilingBtn);
        buttons.setAlignment(Pos.CENTER_RIGHT);

        setSpacing(14);
        setAlignment(Pos.CENTER_LEFT);
        setPadding(new Insets(12));

        getChildren().addAll(left, spacer, sparkBox, buttons);

        setOnMouseClicked(e -> {
            if (e.getTarget() instanceof javafx.scene.Node n && isInside(n, openBtn, clusterBtn, profilingBtn)) return;
            if (lastStatus == ConnectionState.Status.CONNECTED) onOpenMonitoring.run();
        });

        subs.add(bus.onMetrics(this::onSamples));
        subs.add(bus.onState(s -> {
            if (connectionId.equals(s.connectionId())) Platform.runLater(this::applyState);
        }));
        subs.add(bus.onAlertFired(e -> { if (connectionId.equals(e.connectionId())) refreshAlertDot(); }));
        subs.add(bus.onAlertCleared(e -> { if (connectionId.equals(e.connectionId())) refreshAlertDot(); }));
        subs.add(bus.onTopology((id, snap) -> {
            if (connectionId.equals(id) && snap != null) {
                Platform.runLater(() -> applyHealth(HealthScorer.score(snap)));
            }
        }));

        applyState();
        refreshAlertDot();
    }

    /** Public so callers can check whether a card is still alive. */
    public String connectionId() { return connectionId; }

    @Override
    public void close() {
        for (EventBus.Subscription s : subs) {
            try { s.close(); } catch (Throwable ignored) {}
        }
        subs.clear();
    }

    // ---- state-driven re-render -----------------------------------------

    private void applyState() {
        ConnectionState st = manager.state(connectionId);
        ConnectionState.Status prev = lastStatus;
        lastStatus = st.status();
        boolean connected = lastStatus == ConnectionState.Status.CONNECTED;

        statusDot.setStyle("-fx-text-fill: " + statusColour(lastStatus) + "; -fx-font-size: 14px;");
        statusText.setText(lastStatus.name());
        openBtn.setDisable(!connected);
        clusterBtn.setDisable(!connected);
        profilingBtn.setDisable(!connected);
        setStyle(cardStyle(connected));
        setCursor(connected ? Cursor.HAND : Cursor.DEFAULT);
        insertSpark.setOpacity(connected ? 1.0 : 0.35);

        if (!connected) {
            summary.setText("Connect to this cluster to see live topology + metrics.");
            healthPill.setVisible(false);
            return;
        }
        // Only probe when transitioning into CONNECTED — avoids a redundant hello() on every sample batch.
        if (prev != ConnectionState.Status.CONNECTED) {
            Thread.startVirtualThread(() -> {
                String text = buildSummary();
                Platform.runLater(() -> summary.setText(text));
            });
        }
    }

    private void applyHealth(HealthScore score) {
        if (score == null) {
            healthPill.setVisible(false);
            return;
        }
        healthPill.setText("health " + score.score());
        healthPill.setVisible(true);
        String bg, fg;
        switch (score.band()) {
            case GREEN -> { bg = "#dcfce7"; fg = "#166534"; }
            case AMBER -> { bg = "#fef3c7"; fg = "#92400e"; }
            case RED   -> { bg = "#fee2e2"; fg = "#991b1b"; }
            default    -> { bg = "#f3f4f6"; fg = "#374151"; }
        }
        healthPill.setStyle("-fx-background-color: " + bg + "; -fx-text-fill: " + fg + "; "
                + "-fx-font-size: 10px; -fx-font-weight: 700; "
                + "-fx-padding: 2 8 2 8; -fx-background-radius: 10;");
        String tip = score.negatives().isEmpty()
                ? "Cluster health nominal."
                : "Contributing issues:\n• " + String.join("\n• ", score.negatives());
        Tooltip.install(healthPill, new Tooltip(tip));
    }

    private void refreshAlertDot() {
        if (alerter == null) return;
        Severity sev = alerter.severityFor(connectionId);
        List<String> firing = alerter.firingRulesFor(connectionId);
        Platform.runLater(() -> {
            alertDot.setStyle("-fx-text-fill: " + severityColour(sev) + "; -fx-font-size: 14px;");
            String tip = switch (sev) {
                case OK   -> "No active alerts";
                case WARN -> "WARN — " + String.join(", ", firing);
                case CRIT -> "CRIT — " + String.join(", ", firing);
            };
            Tooltip.install(alertDot, new Tooltip(tip));
        });
    }

    private void onSamples(List<MetricSample> batch) {
        if (lastStatus != ConnectionState.Status.CONNECTED) return;
        for (MetricSample s : batch) {
            if (!connectionId.equals(s.connectionId())) continue;
            switch (s.metric()) {
                case INST_OP_1 -> {
                    updatePill(insertPill, String.format("%.0f /s", s.value()));
                    insertSpark.push(List.of(s));
                }
                case INST_OP_2 -> updatePill(queryPill,  String.format("%.0f /s", s.value()));
                case INST_CONN_1 -> updatePill(connPill, String.format("%.0f",    s.value()));
                case WT_3       -> updatePill(cachePill, String.format("%.2f",    s.value()));
                default -> {}
            }
        }
    }

    private static boolean isInside(javafx.scene.Node target, javafx.scene.Node... buttons) {
        javafx.scene.Node n = target;
        while (n != null) {
            for (javafx.scene.Node b : buttons) if (n == b) return true;
            n = n.getParent();
        }
        return false;
    }

    private String buildSummary() {
        MongoService mongo = manager.service(connectionId);
        if (mongo == null) return "Not connected.";
        try {
            Document hello = mongo.hello();
            String topo;
            String detail = "";
            if (Boolean.TRUE.equals(hello.getBoolean("isdbgrid"))
                    || "isdbgrid".equals(hello.getString("msg"))) {
                topo = "Sharded";
            } else if (hello.containsKey("setName")) {
                topo = "Replica set";
                String set = hello.getString("setName");
                String primary = hello.getString("primary");
                detail = " · " + set + (primary != null ? " · primary " + primary : "");
            } else {
                topo = "Standalone";
            }
            String version = mongo.serverVersion();
            String uptime = "";
            try {
                Document status = mongo.database("admin")
                        .runCommand(new Document("serverStatus", 1));
                if (status != null && status.get("uptime") instanceof Number n) {
                    uptime = " · up " + humanDuration(n.longValue());
                }
            } catch (Throwable ignored) {}
            return topo + detail + " · v" + version + uptime;
        } catch (Throwable t) {
            return "Cluster info unavailable (" + t.getClass().getSimpleName() + ").";
        }
    }

    // ----- UI helpers -----

    private static Label pillLabel() {
        Label l = new Label("—");
        l.setStyle("-fx-background-color: #eef2ff; -fx-text-fill: #3730a3; "
                + "-fx-font-size: 11px; -fx-font-weight: bold; "
                + "-fx-padding: 2 8 2 8; -fx-background-radius: 10;");
        return l;
    }

    private static VBox labelledPill(String title, Label pill) {
        Label t = smallLabel(title);
        VBox v = new VBox(2, t, pill);
        v.setAlignment(Pos.TOP_LEFT);
        return v;
    }

    private static Label smallLabel(String text) {
        Label l = new Label(text);
        l.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 10px;");
        return l;
    }

    private static void updatePill(Label pill, String text) {
        Platform.runLater(() -> pill.setText(text));
    }

    private static String cardStyle(boolean connected) {
        String bg = connected ? "#f9fafb" : "#f3f4f6";
        return "-fx-background-color: " + bg
                + "; -fx-border-color: #e5e7eb;"
                + " -fx-border-width: 1;"
                + " -fx-background-radius: 6;"
                + " -fx-border-radius: 6;";
    }

    private static String statusColour(ConnectionState.Status s) {
        return switch (s) {
            case CONNECTED    -> "#16a34a";
            case CONNECTING   -> "#d97706";
            case ERROR        -> "#b91c1c";
            case DISCONNECTED -> "#6b7280";
        };
    }

    private static String severityColour(Severity s) {
        return switch (s) {
            case OK   -> "#16a34a";
            case WARN -> "#d97706";
            case CRIT -> "#b91c1c";
        };
    }

    private static String humanDuration(long seconds) {
        long days = seconds / 86400;
        long hours = (seconds % 86400) / 3600;
        long mins = (seconds % 3600) / 60;
        if (days > 0) return days + "d " + hours + "h";
        if (hours > 0) return hours + "h " + mins + "m";
        return mins + "m";
    }
}

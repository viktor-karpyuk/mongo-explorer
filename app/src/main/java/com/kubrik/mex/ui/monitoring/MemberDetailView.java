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
 * ROW-EXPAND-5 — live replica-set member detail. Subscribes to
 * {@code REPL_NODE_*} samples matching {@code (connectionId, member)} and
 * keeps the state / health / uptime / ping / lag / heartbeat fields current.
 */
public final class MemberDetailView extends VBox implements AutoCloseable {

    private final String connectionId, member;

    private double stateCode = 0;
    private double healthCode = -1;
    private double uptimeSeconds = -1;
    private double pingMs = -1;
    private Double lagSeconds = null;
    private Double heartbeatAgo = null;

    private final Label stateLabel     = valueLabel();
    private final Label healthLabel    = valueLabel();
    private final Label uptimeLabel    = valueLabel();
    private final Label pingLabel      = valueLabel();
    private final Label lagLabel       = valueLabel();
    private final Label heartbeatLabel = valueLabel();

    private final EventBus.Subscription sub;

    public MemberDetailView(String member, String initialState, String initialHealth, String initialUptime,
                            String initialPing, String initialLag, String initialHeartbeat,
                            String connectionId, String connectionDisplayName,
                            EventBus bus) {
        this.connectionId = connectionId;
        this.member = member;

        setPadding(new Insets(16));
        setSpacing(12);

        Label header = new Label("Member · " + member);
        header.setStyle("-fx-font-size: 18px; -fx-font-weight: bold;");
        Label sub1 = new Label(connectionDisplayName);
        sub1.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 12px;");

        // Seed with whatever the row cached before opening so there's no initial "—".
        stateLabel.setText(orDash(initialState));
        healthLabel.setText(orDash(initialHealth));
        uptimeLabel.setText(orDash(initialUptime));
        pingLabel.setText(orDash(initialPing));
        lagLabel.setText(orDash(initialLag));
        heartbeatLabel.setText(orDash(initialHeartbeat));

        GridPane g = new GridPane();
        g.setHgap(16); g.setVgap(6);
        int row = 0;
        g.add(keyLabel("State"),           0, row); g.add(stateLabel,     1, row++);
        g.add(keyLabel("Health"),          0, row); g.add(healthLabel,    1, row++);
        g.add(keyLabel("Uptime"),          0, row); g.add(uptimeLabel,    1, row++);
        g.add(keyLabel("Ping"),            0, row); g.add(pingLabel,      1, row++);
        g.add(keyLabel("Replication lag"), 0, row); g.add(lagLabel,       1, row++);
        g.add(keyLabel("Last heartbeat"),  0, row); g.add(heartbeatLabel, 1, row);

        Label hint = new Label("Tip: the \"Cluster info\" button in the monitoring header "
                + "opens the raw replSetGetStatus JSON.");
        hint.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px; -fx-font-style: italic;");

        getChildren().addAll(header, sub1, g, hint);
        this.sub = bus.onMetrics(this::onSamples);
    }

    @Override public void close() { try { sub.close(); } catch (Throwable ignored) {} }

    private void onSamples(List<MetricSample> batch) {
        boolean changed = false;
        for (MetricSample s : batch) {
            if (!connectionId.equals(s.connectionId())) continue;
            Map<String, String> l = s.labels().labels();
            if (!member.equals(l.get("member"))) continue;
            switch (s.metric()) {
                case REPL_NODE_1 -> { stateCode   = s.value(); changed = true; }
                case REPL_NODE_2 -> { healthCode  = s.value(); changed = true; }
                case REPL_NODE_3 -> { uptimeSeconds = s.value(); changed = true; }
                case REPL_NODE_4 -> { pingMs      = s.value(); changed = true; }
                case REPL_NODE_5 -> { lagSeconds  = s.value(); changed = true; }
                case REPL_NODE_6 -> { heartbeatAgo = s.value(); changed = true; }
                default -> {}
            }
        }
        if (changed) Platform.runLater(this::applyLabels);
    }

    private void applyLabels() {
        stateLabel.setText(stateText((int) stateCode));
        healthLabel.setText(healthCode < 0 ? "—" : (healthCode > 0 ? "OK" : "DOWN"));
        uptimeLabel.setText(uptimeSeconds < 0 ? "—" : humanSeconds((long) uptimeSeconds));
        pingLabel.setText(pingMs < 0 ? "—" : ((long) pingMs) + " ms");
        lagLabel.setText(lagSeconds == null ? "—" : lagSeconds.longValue() + " s");
        heartbeatLabel.setText(heartbeatAgo == null ? "—" : heartbeatAgo.longValue() + " s ago");
    }

    private static String stateText(int code) {
        return switch (code) {
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

    private static String humanSeconds(long s) {
        long d = s / 86400, h = (s % 86400) / 3600, m = (s % 3600) / 60;
        if (d > 0) return d + "d " + h + "h";
        if (h > 0) return h + "h " + m + "m";
        return m + "m " + (s % 60) + "s";
    }

    private static Label keyLabel(String k) {
        Label l = new Label(k);
        l.setStyle("-fx-text-fill: #6b7280;");
        return l;
    }

    private static Label valueLabel() { return new Label("—"); }

    private static String orDash(String s) { return s == null || s.isBlank() ? "—" : s; }
}

package com.kubrik.mex.ui.cluster;

import com.kubrik.mex.cluster.model.ClusterKind;
import com.kubrik.mex.cluster.model.HealthScore;
import com.kubrik.mex.cluster.model.Member;
import com.kubrik.mex.cluster.model.MemberState;
import com.kubrik.mex.cluster.model.Mongos;
import com.kubrik.mex.cluster.model.Shard;
import com.kubrik.mex.cluster.model.TopologySnapshot;
import com.kubrik.mex.cluster.service.HealthScorer;
import com.kubrik.mex.events.EventBus;
import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Label;
import javafx.scene.control.ScrollPane;
import javafx.scene.control.Tooltip;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import org.kordamp.ikonli.javafx.FontIcon;

import java.util.Comparator;
import java.util.List;

/**
 * v2.4 Q2.4-B / TOPO-15..17 — renders the latest {@link TopologySnapshot} for a
 * connection. Subscribes to {@link EventBus#onTopology} so updates flow in
 * without polling; {@link EventBus} replays the last snapshot on subscription
 * so the pane is populated on first focus without waiting for the next tick.
 *
 * <p>Layout per cluster kind:</p>
 * <ul>
 *   <li>STANDALONE — single host card + "standalone" empty-state caption.</li>
 *   <li>REPLSET — stack of member cards sorted by priority desc, host asc.</li>
 *   <li>SHARDED — shards section (each shard a mini replset card group),
 *       config-server section, mongos section. Warnings from the sampler
 *       surface as an amber banner above the grid.</li>
 * </ul>
 */
public final class TopologyPane extends VBox implements AutoCloseable {

    private final String connectionId;
    private final EventBus bus;
    private final EventBus.Subscription sub;

    private final Label headline = new Label("Resolving topology…");
    private final Label subline = new Label("Waiting for the first sample.");
    private final Label healthPill = new Label();
    private final VBox warningBox = new VBox(4);
    private final VBox body = new VBox(12);

    public TopologyPane(String connectionId, EventBus bus) {
        this.connectionId = connectionId;
        this.bus = bus;

        setSpacing(14);
        setPadding(new Insets(18, 22, 18, 22));
        setStyle("-fx-background-color: white;");

        headline.setStyle("-fx-font-size: 18px; -fx-font-weight: 700; -fx-text-fill: #111827;");
        subline.setStyle("-fx-font-size: 12px; -fx-text-fill: #6b7280;");
        healthPill.setVisible(false);

        Region grow = new Region();
        HBox.setHgrow(grow, Priority.ALWAYS);
        HBox header = new HBox(12, new VBox(2, headline, subline), grow, healthPill);
        header.setAlignment(Pos.CENTER_LEFT);

        warningBox.setVisible(false);
        warningBox.managedProperty().bind(warningBox.visibleProperty());

        ScrollPane scroll = new ScrollPane(body);
        scroll.setFitToWidth(true);
        scroll.setStyle("-fx-background-color: transparent; -fx-background: transparent; -fx-border-color: transparent;");
        VBox.setVgrow(scroll, Priority.ALWAYS);

        getChildren().setAll(header, warningBox, scroll);

        sub = bus.onTopology((id, snap) -> {
            if (!connectionId.equals(id)) return;
            Platform.runLater(() -> render(snap));
        });

        TopologySnapshot pre = bus.latestTopology(connectionId);
        if (pre != null) render(pre);
    }

    @Override
    public void close() {
        try { sub.close(); } catch (Exception ignored) {}
    }

    /* =========================== rendering ============================== */

    private void render(TopologySnapshot snap) {
        if (snap == null) {
            body.getChildren().setAll(centered("Waiting for the first topology sample…"));
            return;
        }
        HealthScore score = HealthScorer.score(snap);
        updateHealthPill(score);
        updateHeader(snap);
        updateWarnings(snap.warnings());

        switch (snap.clusterKind()) {
            case STANDALONE -> renderStandalone(snap);
            case REPLSET    -> renderReplset(snap);
            case SHARDED    -> renderSharded(snap);
        }
    }

    private void updateHeader(TopologySnapshot snap) {
        String kind = switch (snap.clusterKind()) {
            case STANDALONE -> "Standalone";
            case REPLSET -> "Replica set";
            case SHARDED -> "Sharded cluster";
        };
        headline.setText(kind + "  ·  MongoDB " + snap.version());
        String sub = snap.clusterKind() == ClusterKind.SHARDED
                ? snap.shardCount() + " shards  ·  " + snap.mongos().size() + " mongos  ·  "
                        + snap.configServers().size() + " config servers"
                : snap.members().size() + " members";
        subline.setText(sub);
    }

    private void updateHealthPill(HealthScore score) {
        healthPill.setText("Health  " + score.score());
        healthPill.setVisible(true);
        healthPill.setStyle(pillStyle(score.band()));
        if (score.negatives().isEmpty()) {
            Tooltip.install(healthPill, new Tooltip("No issues detected."));
        } else {
            String tip = "Contributing issues:\n• " + String.join("\n• ", score.negatives());
            Tooltip.install(healthPill, new Tooltip(tip));
        }
    }

    private void updateWarnings(List<String> warnings) {
        if (warnings == null || warnings.isEmpty()) {
            warningBox.getChildren().clear();
            warningBox.setVisible(false);
            return;
        }
        warningBox.getChildren().clear();
        for (String w : warnings) {
            FontIcon icon = new FontIcon("fth-alert-triangle");
            icon.setIconSize(14);
            icon.setIconColor(javafx.scene.paint.Color.web("#b45309"));
            Label l = new Label(w);
            l.setStyle("-fx-text-fill: #92400e; -fx-font-size: 12px;");
            HBox row = new HBox(8, icon, l);
            row.setAlignment(Pos.CENTER_LEFT);
            warningBox.getChildren().add(row);
        }
        warningBox.setStyle("-fx-background-color: #fffbeb; -fx-background-radius: 6; "
                + "-fx-border-color: #fde68a; -fx-border-radius: 6; -fx-border-width: 1; "
                + "-fx-padding: 10 14 10 14;");
        warningBox.setVisible(true);
    }

    private void renderStandalone(TopologySnapshot snap) {
        VBox box = new VBox(8);
        box.getChildren().add(sectionHeading("Instance"));
        Member m = snap.members().isEmpty() ? Member.unknownAt("unknown host") : snap.members().get(0);
        box.getChildren().add(memberCard(m));
        Label tail = new Label("This is a standalone mongod. Replica-set actions and "
                + "sharding views are unavailable.");
        tail.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 12px;");
        tail.setWrapText(true);
        box.getChildren().add(tail);
        body.getChildren().setAll(box);
    }

    private void renderReplset(TopologySnapshot snap) {
        VBox box = new VBox(10);
        box.getChildren().add(sectionHeading("Members"));
        List<Member> ordered = sortForDisplay(snap.members());
        for (Member m : ordered) box.getChildren().add(memberCard(m));
        body.getChildren().setAll(box);
    }

    private void renderSharded(TopologySnapshot snap) {
        VBox all = new VBox(18);

        VBox shardsBox = new VBox(12);
        shardsBox.getChildren().add(sectionHeading("Shards"));
        if (snap.shards().isEmpty()) {
            shardsBox.getChildren().add(centered("No shards reported."));
        } else {
            for (Shard s : snap.shards()) shardsBox.getChildren().add(shardBlock(s));
        }
        all.getChildren().add(shardsBox);

        if (!snap.configServers().isEmpty()) {
            VBox cfg = new VBox(10);
            cfg.getChildren().add(sectionHeading("Config servers"));
            for (Member m : sortForDisplay(snap.configServers())) cfg.getChildren().add(memberCard(m));
            all.getChildren().add(cfg);
        }

        if (!snap.mongos().isEmpty()) {
            VBox mgs = new VBox(8);
            mgs.getChildren().add(sectionHeading("Mongos"));
            for (Mongos m : snap.mongos()) mgs.getChildren().add(mongosCard(m));
            all.getChildren().add(mgs);
        }

        body.getChildren().setAll(all);
    }

    /* ============================ member card ============================ */

    private static VBox shardBlock(Shard s) {
        VBox outer = new VBox(6);
        Label name = new Label(s.id() + (s.draining() ? "  (draining)" : ""));
        name.setStyle("-fx-font-weight: 700; -fx-font-size: 13px; -fx-text-fill: #1f2937;");
        Label host = new Label(s.rsHost());
        host.setStyle("-fx-font-size: 11px; -fx-text-fill: #6b7280;");
        outer.getChildren().addAll(new VBox(2, name, host));
        if (s.members().isEmpty()) {
            outer.getChildren().add(centered("No live member data for this shard."));
        } else {
            for (Member m : sortForDisplay(s.members())) outer.getChildren().add(memberCard(m));
        }
        outer.setPadding(new Insets(10, 12, 12, 12));
        outer.setStyle("-fx-background-color: #f9fafb; -fx-background-radius: 8; "
                + "-fx-border-color: #e5e7eb; -fx-border-radius: 8; -fx-border-width: 1;");
        return outer;
    }

    private static HBox memberCard(Member m) {
        FontIcon icon = stateIcon(m.state());
        Label host = new Label(m.host());
        host.setStyle("-fx-font-size: 13px; -fx-font-weight: 700; -fx-text-fill: #111827;");
        Label badge = new Label(m.state().name());
        badge.setStyle(stateBadgeStyle(m.state()));

        Label prio = metaLabel("Priority " + formatInt(m.priority()) + "  ·  Votes "
                + formatInt(m.votes()) + "  ·  Uptime " + formatUptime(m.uptimeSecs()));
        Label lag = metaLabel("Lag " + formatLag(m.lagMs())
                + "  ·  Ping " + formatMs(m.pingMs())
                + "  ·  Sync " + formatSync(m.syncSourceHost()));
        Label tail = metaLabel("Config v" + formatInt(m.configVersion())
                + (Boolean.TRUE.equals(m.hidden()) ? "  ·  hidden" : "")
                + (Boolean.TRUE.equals(m.arbiterOnly()) ? "  ·  arbiter" : ""));

        VBox lines = new VBox(2, new HBox(10, host, badge), prio, lag, tail);
        lines.setAlignment(Pos.CENTER_LEFT);

        HBox card = new HBox(14, icon, lines);
        card.setAlignment(Pos.TOP_LEFT);
        card.setPadding(new Insets(12, 14, 12, 14));
        card.setStyle("-fx-background-color: white; -fx-background-radius: 8; "
                + "-fx-border-color: " + stateBorder(m.state()) + "; "
                + "-fx-border-radius: 8; -fx-border-width: 1;");
        return card;
    }

    private static HBox mongosCard(Mongos m) {
        FontIcon icon = new FontIcon("fth-server");
        icon.setIconSize(16);
        icon.setIconColor(javafx.scene.paint.Color.web("#0b8585"));
        Label host = new Label(m.host());
        host.setStyle("-fx-font-size: 13px; -fx-font-weight: 700;");
        Label meta = metaLabel("v" + (m.version().isBlank() ? "?" : m.version())
                + "  ·  Uptime " + formatUptime(m.uptimeSecs()));
        VBox lines = new VBox(2, host, meta);
        HBox row = new HBox(12, icon, lines);
        row.setAlignment(Pos.CENTER_LEFT);
        row.setPadding(new Insets(10, 14, 10, 14));
        row.setStyle("-fx-background-color: white; -fx-background-radius: 8; "
                + "-fx-border-color: #e5e7eb; -fx-border-radius: 8; -fx-border-width: 1;");
        return row;
    }

    /* ============================ helpers =============================== */

    private static Label sectionHeading(String text) {
        Label l = new Label(text);
        l.setStyle("-fx-font-weight: 700; -fx-font-size: 12px; "
                + "-fx-text-fill: #374151; -fx-padding: 0 0 2 0;");
        return l;
    }

    private static HBox centered(String text) {
        Label l = new Label(text);
        l.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 12px;");
        HBox h = new HBox(l);
        h.setAlignment(Pos.CENTER);
        h.setPadding(new Insets(30, 0, 30, 0));
        return h;
    }

    private static Label metaLabel(String s) {
        Label l = new Label(s);
        l.setStyle("-fx-font-size: 11px; -fx-text-fill: #6b7280;");
        return l;
    }

    private static List<Member> sortForDisplay(List<Member> ms) {
        return ms.stream()
                .sorted(Comparator.<Member, Integer>comparing(m -> priorityScore(m), Comparator.reverseOrder())
                        .thenComparing(Member::host))
                .toList();
    }

    private static int priorityScore(Member m) {
        // PRIMARY first, SECONDARY next (highest priority first within), others below.
        int base = switch (m.state()) {
            case PRIMARY -> 1_000_000;
            case SECONDARY -> 1_000;
            case ARBITER -> 100;
            default -> 0;
        };
        return base + (m.priority() == null ? 0 : m.priority());
    }

    private static FontIcon stateIcon(MemberState state) {
        String code = switch (state) {
            case PRIMARY -> "fth-star";
            case SECONDARY -> "fth-rotate-cw";
            case ARBITER -> "fth-shield";
            case DOWN -> "fth-x-circle";
            case STARTUP, STARTUP2, RECOVERING, ROLLBACK, REMOVED, UNKNOWN -> "fth-help-circle";
        };
        FontIcon i = new FontIcon(code);
        i.setIconSize(20);
        i.setIconColor(javafx.scene.paint.Color.web(stateColor(state)));
        return i;
    }

    private static String stateColor(MemberState state) {
        return switch (state) {
            case PRIMARY -> "#6940a3";
            case SECONDARY -> "#0b8585";
            case ARBITER -> "#1f2937";
            case DOWN -> "#b42318";
            case STARTUP, STARTUP2, RECOVERING, ROLLBACK -> "#d97706";
            case REMOVED, UNKNOWN -> "#9ca3af";
        };
    }

    private static String stateBadgeStyle(MemberState state) {
        return "-fx-background-color: " + stateColor(state) + "1a; "
                + "-fx-text-fill: " + stateColor(state) + "; "
                + "-fx-font-size: 10px; -fx-font-weight: 700; "
                + "-fx-padding: 2 8 2 8; -fx-background-radius: 10;";
    }

    private static String stateBorder(MemberState state) {
        return switch (state) {
            case PRIMARY, SECONDARY -> "#e5e7eb";
            case DOWN -> "#fecaca";
            default -> "#e5e7eb";
        };
    }

    private static String pillStyle(HealthScore.Band band) {
        String bg, fg;
        switch (band) {
            case GREEN -> { bg = "#dcfce7"; fg = "#166534"; }
            case AMBER -> { bg = "#fef3c7"; fg = "#92400e"; }
            case RED   -> { bg = "#fee2e2"; fg = "#991b1b"; }
            default    -> { bg = "#f3f4f6"; fg = "#374151"; }
        }
        return "-fx-background-color: " + bg + "; -fx-text-fill: " + fg + "; "
                + "-fx-font-weight: 700; -fx-font-size: 12px; "
                + "-fx-padding: 4 12 4 12; -fx-background-radius: 999;";
    }

    private static String formatInt(Integer v) { return v == null ? "—" : v.toString(); }

    private static String formatMs(Long ms) {
        if (ms == null) return "—";
        if (ms < 1) return "<1 ms";
        return ms + " ms";
    }

    private static String formatLag(Long ms) {
        if (ms == null) return "—";
        if (ms < 100) return "<0.1 s";
        return String.format("%.1f s", ms / 1000.0);
    }

    private static String formatSync(String host) {
        return (host == null || host.isBlank()) ? "—" : "↶ " + host;
    }

    private static String formatUptime(Long seconds) {
        if (seconds == null) return "—";
        long days = seconds / 86400;
        long hours = (seconds % 86400) / 3600;
        long mins = (seconds % 3600) / 60;
        if (days > 0) return days + "d " + hours + "h";
        if (hours > 0) return hours + "h " + mins + "m";
        return mins + "m";
    }
}

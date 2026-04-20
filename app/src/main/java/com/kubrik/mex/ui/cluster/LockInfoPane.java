package com.kubrik.mex.ui.cluster;

import com.kubrik.mex.cluster.ops.LockInfo;
import com.kubrik.mex.cluster.service.LockInfoService;
import com.kubrik.mex.core.ConnectionManager;
import com.kubrik.mex.core.MongoService;
import javafx.animation.KeyFrame;
import javafx.animation.Timeline;
import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Label;
import javafx.scene.control.ScrollPane;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.StackPane;
import javafx.scene.layout.VBox;
import javafx.util.Duration;

/**
 * v2.4 LOCK-1..4 — lock-analytics pane. Polls {@code lockInfo} at 5 s while
 * attached to a scene; renders one row per resource with a scaled heat-bar
 * (width ∝ holder count, colour gated by waiter ratio). Servers older than
 * 3.6 (or connections without the right privilege) show a "not supported"
 * card instead of an error bar.
 */
public final class LockInfoPane extends BorderPane implements AutoCloseable {

    private static final Duration POLL_INTERVAL = Duration.seconds(5);

    private final String connectionId;
    private final ConnectionManager connManager;
    private final Label headline = new Label("Resolving lockInfo…");
    private final VBox body = new VBox(6);

    private final Timeline poller;
    private volatile boolean closed = false;

    public LockInfoPane(String connectionId, ConnectionManager connManager) {
        this.connectionId = connectionId;
        this.connManager = connManager;

        setStyle("-fx-background-color: white;");
        setPadding(new Insets(14, 16, 14, 16));

        headline.setStyle("-fx-font-weight: 700; -fx-font-size: 13px; -fx-text-fill: #374151;");
        ScrollPane scroll = new ScrollPane(body);
        scroll.setFitToWidth(true);
        scroll.setStyle("-fx-background-color: transparent; -fx-background: transparent;");
        setTop(new VBox(6, headline));
        setCenter(scroll);

        this.poller = new Timeline(new KeyFrame(POLL_INTERVAL, e -> tick()));
        poller.setCycleCount(Timeline.INDEFINITE);
        sceneProperty().addListener((obs, oldScene, newScene) -> {
            if (newScene != null) {
                tick();
                poller.playFromStart();
            } else {
                poller.stop();
            }
        });
    }

    @Override
    public void close() {
        closed = true;
        poller.stop();
    }

    private void tick() {
        if (closed) return;
        MongoService svc = connManager.service(connectionId);
        if (svc == null) return;
        Thread.startVirtualThread(() -> {
            LockInfo info = LockInfoService.sample(svc);
            Platform.runLater(() -> render(info));
        });
    }

    private void render(LockInfo info) {
        if (info == null || !info.supported()) {
            headline.setText("Lock analytics");
            body.getChildren().setAll(notSupportedCard());
            return;
        }
        headline.setText(info.holderCount() + " active locks · " + info.waiterCount() + " waiters");
        body.getChildren().clear();
        if (info.entries().isEmpty()) {
            body.getChildren().add(emptyCard());
            return;
        }
        int maxHolders = info.entries().stream().mapToInt(LockInfo.Entry::holders).max().orElse(1);
        for (LockInfo.Entry e : info.entries()) {
            body.getChildren().add(rowFor(e, maxHolders));
        }
        if (!info.topHolders().isEmpty()) {
            Label h = new Label("Top holders");
            h.setStyle("-fx-font-size: 11px; -fx-font-weight: 700; "
                    + "-fx-text-fill: #374151; -fx-padding: 10 0 4 0;");
            body.getChildren().add(h);
            for (LockInfo.TopHolder t : info.topHolders()) {
                Label l = new Label("opid " + t.opid() + " · " + t.resource()
                        + " · " + (t.heldMs() / 1000.0) + " s");
                l.setStyle("-fx-font-size: 11px; -fx-text-fill: #1f2937;");
                body.getChildren().add(l);
            }
        }
    }

    /* ============================= rows =============================== */

    private HBox rowFor(LockInfo.Entry e, int maxHolders) {
        Label name = new Label(e.resource());
        name.setPrefWidth(200);
        name.setStyle("-fx-font-size: 11px; -fx-text-fill: #1f2937;");

        double fraction = maxHolders <= 0 ? 0 : (double) e.holders() / maxHolders;
        Region bar = new Region();
        bar.setPrefHeight(12);
        bar.setPrefWidth(Math.max(8, 260 * fraction));
        bar.setStyle("-fx-background-color: " + heatColour(e) + "; -fx-background-radius: 6;");
        StackPane barCell = new StackPane(bar);
        barCell.setAlignment(Pos.CENTER_LEFT);
        barCell.setPrefWidth(260);

        Label meta = new Label(e.holders() + " held · " + e.waiters() + " waiters · "
                + (e.maxHoldMs() / 1000.0) + " s max");
        meta.setStyle("-fx-font-size: 11px; -fx-text-fill: #6b7280;");

        HBox row = new HBox(12, name, barCell, meta);
        row.setAlignment(Pos.CENTER_LEFT);
        row.setPadding(new Insets(4, 0, 4, 0));
        return row;
    }

    private static String heatColour(LockInfo.Entry e) {
        if (e.holders() == 0) return "#e5e7eb";
        double waitRatio = e.holders() == 0 ? 0 : (double) e.waiters() / Math.max(1, e.holders());
        if (waitRatio >= 1.0) return "#b42318";   // red
        if (waitRatio >= 0.3) return "#d97706";   // amber
        return "#0b8585";                         // teal
    }

    private static Region notSupportedCard() {
        Label l = new Label("Lock analytics requires MongoDB 3.6+ or the \"serverStatus\" privilege.");
        l.setWrapText(true);
        l.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 12px;");
        VBox v = new VBox(l);
        v.setPadding(new Insets(30, 20, 30, 20));
        v.setStyle("-fx-background-color: #f9fafb; -fx-background-radius: 8; "
                + "-fx-border-color: #e5e7eb; -fx-border-radius: 8; -fx-border-width: 1;");
        return v;
    }

    private static Region emptyCard() {
        Label l = new Label("No contended locks right now. This is the healthy steady state.");
        l.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 12px;");
        VBox v = new VBox(l);
        v.setAlignment(Pos.CENTER);
        v.setPadding(new Insets(20, 0, 20, 0));
        return v;
    }
}

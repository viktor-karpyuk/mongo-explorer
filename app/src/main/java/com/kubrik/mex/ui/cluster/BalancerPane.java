package com.kubrik.mex.ui.cluster;

import com.kubrik.mex.cluster.audit.Outcome;
import com.kubrik.mex.cluster.dryrun.DryRunRenderer;
import com.kubrik.mex.cluster.ops.BalancerStatus;
import com.kubrik.mex.cluster.safety.Command;
import com.kubrik.mex.cluster.safety.DryRunResult;
import com.kubrik.mex.cluster.safety.TypedConfirmDialog;
import com.kubrik.mex.cluster.safety.TypedConfirmModel;
import com.kubrik.mex.cluster.service.BalancerService;
import com.kubrik.mex.cluster.service.OpsExecutor;
import com.kubrik.mex.core.ConnectionManager;
import com.kubrik.mex.core.MongoService;
import javafx.animation.KeyFrame;
import javafx.animation.Timeline;
import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.Tooltip;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import javafx.util.Duration;

import java.util.Optional;

/**
 * v2.4 SHARD-5..9 (part 1) — balancer pane. Shows the current balancer state
 * pill, active migration count, rolling 24 h chunk-move tally, and the
 * configured activeWindow (when set). Start / Stop buttons go through
 * {@link OpsExecutor} so the three safety gates fire. The window editor and
 * chunk-move dispatch land with the next Q2.4-G part.
 */
public final class BalancerPane extends BorderPane implements AutoCloseable {

    private static final Duration POLL_INTERVAL = Duration.seconds(5);

    private final String connectionId;
    private final ConnectionManager connManager;
    private final OpsExecutor executor;
    private final BalancerHandler handler;

    private final Label headline = new Label("Resolving balancer…");
    private final Label modePill = new Label();
    private final Label counters = new Label();
    private final Label windowLbl = new Label();
    private final Button startBtn = new Button("Start balancer…");
    private final Button stopBtn = new Button("Stop balancer…");
    private final Label note = new Label("");

    private final Timeline poller;
    private volatile boolean closed = false;

    public interface BalancerHandler {
        boolean allowed(String connectionId);
        String callerUser();
        String callerHost();
    }

    public BalancerPane(String connectionId, ConnectionManager connManager,
                        OpsExecutor executor, BalancerHandler handler) {
        this.connectionId = connectionId;
        this.connManager = connManager;
        this.executor = executor;
        this.handler = handler;

        setStyle("-fx-background-color: white;");
        setPadding(new Insets(14, 16, 14, 16));
        setTop(buildHeader());
        setCenter(buildBody());

        poller = new Timeline(new KeyFrame(POLL_INTERVAL, e -> tick()));
        poller.setCycleCount(Timeline.INDEFINITE);
        sceneProperty().addListener((obs, o, n) -> {
            if (n != null) { tick(); poller.playFromStart(); }
            else { poller.stop(); }
        });
    }

    @Override
    public void close() {
        closed = true;
        poller.stop();
    }

    /* =============================== UI =============================== */

    private Region buildHeader() {
        headline.setStyle("-fx-font-size: 18px; -fx-font-weight: 700; -fx-text-fill: #111827;");
        modePill.setStyle(pillStyle("neutral"));
        modePill.setText("—");
        Region grow = new Region();
        HBox.setHgrow(grow, Priority.ALWAYS);
        HBox row = new HBox(12, headline, grow, modePill);
        row.setAlignment(Pos.CENTER_LEFT);
        Label sub = new Label("Sharded-cluster migrations live here.");
        sub.setStyle("-fx-font-size: 11px; -fx-text-fill: #6b7280;");
        VBox head = new VBox(2, row, sub);
        head.setPadding(new Insets(0, 0, 12, 0));
        return head;
    }

    private Region buildBody() {
        counters.setStyle("-fx-font-size: 12px; -fx-text-fill: #374151;");
        windowLbl.setStyle("-fx-font-size: 12px; -fx-text-fill: #374151;");
        note.setStyle("-fx-font-size: 11px; -fx-text-fill: #6b7280;");

        startBtn.setDisable(true);
        stopBtn.setDisable(true);
        startBtn.setOnAction(e -> dispatch(true));
        stopBtn.setOnAction(e -> dispatch(false));

        HBox buttons = new HBox(8, startBtn, stopBtn);
        buttons.setAlignment(Pos.CENTER_LEFT);

        VBox body = new VBox(10, counters, windowLbl, buttons, note);
        body.setPadding(new Insets(4, 0, 0, 0));
        return body;
    }

    /* ============================ polling ============================= */

    private void tick() {
        if (closed) return;
        MongoService svc = connManager.service(connectionId);
        if (svc == null) return;
        Thread.startVirtualThread(() -> {
            BalancerStatus s = BalancerService.sample(svc);
            Platform.runLater(() -> render(s));
        });
    }

    private void render(BalancerStatus s) {
        if (!s.supported()) {
            headline.setText("Balancer unavailable");
            modePill.setText("n/a");
            modePill.setStyle(pillStyle("neutral"));
            counters.setText(s.errorMessage() == null ? "" : s.errorMessage());
            windowLbl.setText("");
            startBtn.setDisable(true);
            stopBtn.setDisable(true);
            return;
        }
        headline.setText("Balancer · " + s.mode());
        modePill.setText(s.enabled() ? (s.inRound() ? "running" : "idle") : "off");
        modePill.setStyle(pillStyle(s.enabled() ? (s.inRound() ? "green" : "amber") : "red"));

        counters.setText(s.activeMigrations() + " active migrations  ·  "
                + s.chunksMovedLast24h() + " chunks moved in last 24 h  ·  "
                + s.rounds() + " balancer rounds");
        windowLbl.setText(s.hasWindow()
                ? "Window: " + s.windowStart() + " → " + s.windowStop() + " UTC"
                : "No active window — balancer runs continuously.");

        boolean role = handler != null && handler.allowed(connectionId);
        startBtn.setDisable(!role || s.enabled());
        stopBtn.setDisable(!role || !s.enabled());
        if (!role) {
            Tooltip.install(startBtn, new Tooltip("Requires clusterManager or root."));
            Tooltip.install(stopBtn, new Tooltip("Requires clusterManager or root."));
        }
    }

    /* =========================== dispatch ============================= */

    private void dispatch(boolean starting) {
        if (executor == null || handler == null) return;
        String cluster = connectionId;
        Command cmd = starting
                ? new Command.BalancerStart(cluster)
                : new Command.BalancerStop(cluster);
        DryRunResult preview = DryRunRenderer.render(cmd);
        TypedConfirmModel model = new TypedConfirmModel(cluster, preview);
        Optional<TypedConfirmModel.Outcome> picked = TypedConfirmDialog.showAndWait(
                getScene() == null ? null : getScene().getWindow(), model);
        if (picked.isEmpty() || picked.get() != TypedConfirmModel.Outcome.CONFIRMED) {
            note.setText("Cancelled.");
            return;
        }
        Thread.startVirtualThread(() -> {
            OpsExecutor.Result r = executor.execute(connectionId, cmd, preview,
                    model.paste(), handler.callerUser(), handler.callerHost());
            Platform.runLater(() -> {
                note.setText(r.outcome() == Outcome.OK
                        ? (starting ? "Balancer started." : "Balancer stopped.")
                        : "Dispatch " + r.outcome() + ": " + r.serverMessage());
                tick();
            });
        });
    }

    /* =========================== helpers ============================== */

    private static String pillStyle(String band) {
        String bg, fg;
        switch (band) {
            case "green" -> { bg = "#dcfce7"; fg = "#166534"; }
            case "amber" -> { bg = "#fef3c7"; fg = "#92400e"; }
            case "red"   -> { bg = "#fee2e2"; fg = "#991b1b"; }
            default      -> { bg = "#f3f4f6"; fg = "#374151"; }
        }
        return "-fx-background-color: " + bg + "; -fx-text-fill: " + fg + "; "
                + "-fx-font-size: 11px; -fx-font-weight: 700; "
                + "-fx-padding: 4 12 4 12; -fx-background-radius: 999;";
    }
}

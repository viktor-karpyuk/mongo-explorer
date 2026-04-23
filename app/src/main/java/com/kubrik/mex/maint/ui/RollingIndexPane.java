package com.kubrik.mex.maint.ui;

import com.kubrik.mex.maint.index.RollingIndexPlanner;
import com.kubrik.mex.maint.index.RollingIndexRunner;
import com.kubrik.mex.maint.model.IndexBuildSpec;
import com.kubrik.mex.maint.model.ReconfigSpec.Member;
import com.mongodb.client.MongoClient;
import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.scene.control.Button;
import javafx.scene.control.CheckBox;
import javafx.scene.control.Label;
import javafx.scene.control.TextField;
import javafx.scene.control.Tooltip;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import javafx.util.Duration;
import org.bson.Document;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * v2.7 IDX-BLD-* UI — Plans a rolling index build across the
 * members of the current replica set, renders a per-member
 * progress strip, and dispatches.
 */
public final class RollingIndexPane extends BorderPane {

    private final RollingIndexPlanner planner = new RollingIndexPlanner();
    private final RollingIndexRunner runner = new RollingIndexRunner();

    private final java.util.function.Supplier<MongoClient> clientSupplier;
    /** Opens an auth-aware direct-connection client to a named
     *  member (reusing credentials + TLS from the active service).
     *  The earlier draft used raw {@code mongodb://host} URIs which
     *  skipped auth entirely and silently failed on secured clusters. */
    private final java.util.function.Function<String, MongoClient> memberOpener;

    private final TextField dbField = new TextField();
    private final TextField collField = new TextField();
    private final TextField keysField = new TextField();
    private final TextField nameField = new TextField();
    private final CheckBox uniqueBox = new CheckBox("unique");
    private final CheckBox sparseBox = new CheckBox("sparse");
    private final VBox progressStrip = new VBox(4);
    private final Label statusLabel = new Label("—");

    private final Map<Integer, Label> memberLabels = new LinkedHashMap<>();

    public RollingIndexPane(java.util.function.Supplier<MongoClient> clientSupplier,
                            java.util.function.Function<String, MongoClient> memberOpener) {
        this.clientSupplier = clientSupplier;
        this.memberOpener = memberOpener;
        setStyle("-fx-background-color: white;");
        setPadding(new Insets(14, 16, 14, 16));
        setAccessibleText("Rolling index build pane");
        setAccessibleHelp(
                "Build an index across replica-set members one node at a "
                + "time. Plan shows the member order; Build walks the plan "
                + "with progress updates per member.");
        setTop(buildHeader());
        setCenter(buildCenter());
        setBottom(buildActions());
    }

    private Region buildHeader() {
        Label title = new Label("Rolling index build");
        title.setStyle("-fx-font-size: 14px; -fx-font-weight: 700;");
        Label hint = new Label(
                "Build an index one member at a time — secondaries first "
                + "(lowest priority first), then step-down + primary "
                + "last. commitQuorum=0 keeps the build node-local.");
        hint.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px;");
        hint.setWrapText(true);
        VBox v = new VBox(6, title, hint);
        v.setPadding(new Insets(0, 0, 10, 0));
        return v;
    }

    private Region buildCenter() {
        dbField.setPromptText("app");
        collField.setPromptText("users");
        keysField.setPromptText("{ \"userId\": 1 }");
        nameField.setPromptText("userId_1");

        GridPane g = new GridPane();
        g.setHgap(8); g.setVgap(6);
        int row = 0;
        g.add(small("Database"), 0, row); g.add(dbField, 1, row++, 3, 1);
        g.add(small("Collection"), 0, row); g.add(collField, 1, row++, 3, 1);
        g.add(small("Keys JSON"), 0, row); g.add(keysField, 1, row++, 3, 1);
        g.add(small("Index name"), 0, row); g.add(nameField, 1, row++, 3, 1);
        g.add(small("Options"), 0, row);
        g.add(new HBox(12, uniqueBox, sparseBox), 1, row++, 3, 1);

        Label stripLabel = new Label("Progress");
        stripLabel.setStyle("-fx-text-fill: #4b5563; -fx-font-size: 11px; -fx-font-weight: 600;");
        VBox v = new VBox(10, g, stripLabel, progressStrip);
        return v;
    }

    private Region buildActions() {
        Button planBtn = new Button("Plan");
        planBtn.setTooltip(tip(
                "Loads the cluster's member list + primary and renders "
                + "the planned build order."));
        planBtn.setOnAction(e -> onPlan());
        Button runBtn = new Button("Build…");
        runBtn.setOnAction(e -> onRun());

        Region grow = new Region();
        HBox.setHgrow(grow, Priority.ALWAYS);
        HBox actions = new HBox(8, planBtn, grow, runBtn);
        actions.setPadding(new Insets(10, 0, 0, 0));

        statusLabel.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px;");
        statusLabel.setWrapText(true);
        VBox v = new VBox(6, actions, statusLabel);
        return v;
    }

    /* =============================== actions =============================== */

    private void onPlan() {
        MongoClient client = clientSupplier.get();
        if (client == null) { fail("No active connection."); return; }
        try {
            Document cfgReply = client.getDatabase("admin").runCommand(
                    new Document("replSetGetConfig", 1));
            Document status = client.getDatabase("admin").runCommand(
                    new Document("replSetGetStatus", 1));
            List<Member> members = parseMembers(cfgReply);
            int primaryId = findPrimaryId(status);
            List<RollingIndexPlanner.Step> plan = planner.plan(members, primaryId);

            progressStrip.getChildren().clear();
            memberLabels.clear();
            for (RollingIndexPlanner.Step step : plan) {
                Label l = new Label("⏳ " + step.member().host()
                        + (step.isPrimary() ? " (primary, step-down)" : ""));
                l.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 12px;");
                progressStrip.getChildren().add(l);
                memberLabels.put(step.member().id(), l);
            }
            ok(plan.size() + " steps planned.");
        } catch (Exception ex) {
            fail("Plan failed: " + ex.getMessage());
        }
    }

    private void onRun() {
        MongoClient client = clientSupplier.get();
        if (client == null) { fail("No active connection."); return; }
        IndexBuildSpec spec = buildSpec();
        if (spec == null) return;

        try {
            Document cfgReply = client.getDatabase("admin").runCommand(
                    new Document("replSetGetConfig", 1));
            Document status = client.getDatabase("admin").runCommand(
                    new Document("replSetGetStatus", 1));
            List<Member> members = parseMembers(cfgReply);
            int primaryId = findPrimaryId(status);
            List<RollingIndexPlanner.Step> plan = planner.plan(members, primaryId);

            statusLabel.setText("Running rolling build…");
            Thread.startVirtualThread(() -> runPlanWithUi(spec, plan));
        } catch (Exception ex) {
            fail("Run failed: " + ex.getMessage());
        }
    }

    private void runPlanWithUi(IndexBuildSpec spec, List<RollingIndexPlanner.Step> plan) {
        // Per-step: update the strip label before + after. memberOpener
        // pulls credentials + TLS from the active MongoService so the
        // direct connection authenticates properly on secured clusters.
        RollingIndexRunner.DispatchContext ctx = memberOpener::apply;
        // Walk the plan one step at a time so we can update the UI
        // between members.
        for (RollingIndexPlanner.Step step : plan) {
            Platform.runLater(() -> mark(step.member().id(), "⚙️", "building"));
            RollingIndexRunner.Result partial = runner.run(ctx, spec, List.of(step));
            RollingIndexRunner.MemberOutcome outcome = partial.perMember().get(0);
            Platform.runLater(() -> {
                if (outcome.success()) {
                    mark(step.member().id(), "✅", "done in " + outcome.elapsedMs() + "ms");
                } else {
                    mark(step.member().id(), "❌", outcome.errorCode() + ": " + outcome.errorMessage());
                }
            });
            if (!outcome.success()) {
                Platform.runLater(() -> fail(
                        "Stopped at " + outcome.host() + " — " + outcome.errorMessage()));
                return;
            }
        }
        Platform.runLater(() -> ok("Rolling build complete on "
                + plan.size() + " member(s)."));
    }

    private void mark(int memberId, String emoji, String detail) {
        Label l = memberLabels.get(memberId);
        if (l == null) return;
        l.setText(emoji + " member " + memberId + " — " + detail);
    }

    /* =============================== helpers =============================== */

    private IndexBuildSpec buildSpec() {
        String db = dbField.getText();
        String coll = collField.getText();
        String keys = keysField.getText();
        String name = nameField.getText();
        if (blank(db) || blank(coll) || blank(keys) || blank(name)) {
            fail("db, collection, keys JSON, and name are required");
            return null;
        }
        try {
            return new IndexBuildSpec(db.trim(), coll.trim(),
                    Document.parse(keys), name.trim(),
                    uniqueBox.isSelected(), sparseBox.isSelected(),
                    Optional.empty(), Optional.empty(), Optional.empty(),
                    Optional.empty(), Optional.empty());
        } catch (Exception ex) {
            fail("Invalid keys JSON: " + ex.getMessage());
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private static List<Member> parseMembers(Document cfgReply) {
        Document cfg = cfgReply.get("config", Document.class);
        if (cfg == null) return List.of();
        List<Document> raw = (List<Document>) cfg.get("members", List.class);
        if (raw == null) return List.of();
        return raw.stream().map(m -> new Member(
                m.getInteger("_id", 0),
                m.getString("host"),
                m.getInteger("priority", 1),
                m.getInteger("votes", 1),
                m.getBoolean("hidden", false),
                m.getBoolean("arbiterOnly", false),
                m.getBoolean("buildIndexes", true),
                0.0)).toList();
    }

    @SuppressWarnings("unchecked")
    private static int findPrimaryId(Document status) {
        List<Document> members = (List<Document>) status.get("members", List.class);
        if (members == null) return -1;
        for (Document m : members) {
            if ("PRIMARY".equals(m.getString("stateStr"))) {
                return m.getInteger("_id", -1);
            }
        }
        return -1;
    }

    private void ok(String msg) {
        statusLabel.setText(msg);
        statusLabel.setStyle("-fx-text-fill: #166534; -fx-font-size: 11px; -fx-font-weight: 600;");
    }

    private void fail(String msg) {
        statusLabel.setText(msg);
        statusLabel.setStyle("-fx-text-fill: #b91c1c; -fx-font-size: 11px; -fx-font-weight: 600;");
    }

    private static boolean blank(String s) { return s == null || s.isBlank(); }

    private static Label small(String text) {
        Label l = new Label(text);
        l.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px;");
        return l;
    }

    private static Tooltip tip(String body) {
        Tooltip t = new Tooltip(body);
        t.setShowDelay(Duration.millis(250));
        t.setShowDuration(Duration.seconds(20));
        t.setWrapText(true);
        t.setMaxWidth(360);
        return t;
    }
}

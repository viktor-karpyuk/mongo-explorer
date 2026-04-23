package com.kubrik.mex.maint.ui;

import com.kubrik.mex.maint.compact.CompactRunner;
import com.kubrik.mex.maint.compact.ResyncRunner;
import com.kubrik.mex.maint.model.CompactSpec;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.CheckBox;
import javafx.scene.control.ChoiceBox;
import javafx.scene.control.Label;
import javafx.scene.control.TextField;
import javafx.scene.control.TextInputDialog;
import javafx.scene.control.Tooltip;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import javafx.util.Duration;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

/**
 * v2.7 CMPT-* UI — Compact + resync wizard. One pane, two tabs-
 * worth of inputs, switchable via the mode picker. Primary refusal
 * is enforced by the runner too.
 */
public final class CompactResyncPane extends BorderPane {

    private final CompactRunner compactRunner = new CompactRunner();
    private final ResyncRunner resyncRunner = new ResyncRunner();

    private final java.util.function.Supplier<MongoClient> clientSupplier;

    private final ChoiceBox<String> modePicker = new ChoiceBox<>();
    private final ChoiceBox<String> targetPicker = new ChoiceBox<>();
    private final TextField dbField = new TextField();
    private final TextField collField = new TextField();
    private final CheckBox forceBox = new CheckBox("force (primary-voting secondaries)");
    private final Label statusLabel = new Label("—");

    private String primaryHost;  // learned from replSetGetStatus

    public CompactResyncPane(java.util.function.Supplier<MongoClient> clientSupplier) {
        this.clientSupplier = clientSupplier;
        setStyle("-fx-background-color: white;");
        setPadding(new Insets(14, 16, 14, 16));
        setAccessibleText("Compact and resync wizard");
        setAccessibleHelp(
                "Run compact or trigger resync on a chosen secondary. "
                + "Primary-host refusal enforced both client and "
                + "server side; typed-confirm on the target host:port.");
        modePicker.getItems().addAll("Compact", "Resync");
        modePicker.setValue("Compact");

        setTop(buildHeader());
        setCenter(buildCenter());
        setBottom(buildActions());
    }

    private Region buildHeader() {
        Label title = new Label("Compact / Resync");
        title.setStyle("-fx-font-size: 14px; -fx-font-weight: 700;");
        Label hint = new Label(
                "Run compact or trigger resync on a chosen secondary. "
                + "Primary is refused both client-side and server-side.");
        hint.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px;");
        hint.setWrapText(true);
        VBox v = new VBox(6, title, hint);
        v.setPadding(new Insets(0, 0, 10, 0));
        return v;
    }

    private Region buildCenter() {
        dbField.setPromptText("app");
        collField.setPromptText("users,orders   (comma-separated; compact only)");

        GridPane g = new GridPane();
        g.setHgap(8); g.setVgap(6);
        int row = 0;
        g.add(small("Mode"), 0, row); g.add(modePicker, 1, row++);
        g.add(small("Target host:port"), 0, row); g.add(targetPicker, 1, row++);
        g.add(small("Database"), 0, row); g.add(dbField, 1, row++);
        g.add(small("Collections"), 0, row); g.add(collField, 1, row++);
        g.add(small("Options"), 0, row); g.add(forceBox, 1, row++);

        return new VBox(g);
    }

    private Region buildActions() {
        Button loadBtn = new Button("Load hosts");
        loadBtn.setTooltip(tip("Reads replSetGetStatus to populate the target picker + remember the primary."));
        loadBtn.setOnAction(e -> onLoadHosts());
        Button runBtn = new Button("Run…");
        runBtn.setOnAction(e -> onRun());

        Region grow = new Region();
        HBox.setHgrow(grow, Priority.ALWAYS);
        HBox actions = new HBox(8, loadBtn, grow, runBtn);
        actions.setPadding(new Insets(10, 0, 0, 0));

        statusLabel.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px;");
        statusLabel.setWrapText(true);
        return new VBox(6, actions, statusLabel);
    }

    @SuppressWarnings("unchecked")
    private void onLoadHosts() {
        MongoClient client = clientSupplier.get();
        if (client == null) { fail("No active connection."); return; }
        try {
            Document status = client.getDatabase("admin").runCommand(
                    new Document("replSetGetStatus", 1));
            List<Document> members = (List<Document>) status.get("members", List.class);
            if (members == null) { fail("Not a replica set."); return; }
            List<String> hosts = new ArrayList<>();
            primaryHost = null;
            for (Document m : members) {
                hosts.add(m.getString("name"));
                if ("PRIMARY".equals(m.getString("stateStr"))) {
                    primaryHost = m.getString("name");
                }
            }
            targetPicker.getItems().setAll(hosts);
            ok("Loaded " + hosts.size() + " members. Primary: " + primaryHost);
        } catch (Exception ex) {
            fail("Load failed: " + ex.getMessage());
        }
    }

    private void onRun() {
        String target = targetPicker.getValue();
        if (target == null) { fail("Pick a target host first."); return; }
        if (primaryHost != null && CompactRunner.wouldTargetPrimary(target, primaryHost)) {
            fail("Target is the current primary — refusing.");
            return;
        }
        TextInputDialog typed = new TextInputDialog();
        typed.setHeaderText("Confirm " + modePicker.getValue()
                + " on " + target);
        typed.setContentText("Type the host:port to confirm:");
        typed.showAndWait().ifPresent(t -> {
            if (!target.equals(t.trim())) {
                fail("Typed-confirm mismatch — aborting.");
                return;
            }
            if ("Compact".equals(modePicker.getValue())) {
                runCompact(target);
            } else {
                runResync(target);
            }
        });
    }

    private void runCompact(String host) {
        String db = dbField.getText();
        String colls = collField.getText();
        if (blank(db) || blank(colls)) {
            fail("db + comma-separated collection list required");
            return;
        }
        List<String> collList = java.util.Arrays.stream(colls.split(","))
                .map(String::trim).filter(s -> !s.isEmpty()).toList();
        CompactSpec.Compact spec = new CompactSpec.Compact(host, db.trim(),
                collList, /*takeOutOfRotation=*/false,
                forceBox.isSelected());

        statusLabel.setText("Running compact on " + host + "…");
        Thread.startVirtualThread(() -> {
            try (MongoClient target = MongoClients.create(
                    "mongodb://" + host + "/?directConnection=true")) {
                CompactRunner.Result result = compactRunner.run(target, spec);
                Platform.runLater(() -> {
                    if (result.primaryRefused()) {
                        fail("Runner refused — target turned out to be primary.");
                        return;
                    }
                    long failed = result.perCollection().stream()
                            .filter(o -> !o.success()).count();
                    if (failed == 0) {
                        ok("Compacted " + result.perCollection().size()
                                + " collection(s) in "
                                + result.totalElapsedMs() + "ms.");
                    } else {
                        fail(failed + " of " + result.perCollection().size()
                                + " compact calls failed — see logs.");
                    }
                });
            } catch (Exception ex) {
                Platform.runLater(() -> fail("Compact failed: " + ex.getMessage()));
            }
        });
    }

    private void runResync(String host) {
        CompactSpec.Resync spec = new CompactSpec.Resync(host,
                /*waitForCompletion=*/false);
        Alert warn = new Alert(Alert.AlertType.WARNING,
                "Resync on " + host + " drops the data files and resyncs "
                + "from scratch — this can take hours. Continue?",
                javafx.scene.control.ButtonType.OK,
                javafx.scene.control.ButtonType.CANCEL);
        warn.showAndWait().ifPresent(b -> {
            if (b != javafx.scene.control.ButtonType.OK) return;
            statusLabel.setText("Triggering resync on " + host + "…");
            Thread.startVirtualThread(() -> {
                try (MongoClient target = MongoClients.create(
                        "mongodb://" + host + "/?directConnection=true")) {
                    ResyncRunner.Outcome outcome = resyncRunner.run(target, spec);
                    Platform.runLater(() -> {
                        if (outcome instanceof ResyncRunner.Outcome.Ok) {
                            ok("Resync triggered; monitor progress via the "
                                    + "Monitoring tab.");
                        } else if (outcome instanceof ResyncRunner.Outcome.PrimaryRefused) {
                            fail("Server refused — target is primary.");
                        } else if (outcome instanceof ResyncRunner.Outcome.NotSupported ns) {
                            fail("Resync command removed in MongoDB 5.0 "
                                    + "(server is " + ns.serverVersion()
                                    + "). Use the dbpath-wipe + restart "
                                    + "flow: shut down the member, delete "
                                    + "its data files, start it again — "
                                    + "it will initial-sync from scratch.");
                        } else if (outcome instanceof ResyncRunner.Outcome.Failed f) {
                            fail("Resync failed: " + f.code() + " — " + f.message());
                        }
                    });
                } catch (Exception ex) {
                    Platform.runLater(() -> fail("Resync failed: " + ex.getMessage()));
                }
            });
        });
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

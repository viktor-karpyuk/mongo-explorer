package com.kubrik.mex.maint.ui;

import com.kubrik.mex.maint.model.ReconfigSpec;
import com.kubrik.mex.maint.model.ReconfigSpec.Member;
import com.kubrik.mex.maint.reconfig.PostChangeVerifier;
import com.kubrik.mex.maint.reconfig.ReconfigPreflight;
import com.kubrik.mex.maint.reconfig.ReconfigRunner;
import com.kubrik.mex.maint.reconfig.ReconfigSerializer;
import com.mongodb.client.MongoClient;
import javafx.application.Platform;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.ChoiceBox;
import javafx.scene.control.Label;
import javafx.scene.control.Spinner;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextField;
import javafx.scene.control.Tooltip;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import javafx.util.Duration;

import java.util.List;
import java.util.Optional;

/**
 * v2.7 RCFG-* UI — One wizard per RCFG-1 change kind. The kind
 * picker swaps which input fields show; the preflight result
 * table renders BLOCKING (red) vs WARN (amber) findings; Apply
 * dispatches through {@link ReconfigRunner} on a virtual thread.
 */
public final class ReconfigWizardPane extends BorderPane {

    /** Surfaced change kinds. Keep in sync with ReconfigSpec.Change's
     *  sealed permit list. */
    public enum ChangeKind {
        ADD("Add member"),
        REMOVE("Remove member"),
        PRIORITY("Change priority"),
        VOTES("Change votes"),
        HIDDEN("Toggle hidden"),
        ARBITER("Toggle arbiter"),
        RENAME("Rename member");

        private final String display;
        ChangeKind(String d) { this.display = d; }
    }

    private final ReconfigPreflight preflight = new ReconfigPreflight();
    private final ReconfigSerializer serializer = new ReconfigSerializer();
    private final ReconfigRunner runner = new ReconfigRunner();
    private final PostChangeVerifier verifier = new PostChangeVerifier();

    private final java.util.function.Supplier<MongoClient> clientSupplier;

    private final ChoiceBox<ChangeKind> kindPicker = new ChoiceBox<>();
    private final TableView<Member> currentMembers = new TableView<>(
            FXCollections.observableArrayList());
    private final VBox paramsBox = new VBox(6);
    private final ObservableList<ReconfigPreflight.Finding> findings =
            FXCollections.observableArrayList();
    private final TableView<ReconfigPreflight.Finding> findingsTable =
            new TableView<>(findings);
    private final Label statusLabel = new Label("—");

    // Per-kind inputs — only the relevant ones are mounted into
    // paramsBox at a time.
    private final Spinner<Integer> memberIdSpinner = new Spinner<>(0, 99, 0);
    private final TextField hostField = new TextField();
    private final Spinner<Integer> prioritySpinner = new Spinner<>(0, 1000, 1);
    private final Spinner<Integer> votesSpinner = new Spinner<>(0, 1, 1);
    private final ChoiceBox<Boolean> flagPicker = new ChoiceBox<>();

    private ReconfigSpec.Request loadedRequest;

    public ReconfigWizardPane(java.util.function.Supplier<MongoClient> clientSupplier) {
        this.clientSupplier = clientSupplier;
        setStyle("-fx-background-color: white;");
        setPadding(new Insets(14, 16, 14, 16));

        kindPicker.getItems().addAll(ChangeKind.values());
        kindPicker.setValue(ChangeKind.PRIORITY);
        kindPicker.setConverter(new javafx.util.StringConverter<>() {
            @Override public String toString(ChangeKind k) {
                return k == null ? "" : k.display;
            }
            @Override public ChangeKind fromString(String s) { return null; }
        });
        kindPicker.valueProperty().addListener((o, a, b) -> renderParamsFor(b));

        flagPicker.getItems().addAll(Boolean.TRUE, Boolean.FALSE);
        flagPicker.setValue(Boolean.FALSE);

        setTop(buildHeader());
        setCenter(buildCenter());
        setBottom(buildActions());
        renderParamsFor(ChangeKind.PRIORITY);
    }

    /* =============================== layout =============================== */

    private Region buildHeader() {
        Label title = new Label("Replica-set reconfig");
        title.setStyle("-fx-font-size: 14px; -fx-font-weight: 700;");
        Label hint = new Label(
                "Guided rs.reconfig wizard with full preflight arithmetic "
                + "(quorum / votes / priority). Load pulls the current "
                + "replSetGetConfig; Preview runs the preflight client-"
                + "side; Apply dispatches with writeConcern majority + "
                + "emits a rollback plan.");
        hint.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px;");
        hint.setWrapText(true);

        HBox pickerRow = new HBox(8, small("Change kind"), kindPicker);
        VBox v = new VBox(6, title, hint, pickerRow);
        v.setPadding(new Insets(0, 0, 10, 0));
        return v;
    }

    private Region buildCenter() {
        currentMembers.setPlaceholder(new Label("Load the replica set to populate."));
        currentMembers.getColumns().setAll(
                col("_id", 50, m -> String.valueOf(m.id())),
                col("host", 220, Member::host),
                col("priority", 80, m -> String.valueOf(m.priority())),
                col("votes", 60, m -> String.valueOf(m.votes())),
                col("hidden", 70, m -> m.hidden() ? "yes" : ""),
                col("arbiter", 70, m -> m.arbiterOnly() ? "yes" : ""));
        currentMembers.setPrefHeight(170);

        findingsTable.setPlaceholder(new Label("Preview to populate."));
        findingsTable.getColumns().setAll(
                col("severity", 90, f -> f.severity().name()),
                col("code", 160, ReconfigPreflight.Finding::code),
                col("detail", 480, ReconfigPreflight.Finding::message));
        findingsTable.setRowFactory(tv -> new javafx.scene.control.TableRow<>() {
            @Override
            protected void updateItem(ReconfigPreflight.Finding item, boolean empty) {
                super.updateItem(item, empty);
                if (empty || item == null) {
                    setStyle("");
                } else if (item.severity() == ReconfigPreflight.Severity.BLOCKING) {
                    setStyle("-fx-background-color: #fef2f2;");
                } else {
                    setStyle("-fx-background-color: #fffbeb;");
                }
            }
        });
        findingsTable.setPrefHeight(150);

        Label paramsLabel = new Label("Change parameters");
        paramsLabel.setStyle("-fx-text-fill: #4b5563; -fx-font-size: 11px; -fx-font-weight: 600;");
        VBox left = new VBox(6, small("Current members"), currentMembers,
                small("Preflight findings"), findingsTable);
        VBox right = new VBox(6, paramsLabel, paramsBox);
        right.setPrefWidth(260);

        HBox h = new HBox(12, left, right);
        HBox.setHgrow(left, Priority.ALWAYS);
        return h;
    }

    private Region buildActions() {
        Button loadBtn = new Button("Load");
        loadBtn.setOnAction(e -> onLoad());
        Button previewBtn = new Button("Preview");
        previewBtn.setOnAction(e -> onPreview());
        Button applyBtn = new Button("Apply…");
        applyBtn.setTooltip(tip(
                "Dispatches rs.reconfig with writeConcern majority. A "
                + "60 s watchdog surfaces a slow election. On success, "
                + "PostChangeVerifier waits for majority convergence."));
        applyBtn.setOnAction(e -> onApply());

        Region grow = new Region();
        HBox.setHgrow(grow, Priority.ALWAYS);
        HBox actions = new HBox(8, loadBtn, previewBtn, grow, applyBtn);
        actions.setPadding(new Insets(10, 0, 0, 0));

        statusLabel.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px;");
        statusLabel.setWrapText(true);
        VBox v = new VBox(6, actions, statusLabel);
        return v;
    }

    private void renderParamsFor(ChangeKind k) {
        paramsBox.getChildren().clear();
        switch (k) {
            case ADD -> {
                paramsBox.getChildren().addAll(
                        small("New _id"), memberIdSpinner,
                        small("host:port"), hostField,
                        small("priority"), prioritySpinner,
                        small("votes"), votesSpinner);
            }
            case REMOVE -> paramsBox.getChildren().addAll(
                    small("Member _id to remove"), memberIdSpinner);
            case PRIORITY -> paramsBox.getChildren().addAll(
                    small("Target _id"), memberIdSpinner,
                    small("New priority"), prioritySpinner);
            case VOTES -> paramsBox.getChildren().addAll(
                    small("Target _id"), memberIdSpinner,
                    small("New votes (0 or 1)"), votesSpinner);
            case HIDDEN, ARBITER -> paramsBox.getChildren().addAll(
                    small("Target _id"), memberIdSpinner,
                    small("Flag"), flagPicker);
            case RENAME -> paramsBox.getChildren().addAll(
                    small("Target _id"), memberIdSpinner,
                    small("New host:port"), hostField);
        }
    }

    /* =============================== actions =============================== */

    private void onLoad() {
        MongoClient client = clientSupplier.get();
        if (client == null) { fail("No active connection."); return; }
        statusLabel.setText("Loading current config…");
        Thread.startVirtualThread(() -> {
            try {
                var reply = client.getDatabase("admin").runCommand(
                        new org.bson.Document("replSetGetConfig", 1));
                Optional<ReconfigSpec.Request> parsed = serializer.fromConfigReply(
                        reply, new ReconfigSpec.ChangePriority(0, 1));
                if (parsed.isEmpty()) {
                    Platform.runLater(() -> fail(
                            "Cluster is not a replica set, or reply was malformed."));
                    return;
                }
                Platform.runLater(() -> {
                    loadedRequest = parsed.get();
                    currentMembers.getItems().setAll(loadedRequest.currentMembers());
                    ok("Loaded " + loadedRequest.replicaSetName()
                            + " (config v" + loadedRequest.currentConfigVersion()
                            + ", " + loadedRequest.currentMembers().size()
                            + " members).");
                });
            } catch (Exception ex) {
                Platform.runLater(() -> fail("Load failed: " + ex.getMessage()));
            }
        });
    }

    private void onPreview() {
        if (loadedRequest == null) { fail("Load the config first."); return; }
        ReconfigSpec.Change change = buildChange();
        if (change == null) return;  // fail() already set
        ReconfigSpec.Request req = new ReconfigSpec.Request(
                loadedRequest.replicaSetName(),
                loadedRequest.currentConfigVersion(),
                loadedRequest.currentMembers(),
                change);
        ReconfigPreflight.Result r = preflight.check(req);
        findings.setAll(r.findings());
        if (r.hasBlocking()) {
            fail("Preflight: " + r.findings().size()
                    + " finding(s); Apply blocked.");
        } else if (r.findings().isEmpty()) {
            ok("Preflight clean.");
        } else {
            statusLabel.setText("Preflight warnings only — Apply allowed.");
            statusLabel.setStyle(
                    "-fx-text-fill: #d97706; -fx-font-size: 11px; -fx-font-weight: 600;");
        }
    }

    private void onApply() {
        if (loadedRequest == null) { fail("Load the config first."); return; }
        ReconfigSpec.Change change = buildChange();
        if (change == null) return;
        ReconfigSpec.Request req = new ReconfigSpec.Request(
                loadedRequest.replicaSetName(),
                loadedRequest.currentConfigVersion(),
                loadedRequest.currentMembers(),
                change);
        ReconfigPreflight.Result pre = preflight.check(req);
        findings.setAll(pre.findings());
        if (pre.hasBlocking()) {
            fail("Refusing to Apply — preflight has BLOCKING finding(s).");
            return;
        }

        Alert confirm = new Alert(Alert.AlertType.CONFIRMATION,
                "rs.reconfig on " + req.replicaSetName()
                        + ". writeConcern=majority. Proceed?",
                javafx.scene.control.ButtonType.OK,
                javafx.scene.control.ButtonType.CANCEL);
        confirm.showAndWait().ifPresent(b -> {
            if (b != javafx.scene.control.ButtonType.OK) return;
            dispatch(req);
        });
    }

    private void dispatch(ReconfigSpec.Request req) {
        MongoClient client = clientSupplier.get();
        if (client == null) { fail("No active connection."); return; }
        statusLabel.setText("Dispatching rs.reconfig (60 s watchdog)…");
        Thread.startVirtualThread(() -> {
            ReconfigRunner.Outcome outcome = runner.dispatch(client, req);
            Platform.runLater(() -> {
                if (outcome instanceof ReconfigRunner.Outcome.Ok ok) {
                    statusLabel.setText("Dispatched; awaiting majority convergence "
                            + "on config v" + ok.newConfigVersion() + "…");
                    verify(client, ok.newConfigVersion());
                } else if (outcome instanceof ReconfigRunner.Outcome.Failed f) {
                    fail("rs.reconfig failed: " + f.code() + " — " + f.message());
                } else if (outcome instanceof ReconfigRunner.Outcome.TimedOut t) {
                    fail("rs.reconfig timed out after " + t.elapsed().toSeconds() + "s.");
                }
            });
        });
    }

    private void verify(MongoClient client, int expectedVersion) {
        Thread.startVirtualThread(() -> {
            PostChangeVerifier.Verdict v = verifier.awaitConvergence(
                    client, expectedVersion);
            Platform.runLater(() -> {
                if (v.converged()) {
                    ok("Converged: " + v.caughtUpMembers() + "/"
                            + v.reachableMembers() + " members in "
                            + v.elapsed().toSeconds() + "s.");
                } else {
                    fail("Did not converge — lagging: " + v.lagging());
                }
                onLoad();  // refresh the table from server
            });
        });
    }

    private ReconfigSpec.Change buildChange() {
        try {
            int id = memberIdSpinner.getValue();
            return switch (kindPicker.getValue()) {
                case ADD -> new ReconfigSpec.AddMember(new Member(id,
                        hostField.getText().trim(),
                        prioritySpinner.getValue(), votesSpinner.getValue(),
                        false, false, true, 0.0));
                case REMOVE -> new ReconfigSpec.RemoveMember(id);
                case PRIORITY -> new ReconfigSpec.ChangePriority(id,
                        prioritySpinner.getValue());
                case VOTES -> new ReconfigSpec.ChangeVotes(id,
                        votesSpinner.getValue());
                case HIDDEN -> new ReconfigSpec.ToggleHidden(id,
                        flagPicker.getValue());
                case ARBITER -> new ReconfigSpec.ToggleArbiter(id,
                        flagPicker.getValue());
                case RENAME -> new ReconfigSpec.RenameMember(id,
                        hostField.getText().trim());
            };
        } catch (IllegalArgumentException bad) {
            fail(bad.getMessage());
            return null;
        }
    }

    /* =============================== helpers =============================== */

    private void ok(String msg) {
        statusLabel.setText(msg);
        statusLabel.setStyle(
                "-fx-text-fill: #166534; -fx-font-size: 11px; -fx-font-weight: 600;");
    }

    private void fail(String msg) {
        statusLabel.setText(msg);
        statusLabel.setStyle(
                "-fx-text-fill: #b91c1c; -fx-font-size: 11px; -fx-font-weight: 600;");
    }

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

    private static <T> TableColumn<T, String> col(String title, int width,
                                                   java.util.function.Function<T, String> getter) {
        TableColumn<T, String> c = new TableColumn<>(title);
        c.setPrefWidth(width);
        c.setCellValueFactory(cd -> new SimpleStringProperty(getter.apply(cd.getValue())));
        return c;
    }
}

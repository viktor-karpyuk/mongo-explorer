package com.kubrik.mex.k8s.ui;

import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.k8s.apply.ApplyOrchestrator;
import com.kubrik.mex.k8s.events.ProvisionEvent;
import com.kubrik.mex.k8s.model.K8sClusterRef;
import com.kubrik.mex.k8s.preflight.PreflightResult;
import com.kubrik.mex.k8s.preflight.PreflightSummary;
import com.kubrik.mex.k8s.provision.OperatorId;
import com.kubrik.mex.k8s.provision.ProfileEnforcer;
import com.kubrik.mex.k8s.provision.ProvisionModel;
import com.kubrik.mex.k8s.provision.ProvisioningService;
import com.kubrik.mex.k8s.provision.Profile;
import com.kubrik.mex.k8s.provision.Topology;
import com.kubrik.mex.k8s.provision.TopologyPicker;
import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.scene.Node;
import javafx.scene.control.Button;
import javafx.scene.control.ButtonType;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Dialog;
import javafx.scene.control.Label;
import javafx.scene.control.ProgressIndicator;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;

import java.time.Instant;

/**
 * v2.8.1 Q2.8.1-L — Minimal single-page Provisioning dialog.
 *
 * <p>Not the full 14-step wizard the milestone describes; a single
 * form exposing the headline choices (profile / operator /
 * topology / namespace / name / mongo version) with a live pre-
 * flight panel + an Apply button that runs
 * {@link ProvisioningService#provision} on a virtual thread. The
 * log area tails {@link com.kubrik.mex.k8s.rollout.RolloutEvent}s
 * via the EventBus so the user sees progress.</p>
 *
 * <p>Full multi-step wizard work (TLS tabs, storage picker, resource
 * tuning, advanced YAML escape) is deferred; this dialog gives us a
 * usable provisioning surface against kind + the operator adapters
 * today.</p>
 */
public final class ProvisionDialog extends Dialog<Void> {

    private final ProvisioningService service;
    private final EventBus events;
    private final K8sClusterRef clusterRef;

    private final ComboBox<Profile> profileBox = new ComboBox<>();
    private final ComboBox<OperatorId> operatorBox = new ComboBox<>();
    private final ComboBox<Topology> topologyBox = new ComboBox<>();
    private final TextField namespaceField = new TextField("mongo");
    private final TextField deploymentNameField = new TextField("dev-rs");
    private final TextField mongoVersionField = new TextField("7.0");

    private final TextArea preflightArea = new TextArea();
    private final TextArea logArea = new TextArea();
    private final Label statusLabel = new Label("Fill the form, then click Pre-flight.");
    private final ProgressIndicator spinner = new ProgressIndicator();

    private final Button preflightBtn = new Button("Pre-flight");
    private final Button applyBtn = new Button("Apply");

    private EventBus.Subscription provisionSub;
    private volatile boolean preflightPassed = false;

    public ProvisionDialog(ProvisioningService service, EventBus events, K8sClusterRef clusterRef) {
        this.service = service;
        this.events = events;
        this.clusterRef = clusterRef;

        setTitle("Provision MongoDB on Kubernetes");
        setHeaderText("Target: " + clusterRef.displayName()
                + " (context " + clusterRef.contextName() + ")");
        getDialogPane().getButtonTypes().setAll(ButtonType.CLOSE);

        profileBox.getItems().setAll(Profile.values());
        profileBox.getSelectionModel().select(Profile.DEV_TEST);
        operatorBox.getItems().setAll(OperatorId.values());
        operatorBox.getSelectionModel().select(OperatorId.MCO);
        refreshTopologyChoices();
        topologyBox.getSelectionModel().select(Topology.RS3);

        profileBox.valueProperty().addListener((o, a, b) -> {
            refreshTopologyChoices();
            invalidatePreflight();
        });
        operatorBox.valueProperty().addListener((o, a, b) -> {
            refreshTopologyChoices();
            invalidatePreflight();
        });
        topologyBox.valueProperty().addListener((o, a, b) -> invalidatePreflight());
        namespaceField.textProperty().addListener((o, a, b) -> invalidatePreflight());
        deploymentNameField.textProperty().addListener((o, a, b) -> invalidatePreflight());
        mongoVersionField.textProperty().addListener((o, a, b) -> invalidatePreflight());

        preflightBtn.setOnAction(e -> runPreflight());
        preflightBtn.getStyleClass().add("accent");
        applyBtn.setDisable(true);
        applyBtn.setOnAction(e -> runApply());
        applyBtn.getStyleClass().add("accent");

        preflightArea.setEditable(false);
        preflightArea.setPrefRowCount(6);
        preflightArea.setWrapText(true);
        preflightArea.setStyle("-fx-font-family: 'Menlo', monospace; -fx-font-size: 11px;");
        preflightArea.setText("(not yet run)");

        logArea.setEditable(false);
        logArea.setPrefRowCount(10);
        logArea.setWrapText(true);
        logArea.setStyle("-fx-font-family: 'Menlo', monospace; -fx-font-size: 11px;");
        logArea.setText("(apply not yet run)");

        spinner.setVisible(false);
        spinner.setPrefSize(18, 18);

        statusLabel.setStyle("-fx-text-fill: -color-fg-muted; -fx-font-size: 11px;");
        statusLabel.setWrapText(true);

        getDialogPane().setContent(buildContent());
        getDialogPane().setPrefWidth(760);
        getDialogPane().setPrefHeight(620);

        // Tear down the bus subscription when the dialog closes.
        setOnCloseRequest(e -> {
            if (provisionSub != null) provisionSub.close();
        });
    }

    private Region buildContent() {
        GridPane form = new GridPane();
        form.setHgap(10);
        form.setVgap(8);
        form.setPadding(new Insets(6, 0, 12, 0));
        int row = 0;
        form.add(new Label("Profile"), 0, row);     form.add(profileBox, 1, row++);
        form.add(new Label("Operator"), 0, row);    form.add(operatorBox, 1, row++);
        form.add(new Label("Topology"), 0, row);    form.add(topologyBox, 1, row++);
        form.add(new Label("Namespace"), 0, row);   form.add(namespaceField, 1, row++);
        form.add(new Label("Name"), 0, row);        form.add(deploymentNameField, 1, row++);
        form.add(new Label("Mongo version"), 0, row); form.add(mongoVersionField, 1, row++);

        Label pfHead = new Label("Pre-flight");
        pfHead.setStyle("-fx-font-weight: 600; -fx-font-size: 11px;");
        Label logHead = new Label("Rollout log");
        logHead.setStyle("-fx-font-weight: 600; -fx-font-size: 11px;");

        HBox actions = new HBox(8, preflightBtn, applyBtn, spinner, statusLabel);
        actions.setPadding(new Insets(8, 0, 0, 0));

        VBox v = new VBox(6, form, pfHead, preflightArea, logHead, logArea, actions);
        VBox.setVgrow(logArea, Priority.ALWAYS);
        return v;
    }

    private void refreshTopologyChoices() {
        Profile p = profileBox.getValue();
        OperatorId op = operatorBox.getValue();
        if (p == null || op == null) return;
        Topology current = topologyBox.getValue();
        topologyBox.getItems().setAll(TopologyPicker.availableTopologies(p, op));
        Topology next = TopologyPicker.defaultFor(p, op, current == null ? Topology.RS3 : current);
        topologyBox.getSelectionModel().select(next);
    }

    private void invalidatePreflight() {
        preflightPassed = false;
        applyBtn.setDisable(true);
    }

    /* ============================ actions ============================ */

    private void runPreflight() {
        ProvisionModel m = buildModel();
        statusLabel.setText("Running pre-flight…");
        spinner.setVisible(true);
        preflightBtn.setDisable(true);
        Thread.startVirtualThread(() -> {
            PreflightSummary summary;
            try {
                summary = service.preflight(clusterRef, m);
            } catch (Throwable t) {
                Platform.runLater(() -> {
                    preflightArea.setText("pre-flight errored: " + t.getMessage());
                    statusLabel.setText("Pre-flight errored.");
                    spinner.setVisible(false);
                    preflightBtn.setDisable(false);
                });
                return;
            }
            Platform.runLater(() -> renderPreflight(summary));
        });
    }

    private void renderPreflight(PreflightSummary summary) {
        spinner.setVisible(false);
        preflightBtn.setDisable(false);
        StringBuilder sb = new StringBuilder();
        for (PreflightResult r : summary.results()) {
            sb.append(pad(r.status().name(), 6))
                    .append(pad(r.checkId(), 32));
            r.message().ifPresent(msg -> sb.append(" — ").append(msg));
            sb.append('\n');
            r.hint().ifPresent(h -> sb.append("       hint: ").append(h).append('\n'));
        }
        preflightArea.setText(sb.toString());
        preflightPassed = !summary.hasAnyFail();
        applyBtn.setDisable(!preflightPassed);
        if (summary.hasAnyFail()) {
            statusLabel.setText("Pre-flight has " + summary.failing().size()
                    + " failure(s). Fix them before Apply.");
        } else if (!summary.warnsToAck().isEmpty()) {
            statusLabel.setText("Pre-flight passed with " + summary.warnsToAck().size()
                    + " warning(s). Review before Apply.");
        } else {
            statusLabel.setText("Pre-flight passed. Apply is unlocked.");
        }
    }

    private void runApply() {
        if (!preflightPassed) {
            statusLabel.setText("Run Pre-flight first.");
            return;
        }
        ProvisionModel m = buildModel();
        statusLabel.setText("Applying " + m.deploymentName() + "…");
        logArea.clear();
        logArea.appendText(stamp() + " starting apply\n");
        spinner.setVisible(true);
        applyBtn.setDisable(true);
        preflightBtn.setDisable(true);

        // Subscribe for the lifetime of the apply — close on terminal.
        if (provisionSub != null) provisionSub.close();
        provisionSub = events.onProvision(evt -> Platform.runLater(() -> {
            logArea.appendText(stamp() + " " + evt + "\n");
            if (evt instanceof ProvisionEvent.Ready || evt instanceof ProvisionEvent.Failed) {
                spinner.setVisible(false);
                applyBtn.setDisable(true);
                preflightBtn.setDisable(false);
                if (evt instanceof ProvisionEvent.Ready r) {
                    statusLabel.setText("Ready — " + r.deploymentName()
                            + " (connection row + port-forward created).");
                } else if (evt instanceof ProvisionEvent.Failed f) {
                    statusLabel.setText("Failed: " + f.reason());
                }
            }
        }));

        Thread.startVirtualThread(() -> {
            try {
                ApplyOrchestrator.ApplyResult result = service.provision(clusterRef, m);
                if (!result.ok()) {
                    Platform.runLater(() -> {
                        logArea.appendText(stamp() + " apply failed: "
                                + result.error().orElse("?") + "\n");
                        statusLabel.setText("Apply failed.");
                        spinner.setVisible(false);
                        applyBtn.setDisable(true);
                        preflightBtn.setDisable(false);
                    });
                }
            } catch (Throwable t) {
                Platform.runLater(() -> {
                    logArea.appendText(stamp() + " provision errored: " + t.getMessage() + "\n");
                    statusLabel.setText("Provision errored.");
                    spinner.setVisible(false);
                    applyBtn.setDisable(true);
                    preflightBtn.setDisable(false);
                });
            }
        });
    }

    /* ============================ helpers ============================ */

    private ProvisionModel buildModel() {
        ProvisionModel base = ProvisionModel.defaults(
                clusterRef.id(),
                namespaceField.getText().trim(),
                deploymentNameField.getText().trim())
                .withOperator(operatorBox.getValue())
                .withTopology(topologyBox.getValue())
                .withMongoVersion(mongoVersionField.getText().trim());
        // Run through the enforcer so Prod locks are applied before
        // pre-flight sees the model.
        return new ProfileEnforcer().switchProfile(base, profileBox.getValue()).model();
    }

    private static String pad(String s, int width) {
        if (s == null) return " ".repeat(width);
        return s.length() >= width ? s : s + " ".repeat(width - s.length());
    }

    private static String stamp() {
        return Instant.now().toString();
    }

    /** Static accessor so ClustersPane can anchor the owner. */
    public static Node dummy() { return new Label(); }
}

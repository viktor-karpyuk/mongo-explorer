package com.kubrik.mex.k8s.ui;

import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.k8s.apply.ApplyOrchestrator;
import com.kubrik.mex.k8s.compute.ComputeStrategy;
import com.kubrik.mex.k8s.compute.ComputeStrategyRegistry;
import com.kubrik.mex.k8s.compute.LabelPair;
import com.kubrik.mex.k8s.compute.SpreadScope;
import com.kubrik.mex.k8s.compute.StrategyId;
import com.kubrik.mex.k8s.compute.Toleration;
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
import javafx.scene.control.RadioButton;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.control.ToggleGroup;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;

import java.util.ArrayList;
import java.util.List;

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

    // v2.8.2 Q2.8.2-D — Dedicated compute controls. Rendered as a
    // four-radio group per milestone §1.3 with Karpenter + managed-
    // pool greyed out via their registry-lock labels; Node-pool opens
    // a small label + toleration form when selected.
    private final ToggleGroup strategyGroup = new ToggleGroup();
    private final RadioButton strategyNoneRadio = new RadioButton("None / Cluster default scheduler");
    private final RadioButton strategyNodePoolRadio = new RadioButton("Use existing node pool");
    private final RadioButton strategyKarpenterRadio = new RadioButton("Karpenter-provisioned on-demand (AWS defaults)");
    private final RadioButton strategyManagedRadio = new RadioButton("Mongo Explorer creates a managed pool (AWS EKS stub)");
    private final TextField poolLabelKeyField = new TextField("workload");
    private final TextField poolLabelValueField = new TextField("mongodb");
    private final TextField poolTaintKeyField = new TextField("dedicated");
    private final TextField poolTaintValueField = new TextField("mongo");
    private final ComboBox<Toleration.Effect> poolTaintEffectBox = new ComboBox<>();
    private final VBox nodePoolForm = new VBox(6);

    // v2.8.3 Q2.8.3-D — Full Karpenter sub-form.
    private final TextField kpNodeClassNameField = new TextField("default");
    private final ComboBox<String> kpNodeClassKindBox = new ComboBox<>();
    private final TextField kpInstanceFamiliesField = new TextField("m6i,r6i");
    private final ComboBox<String> kpCapacityTypeBox = new ComboBox<>();
    private final ComboBox<String> kpArchBox = new ComboBox<>();
    private final TextField kpCpuMinField = new TextField("2");
    private final TextField kpCpuMaxField = new TextField("32");
    private final TextField kpMemMinField = new TextField("4Gi");
    private final TextField kpMemMaxField = new TextField("128Gi");
    private final TextField kpExpireAfterField = new TextField("720h");
    private final TextField kpLimitCpuField = new TextField("100");
    private final TextField kpLimitMemField = new TextField("400Gi");
    private final javafx.scene.control.CheckBox kpConsolidationBox =
            new javafx.scene.control.CheckBox("Consolidate when underutilized");
    private final VBox karpenterForm = new VBox(6);

    // v2.8.4 — Full Managed-pool sub-form.
    private final ComboBox<com.kubrik.mex.k8s.compute.managedpool.CloudCredential>
            mpCredentialBox = new ComboBox<>();
    private final TextField mpRegionField = new TextField("us-east-1");
    private final TextField mpInstanceTypeField = new TextField("m6i.large");
    // v2.8.4 Q2.8.4-A — Per-cloud extras. Visible only for the
    // matching provider when the credential picker resolves; values
    // round-trip through ManagedPoolSpec.cloudExtras.
    private final TextField mpAksResourceGroupField = new TextField();
    private final TextField mpAksClusterField = new TextField();
    private final ComboBox<String> mpAksOsTypeBox = new ComboBox<>();
    private final TextField mpEksClusterField = new TextField();
    private final TextField mpEksNodeRoleArnField = new TextField();
    private final TextField mpGkeClusterField = new TextField();
    private final TextField mpGkeProjectField = new TextField();
    private final VBox mpAksRow = new VBox(4);
    private final VBox mpEksRow = new VBox(4);
    private final VBox mpGkeRow = new VBox(4);
    private final ComboBox<com.kubrik.mex.k8s.compute.managedpool.ManagedPoolSpec.CapacityType>
            mpCapacityBox = new ComboBox<>();
    private final TextField mpMinNodesField = new TextField("3");
    private final TextField mpDesiredNodesField = new TextField("3");
    private final TextField mpMaxNodesField = new TextField("5");
    private final ComboBox<String> mpArchBox = new ComboBox<>();
    private final TextField mpZonesField = new TextField();
    private final TextField mpSubnetsField = new TextField();
    private final VBox managedPoolForm = new VBox(6);
    private final com.kubrik.mex.k8s.compute.managedpool.CloudCredentialDao mpCredDao;

    private final TextArea preflightArea = new TextArea();
    private final TextArea logArea = new TextArea();
    private final Label statusLabel = new Label("Fill the form, then click Pre-flight.");
    private final ProgressIndicator spinner = new ProgressIndicator();

    private final Button preflightBtn = new Button("Pre-flight");
    private final Button applyBtn = new Button("Apply");

    private EventBus.Subscription provisionSub;
    private volatile boolean preflightPassed = false;

    public ProvisionDialog(ProvisioningService service, EventBus events, K8sClusterRef clusterRef) {
        this(service, events, clusterRef, null);
    }

    public ProvisionDialog(ProvisioningService service, EventBus events, K8sClusterRef clusterRef,
                            com.kubrik.mex.k8s.compute.managedpool.CloudCredentialDao credDao) {
        this.service = service;
        this.events = events;
        this.clusterRef = clusterRef;
        this.mpCredDao = credDao;

        setTitle("Provision MongoDB on Kubernetes");
        setHeaderText("Target: " + clusterRef.displayName()
                + " (context " + clusterRef.contextName() + ")");
        getDialogPane().getButtonTypes().setAll(ButtonType.CLOSE);

        profileBox.getItems().setAll(Profile.values());
        profileBox.getSelectionModel().select(Profile.DEV_TEST);

        wireStrategyGroup();
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

        // Tear down the bus subscription on every visibility-loss
        // pathway. setOnCloseRequest only fires when the user clicks
        // the close button — if the dialog is hidden() programmatically
        // (parent stage closes, scene swap, etc.) the close-request
        // handler never runs and the subscription leaks. Hooking
        // dialogPane().sceneProperty().getWindow() onHidden covers
        // every visibility-loss path the JavaFX dialog API exposes.
        Runnable teardown = () -> {
            if (provisionSub != null) {
                provisionSub.close();
                provisionSub = null;
            }
        };
        setOnCloseRequest(e -> teardown.run());
        setOnHidden(e -> teardown.run());
    }

    private void wireStrategyGroup() {
        ComputeStrategyRegistry reg = ComputeStrategyRegistry.current();
        strategyNoneRadio.setUserData(StrategyId.NONE);
        strategyNodePoolRadio.setUserData(StrategyId.NODE_POOL);
        strategyKarpenterRadio.setUserData(StrategyId.KARPENTER);
        strategyManagedRadio.setUserData(StrategyId.MANAGED_POOL);
        for (RadioButton rb : List.of(strategyNoneRadio, strategyNodePoolRadio,
                strategyKarpenterRadio, strategyManagedRadio)) {
            rb.setToggleGroup(strategyGroup);
            if (!reg.isShipped((StrategyId) rb.getUserData())) rb.setDisable(true);
        }
        strategyNoneRadio.setSelected(true);
        strategyGroup.selectedToggleProperty().addListener((o, a, b) -> {
            Object id = b == null ? null : b.getUserData();
            boolean showNp = id == StrategyId.NODE_POOL;
            boolean showKp = id == StrategyId.KARPENTER;
            boolean showMp = id == StrategyId.MANAGED_POOL;
            nodePoolForm.setVisible(showNp);
            nodePoolForm.setManaged(showNp);
            karpenterForm.setVisible(showKp);
            karpenterForm.setManaged(showKp);
            managedPoolForm.setVisible(showMp);
            managedPoolForm.setManaged(showMp);
            if (showMp) refreshCredentialBox();
            invalidatePreflight();
        });
        poolLabelKeyField.textProperty().addListener((o, a, b) -> invalidatePreflight());
        poolLabelValueField.textProperty().addListener((o, a, b) -> invalidatePreflight());
        poolTaintKeyField.textProperty().addListener((o, a, b) -> invalidatePreflight());
        poolTaintValueField.textProperty().addListener((o, a, b) -> invalidatePreflight());
        poolTaintEffectBox.getItems().setAll(Toleration.Effect.values());
        poolTaintEffectBox.getSelectionModel().select(Toleration.Effect.NO_SCHEDULE);

        // Karpenter sub-form defaults.
        kpNodeClassKindBox.getItems().setAll("EC2NodeClass", "AKSNodeClass", "GCENodeClass");
        kpNodeClassKindBox.getSelectionModel().select("EC2NodeClass");
        kpCapacityTypeBox.getItems().setAll("spot", "on-demand", "spot,on-demand");
        kpCapacityTypeBox.getSelectionModel().select("spot,on-demand");
        kpArchBox.getItems().setAll("amd64", "arm64", "amd64,arm64");
        kpArchBox.getSelectionModel().select("amd64");
        kpConsolidationBox.setSelected(true);
        for (TextField tf : List.of(kpNodeClassNameField, kpInstanceFamiliesField,
                kpCpuMinField, kpCpuMaxField, kpMemMinField, kpMemMaxField,
                kpExpireAfterField, kpLimitCpuField, kpLimitMemField)) {
            tf.textProperty().addListener((o, a, b) -> invalidatePreflight());
        }

        // Managed-pool sub-form defaults.
        mpCapacityBox.getItems().setAll(
                com.kubrik.mex.k8s.compute.managedpool.ManagedPoolSpec.CapacityType.values());
        mpCapacityBox.getSelectionModel().select(
                com.kubrik.mex.k8s.compute.managedpool.ManagedPoolSpec.CapacityType.ON_DEMAND);
        mpArchBox.getItems().setAll("amd64", "arm64");
        mpArchBox.getSelectionModel().select("amd64");
        mpCredentialBox.setConverter(
                new javafx.util.StringConverter<com.kubrik.mex.k8s.compute.managedpool.CloudCredential>() {
                    @Override public String toString(
                            com.kubrik.mex.k8s.compute.managedpool.CloudCredential c) {
                        return c == null ? "(none)"
                                : c.displayName() + " (" + c.provider() + ")";
                    }
                    @Override public com.kubrik.mex.k8s.compute.managedpool.CloudCredential
                            fromString(String s) { return null; }
                });
        for (TextField tf : List.of(mpRegionField, mpInstanceTypeField,
                mpMinNodesField, mpDesiredNodesField, mpMaxNodesField,
                mpZonesField, mpSubnetsField,
                mpAksResourceGroupField, mpAksClusterField,
                mpEksClusterField, mpEksNodeRoleArnField,
                mpGkeClusterField, mpGkeProjectField)) {
            tf.textProperty().addListener((o, a, b) -> invalidatePreflight());
        }
        mpCredentialBox.valueProperty().addListener((o, a, b) -> {
            refreshPerCloudRows();
            invalidatePreflight();
        });
    }

    private void refreshCredentialBox() {
        if (mpCredDao == null) return;
        try {
            mpCredentialBox.getItems().setAll(mpCredDao.listAll());
            if (!mpCredentialBox.getItems().isEmpty()
                    && mpCredentialBox.getValue() == null) {
                mpCredentialBox.getSelectionModel().selectFirst();
            }
        } catch (java.sql.SQLException ignored) { /* best effort */ }
        refreshPerCloudRows();
    }

    /** Show only the per-cloud extras row that matches the picked
     *  credential's provider. Called whenever the credential picker
     *  changes its selection. */
    private void refreshPerCloudRows() {
        var sel = mpCredentialBox.getValue();
        com.kubrik.mex.k8s.compute.managedpool.CloudProvider p = sel == null
                ? null : sel.provider();
        boolean aks = p == com.kubrik.mex.k8s.compute.managedpool.CloudProvider.AZURE;
        boolean eks = p == com.kubrik.mex.k8s.compute.managedpool.CloudProvider.AWS;
        boolean gke = p == com.kubrik.mex.k8s.compute.managedpool.CloudProvider.GCP;
        mpAksRow.setVisible(aks); mpAksRow.setManaged(aks);
        mpEksRow.setVisible(eks); mpEksRow.setManaged(eks);
        mpGkeRow.setVisible(gke); mpGkeRow.setManaged(gke);
    }

    private static javafx.scene.layout.HBox labelled(String label,
            javafx.scene.Node ctrl) {
        Label l = new Label(label);
        l.setStyle("-fx-text-fill: -color-fg-muted; -fx-font-size: 11px;");
        javafx.scene.layout.HBox h = new javafx.scene.layout.HBox(6, l, ctrl);
        h.setAlignment(javafx.geometry.Pos.CENTER_LEFT);
        return h;
    }

    private static void populatePerCloudRow(VBox target, String head,
            javafx.scene.layout.HBox... entries) {
        Label header = new Label(head + " sub-form");
        header.setStyle("-fx-font-weight: 600; -fx-font-size: 11px;");
        target.setPadding(new Insets(6, 0, 0, 24));
        target.getChildren().clear();
        target.getChildren().add(header);
        for (var e : entries) target.getChildren().add(e);
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

        Label strategyHead = new Label("Dedicated compute");
        strategyHead.setStyle("-fx-font-weight: 600; -fx-font-size: 11px;");
        Label strategyHint = new Label(
                "Prod's topology spread already prevents two RS members from sharing a node. "
              + "Pick \"Use existing node pool\" to additionally pin Mongo to a pre-labelled "
              + "pool where nothing else schedules.");
        strategyHint.setStyle("-fx-text-fill: -color-fg-muted; -fx-font-size: 10px;");
        strategyHint.setWrapText(true);

        GridPane npGrid = new GridPane();
        npGrid.setHgap(8); npGrid.setVgap(4);
        npGrid.setPadding(new Insets(4, 0, 0, 24));
        int npRow = 0;
        npGrid.add(new Label("Label key"), 0, npRow);   npGrid.add(poolLabelKeyField, 1, npRow);
        npGrid.add(new Label("Label value"), 2, npRow); npGrid.add(poolLabelValueField, 3, npRow++);
        npGrid.add(new Label("Taint key"), 0, npRow);   npGrid.add(poolTaintKeyField, 1, npRow);
        npGrid.add(new Label("Taint value"), 2, npRow); npGrid.add(poolTaintValueField, 3, npRow++);
        npGrid.add(new Label("Taint effect"), 0, npRow); npGrid.add(poolTaintEffectBox, 1, npRow++);
        nodePoolForm.getChildren().setAll(npGrid);
        nodePoolForm.setVisible(false);
        nodePoolForm.setManaged(false);

        // Karpenter sub-form (UI-DC-KP-* — milestone-v2.8.3 §2.1).
        GridPane kpGrid = new GridPane();
        kpGrid.setHgap(8); kpGrid.setVgap(4);
        kpGrid.setPadding(new Insets(4, 0, 0, 24));
        int kr = 0;
        kpGrid.add(new Label("NodeClass kind"), 0, kr); kpGrid.add(kpNodeClassKindBox, 1, kr);
        kpGrid.add(new Label("NodeClass name"), 2, kr); kpGrid.add(kpNodeClassNameField, 3, kr++);
        kpGrid.add(new Label("Instance families"), 0, kr); kpGrid.add(kpInstanceFamiliesField, 1, kr, 3, 1); kr++;
        kpGrid.add(new Label("Capacity type"), 0, kr); kpGrid.add(kpCapacityTypeBox, 1, kr);
        kpGrid.add(new Label("Architecture"), 2, kr); kpGrid.add(kpArchBox, 3, kr++);
        kpGrid.add(new Label("CPU min"), 0, kr); kpGrid.add(kpCpuMinField, 1, kr);
        kpGrid.add(new Label("CPU max"), 2, kr); kpGrid.add(kpCpuMaxField, 3, kr++);
        kpGrid.add(new Label("Mem min"), 0, kr); kpGrid.add(kpMemMinField, 1, kr);
        kpGrid.add(new Label("Mem max"), 2, kr); kpGrid.add(kpMemMaxField, 3, kr++);
        kpGrid.add(kpConsolidationBox, 0, kr, 2, 1);
        kpGrid.add(new Label("Expire after"), 2, kr); kpGrid.add(kpExpireAfterField, 3, kr++);
        kpGrid.add(new Label("Limit CPU"), 0, kr); kpGrid.add(kpLimitCpuField, 1, kr);
        kpGrid.add(new Label("Limit memory"), 2, kr); kpGrid.add(kpLimitMemField, 3, kr++);
        karpenterForm.getChildren().setAll(kpGrid);
        karpenterForm.setVisible(false);
        karpenterForm.setManaged(false);

        // Managed-pool sub-form (UI-CLOUD-* — milestone-v2.8.4 §2.2).
        GridPane mpGrid = new GridPane();
        mpGrid.setHgap(8); mpGrid.setVgap(4);
        mpGrid.setPadding(new Insets(4, 0, 0, 24));
        int mr = 0;
        mpGrid.add(new Label("Cloud credential"), 0, mr);
        mpGrid.add(mpCredentialBox, 1, mr, 3, 1); mr++;
        mpGrid.add(new Label("Region"), 0, mr); mpGrid.add(mpRegionField, 1, mr);
        mpGrid.add(new Label("Capacity"), 2, mr); mpGrid.add(mpCapacityBox, 3, mr++);
        mpGrid.add(new Label("Instance type"), 0, mr); mpGrid.add(mpInstanceTypeField, 1, mr);
        mpGrid.add(new Label("Architecture"), 2, mr); mpGrid.add(mpArchBox, 3, mr++);
        mpGrid.add(new Label("Min nodes"), 0, mr); mpGrid.add(mpMinNodesField, 1, mr);
        mpGrid.add(new Label("Desired"), 2, mr); mpGrid.add(mpDesiredNodesField, 3, mr++);
        mpGrid.add(new Label("Max nodes"), 0, mr); mpGrid.add(mpMaxNodesField, 1, mr++);
        mpGrid.add(new Label("Zones (CSV)"), 0, mr); mpGrid.add(mpZonesField, 1, mr, 3, 1); mr++;
        mpGrid.add(new Label("Subnet IDs (CSV)"), 0, mr); mpGrid.add(mpSubnetsField, 1, mr, 3, 1); mr++;

        // Per-cloud extras — only the row matching the picked
        // credential's provider is visible at any time.
        mpAksOsTypeBox.getItems().setAll("Linux", "Windows");
        mpAksOsTypeBox.getSelectionModel().select("Linux");
        populatePerCloudRow(mpAksRow, "AKS",
                labelled("Resource group", mpAksResourceGroupField),
                labelled("Cluster", mpAksClusterField),
                labelled("OS type", mpAksOsTypeBox));
        populatePerCloudRow(mpEksRow, "EKS",
                labelled("Cluster", mpEksClusterField),
                labelled("Node-role ARN", mpEksNodeRoleArnField));
        populatePerCloudRow(mpGkeRow, "GKE",
                labelled("Cluster", mpGkeClusterField),
                labelled("Project", mpGkeProjectField));
        mpAksRow.setVisible(false); mpAksRow.setManaged(false);
        mpEksRow.setVisible(false); mpEksRow.setManaged(false);
        mpGkeRow.setVisible(false); mpGkeRow.setManaged(false);

        managedPoolForm.getChildren().setAll(mpGrid, mpAksRow, mpEksRow, mpGkeRow);
        managedPoolForm.setVisible(false);
        managedPoolForm.setManaged(false);

        VBox strategyBlock = new VBox(4, strategyHead, strategyHint,
                strategyNoneRadio, strategyNodePoolRadio, nodePoolForm,
                strategyKarpenterRadio, karpenterForm,
                strategyManagedRadio, managedPoolForm);
        strategyBlock.setPadding(new Insets(0, 0, 10, 0));

        Label pfHead = new Label("Pre-flight");
        pfHead.setStyle("-fx-font-weight: 600; -fx-font-size: 11px;");
        Label logHead = new Label("Rollout log");
        logHead.setStyle("-fx-font-weight: 600; -fx-font-size: 11px;");

        HBox actions = new HBox(8, preflightBtn, applyBtn, spinner, statusLabel);
        actions.setPadding(new Insets(8, 0, 0, 0));

        VBox v = new VBox(6, form, strategyBlock, pfHead, preflightArea, logHead, logArea, actions);
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
                .withMongoVersion(mongoVersionField.getText().trim())
                .withComputeStrategy(buildComputeStrategy());
        // Run through the enforcer so Prod locks are applied before
        // pre-flight sees the model.
        return new ProfileEnforcer().switchProfile(base, profileBox.getValue()).model();
    }

    private ComputeStrategy buildComputeStrategy() {
        Object picked = strategyGroup.getSelectedToggle() == null
                ? StrategyId.NONE
                : strategyGroup.getSelectedToggle().getUserData();
        if (picked == StrategyId.KARPENTER) {
            return new ComputeStrategy.Karpenter(buildKarpenterSpec());
        }
        if (picked == StrategyId.MANAGED_POOL) {
            return new ComputeStrategy.ManagedPool(buildManagedPoolSpec());
        }
        if (picked != StrategyId.NODE_POOL) return ComputeStrategy.NONE;
        String labelKey = poolLabelKeyField.getText().trim();
        String labelValue = poolLabelValueField.getText().trim();
        if (labelKey.isEmpty() || labelValue.isEmpty()) return ComputeStrategy.NONE;
        List<LabelPair> selector = List.of(new LabelPair(labelKey, labelValue));
        List<Toleration> tolerations = new ArrayList<>();
        String tKey = poolTaintKeyField.getText().trim();
        if (!tKey.isEmpty()) {
            String tVal = poolTaintValueField.getText().trim();
            Toleration.Effect effect = poolTaintEffectBox.getValue() == null
                    ? Toleration.Effect.NO_SCHEDULE : poolTaintEffectBox.getValue();
            tolerations.add(new Toleration(tKey, tVal.isEmpty() ? null : tVal, effect));
        }
        return new ComputeStrategy.NodePool(selector, tolerations, SpreadScope.WITHIN_POOL);
    }

    private com.kubrik.mex.k8s.compute.karpenter.KarpenterSpec buildKarpenterSpec() {
        String apiVersion = switch (kpNodeClassKindBox.getValue()) {
            case "AKSNodeClass" -> "karpenter.azure.com/v1";
            case "GCENodeClass" -> "karpenter.gce.gke.io/v1";
            default -> "karpenter.k8s.aws/v1";
        };
        com.kubrik.mex.k8s.compute.karpenter.KarpenterSpec.NodeClassRef ref =
                new com.kubrik.mex.k8s.compute.karpenter.KarpenterSpec.NodeClassRef(
                        apiVersion, kpNodeClassKindBox.getValue(),
                        kpNodeClassNameField.getText().trim());
        return new com.kubrik.mex.k8s.compute.karpenter.KarpenterSpec(
                ref,
                splitCsv(kpCapacityTypeBox.getValue()),
                splitCsv(kpInstanceFamiliesField.getText()),
                splitCsv(kpArchBox.getValue()),
                blankToNull(kpCpuMinField.getText()),
                blankToNull(kpCpuMaxField.getText()),
                blankToNull(kpMemMinField.getText()),
                blankToNull(kpMemMaxField.getText()),
                kpConsolidationBox.isSelected(),
                blankToNull(kpExpireAfterField.getText()),
                blankToNull(kpLimitCpuField.getText()),
                blankToNull(kpLimitMemField.getText()));
    }

    private com.kubrik.mex.k8s.compute.managedpool.ManagedPoolSpec buildManagedPoolSpec() {
        com.kubrik.mex.k8s.compute.managedpool.CloudCredential cred =
                mpCredentialBox.getValue();
        com.kubrik.mex.k8s.compute.managedpool.CloudProvider provider =
                cred == null ? com.kubrik.mex.k8s.compute.managedpool.CloudProvider.AWS
                        : cred.provider();
        long credId = cred == null ? 0L : cred.id();
        int min = parseInt(mpMinNodesField.getText(), 1);
        int desired = parseInt(mpDesiredNodesField.getText(), Math.max(1, min));
        int max = parseInt(mpMaxNodesField.getText(), Math.max(desired, min));
        // Re-shape to satisfy the record's invariants if the user typed
        // an out-of-order pair; the preflight surfaces the real shape
        // mismatch with a hint, not a stack trace.
        if (desired < min) desired = min;
        if (max < desired) max = desired;
        String poolName = "mex-" + deploymentNameField.getText().trim()
                .toLowerCase().replaceAll("[^a-z0-9-]", "-").replaceAll("-+", "-");
        java.util.Map<String, String> extras = collectCloudExtras(provider);
        try {
            return new com.kubrik.mex.k8s.compute.managedpool.ManagedPoolSpec(
                    provider, credId,
                    blankToDefault(mpRegionField.getText(), "us-east-1"),
                    poolName,
                    blankToDefault(mpInstanceTypeField.getText(), "m6i.large"),
                    mpCapacityBox.getValue() == null
                            ? com.kubrik.mex.k8s.compute.managedpool.ManagedPoolSpec.CapacityType.ON_DEMAND
                            : mpCapacityBox.getValue(),
                    min, desired, max,
                    mpArchBox.getValue() == null ? "amd64" : mpArchBox.getValue(),
                    splitCsv(mpZonesField.getText()),
                    splitCsv(mpSubnetsField.getText()),
                    extras);
        } catch (IllegalArgumentException iae) {
            return com.kubrik.mex.k8s.compute.managedpool.ManagedPoolSpec
                    .sensibleEksDefaults(credId,
                            blankToDefault(mpRegionField.getText(), "us-east-1"),
                            deploymentNameField.getText().trim());
        }
    }

    /** Pull the per-cloud extras row's values into the map shape
     *  ManagedPoolSpec.cloudExtras expects. Empty / blank fields are
     *  omitted so the spec stays clean. */
    private java.util.Map<String, String> collectCloudExtras(
            com.kubrik.mex.k8s.compute.managedpool.CloudProvider provider) {
        java.util.Map<String, String> out = new java.util.LinkedHashMap<>();
        if (provider == null) return out;
        switch (provider) {
            case AZURE -> {
                putIfPresent(out, com.kubrik.mex.k8s.compute.managedpool.ManagedPoolSpec
                        .EXTRA_AKS_RESOURCE_GROUP, mpAksResourceGroupField.getText());
                putIfPresent(out, com.kubrik.mex.k8s.compute.managedpool.ManagedPoolSpec
                        .EXTRA_AKS_CLUSTER, mpAksClusterField.getText());
                putIfPresent(out, com.kubrik.mex.k8s.compute.managedpool.ManagedPoolSpec
                        .EXTRA_AKS_OS_TYPE,
                        mpAksOsTypeBox.getValue() == null ? null
                                : mpAksOsTypeBox.getValue().toLowerCase());
            }
            case AWS -> {
                putIfPresent(out, com.kubrik.mex.k8s.compute.managedpool.ManagedPoolSpec
                        .EXTRA_EKS_CLUSTER, mpEksClusterField.getText());
                putIfPresent(out, com.kubrik.mex.k8s.compute.managedpool.ManagedPoolSpec
                        .EXTRA_EKS_NODE_ROLE_ARN, mpEksNodeRoleArnField.getText());
            }
            case GCP -> {
                putIfPresent(out, com.kubrik.mex.k8s.compute.managedpool.ManagedPoolSpec
                        .EXTRA_GKE_CLUSTER, mpGkeClusterField.getText());
                putIfPresent(out, com.kubrik.mex.k8s.compute.managedpool.ManagedPoolSpec
                        .EXTRA_GKE_PROJECT, mpGkeProjectField.getText());
            }
        }
        return out;
    }

    private static void putIfPresent(java.util.Map<String, String> m,
            String key, String value) {
        if (value != null && !value.isBlank()) m.put(key, value.trim());
    }

    private static int parseInt(String raw, int fallback) {
        if (raw == null || raw.isBlank()) return fallback;
        try { return Integer.parseInt(raw.trim()); }
        catch (NumberFormatException nfe) { return fallback; }
    }

    private static String blankToDefault(String raw, String fallback) {
        return raw == null || raw.isBlank() ? fallback : raw.trim();
    }

    private static List<String> splitCsv(String raw) {
        if (raw == null || raw.isBlank()) return List.of();
        List<String> out = new ArrayList<>();
        for (String t : raw.split(",")) {
            String s = t.trim();
            if (!s.isEmpty()) out.add(s);
        }
        return List.copyOf(out);
    }

    private static String blankToNull(String raw) {
        return raw == null || raw.isBlank() ? null : raw.trim();
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

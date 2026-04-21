package com.kubrik.mex.ui.migration;

import com.kubrik.mex.core.ConnectionManager;
import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.migration.spec.MigrationKind;
import com.kubrik.mex.model.ConnectionState;
import com.kubrik.mex.model.MongoConnection;
import com.kubrik.mex.store.ConnectionStore;
import javafx.application.Platform;
import javafx.beans.binding.Bindings;
import javafx.beans.binding.BooleanBinding;
import javafx.collections.FXCollections;
import javafx.geometry.Insets;
import javafx.scene.control.Button;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Label;
import javafx.scene.control.RadioButton;
import javafx.scene.control.TextField;
import javafx.scene.control.ToggleGroup;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Region;

/** Step 2: target connection, target database, and DATA_TRANSFER vs VERSIONED mode. */
public final class WizardStepTarget implements WizardStep {

    private final WizardModel model;
    private final ConnectionManager manager;
    private final GridPane root = new GridPane();
    private final ComboBox<MongoConnection> connectionBox = new ComboBox<>();
    private final TextField databaseField = new TextField();
    private final RadioButton dataBtn = new RadioButton("Data transfer");
    private final RadioButton versionedBtn = new RadioButton("Versioned migrations");
    private final Button connectBtn = new Button("Connect");
    private final Label connectionStatus = new Label();

    public WizardStepTarget(WizardModel model,
                            ConnectionStore store,
                            ConnectionManager manager,
                            EventBus bus) {
        this.model = model;
        this.manager = manager;

        connectionBox.setItems(FXCollections.observableArrayList(store.list()));
        // UX-9: status-dot cells so the user sees the target's live connection state.
        connectionBox.setCellFactory(lv -> new ConnectionStatusCell(manager, bus));
        connectionBox.setButtonCell(new ConnectionStatusCell(manager, bus));
        connectionBox.valueProperty().addListener((o, a, b) -> {
            model.targetConnectionId.set(b == null ? null : b.id());
            syncStateFromManager();
        });

        databaseField.textProperty().bindBidirectional(model.targetDatabase);

        ToggleGroup tg = new ToggleGroup();
        dataBtn.setToggleGroup(tg);
        versionedBtn.setToggleGroup(tg);
        dataBtn.setSelected(model.kind.get() == MigrationKind.DATA_TRANSFER);
        versionedBtn.setSelected(model.kind.get() == MigrationKind.VERSIONED);
        tg.selectedToggleProperty().addListener((o, a, b) ->
                model.kind.set(b == versionedBtn ? MigrationKind.VERSIONED : MigrationKind.DATA_TRANSFER));

        connectBtn.setOnAction(e -> {
            String id = model.targetConnectionId.get();
            if (id != null) manager.connect(id);
        });
        connectBtn.disableProperty().bind(Bindings.createBooleanBinding(() -> {
            String id = model.targetConnectionId.get();
            return id == null || id.isBlank() || model.targetConnected.get();
        }, model.targetConnectionId, model.targetConnected));

        bus.onState(s -> {
            if (s.connectionId() == null) return;
            if (!s.connectionId().equals(model.targetConnectionId.get())) return;
            Platform.runLater(this::syncStateFromManager);
        });

        HBox modeRow = new HBox(16, dataBtn, versionedBtn);
        HBox connRow = new HBox(8, connectionBox, connectBtn);
        HBox.setHgrow(connectionBox, javafx.scene.layout.Priority.ALWAYS);

        connectionStatus.setStyle("-fx-font-size: 11px;");

        root.setHgap(12);
        root.setVgap(10);
        root.setPadding(new Insets(16));
        root.addRow(0, new Label("Mode:"), modeRow);
        root.addRow(1, new Label("Target connection:"), connRow);
        root.addRow(2, new Label(""), connectionStatus);
        root.addRow(3, new Label("Target database:"), databaseField);
        root.addRow(4, new Label(), hint());
        connectionBox.setMaxWidth(Double.MAX_VALUE);
        GridPane.setHgrow(databaseField, javafx.scene.layout.Priority.ALWAYS);
    }

    private void syncStateFromManager() {
        String id = model.targetConnectionId.get();
        if (id == null || id.isBlank()) {
            model.targetConnected.set(false);
            connectionStatus.setText("");
            return;
        }
        ConnectionState s = manager.state(id);
        boolean connected = s.status() == ConnectionState.Status.CONNECTED;
        model.targetConnected.set(connected);
        connectionStatus.setText(switch (s.status()) {
            case CONNECTED -> "● connected (mongo " + s.serverVersion() + ")";
            case CONNECTING -> "● connecting…";
            case ERROR -> "● error: " + (s.lastError() == null ? "unknown" : s.lastError());
            case DISCONNECTED -> "● not connected — click Connect before advancing.";
        });
        connectionStatus.setStyle(switch (s.status()) {
            case CONNECTED -> "-fx-text-fill: #16a34a; -fx-font-size: 11px;";
            case CONNECTING -> "-fx-text-fill: #b45309; -fx-font-size: 11px;";
            case ERROR -> "-fx-text-fill: #dc2626; -fx-font-size: 11px;";
            case DISCONNECTED -> "-fx-text-fill: #6b7280; -fx-font-size: 11px;";
        });
    }

    private Label hint() {
        Label l = new Label("Database is required for versioned migrations; optional for data transfer.");
        l.setStyle("-fx-text-fill: #6b7280; -fx-font-style: italic;");
        return l;
    }

    @Override public String title() { return "2. Target"; }
    @Override public Region view() { return root; }
    @Override public void onEnter() { syncStateFromManager(); }

    @Override public BooleanBinding validProperty() {
        return Bindings.createBooleanBinding(() -> {
            if (model.targetConnectionId.get() == null || model.targetConnectionId.get().isBlank()) return false;
            if (!model.targetConnected.get()) return false;
            if (model.kind.get() == MigrationKind.VERSIONED) {
                return model.targetDatabase.get() != null && !model.targetDatabase.get().isBlank();
            }
            return true;
        }, model.targetConnectionId, model.targetDatabase, model.kind, model.targetConnected);
    }
}

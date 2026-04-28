package com.kubrik.mex.ui.migration;

import com.kubrik.mex.core.ConnectionManager;
import com.kubrik.mex.events.EventBus;
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
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Region;

import java.util.List;

/** Step 1: pick source connection + read preference (SRC-1, SRC-7).
 *  Target collection/database selection lives on Step 3 (scope). */
public final class WizardStepSource implements WizardStep {

    private final WizardModel model;
    private final ConnectionManager manager;
    private final GridPane root = new GridPane();
    private final ComboBox<MongoConnection> connectionBox = new ComboBox<>();
    private final ComboBox<String> readPrefBox = new ComboBox<>();
    private final Button connectBtn = new Button("Connect");
    private final Label connectionStatus = new Label();

    public WizardStepSource(WizardModel model,
                            ConnectionStore store,
                            ConnectionManager manager,
                            EventBus bus) {
        this.model = model;
        this.manager = manager;

        List<MongoConnection> all = store.list();
        connectionBox.setItems(FXCollections.observableArrayList(all));
        // UX-9: status-dot cell + button-cell for the selected value.
        connectionBox.setCellFactory(lv -> new ConnectionStatusCell(manager, bus));
        connectionBox.setButtonCell(new ConnectionStatusCell(manager, bus));

        readPrefBox.setItems(FXCollections.observableArrayList(
                "primary", "primaryPreferred", "secondary", "secondaryPreferred", "nearest"));
        readPrefBox.getSelectionModel().select("primary");

        // Bind user choices back into the model.
        connectionBox.valueProperty().addListener((o, a, b) -> {
            String id = b == null ? null : b.id();
            model.sourceConnectionId.set(id);
            syncStateFromManager();
        });
        readPrefBox.valueProperty().addListener((o, a, b) -> model.readPreference.set(b));

        connectBtn.setOnAction(e -> {
            String id = model.sourceConnectionId.get();
            if (id != null) manager.connect(id);
        });
        connectBtn.disableProperty().bind(Bindings.createBooleanBinding(() -> {
            String id = model.sourceConnectionId.get();
            return id == null || id.isBlank() || model.sourceConnected.get();
        }, model.sourceConnectionId, model.sourceConnected));

        // Live-update the connected property as state changes flow through the bus.
        bus.onState(s -> {
            if (s.connectionId() == null) return;
            if (!s.connectionId().equals(model.sourceConnectionId.get())) return;
            Platform.runLater(this::syncStateFromManager);
        });

        HBox connRow = new HBox(8, connectionBox, connectBtn);
        HBox.setHgrow(connectionBox, javafx.scene.layout.Priority.ALWAYS);

        connectionStatus.setStyle("-fx-font-size: 11px;");

        root.setHgap(12);
        root.setVgap(10);
        root.setPadding(new Insets(16));
        root.addRow(0, new Label("Connection:"), connRow);
        root.addRow(1, new Label(""), connectionStatus);
        root.addRow(2, new Label("Read preference:"), readPrefBox);
        root.addRow(3, new Label(), hint());
        connectionBox.setMaxWidth(Double.MAX_VALUE);
        readPrefBox.setMaxWidth(Double.MAX_VALUE);
    }

    private void syncStateFromManager() {
        String id = model.sourceConnectionId.get();
        if (id == null || id.isBlank()) {
            model.sourceConnected.set(false);
            connectionStatus.setText("");
            return;
        }
        ConnectionState s = manager.state(id);
        boolean connected = s.status() == ConnectionState.Status.CONNECTED;
        model.sourceConnected.set(connected);
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
        Label l = new Label("Collections are chosen on the Scope step.");
        l.setStyle("-fx-text-fill: #6b7280; -fx-font-style: italic;");
        return l;
    }

    @Override public String title() { return "1. Source"; }
    @Override public Region view() { return root; }
    @Override public void onEnter() { syncStateFromManager(); }
    @Override public BooleanBinding validProperty() {
        return Bindings.createBooleanBinding(
                () -> model.sourceConnectionId.get() != null
                        && !model.sourceConnectionId.get().isBlank()
                        && model.sourceConnected.get(),
                model.sourceConnectionId, model.sourceConnected);
    }
}

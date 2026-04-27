package com.kubrik.mex.k8s.compute.managedpool.ui;

import com.kubrik.mex.k8s.compute.managedpool.CloudCredential;
import com.kubrik.mex.k8s.compute.managedpool.CloudCredentialDao;
import com.kubrik.mex.k8s.compute.managedpool.CloudProvider;
import com.kubrik.mex.k8s.compute.managedpool.SecretStore;
import javafx.application.Platform;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Dialog;
import javafx.scene.control.Label;
import javafx.scene.control.PasswordField;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextField;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;

import java.sql.SQLException;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * v2.8.4 Q2.8.4-A — Cloud credentials management pane.
 *
 * <p>Lists rows from {@code cloud_credentials}, lets users add /
 * delete entries, and routes the secret bodies through the injected
 * {@link SecretStore} (production: {@link
 * com.kubrik.mex.k8s.compute.managedpool.OsKeychainSecretStore}).
 * Mongo Explorer's SQLite never sees the secret material — only
 * the keychain ref.</p>
 */
public final class CloudCredentialsPane extends BorderPane {

    private final CloudCredentialDao dao;
    private final SecretStore secretStore;
    private final ObservableList<CloudCredential> rows = FXCollections.observableArrayList();
    private final TableView<CloudCredential> table = new TableView<>(rows);
    private final Label statusLabel = new Label(
            "Cloud credentials are stored in the OS keychain. Mongo Explorer "
          + "saves only a keychain reference here.");

    public CloudCredentialsPane(CloudCredentialDao dao, SecretStore secretStore) {
        this.dao = Objects.requireNonNull(dao, "dao");
        this.secretStore = Objects.requireNonNull(secretStore, "secretStore");

        setStyle("-fx-background-color: -color-bg-default;");
        setPadding(new Insets(14, 16, 14, 16));

        Label title = new Label("Cloud credentials");
        title.setStyle("-fx-font-size: 14px; -fx-font-weight: 700;");
        Label hint = new Label(
                "Pointers used by v2.8.4 managed-pool provisioning. The "
              + "actual secret material lives in the OS keychain via the "
              + "platform CLI (security / cmdkey / secret-tool).");
        hint.setStyle("-fx-text-fill: -color-fg-muted; -fx-font-size: 11px;");
        hint.setWrapText(true);

        table.setPlaceholder(new Label("No cloud credentials yet."));
        table.getColumns().setAll(
                col("Name", 180, CloudCredential::displayName),
                col("Provider", 90, c -> c.provider().wireValue()),
                col("Auth mode", 140, c -> c.authMode().name()),
                col("Account", 140, c -> c.awsAccountId().or(c::gcpProject)
                        .or(c::azureSubscription).orElse("")),
                col("Region", 100, c -> c.defaultRegion().orElse("")),
                col("Probed", 120, c -> c.probeStatus().map(Enum::name).orElse("(never)")),
                col("Created", 140, c -> Instant.ofEpochMilli(c.createdAt()).toString()));

        Button addBtn = new Button("Add credential…");
        addBtn.setOnAction(e -> showAddDialog());
        addBtn.getStyleClass().add("accent");
        Button deleteBtn = new Button("Delete");
        deleteBtn.setOnAction(e -> deleteSelected());
        deleteBtn.getStyleClass().add("danger");
        Button reloadBtn = new Button("Reload");
        reloadBtn.setOnAction(e -> reload());

        HBox actions = new HBox(8, addBtn, deleteBtn, reloadBtn);
        actions.setPadding(new Insets(8, 0, 0, 0));

        VBox header = new VBox(4, title, hint);
        header.setPadding(new Insets(0, 0, 8, 0));

        statusLabel.setStyle("-fx-text-fill: -color-fg-muted; -fx-font-size: 11px;");
        statusLabel.setWrapText(true);

        VBox center = new VBox(6, header, table, actions, statusLabel);
        VBox.setVgrow(table, Priority.ALWAYS);
        setCenter(center);
        reload();
    }

    private void reload() {
        try {
            rows.setAll(dao.listAll());
            statusLabel.setText("Showing " + rows.size() + " credential(s).");
        } catch (SQLException sqle) {
            statusLabel.setText("Reload failed: " + sqle.getMessage());
        }
    }

    private void showAddDialog() {
        Dialog<CloudCredential> dlg = new Dialog<>();
        dlg.setTitle("Add cloud credential");
        if (getScene() != null && getScene().getWindow() != null) {
            dlg.initOwner(getScene().getWindow());
        }

        TextField nameField = new TextField();
        nameField.setPromptText("display name");
        ComboBox<CloudProvider> providerBox = new ComboBox<>();
        providerBox.getItems().setAll(CloudProvider.values());
        providerBox.getSelectionModel().select(CloudProvider.AWS);
        ComboBox<CloudCredential.AuthMode> authModeBox = new ComboBox<>();
        authModeBox.getItems().setAll(CloudCredential.AuthMode.values());
        authModeBox.getSelectionModel().select(CloudCredential.AuthMode.IRSA);
        TextField accountField = new TextField();
        accountField.setPromptText("aws account id / gcp project / azure subscription");
        TextField regionField = new TextField();
        regionField.setPromptText("us-east-1");
        PasswordField secretField = new PasswordField();
        secretField.setPromptText("secret material (skip for IRSA / Workload Identity / Managed Identity)");

        GridPane g = new GridPane();
        g.setHgap(8); g.setVgap(6);
        g.setPadding(new Insets(8));
        int r = 0;
        g.addRow(r++, new Label("Name"), nameField);
        g.addRow(r++, new Label("Provider"), providerBox);
        g.addRow(r++, new Label("Auth mode"), authModeBox);
        g.addRow(r++, new Label("Account"), accountField);
        g.addRow(r++, new Label("Region"), regionField);
        g.addRow(r++, new Label("Secret"), secretField);
        dlg.getDialogPane().setContent(g);
        dlg.getDialogPane().getButtonTypes().addAll(
                javafx.scene.control.ButtonType.OK,
                javafx.scene.control.ButtonType.CANCEL);

        dlg.setResultConverter(bt -> {
            if (bt != javafx.scene.control.ButtonType.OK) return null;
            String name = nameField.getText().trim();
            if (name.isBlank()) return null;
            CloudProvider provider = providerBox.getValue();
            CloudCredential.AuthMode authMode = authModeBox.getValue();
            String ref = SecretStore.newRef(provider, name);
            String secret = secretField.getText();
            if (!secret.isBlank()) secretStore.store(ref, secret);
            try {
                long id = dao.insert(name, provider, authMode, ref,
                        provider == CloudProvider.AWS && !accountField.getText().isBlank()
                                ? Optional.of(accountField.getText().trim()) : Optional.empty(),
                        provider == CloudProvider.GCP && !accountField.getText().isBlank()
                                ? Optional.of(accountField.getText().trim()) : Optional.empty(),
                        provider == CloudProvider.AZURE && !accountField.getText().isBlank()
                                ? Optional.of(accountField.getText().trim()) : Optional.empty(),
                        regionField.getText().isBlank() ? Optional.empty()
                                : Optional.of(regionField.getText().trim()));
                return dao.findById(id).orElse(null);
            } catch (SQLException sqle) {
                Platform.runLater(() -> statusLabel.setText(
                        "Insert failed: " + sqle.getMessage()));
                return null;
            }
        });

        dlg.showAndWait().ifPresent(c -> { rows.add(c); statusLabel.setText(
                "Added '" + c.displayName() + "' (" + c.provider() + ")."); });
    }

    private void deleteSelected() {
        CloudCredential sel = table.getSelectionModel().getSelectedItem();
        if (sel == null) { statusLabel.setText("Pick a credential first."); return; }
        Alert confirm = new Alert(Alert.AlertType.CONFIRMATION,
                "Delete cloud credential '" + sel.displayName() + "'? "
                + "The keychain entry will also be removed.",
                javafx.scene.control.ButtonType.OK,
                javafx.scene.control.ButtonType.CANCEL);
        if (getScene() != null && getScene().getWindow() != null) {
            confirm.initOwner(getScene().getWindow());
        }
        if (confirm.showAndWait().orElse(javafx.scene.control.ButtonType.CANCEL)
                != javafx.scene.control.ButtonType.OK) return;

        try {
            secretStore.delete(sel.keychainRef());
            dao.delete(sel.id());
            rows.remove(sel);
            statusLabel.setText("Deleted '" + sel.displayName() + "'.");
        } catch (Exception ex) {
            statusLabel.setText("Delete failed: " + ex.getMessage());
        }
    }

    private static <T> TableColumn<T, String> col(String title, int width,
                                                    Function<T, String> getter) {
        TableColumn<T, String> c = new TableColumn<>(title);
        c.setPrefWidth(width);
        c.setCellValueFactory(cd -> new SimpleStringProperty(getter.apply(cd.getValue())));
        return c;
    }
}

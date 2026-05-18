package com.kubrik.mex.ui;

import com.kubrik.mex.core.ConnectionManager;
import com.kubrik.mex.core.ConnectionUriBuilder;
import com.kubrik.mex.core.Crypto;
import com.kubrik.mex.model.MongoConnection;
import javafx.application.Platform;
import javafx.collections.FXCollections;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.control.Button;
import javafx.scene.control.ButtonType;
import javafx.scene.control.CheckBox;
import javafx.scene.control.ChoiceBox;
import javafx.scene.control.Dialog;
import javafx.scene.control.Label;
import javafx.scene.control.PasswordField;
import javafx.scene.control.RadioButton;
import javafx.scene.control.Separator;
import javafx.scene.control.TextField;
import javafx.scene.control.TextArea;
import javafx.scene.control.ToggleGroup;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import org.kordamp.ikonli.javafx.FontIcon;

import java.util.Optional;

/**
 * Studio-3T-style connection dialog with two modes:
 *   1. From URI — paste a connection string
 *   2. Manual   — Server + Authentication tabs (inline, no rail)
 */
public class ConnectionEditDialog extends Dialog<MongoConnection> {

    private static final String PLACEHOLDER_PWD = "\u0001\u0001UNCHANGED\u0001\u0001";

    private final Crypto crypto;
    private final MongoConnection existing;

    // ---- top
    private final TextField nameField = new TextField();

    // ---- mode toggle
    private final RadioButton modeUri = new RadioButton("From URI");
    private final RadioButton modeManual = new RadioButton("Manual Connection");

    // ---- URI mode
    private final TextArea uriArea = new TextArea();

    // ---- Manual: Server
    private final ChoiceBox<String> connectionType =
            new ChoiceBox<>(FXCollections.observableArrayList("Standalone", "Replica Set", "Sharded Cluster", "DNS Seedlist (SRV)"));
    private final TextField hostField = new TextField();
    private final TextField portField = new TextField();

    // ---- Manual: Authentication
    private final ChoiceBox<String> authMode = new ChoiceBox<>(FXCollections.observableArrayList("None", "Basic (SCRAM)"));
    private final TextField usernameField = new TextField();
    private final PasswordField passwordField = new PasswordField();
    private final TextField authDbField = new TextField();

    // ---- footer
    private final Label testResult = new Label("Not tested yet.");
    private final Label uriPreview = new Label();

    // ---- auth section container for show/hide
    private final VBox authFields = new VBox(8);

    public ConnectionEditDialog(ConnectionManager manager, Crypto crypto, Optional<MongoConnection> existingOpt) {
        this.crypto = crypto;
        this.existing = existingOpt.orElse(MongoConnection.blank());
        setTitle(existingOpt.isPresent() ? "Edit Connection" : "New Connection");
        setResizable(true);

        /* ============ HEADER ============ */
        FontIcon hicon = new FontIcon("fth-database");
        hicon.setIconSize(32);
        hicon.setIconColor(javafx.scene.paint.Color.web("#2563eb"));

        nameField.setPromptText("Connection name");
        nameField.setStyle("-fx-font-size: 15px; -fx-font-weight: bold;");
        nameField.setMaxWidth(Double.MAX_VALUE);

        Label subtitle = new Label("Configure your MongoDB connection.");
        subtitle.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 12px;");

        VBox headerTexts = new VBox(4, nameField, subtitle);
        HBox.setHgrow(headerTexts, Priority.ALWAYS);
        HBox header = new HBox(14, hicon, headerTexts);
        header.setAlignment(Pos.CENTER_LEFT);
        header.setPadding(new Insets(20, 24, 16, 24));
        header.setStyle("-fx-background-color: #f9fafb; -fx-border-color: #e5e7eb; -fx-border-width: 0 0 1 0;");

        /* ============ MODE TOGGLE ============ */
        ToggleGroup modeGroup = new ToggleGroup();
        modeUri.setToggleGroup(modeGroup);
        modeManual.setToggleGroup(modeGroup);
        modeManual.setSelected(true);

        modeUri.setStyle("-fx-font-size: 13px;");
        modeManual.setStyle("-fx-font-size: 13px;");
        HBox modeRow = new HBox(24, modeManual, modeUri);
        modeRow.setPadding(new Insets(14, 24, 10, 24));
        modeRow.setAlignment(Pos.CENTER_LEFT);

        /* ============ URI PANEL ============ */
        uriArea.setPromptText("mongodb://user:pass@host:27017/admin?authSource=admin");
        uriArea.setPrefRowCount(4);
        uriArea.setStyle("-fx-font-family: 'Menlo','Monaco',monospace; -fx-font-size: 12px;");
        Label uriHint = new Label("Paste a full MongoDB connection URI. Supports mongodb:// and mongodb+srv:// schemes.");
        uriHint.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px; -fx-font-style: italic;");
        uriHint.setWrapText(true);
        VBox uriPanel = new VBox(8, uriArea, uriHint);
        uriPanel.setPadding(new Insets(0, 24, 0, 24));

        /* ============ MANUAL PANEL ============ */

        // -- Server section --
        Label serverTitle = new Label("Server");
        serverTitle.setStyle("-fx-font-size: 15px; -fx-font-weight: bold;");
        Label serverDesc = new Label("Where the MongoDB deployment lives.");
        serverDesc.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px;");

        connectionType.setValue("Standalone");
        hostField.setPromptText("localhost");
        hostField.setPrefColumnCount(28);
        portField.setPromptText("27017");
        portField.setPrefColumnCount(8);
        portField.setMinWidth(80);

        GridPane serverGrid = grid();
        int r = 0;
        serverGrid.addRow(r++, formLabel("Connection type"), connectionType);
        HBox hostPort = new HBox(8, hostField, new Label(":"), portField);
        hostPort.setAlignment(Pos.CENTER_LEFT);
        serverGrid.addRow(r++, formLabel("Server"), hostPort);

        VBox serverSection = new VBox(6, serverTitle, serverDesc, new Separator(), serverGrid);

        // -- Authentication section --
        Label authTitle = new Label("Authentication");
        authTitle.setStyle("-fx-font-size: 15px; -fx-font-weight: bold;");
        Label authDesc = new Label("How to identify to the database.");
        authDesc.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px;");

        authMode.setValue("None");
        usernameField.setPromptText("username");
        passwordField.setPromptText(existing.encPassword() != null && !existing.encPassword().isBlank()
                ? "(unchanged)" : "password");
        authDbField.setPromptText("admin");
        authDbField.setText("admin");

        Label lblUser = formLabel("Username");
        Label lblPass = formLabel("Password");
        Label lblAuthDb = formLabel("Auth DB");
        Node passRow = UiHelpers.passwordWithEye(passwordField);

        GridPane authGrid = grid();
        int a = 0;
        authGrid.addRow(a++, formLabel("Mode"), authMode);
        authGrid.addRow(a++, lblUser, usernameField);
        authGrid.addRow(a++, lblPass, passRow);
        authGrid.addRow(a++, lblAuthDb, authDbField);

        Node[] credNodes = { lblUser, usernameField, lblPass, passRow, lblAuthDb, authDbField };
        Runnable syncAuthVisibility = () -> {
            boolean show = "Basic (SCRAM)".equals(authMode.getValue());
            for (Node n : credNodes) { n.setVisible(show); n.setManaged(show); }
        };

        authMode.valueProperty().addListener((o, prev, next) -> syncAuthVisibility.run());
        syncAuthVisibility.run();

        VBox authSection = new VBox(6, authTitle, authDesc, new Separator(), authGrid);

        VBox manualPanel = new VBox(20, serverSection, authSection);
        manualPanel.setPadding(new Insets(0, 24, 0, 24));

        /* ============ BODY ============ */
        VBox bodyContent = new VBox();
        bodyContent.setPadding(new Insets(4, 0, 4, 0));

        javafx.scene.control.ScrollPane bodyScroll = new javafx.scene.control.ScrollPane(bodyContent);
        bodyScroll.setFitToWidth(true);
        bodyScroll.setHbarPolicy(javafx.scene.control.ScrollPane.ScrollBarPolicy.NEVER);
        bodyScroll.setVbarPolicy(javafx.scene.control.ScrollPane.ScrollBarPolicy.AS_NEEDED);
        bodyScroll.setStyle("-fx-background-color: white; -fx-border-color: transparent;");
        VBox.setVgrow(bodyScroll, Priority.ALWAYS);

        Runnable switchMode = () -> {
            boolean uri = modeUri.isSelected();
            bodyContent.getChildren().setAll(uri ? uriPanel : manualPanel);
        };
        modeGroup.selectedToggleProperty().addListener((o, prev, next) -> switchMode.run());
        switchMode.run();

        /* ============ PREVIEW ============ */
        uriPreview.setStyle("-fx-text-fill: #374151; -fx-font-family: 'Menlo','Monaco',monospace; "
                + "-fx-font-size: 11px; -fx-background-color: #f3f4f6; -fx-padding: 6; -fx-background-radius: 4;");
        uriPreview.setWrapText(true);
        uriPreview.setMaxWidth(Double.MAX_VALUE);
        VBox previewBox = new VBox(4, new Label("Connection URI preview"), uriPreview);
        previewBox.setPadding(new Insets(8, 24, 4, 24));
        ((Label) previewBox.getChildren().get(0)).setStyle("-fx-text-fill: #6b7280; -fx-font-size: 10px;");

        /* ============ FOOTER ============ */
        testResult.setStyle("-fx-text-fill: #6b7280;");
        FontIcon testIcon = new FontIcon("fth-zap");
        testIcon.setIconSize(14);
        Button testBtn = new Button("Test Connection");
        testBtn.setGraphic(testIcon);
        testBtn.setOnAction(e -> doTest(manager));

        Region fsp = new Region();
        HBox.setHgrow(fsp, Priority.ALWAYS);
        HBox footer = new HBox(12, testResult, fsp, testBtn);
        footer.setAlignment(Pos.CENTER_LEFT);
        footer.setPadding(new Insets(10, 24, 10, 24));
        footer.setStyle("-fx-background-color: #f9fafb; -fx-border-color: #e5e7eb; -fx-border-width: 1 0 0 0;");

        /* ============ ROOT ============ */
        VBox root = new VBox(header, modeRow, bodyScroll, previewBox, footer);
        root.setPrefSize(620, 560);

        getDialogPane().setContent(root);
        getDialogPane().setStyle("-fx-padding: 0;");
        getDialogPane().getButtonTypes().setAll(ButtonType.CANCEL, ButtonType.OK);

        Button okBtn = (Button) getDialogPane().lookupButton(ButtonType.OK);
        okBtn.setText("Save");
        okBtn.setStyle("-fx-background-color: #16a34a; -fx-text-fill: white; -fx-font-weight: bold;");
        ((Button) getDialogPane().lookupButton(ButtonType.CANCEL)).setText("Cancel");

        load(existing);
        setupLivePreview();
        setResultConverter(bt -> bt == ButtonType.OK ? collect() : null);
    }

    /* ---- helpers ---- */

    private static GridPane grid() {
        GridPane g = new GridPane();
        g.setHgap(12); g.setVgap(8);
        javafx.scene.layout.ColumnConstraints c1 = new javafx.scene.layout.ColumnConstraints();
        c1.setMinWidth(120); c1.setPrefWidth(120); c1.setHalignment(javafx.geometry.HPos.RIGHT);
        javafx.scene.layout.ColumnConstraints c2 = new javafx.scene.layout.ColumnConstraints();
        c2.setHgrow(Priority.ALWAYS); c2.setFillWidth(true);
        g.getColumnConstraints().addAll(c1, c2);
        return g;
    }

    private static Label formLabel(String text) {
        Label l = new Label(text);
        l.setStyle("-fx-text-fill: #374151; -fx-font-size: 12px;");
        return l;
    }

    private static HBox row(String labelText, Node field) {
        Label l = new Label(labelText);
        l.setStyle("-fx-text-fill: #374151; -fx-font-size: 12px;");
        l.setMinWidth(80);
        HBox h = new HBox(12, l, field);
        h.setAlignment(Pos.CENTER_LEFT);
        HBox.setHgrow(field, Priority.ALWAYS);
        return h;
    }

    /* ---- load / collect ---- */

    private void load(MongoConnection c) {
        nameField.setText(c.name() == null ? "" : c.name());

        // Mode
        if ("URI".equals(c.mode())) {
            modeUri.setSelected(true);
            uriArea.setText(c.uri() == null ? "" : c.uri());
        } else {
            modeManual.setSelected(true);
        }
        uriArea.setText(c.uri() == null ? "" : c.uri());

        // Server
        String ct = c.connectionType();
        connectionType.setValue(switch (ct == null ? "" : ct) {
            case "REPLICA_SET" -> "Replica Set";
            case "SHARDED" -> "Sharded Cluster";
            case "DNS_SRV" -> "DNS Seedlist (SRV)";
            default -> "Standalone";
        });

        String hosts = c.hosts() == null ? "localhost:27017" : c.hosts();
        if (hosts.contains(",")) {
            hostField.setText(hosts);
            portField.setText("");
        } else {
            String[] parts = hosts.split(":");
            hostField.setText(parts[0]);
            portField.setText(parts.length > 1 ? parts[1] : "27017");
        }

        // Auth
        boolean hasUser = c.username() != null && !c.username().isBlank();
        String am = c.authMode();
        if (hasUser || (am != null && !"NONE".equals(am))) {
            authMode.setValue("Basic (SCRAM)");
        } else {
            authMode.setValue("None");
        }
        usernameField.setText(c.username() == null ? "" : c.username());
        passwordField.setText(c.encPassword() != null && !c.encPassword().isBlank() ? PLACEHOLDER_PWD : "");
        authDbField.setText(c.authDb() == null || c.authDb().isBlank() ? "admin" : c.authDb());
    }

    private MongoConnection collect() {
        String encPassword = encOrKeep(passwordField.getText(), existing.encPassword());

        // Determine mode and key fields
        boolean uriMode = modeUri.isSelected();
        String modeVal = uriMode ? "URI" : "FORM";
        String uriVal = uriMode ? uriArea.getText().trim() : "";

        // Map connection type back to internal key
        String ctVal = switch (connectionType.getValue()) {
            case "Replica Set" -> "REPLICA_SET";
            case "Sharded Cluster" -> "SHARDED";
            case "DNS Seedlist (SRV)" -> "DNS_SRV";
            default -> "STANDALONE";
        };

        // Build hosts string
        String host = hostField.getText().isBlank() ? "localhost" : hostField.getText().trim();
        String port = portField.getText().isBlank() ? "27017" : portField.getText().trim();
        String hostsVal = host.contains(",") ? host : host + ":" + port;
        String srvHost = "DNS_SRV".equals(ctVal) ? host : "";

        // Auth
        boolean basic = "Basic (SCRAM)".equals(authMode.getValue());
        String authModeVal = basic ? "DEFAULT" : "NONE";
        String user = basic ? usernameField.getText().trim() : "";
        String authDb = basic ? authDbField.getText().trim() : "";

        return new MongoConnection(
                existing.id(),
                nameField.getText().trim().isEmpty() ? "Unnamed" : nameField.getText().trim(),
                modeVal,
                uriVal,
                ctVal,
                hostsVal,
                srvHost,
                authModeVal,
                user,
                basic ? encPassword : null,
                authDb,
                existing.gssapiServiceName(),
                existing.awsSessionToken(),
                existing.tlsEnabled(),
                existing.tlsCaFile(),
                existing.tlsClientCertFile(),
                existing.encTlsClientCertPassword(),
                existing.tlsAllowInvalidHostnames(),
                existing.tlsAllowInvalidCertificates(),
                existing.sshEnabled(),
                existing.sshHost(),
                existing.sshPort(),
                existing.sshUser(),
                existing.sshAuthMode(),
                existing.encSshPassword(),
                existing.sshKeyFile(),
                existing.encSshKeyPassphrase(),
                existing.proxyType(),
                existing.proxyHost(),
                existing.proxyPort(),
                existing.proxyUser(),
                existing.encProxyPassword(),
                existing.replicaSetName(),
                existing.readPreference(),
                existing.defaultDb(),
                existing.appName(),
                existing.manualUriOptions(),
                existing.createdAt(),
                0L
        );
    }

    private String encOrKeep(String fieldValue, String existingEnc) {
        if (PLACEHOLDER_PWD.equals(fieldValue)) return existingEnc;
        if (fieldValue == null || fieldValue.isEmpty()) return null;
        return crypto.encrypt(fieldValue);
    }

    private void setupLivePreview() {
        javafx.beans.InvalidationListener il = obs -> refreshPreview();
        hostField.textProperty().addListener(il);
        portField.textProperty().addListener(il);
        connectionType.valueProperty().addListener(il);
        authMode.valueProperty().addListener(il);
        usernameField.textProperty().addListener(il);
        authDbField.textProperty().addListener(il);
        uriArea.textProperty().addListener(il);
        modeUri.selectedProperty().addListener(il);
        modeManual.selectedProperty().addListener(il);
        refreshPreview();
    }

    private void refreshPreview() {
        try {
            if (modeUri.isSelected()) {
                uriPreview.setText(uriArea.getText());
            } else {
                uriPreview.setText(ConnectionUriBuilder.build(collect(), crypto));
            }
        } catch (Exception e) {
            uriPreview.setText("(invalid)");
        }
    }

    private void doTest(ConnectionManager manager) {
        testResult.setStyle("-fx-text-fill: #6b7280;");
        testResult.setText("Testing…");
        MongoConnection snapshot = collect();
        Thread.startVirtualThread(() -> {
            try {
                String v = manager.testConnection(snapshot);
                Platform.runLater(() -> {
                    testResult.setStyle("-fx-text-fill: #16a34a; -fx-font-weight: bold;");
                    testResult.setText("✓ Connected — MongoDB " + v);
                });
            } catch (Exception ex) {
                Platform.runLater(() -> {
                    testResult.setStyle("-fx-text-fill: #dc2626; -fx-font-weight: bold;");
                    testResult.setText("✗ " + ex.getMessage());
                });
            }
        });
    }
}

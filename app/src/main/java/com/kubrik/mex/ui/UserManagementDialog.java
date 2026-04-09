package com.kubrik.mex.ui;

import com.kubrik.mex.core.MongoService;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Button;
import javafx.scene.control.ButtonType;
import javafx.scene.control.CheckBox;
import javafx.scene.control.ChoiceBox;
import javafx.scene.control.Dialog;
import javafx.scene.control.Label;
import javafx.scene.control.PasswordField;
import javafx.scene.control.Separator;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.VBox;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

/**
 * Studio-3T-style user manager: list / create / drop users,
 * change passwords, assign built-in MongoDB roles.
 */
public class UserManagementDialog extends Dialog<Void> {

    private static final String[] BUILT_IN_ROLES = {
            "read", "readWrite",
            "dbAdmin", "dbOwner", "userAdmin",
            "clusterAdmin", "clusterManager", "clusterMonitor", "hostManager",
            "backup", "restore",
            "readAnyDatabase", "readWriteAnyDatabase",
            "userAdminAnyDatabase", "dbAdminAnyDatabase",
            "root"
    };

    private final MongoService svc;
    private final String db;
    private final TableView<Document> table = new TableView<>();

    public UserManagementDialog(MongoService svc, String db) {
        this.svc = svc;
        this.db = db;
        setTitle("User Management · " + db);
        setResizable(true);

        /* ========== Existing users table ========== */
        Label listTitle = new Label("Users in " + db);
        listTitle.setStyle("-fx-font-size: 14px; -fx-font-weight: bold;");

        TableColumn<Document, String> userCol = new TableColumn<>("Username");
        userCol.setCellValueFactory(c -> new SimpleStringProperty(str(c.getValue(), "user")));
        userCol.setPrefWidth(180);

        TableColumn<Document, String> rolesCol = new TableColumn<>("Roles");
        rolesCol.setCellValueFactory(c -> {
            @SuppressWarnings("unchecked")
            List<Document> roles = (List<Document>) c.getValue().get("roles");
            if (roles == null) return new SimpleStringProperty("");
            StringBuilder sb = new StringBuilder();
            for (Document r : roles) {
                if (sb.length() > 0) sb.append(", ");
                sb.append(r.getString("role"));
                String rdb = r.getString("db");
                if (rdb != null && !rdb.equals(db)) sb.append("@").append(rdb);
            }
            return new SimpleStringProperty(sb.toString());
        });
        rolesCol.setPrefWidth(400);

        TableColumn<Document, String> mechCol = new TableColumn<>("Mechanisms");
        mechCol.setCellValueFactory(c -> new SimpleStringProperty(str(c.getValue(), "mechanisms")));
        mechCol.setPrefWidth(160);

        table.getColumns().addAll(userCol, rolesCol, mechCol);
        table.setColumnResizePolicy(TableView.UNCONSTRAINED_RESIZE_POLICY);
        VBox.setVgrow(table, Priority.ALWAYS);

        // Detail pane — resizable via a SplitPane
        TextArea detailArea = new TextArea();
        detailArea.setEditable(false);
        detailArea.setStyle("-fx-font-family: 'Menlo','Monaco',monospace; -fx-font-size: 11px;");
        table.getSelectionModel().selectedItemProperty().addListener((o, a, b) -> {
            detailArea.setText(b == null ? "" : b.toJson(MongoService.JSON_RELAXED));
        });

        // Action buttons
        Button changePwd = new Button("Change password…");
        Button dropUser = new Button("Drop user");
        dropUser.setStyle("-fx-text-fill: #dc2626;");
        Button grantRole = new Button("Grant role…");
        Button revokeRole = new Button("Revoke role…");
        HBox userActions = new HBox(8, changePwd, grantRole, revokeRole, dropUser);
        userActions.setAlignment(Pos.CENTER_LEFT);

        changePwd.setOnAction(e -> doChangePassword());
        dropUser.setOnAction(e -> doDropUser());
        grantRole.setOnAction(e -> doGrantRole());
        revokeRole.setOnAction(e -> doRevokeRole());

        javafx.scene.control.SplitPane tableSplit = new javafx.scene.control.SplitPane(table, detailArea);
        tableSplit.setOrientation(javafx.geometry.Orientation.VERTICAL);
        tableSplit.setDividerPositions(0.6);
        VBox.setVgrow(tableSplit, Priority.ALWAYS);

        VBox listSection = new VBox(8, listTitle, tableSplit, userActions);

        /* ========== Create new user form ========== */
        Label createTitle = new Label("Create new user");
        createTitle.setStyle("-fx-font-size: 14px; -fx-font-weight: bold;");

        TextField newUsername = new TextField();
        newUsername.setPromptText("username");
        PasswordField newPassword = new PasswordField();
        newPassword.setPromptText("password");

        // Role checkboxes grid
        Label rolesLabel = new Label("Roles");
        rolesLabel.setStyle("-fx-font-weight: bold;");

        javafx.scene.layout.FlowPane roleChecks = new javafx.scene.layout.FlowPane(10, 6);
        List<CheckBox> roleCbs = new ArrayList<>();
        for (String role : BUILT_IN_ROLES) {
            CheckBox cb = new CheckBox(role);
            roleCbs.add(cb);
            roleChecks.getChildren().add(cb);
        }
        // Pre-select readWrite for convenience
        for (CheckBox cb : roleCbs) if ("readWrite".equals(cb.getText())) cb.setSelected(true);

        // Custom role as JSON
        TextField customRoleField = new TextField();
        customRoleField.setPromptText("{ \"role\": \"myRole\", \"db\": \"admin\" }");
        customRoleField.setStyle("-fx-font-family: 'Menlo','Monaco',monospace;");
        Label customHint = new Label("Optional: add a custom role document (JSON).");
        customHint.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 10px;");

        GridPane createGrid = new GridPane();
        createGrid.setHgap(10); createGrid.setVgap(6);
        javafx.scene.layout.ColumnConstraints cl = new javafx.scene.layout.ColumnConstraints();
        cl.setMinWidth(100); cl.setHalignment(javafx.geometry.HPos.RIGHT);
        javafx.scene.layout.ColumnConstraints cr = new javafx.scene.layout.ColumnConstraints();
        cr.setHgrow(Priority.ALWAYS); cr.setFillWidth(true);
        createGrid.getColumnConstraints().addAll(cl, cr);

        int r = 0;
        createGrid.addRow(r++, new Label("Username"), newUsername);
        createGrid.addRow(r++, new Label("Password"), UiHelpers.passwordWithEye(newPassword));
        createGrid.addRow(r++, rolesLabel, roleChecks);
        createGrid.addRow(r++, new Label("Custom role"), customRoleField);
        createGrid.addRow(r++, new Label(""), customHint);

        Button createBtn = new Button("Create user");
        createBtn.setStyle("-fx-background-color: #16a34a; -fx-text-fill: white; -fx-font-weight: bold;");
        createBtn.setOnAction(e -> {
            try {
                List<Document> roles = new ArrayList<>();
                for (CheckBox cb : roleCbs) {
                    if (cb.isSelected()) roles.add(new Document("role", cb.getText()).append("db", db));
                }
                String custom = customRoleField.getText().trim();
                if (!custom.isEmpty()) roles.add(Document.parse(custom));
                if (roles.isEmpty()) {
                    UiHelpers.error(getDialogPane().getScene().getWindow(), "Select at least one role.");
                    return;
                }
                svc.createUser(db, newUsername.getText().trim(), newPassword.getText(), roles);
                newUsername.clear();
                newPassword.clear();
                refresh();
            } catch (Exception ex) {
                UiHelpers.error(getDialogPane().getScene().getWindow(), ex.getMessage());
            }
        });

        VBox createSection = new VBox(8, createTitle, createGrid, createBtn);

        /* ========== Layout ========== */
        VBox content = new VBox(12, listSection, new Separator(), createSection);
        content.setPadding(new Insets(16));
        content.setPrefSize(920, 680);

        getDialogPane().setContent(content);
        getDialogPane().getButtonTypes().setAll(ButtonType.CLOSE);
        refresh();
    }

    private void refresh() {
        try {
            table.setItems(FXCollections.observableArrayList(svc.listUsers(db)));
        } catch (Exception e) {
            UiHelpers.error(getDialogPane().getScene().getWindow(), e.getMessage());
        }
    }

    private String selectedUser() {
        Document sel = table.getSelectionModel().getSelectedItem();
        return sel == null ? null : sel.getString("user");
    }

    private void doChangePassword() {
        String user = selectedUser();
        if (user == null) return;
        PasswordField pf = new PasswordField();
        Dialog<String> d = new Dialog<>();
        d.initOwner(getDialogPane().getScene().getWindow());
        d.setTitle("Change password for " + user);
        d.getDialogPane().setContent(new VBox(8, new Label("New password"), UiHelpers.passwordWithEye(pf)));
        d.getDialogPane().getButtonTypes().addAll(ButtonType.OK, ButtonType.CANCEL);
        d.setResultConverter(bt -> bt == ButtonType.OK ? pf.getText() : null);
        d.showAndWait().ifPresent(pwd -> {
            try { svc.updateUserPassword(db, user, pwd); refresh(); }
            catch (Exception ex) { UiHelpers.error(getDialogPane().getScene().getWindow(), ex.getMessage()); }
        });
    }

    private void doDropUser() {
        String user = selectedUser();
        if (user == null) return;
        if (UiHelpers.confirmTyped(getDialogPane().getScene().getWindow(), user)) {
            try { svc.dropUser(db, user); refresh(); }
            catch (Exception ex) { UiHelpers.error(getDialogPane().getScene().getWindow(), ex.getMessage()); }
        }
    }

    private void doGrantRole() {
        String user = selectedUser();
        if (user == null) return;
        ChoiceBox<String> rolePicker = new ChoiceBox<>(FXCollections.observableArrayList(BUILT_IN_ROLES));
        rolePicker.setValue("readWrite");
        Dialog<String> d = new Dialog<>();
        d.initOwner(getDialogPane().getScene().getWindow());
        d.setTitle("Grant role to " + user);
        d.getDialogPane().setContent(new VBox(8, new Label("Role"), rolePicker));
        d.getDialogPane().getButtonTypes().addAll(ButtonType.OK, ButtonType.CANCEL);
        d.setResultConverter(bt -> bt == ButtonType.OK ? rolePicker.getValue() : null);
        d.showAndWait().ifPresent(role -> {
            try {
                svc.grantRoles(db, user, List.of(new Document("role", role).append("db", db)));
                refresh();
            } catch (Exception ex) { UiHelpers.error(getDialogPane().getScene().getWindow(), ex.getMessage()); }
        });
    }

    private void doRevokeRole() {
        String user = selectedUser();
        if (user == null) return;
        // Get current roles for this user
        Document sel = table.getSelectionModel().getSelectedItem();
        @SuppressWarnings("unchecked")
        List<Document> currentRoles = (List<Document>) sel.get("roles");
        if (currentRoles == null || currentRoles.isEmpty()) return;
        List<String> roleNames = new ArrayList<>();
        for (Document r : currentRoles) roleNames.add(r.getString("role"));

        ChoiceBox<String> rolePicker = new ChoiceBox<>(FXCollections.observableArrayList(roleNames));
        if (!roleNames.isEmpty()) rolePicker.setValue(roleNames.get(0));
        Dialog<String> d = new Dialog<>();
        d.initOwner(getDialogPane().getScene().getWindow());
        d.setTitle("Revoke role from " + user);
        d.getDialogPane().setContent(new VBox(8, new Label("Role to revoke"), rolePicker));
        d.getDialogPane().getButtonTypes().addAll(ButtonType.OK, ButtonType.CANCEL);
        d.setResultConverter(bt -> bt == ButtonType.OK ? rolePicker.getValue() : null);
        d.showAndWait().ifPresent(role -> {
            try {
                svc.revokeRoles(db, user, List.of(new Document("role", role).append("db", db)));
                refresh();
            } catch (Exception ex) { UiHelpers.error(getDialogPane().getScene().getWindow(), ex.getMessage()); }
        });
    }

    private static String str(Document d, String key) {
        Object v = d.get(key);
        return v == null ? "" : String.valueOf(v);
    }
}

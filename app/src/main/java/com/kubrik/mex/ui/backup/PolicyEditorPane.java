package com.kubrik.mex.ui.backup;

import com.kubrik.mex.backup.spec.ArchiveSpec;
import com.kubrik.mex.backup.spec.BackupPolicy;
import com.kubrik.mex.backup.spec.PolicyValidator;
import com.kubrik.mex.backup.spec.RetentionSpec;
import com.kubrik.mex.backup.spec.Scope;
import com.kubrik.mex.backup.store.BackupPolicyDao;
import com.kubrik.mex.backup.store.SinkDao;
import com.kubrik.mex.backup.store.SinkRecord;
import com.kubrik.mex.core.ConnectionManager;
import com.kubrik.mex.store.ConnectionStore;
import javafx.application.Platform;
import javafx.beans.property.SimpleObjectProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.ButtonType;
import javafx.scene.control.CheckBox;
import javafx.scene.control.ChoiceBox;
import javafx.scene.control.Label;
import javafx.scene.control.ListCell;
import javafx.scene.control.ListView;
import javafx.scene.control.RadioButton;
import javafx.scene.control.Spinner;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.control.ToggleGroup;
import javafx.scene.control.Tooltip;
import javafx.util.Duration;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;

import java.util.List;
import java.util.Optional;

/**
 * v2.5 BKP-POLICY-1..10 / UI-BKP-1..2 — policy editor pane. Left: the list of
 * saved policies for the selected connection; right: editor form. The form
 * is validated live via {@link PolicyValidator}; Save stays disabled until
 * {@link PolicyValidator#validate} returns an empty list.
 *
 * <p>This pane is connection-aware via a {@link #connectionProperty()} the
 * parent BackupsTab drives — switching connection reloads the list.</p>
 */
public final class PolicyEditorPane extends BorderPane {

    private final BackupPolicyDao policyDao;
    private final SinkDao sinkDao;
    private final ConnectionManager connManager;
    private final ConnectionStore connectionStore;

    private final SimpleObjectProperty<String> connection = new SimpleObjectProperty<>();

    /* left panel */
    private final ObservableList<BackupPolicy> policyList = FXCollections.observableArrayList();
    private final ListView<BackupPolicy> policyListView = new ListView<>(policyList);
    private final ChoiceBox<String> connectionPicker = new ChoiceBox<>();

    /* right panel — editor fields */
    private final TextField nameField = new TextField();
    private final TextField cronField = new TextField();
    private final ToggleGroup scopeGroup = new ToggleGroup();
    private final RadioButton scopeWhole = new RadioButton("Whole cluster");
    private final RadioButton scopeDatabases = new RadioButton("Databases");
    private final RadioButton scopeNamespaces = new RadioButton("Namespaces");
    private final TextArea scopeList = new TextArea();
    private final CheckBox gzipBox = new CheckBox("Gzip");
    private final Spinner<Integer> gzipLevel = new Spinner<>(1, 9, 6);
    private final TextField archiveTemplate = new TextField();
    private final Spinner<Integer> retentionCount = new Spinner<>(1, 1000, 30);
    private final Spinner<Integer> retentionDays  = new Spinner<>(1, 3650, 30);
    private final ChoiceBox<SinkRecord> sinkPicker = new ChoiceBox<>();
    private final CheckBox enabledBox = new CheckBox("Enabled");
    private final CheckBox oplogBox = new CheckBox("Include oplog");
    private final Label errorLabel = new Label();
    private final Button saveBtn = new Button("Save");
    private final Button newBtn = new Button("New policy");
    private final Button deleteBtn = new Button("Delete");

    /* Help copy for non-DBA users. Installed as hover tooltips on each field's
     * label and as the contents of the "Help" info banner at the top of the
     * editor. Kept as constants so copy changes are one place. */
    private static final String HELP_NAME =
            "A short identifier for this policy. Letters, digits, spaces, "
            + "dots, dashes, and underscores only. Shown in the history tab "
            + "so pick something memorable (e.g., \"nightly-reports\").";
    private static final String HELP_CRON =
            "UTC cron expression — five fields: minute hour day-of-month "
            + "month day-of-week. Examples:\n"
            + "   0 3 * * *      every day at 03:00 UTC\n"
            + "   */15 * * * *   every 15 minutes\n"
            + "   0 2 * * 0      Sundays at 02:00 UTC\n"
            + "Leave blank to run this policy manually only.";
    private static final String HELP_SCOPE =
            "What to back up.\n"
            + "   Whole cluster — every database on the connection.\n"
            + "   Databases — list one db name per line.\n"
            + "   Namespaces — list one db.collection per line for "
            + "collection-level backups.";
    private static final String HELP_ARCHIVE =
            "Gzip compresses the bson files. Level 1 = fastest, 9 = smallest. "
            + "Level 6 is a good default. \"Out\" is the folder-name template "
            + "inside the destination — placeholders: <policy>, "
            + "<yyyy-MM-dd_HH-mm-ss>, <connection>.";
    private static final String HELP_RETENTION =
            "Old backups are removed when either limit is exceeded — whichever "
            + "is tighter wins. Count caps how many runs are kept; days caps "
            + "age. Rows stay in the catalog with status PRUNED for audit.";
    private static final String HELP_SINK =
            "Where dump files are written. Configure destinations in the "
            + "Sinks tab. Local-filesystem sinks go to a folder on disk; S3 "
            + "sinks stage to a temp folder first, then upload.";
    private static final String HELP_ENABLED =
            "Turns the cron schedule on or off. A disabled policy can still "
            + "be run manually from the history tab — useful for rehearsing a "
            + "policy before putting it in rotation.";
    private static final String HELP_OPLOG =
            "Captures the oplog alongside the dump so restores can be aligned "
            + "to a specific point in time (PITR). Requires a replica set. "
            + "Turn off only if your source is standalone or you don't need "
            + "PITR on this policy.";

    public PolicyEditorPane(BackupPolicyDao policyDao, SinkDao sinkDao,
                            ConnectionManager connManager, ConnectionStore connectionStore) {
        this.policyDao = policyDao;
        this.sinkDao = sinkDao;
        this.connManager = connManager;
        this.connectionStore = connectionStore;

        setStyle("-fx-background-color: white;");
        setPadding(new Insets(12));

        setLeft(buildLeftPanel());
        setCenter(buildEditor());

        connection.addListener((obs, old, id) -> reloadPolicies());
        policyListView.getSelectionModel().selectedItemProperty().addListener((obs, old, sel) -> {
            if (sel != null) populateForm(sel);
            deleteBtn.setDisable(sel == null);
        });

        reloadConnections();
        reloadSinks();
    }

    public SimpleObjectProperty<String> connectionProperty() { return connection; }

    /* ============================= left panel ============================= */

    private Region buildLeftPanel() {
        Label label = new Label("Connection");
        label.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px;");

        connectionPicker.valueProperty().addListener((obs, o, n) -> {
            if (n != null) connection.set(n);
        });

        policyListView.setCellFactory(lv -> new ListCell<>() {
            @Override protected void updateItem(BackupPolicy p, boolean empty) {
                super.updateItem(p, empty);
                if (empty || p == null) { setText(""); setStyle(""); return; }
                setText(p.name() + "  " + (p.enabled() ? "● enabled" : "○ disabled")
                        + (p.scheduleCron() == null ? "   (manual)" : "   " + p.scheduleCron()));
                setStyle(p.enabled() ? "" : "-fx-text-fill: #9ca3af;");
            }
        });
        policyListView.setPrefWidth(280);

        newBtn.setOnAction(e -> blankForm());

        VBox left = new VBox(8, label, connectionPicker, new Label("Policies"),
                policyListView, newBtn);
        VBox.setVgrow(policyListView, Priority.ALWAYS);
        left.setPadding(new Insets(0, 12, 0, 0));
        return left;
    }

    /* =============================== editor =============================== */

    private Region buildEditor() {
        nameField.setPromptText("nightly-reports");
        cronField.setPromptText("0 3 * * *  (UTC)  — leave blank for manual-only");
        archiveTemplate.setText(ArchiveSpec.DEFAULT_TEMPLATE);
        scopeWhole.setToggleGroup(scopeGroup);
        scopeDatabases.setToggleGroup(scopeGroup);
        scopeNamespaces.setToggleGroup(scopeGroup);
        scopeWhole.setSelected(true);
        scopeList.setPrefRowCount(3);
        scopeList.setPromptText("One db name per line — or db.collection for Namespaces");
        enabledBox.setSelected(true);
        oplogBox.setSelected(true);
        gzipBox.setSelected(true);
        gzipLevel.setEditable(true);

        // Tooltips mirror the label's help text so hovering over either the
        // label or the field surfaces the same explanation.
        installTip(nameField, HELP_NAME);
        installTip(cronField, HELP_CRON);
        installTip(scopeList, HELP_SCOPE);
        installTip(scopeWhole, HELP_SCOPE);
        installTip(scopeDatabases, HELP_SCOPE);
        installTip(scopeNamespaces, HELP_SCOPE);
        installTip(gzipBox, HELP_ARCHIVE);
        installTip(gzipLevel, HELP_ARCHIVE);
        installTip(archiveTemplate, HELP_ARCHIVE);
        installTip(retentionCount, HELP_RETENTION);
        installTip(retentionDays, HELP_RETENTION);
        installTip(sinkPicker, HELP_SINK);
        installTip(enabledBox, HELP_ENABLED);
        installTip(oplogBox, HELP_OPLOG);

        sinkPicker.setConverter(new javafx.util.StringConverter<>() {
            @Override public String toString(SinkRecord s) {
                return s == null ? "" : s.name() + "  (" + s.kind() + ")";
            }
            @Override public SinkRecord fromString(String t) { return null; }
        });

        errorLabel.setStyle("-fx-text-fill: #b91c1c; -fx-font-size: 11px;");
        errorLabel.setWrapText(true);

        saveBtn.setDisable(true);
        saveBtn.setOnAction(e -> onSave());
        deleteBtn.setDisable(true);
        deleteBtn.setOnAction(e -> onDelete());

        GridPane g = new GridPane();
        g.setHgap(10);
        g.setVgap(10);
        g.setPadding(new Insets(14));
        int row = 0;

        // Info banner at the top so first-time users see what a backup policy
        // is and where to hover for details. Hover on the banner expands the
        // tooltip with the field-level explanations.
        Label intro = new Label(
                "A backup policy tells the scheduler when to dump which "
                + "databases, where to store the files, and how long to keep "
                + "them. Hover any label or field for a short explanation.");
        intro.setWrapText(true);
        intro.setStyle("-fx-text-fill: #1e40af; -fx-font-size: 11px; "
                + "-fx-background-color: #eff6ff; -fx-border-color: #bfdbfe; "
                + "-fx-border-radius: 4; -fx-background-radius: 4; "
                + "-fx-padding: 8 10 8 10;");
        g.add(intro, 0, row++, 3, 1);

        g.add(helpLabel("Name", HELP_NAME), 0, row);
        g.add(nameField, 1, row++, 2, 1);

        g.add(helpLabel("Schedule (cron)", HELP_CRON), 0, row);
        g.add(cronField, 1, row++, 2, 1);

        g.add(helpLabel("Scope", HELP_SCOPE), 0, row);
        HBox scopeRadios = new HBox(10, scopeWhole, scopeDatabases, scopeNamespaces);
        g.add(scopeRadios, 1, row++, 2, 1);
        g.add(new Label(""), 0, row);
        g.add(scopeList, 1, row++, 2, 1);

        g.add(helpLabel("Archive", HELP_ARCHIVE), 0, row);
        HBox archiveRow = new HBox(10, gzipBox,
                small("Level"), gzipLevel,
                small("Output"), archiveTemplate);
        HBox.setHgrow(archiveTemplate, Priority.ALWAYS);
        g.add(archiveRow, 1, row++, 2, 1);

        g.add(helpLabel("Retention", HELP_RETENTION), 0, row);
        g.add(new HBox(10,
                small("Keep last"), retentionCount, small("runs"),
                small("or up to"), retentionDays, small("days"),
                small("— whichever is tighter")),
                1, row++, 2, 1);

        g.add(helpLabel("Destination", HELP_SINK), 0, row);
        g.add(sinkPicker, 1, row++, 2, 1);

        g.add(helpLabel("Options", HELP_ENABLED + "\n\n" + HELP_OPLOG), 0, row);
        g.add(new HBox(10, enabledBox, oplogBox), 1, row++, 2, 1);

        g.add(errorLabel, 0, row++, 3, 1);

        HBox actions = new HBox(8, saveBtn, deleteBtn);
        actions.setAlignment(Pos.CENTER_LEFT);
        g.add(actions, 0, row, 3, 1);

        // Validate on every change.
        installLiveValidation();

        return g;
    }

    private static Label helpLabel(String text, String helpBody) {
        Label l = new Label(text);
        l.setStyle("-fx-text-fill: #374151; -fx-font-size: 12px; -fx-font-weight: 600;");
        Tooltip t = new Tooltip(helpBody);
        t.setShowDelay(Duration.millis(250));
        t.setShowDuration(Duration.seconds(30));
        t.setWrapText(true);
        t.setMaxWidth(360);
        l.setTooltip(t);
        return l;
    }

    private static Label small(String text) {
        Label l = new Label(text);
        l.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px;");
        return l;
    }

    private static void installTip(javafx.scene.control.Control c, String body) {
        Tooltip t = new Tooltip(body);
        t.setShowDelay(Duration.millis(250));
        t.setShowDuration(Duration.seconds(30));
        t.setWrapText(true);
        t.setMaxWidth(360);
        Tooltip.install(c, t);
    }

    private void installLiveValidation() {
        nameField.textProperty().addListener((o, a, b) -> revalidate());
        cronField.textProperty().addListener((o, a, b) -> revalidate());
        scopeGroup.selectedToggleProperty().addListener((o, a, b) -> revalidate());
        scopeList.textProperty().addListener((o, a, b) -> revalidate());
        gzipBox.selectedProperty().addListener((o, a, b) -> revalidate());
        gzipLevel.valueProperty().addListener((o, a, b) -> revalidate());
        archiveTemplate.textProperty().addListener((o, a, b) -> revalidate());
        retentionCount.valueProperty().addListener((o, a, b) -> revalidate());
        retentionDays.valueProperty().addListener((o, a, b) -> revalidate());
        sinkPicker.valueProperty().addListener((o, a, b) -> revalidate());
    }

    /* ============================== actions ============================== */

    private void revalidate() {
        List<String> errors = validateForm();
        if (errors.isEmpty()) {
            errorLabel.setText("");
            saveBtn.setDisable(connection.get() == null);
        } else {
            errorLabel.setText(String.join(" · ", errors));
            saveBtn.setDisable(true);
        }
    }

    private List<String> validateForm() {
        if (connection.get() == null) return List.of("pick a connection first");

        // Per-line scope checks surface the specific problem (empty list,
        // missing db.collection separator) instead of the old catch-all
        // "scope is empty / malformed" that hid the real cause.
        List<String> scopeErrors = validateScopeText();
        if (!scopeErrors.isEmpty()) return scopeErrors;

        // Guard against a deleted sink: the picker could hold a stale selection
        // after the user removes a sink from the Sinks editor in another pane.
        SinkRecord pickedSink = sinkPicker.getValue();
        if (pickedSink != null && sinkDao.byId(pickedSink.id()).isEmpty()) {
            return List.of("selected sink was deleted — pick another");
        }

        BackupPolicy draft = buildFromForm(policyListView.getSelectionModel().getSelectedItem());
        if (draft == null) return List.of("archive / retention values are out of range");
        return PolicyValidator.validate(draft);
    }

    /** Line-level checks over the scope textarea. Namespaces must all contain
     *  a dot separator, and the databases/namespaces modes both require at
     *  least one entry. WholeCluster ignores the textarea entirely. */
    private List<String> validateScopeText() {
        if (scopeWhole.isSelected()) return List.of();
        List<String> lines = parseList(scopeList.getText());
        if (lines.isEmpty()) {
            return List.of(scopeDatabases.isSelected()
                    ? "databases scope needs at least one database name"
                    : "namespaces scope needs at least one db.collection entry");
        }
        if (scopeNamespaces.isSelected()) {
            List<String> errs = new java.util.ArrayList<>();
            int idx = 0;
            for (String ns : lines) {
                idx++;
                if (!ns.contains(".")) {
                    errs.add("line " + idx + ": \"" + ns + "\" — expected db.collection");
                }
            }
            return errs;
        }
        return List.of();
    }

    private void onSave() {
        BackupPolicy existing = policyListView.getSelectionModel().getSelectedItem();
        BackupPolicy draft = buildFromForm(existing);
        if (draft == null) return;
        BackupPolicy saved = existing == null ? policyDao.insert(draft)
                : policyDao.update(draft);
        Platform.runLater(() -> {
            reloadPolicies();
            policyListView.getSelectionModel().select(saved);
        });
    }

    private void onDelete() {
        BackupPolicy sel = policyListView.getSelectionModel().getSelectedItem();
        if (sel == null) return;
        Alert a = new Alert(Alert.AlertType.CONFIRMATION);
        a.initOwner(getScene() == null ? null : getScene().getWindow());
        a.setTitle("Delete policy");
        a.setHeaderText("Delete " + sel.name() + "?");
        a.setContentText("Existing catalog rows are kept (policy_id set to null).");
        Optional<ButtonType> pick = a.showAndWait();
        if (pick.isPresent() && pick.get() == ButtonType.OK) {
            policyDao.delete(sel.id());
            reloadPolicies();
            blankForm();
        }
    }

    /* =========================== form <-> record ========================== */

    private BackupPolicy buildFromForm(BackupPolicy existing) {
        Scope scope;
        try {
            if (scopeWhole.isSelected()) scope = new Scope.WholeCluster();
            else if (scopeDatabases.isSelected()) scope = new Scope.Databases(
                    parseList(scopeList.getText()));
            else scope = new Scope.Namespaces(parseList(scopeList.getText()));
        } catch (Exception bad) {
            return null;
        }
        ArchiveSpec archive;
        try {
            archive = new ArchiveSpec(gzipBox.isSelected(),
                    gzipBox.isSelected() ? gzipLevel.getValue() : 0,
                    archiveTemplate.getText());
        } catch (Exception bad) { return null; }
        RetentionSpec retention;
        try {
            retention = new RetentionSpec(retentionCount.getValue(), retentionDays.getValue());
        } catch (Exception bad) { return null; }

        SinkRecord sink = sinkPicker.getValue();
        long sinkId = sink == null ? 0L : sink.id();
        String cron = cronField.getText() == null || cronField.getText().isBlank()
                ? null : cronField.getText().trim();
        long now = System.currentTimeMillis();
        long createdAt = existing == null ? now : existing.createdAt();
        long id = existing == null ? -1L : existing.id();
        try {
            return new BackupPolicy(id, connection.get(), nameField.getText().trim(),
                    enabledBox.isSelected(), cron, scope, archive, retention,
                    sinkId, oplogBox.isSelected(), createdAt, now);
        } catch (IllegalArgumentException bad) {
            return null;
        }
    }

    private void populateForm(BackupPolicy p) {
        nameField.setText(p.name());
        cronField.setText(p.scheduleCron() == null ? "" : p.scheduleCron());
        switch (p.scope()) {
            case Scope.WholeCluster w -> { scopeWhole.setSelected(true); scopeList.setText(""); }
            case Scope.Databases d -> {
                scopeDatabases.setSelected(true);
                scopeList.setText(String.join("\n", d.names()));
            }
            case Scope.Namespaces ns -> {
                scopeNamespaces.setSelected(true);
                scopeList.setText(String.join("\n", ns.namespaces()));
            }
        }
        gzipBox.setSelected(p.archive().gzip());
        gzipLevel.getValueFactory().setValue(
                p.archive().gzip() ? p.archive().level() : 6);
        archiveTemplate.setText(p.archive().outputDirTemplate());
        retentionCount.getValueFactory().setValue(p.retention().maxCount());
        retentionDays.getValueFactory().setValue(p.retention().maxAgeDays());
        enabledBox.setSelected(p.enabled());
        oplogBox.setSelected(p.includeOplog());
        SinkRecord match = sinkPicker.getItems().stream()
                .filter(s -> s.id() == p.sinkId()).findFirst().orElse(null);
        sinkPicker.setValue(match);
        revalidate();
    }

    private void blankForm() {
        policyListView.getSelectionModel().clearSelection();
        nameField.clear();
        cronField.clear();
        scopeWhole.setSelected(true);
        scopeList.clear();
        gzipBox.setSelected(true);
        gzipLevel.getValueFactory().setValue(6);
        archiveTemplate.setText(ArchiveSpec.DEFAULT_TEMPLATE);
        retentionCount.getValueFactory().setValue(30);
        retentionDays.getValueFactory().setValue(30);
        enabledBox.setSelected(true);
        oplogBox.setSelected(true);
        if (!sinkPicker.getItems().isEmpty()) sinkPicker.setValue(sinkPicker.getItems().get(0));
        revalidate();
    }

    /* ============================= refresh ============================== */

    void reloadConnections() {
        String current = connectionPicker.getValue();
        connectionPicker.getItems().setAll(
                connectionStore.list().stream().map(c -> c.id()).toList());
        // Stored value is the connection id (connection.get() callers rely on
        // that); the picker cell renders "<human name>  ·  <id>" so the user
        // sees what they typed in the connection editor instead of a raw id.
        connectionPicker.setConverter(new javafx.util.StringConverter<>() {
            @Override public String toString(String id) {
                if (id == null) return "";
                com.kubrik.mex.model.MongoConnection c = connectionStore.get(id);
                String name = c == null ? null : c.name();
                // "Connection Name (cluster id)" is the format picked in the
                // v2.5 UI polish pass — the human name leads so the operator
                // sees what they typed in the connection editor, with the id
                // in parens as a disambiguator when two connections share a
                // display name.
                return (name == null || name.isBlank()) ? id : name + " (" + id + ")";
            }
            @Override public String fromString(String s) { return s; }
        });
        if (current != null && connectionPicker.getItems().contains(current)) {
            connectionPicker.setValue(current);
        } else if (!connectionPicker.getItems().isEmpty()) {
            connectionPicker.setValue(connectionPicker.getItems().get(0));
        }
    }

    void reloadSinks() {
        List<SinkRecord> sinks = sinkDao.listAll();
        sinkPicker.getItems().setAll(sinks);
        if (!sinks.isEmpty() && sinkPicker.getValue() == null) {
            sinkPicker.setValue(sinks.get(0));
        }
    }

    void reloadPolicies() {
        String cx = connection.get();
        if (cx == null) { policyList.clear(); return; }
        policyList.setAll(policyDao.listForConnection(cx));
    }

    private static List<String> parseList(String text) {
        if (text == null || text.isBlank()) return List.of();
        List<String> out = new java.util.ArrayList<>();
        for (String line : text.split("\\R")) {
            String t = line.trim();
            if (!t.isEmpty()) out.add(t);
        }
        return out;
    }

}

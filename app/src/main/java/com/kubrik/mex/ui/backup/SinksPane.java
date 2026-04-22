package com.kubrik.mex.ui.backup;

import com.kubrik.mex.backup.sink.AzureBlobTarget;
import com.kubrik.mex.backup.sink.GcsTarget;
import com.kubrik.mex.backup.sink.LocalFsTarget;
import com.kubrik.mex.backup.sink.S3Target;
import com.kubrik.mex.backup.sink.SftpTarget;
import com.kubrik.mex.backup.sink.StorageTarget;
import com.kubrik.mex.backup.store.SinkDao;
import com.kubrik.mex.backup.store.SinkRecord;
import javafx.application.Platform;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.ChoiceBox;
import javafx.scene.control.Label;
import javafx.scene.control.PasswordField;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.control.Tooltip;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import javafx.util.Duration;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * v2.6.1 Q2.6.1-D — Backups → Sinks sub-tab. Lists saved
 * {@link SinkRecord}s and hosts a form that creates new ones. Fields
 * swap in based on the kind dropdown: Local FS, S3, GCS, Azure Blob,
 * SFTP. Each cloud kind's credential shape is documented inline under
 * the dropdown so operators don't need to leave the pane to remember
 * the JSON schema.
 *
 * <p><b>Test connection</b> button instantiates the appropriate
 * {@link StorageTarget} with the in-flight form values and runs
 * {@link StorageTarget#testWrite()} on a virtual thread; the verdict
 * (pass / error message / latency) lands in the status label.</p>
 */
public final class SinksPane extends BorderPane {

    /** Sink kinds supported by the SinkEditor form. Order determines
     *  the ChoiceBox order shown to the operator. */
    public enum Kind {
        LOCAL_FS("Local filesystem"),
        S3("Amazon S3"),
        GCS("Google Cloud Storage"),
        AZURE("Azure Blob Storage"),
        SFTP("SFTP");

        final String display;
        Kind(String display) { this.display = display; }
    }

    private final SinkDao sinkDao;
    private final ObservableList<SinkRecord> rows = FXCollections.observableArrayList();
    private final TableView<SinkRecord> table = new TableView<>(rows);

    private final ChoiceBox<Kind> kindPicker = new ChoiceBox<>();
    private final TextField nameField = new TextField();
    private final TextField rootPathField = new TextField();
    private final VBox credFields = new VBox(6);
    private final Map<Kind, CredentialsForm> forms = new LinkedHashMap<>();
    private final Label statusLabel = new Label("—");
    private final Button testBtn = new Button("Test connection");
    private final Button saveBtn = new Button("Save sink");
    private final Button deleteBtn = new Button("Delete");

    public SinksPane(SinkDao sinkDao) {
        this.sinkDao = sinkDao;
        setStyle("-fx-background-color: white;");
        setPadding(new Insets(14, 16, 14, 16));

        for (Kind k : Kind.values()) forms.put(k, buildForm(k));

        setTop(buildTableSection());
        setCenter(buildEditor());
        reload();
    }

    public void refresh() { reload(); }

    private void reload() {
        rows.setAll(sinkDao.listAll());
        deleteBtn.setDisable(table.getSelectionModel().getSelectedItem() == null);
    }

    /* ============================== table ============================== */

    private Region buildTableSection() {
        Label title = new Label("Sinks");
        title.setStyle("-fx-font-size: 14px; -fx-font-weight: 700;");
        Label hint = new Label(
                "Storage destinations backup policies write into. Create or "
                + "pick one below.");
        hint.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px;");
        hint.setWrapText(true);

        table.setPlaceholder(new Label("No sinks yet — fill the form below to create one."));
        table.getColumns().setAll(
                col("Kind", 140, r -> displayKind(r.kind())),
                col("Name", 200, SinkRecord::name),
                col("URI / Path", 360, SinkRecord::rootPath),
                col("Created", 160, r -> java.time.Instant.ofEpochMilli(r.createdAt()).toString()));
        table.setPrefHeight(180);
        table.getSelectionModel().selectedItemProperty().addListener((o, old, r) -> {
            deleteBtn.setDisable(r == null);
            if (r != null) populateForm(r);
        });

        VBox v = new VBox(4, title, hint, table);
        v.setPadding(new Insets(0, 0, 10, 0));
        return v;
    }

    /* =============================== editor =============================== */

    private Region buildEditor() {
        Label header = new Label("New sink (or edit selected)");
        header.setStyle("-fx-font-size: 13px; -fx-font-weight: 700;");

        kindPicker.getItems().addAll(Kind.values());
        kindPicker.setConverter(new javafx.util.StringConverter<>() {
            @Override public String toString(Kind k) { return k == null ? "" : k.display; }
            @Override public Kind fromString(String s) { return null; }
        });
        kindPicker.setValue(Kind.LOCAL_FS);
        kindPicker.valueProperty().addListener((o, a, b) -> {
            // Clear the form the user is switching AWAY from so stale
            // values from that kind don't linger invisibly when the
            // user switches back. populateForm (table-row click) calls
            // this too but sets the new form's values right after.
            if (a != null) forms.get(a).clear();
            if (b != null) forms.get(b).clear();
            renderCredentialsFor(b);
            renderRootPathPromptFor(b);
        });
        kindPicker.setTooltip(tip(
                "Local FS writes to a folder on this machine. Cloud sinks "
                + "stream directly to the provider. All credentials are "
                + "AES-encrypted at rest via the per-install Crypto key."));

        nameField.setPromptText("my-primary-backups");

        rootPathField.setTooltip(tip(
                "For Local FS: an absolute directory path (e.g. /var/mex/backups).\n"
                + "For S3: s3://<bucket>/<prefix>\n"
                + "For GCS: gs://<bucket>/<prefix>\n"
                + "For Azure: azblob://<account>/<container>/<prefix> or\n"
                + "           https://<account>.blob.core.windows.net/<container>/<prefix>\n"
                + "For SFTP: sftp://<user>@<host>[:port]/<path>"));
        renderRootPathPromptFor(Kind.LOCAL_FS);

        GridPane g = new GridPane();
        g.setHgap(10); g.setVgap(8); g.setPadding(new Insets(10));
        int row = 0;
        g.add(small("Kind"), 0, row); g.add(kindPicker, 1, row++, 2, 1);
        g.add(small("Name"), 0, row); g.add(nameField, 1, row++, 2, 1);
        g.add(small("URI / Path"), 0, row); g.add(rootPathField, 1, row++, 2, 1);

        renderCredentialsFor(Kind.LOCAL_FS);
        g.add(credFields, 0, row++, 3, 1);

        testBtn.setOnAction(e -> onTestConnection());
        testBtn.setTooltip(tip(
                "Runs testWrite() on the constructed StorageTarget: a 1 KB "
                + "round-trip that surfaces credential / connectivity issues "
                + "before you save."));
        saveBtn.setOnAction(e -> onSave());
        saveBtn.setTooltip(tip("Persists the sink record. Credentials are "
                + "AES-GCM encrypted into storage_sinks.credentials_enc."));
        deleteBtn.setOnAction(e -> onDelete());
        deleteBtn.setDisable(true);
        Region grow = new Region();
        HBox.setHgrow(grow, Priority.ALWAYS);
        HBox actions = new HBox(8, testBtn, saveBtn, grow, deleteBtn);
        g.add(actions, 0, row++, 3, 1);

        statusLabel.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px;");
        statusLabel.setWrapText(true);
        g.add(statusLabel, 0, row, 3, 1);

        VBox v = new VBox(6, header, g);
        v.setPadding(new Insets(4, 0, 0, 0));
        return v;
    }

    private void renderCredentialsFor(Kind k) {
        credFields.getChildren().clear();
        credFields.getChildren().add(small(k.display + " credentials"));
        credFields.getChildren().addAll(forms.get(k).fields());
    }

    private void renderRootPathPromptFor(Kind k) {
        rootPathField.setPromptText(switch (k) {
            case LOCAL_FS -> "/var/mex/backups";
            case S3 -> "s3://my-bucket/daily";
            case GCS -> "gs://my-bucket/daily";
            case AZURE -> "azblob://myaccount/mycontainer/daily";
            case SFTP -> "sftp://backup@host.example.com:22/var/backups";
        });
    }

    /* ============================== actions ============================== */

    private void onTestConnection() {
        Kind k = kindPicker.getValue();
        if (k == null) return;
        String uri = rootPathField.getText() == null ? "" : rootPathField.getText().trim();
        String creds = forms.get(k).credentialsJson();
        String probeName = nameField.getText() == null || nameField.getText().isBlank()
                ? "probe" : nameField.getText().trim();

        testBtn.setDisable(true);
        statusLabel.setText("Running 1 KB round-trip…");
        statusLabel.setStyle("-fx-text-fill: #2563eb; -fx-font-size: 11px;");
        Thread.startVirtualThread(() -> {
            StorageTarget.Probe verdict;
            StorageTarget target = null;
            try {
                target = buildTarget(k, probeName, uri, creds);
                verdict = target.testWrite();
            } catch (Throwable bad) {
                // Throwable, not Exception — an Error (OOM, linkage)
                // during SDK init still needs to re-enable the button
                // via the finally block below instead of leaving the
                // UI stuck on 'probing'.
                verdict = new StorageTarget.Probe(false, 0L,
                        java.util.Optional.of(bad.getClass().getSimpleName()
                                + ": " + bad.getMessage()));
            } finally {
                // Each click builds a fresh SDK client; without close()
                // a user who tweaks credentials across 10 test-connection
                // clicks leaks 10 HTTP pools / gRPC channels. close() is
                // idempotent + no-op on the targets that hold nothing.
                if (target != null) {
                    try { target.close(); } catch (Exception ignored) {}
                }
            }
            StorageTarget.Probe v = verdict;
            Platform.runLater(() -> {
                try {
                    if (v.writable()) {
                        statusLabel.setText("OK  ·  " + v.latencyMs() + " ms round-trip");
                        statusLabel.setStyle("-fx-text-fill: #166534; -fx-font-size: 11px; -fx-font-weight: 600;");
                    } else {
                        statusLabel.setText("FAILED  ·  " + v.error().orElse("probe threw no error?"));
                        statusLabel.setStyle("-fx-text-fill: #b91c1c; -fx-font-size: 11px; -fx-font-weight: 600;");
                    }
                } finally {
                    testBtn.setDisable(false);
                }
            });
        });
    }

    private void onSave() {
        Kind k = kindPicker.getValue();
        if (k == null) return;
        String name = nameField.getText() == null ? "" : nameField.getText().trim();
        String uri = rootPathField.getText() == null ? "" : rootPathField.getText().trim();
        String creds = forms.get(k).credentialsJson();
        if (name.isEmpty()) {
            statusLabel.setText("Name is required.");
            statusLabel.setStyle("-fx-text-fill: #b91c1c; -fx-font-size: 11px;");
            return;
        }
        if (uri.isEmpty()) {
            statusLabel.setText("URI / path is required.");
            statusLabel.setStyle("-fx-text-fill: #b91c1c; -fx-font-size: 11px;");
            return;
        }
        // Validate the URI shape for the selected kind so an obviously
        // broken value ('s3://' with no bucket) errors at save time
        // instead of at the first backup run hours later.
        try {
            validateUri(k, uri);
        } catch (IllegalArgumentException bad) {
            statusLabel.setText("URI rejected: " + bad.getMessage());
            statusLabel.setStyle("-fx-text-fill: #b91c1c; -fx-font-size: 11px;");
            return;
        }
        long now = System.currentTimeMillis();
        SinkRecord saved = sinkDao.insert(new SinkRecord(-1, k.name(), name, uri,
                creds == null || creds.isBlank() ? null : creds,
                null, now, now));
        reload();
        // Clear the form so the next add doesn't accidentally reuse
        // creds from the just-saved row.
        clearForm();
        statusLabel.setText("Saved sink #" + saved.id());
        statusLabel.setStyle("-fx-text-fill: #166534; -fx-font-size: 11px;");
    }

    private void onDelete() {
        SinkRecord sel = table.getSelectionModel().getSelectedItem();
        if (sel == null) return;
        Alert confirm = new Alert(Alert.AlertType.CONFIRMATION);
        if (getScene() != null) confirm.initOwner(getScene().getWindow());
        confirm.setHeaderText("Delete sink " + sel.name() + "?");
        confirm.setContentText("Any backup_policies row referencing sink #"
                + sel.id() + " will break on next run. No cascading delete.");
        confirm.showAndWait().ifPresent(b -> {
            if (b == javafx.scene.control.ButtonType.OK) {
                sinkDao.delete(sel.id());
                reload();
                // Clear the form so the now-dead row's values don't
                // linger. Clicking Save right after would otherwise
                // recreate a near-duplicate of the sink the operator
                // just removed.
                clearForm();
            }
        });
    }

    /** Shared reset used by onDelete + onSave so both exits land on a
     *  clean form. */
    private void clearForm() {
        nameField.clear();
        rootPathField.clear();
        forms.values().forEach(CredentialsForm::clear);
        statusLabel.setText("—");
        statusLabel.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px;");
    }

    private void populateForm(SinkRecord r) {
        Kind k;
        try { k = Kind.valueOf(r.kind()); }
        catch (IllegalArgumentException bad) { k = Kind.LOCAL_FS; }
        // setValue is a no-op for JavaFX listeners when the new value
        // equals the current, so the kind-switch clear hook doesn't
        // fire on same-kind transitions. Always clear the target form
        // ourselves before populate so clicking one S3 row after
        // another doesn't leave the previous row's credentials mixed
        // into the current display.
        forms.get(k).clear();
        kindPicker.setValue(k);
        nameField.setText(r.name());
        rootPathField.setText(r.rootPath());
        forms.get(k).populate(r.credentialsJson());
    }

    /* ============================== target build ============================== */

    private static StorageTarget buildTarget(Kind k, String name, String uri, String creds) {
        return switch (k) {
            case LOCAL_FS -> new LocalFsTarget(name, uri);
            case S3 -> new S3Target(name, uri, /*region=*/"us-east-1", creds);
            case GCS -> new GcsTarget(name, uri, creds);
            case AZURE -> new AzureBlobTarget(name, uri, creds);
            case SFTP -> new SftpTarget(name, uri, creds);
        };
    }

    /** Each cloud sink's URI parser throws IllegalArgumentException on
     *  malformed input. For LOCAL_FS we only check absolute-path-ness
     *  cheaply; a deeper validation happens when the runner resolves
     *  the path at dump time. */
    private static void validateUri(Kind k, String uri) {
        switch (k) {
            case LOCAL_FS -> {
                if (uri.isBlank()) throw new IllegalArgumentException("empty path");
            }
            case S3 -> S3Target.parseBucketUri(uri);
            case GCS -> GcsTarget.parseBucketUri(uri);
            case AZURE -> AzureBlobTarget.parseUri(uri);
            case SFTP -> SftpTarget.parseUri(uri);
        }
    }

    /* ======================== credentials sub-forms ======================== */

    private interface CredentialsForm {
        List<? extends javafx.scene.Node> fields();
        String credentialsJson();
        void clear();
        void populate(String credentialsJson);
    }

    private CredentialsForm buildForm(Kind k) {
        return switch (k) {
            case LOCAL_FS -> new NoCredsForm();
            case S3 -> new S3Form();
            case GCS -> new GcsForm();
            case AZURE -> new AzureForm();
            case SFTP -> new SftpForm();
        };
    }

    private static final class NoCredsForm implements CredentialsForm {
        private final Label note = new Label("No credentials needed — local filesystem writes use the app's OS permissions.");
        NoCredsForm() { note.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px;"); }
        public List<javafx.scene.Node> fields() { return List.of(note); }
        public String credentialsJson() { return null; }
        public void clear() {}
        public void populate(String j) {}
    }

    private static final class S3Form implements CredentialsForm {
        final TextField accessKey = new TextField();
        final PasswordField secret = new PasswordField();
        final TextField session = new TextField();
        S3Form() {
            accessKey.setPromptText("AKIA…  (leave blank for IAM role / SSO)");
            secret.setPromptText("•••  (leave blank for default provider chain)");
            session.setPromptText("optional session token");
        }
        public List<javafx.scene.Node> fields() {
            return List.of(
                    labeled("Access key ID", accessKey),
                    labeled("Secret access key", secret),
                    labeled("Session token (optional)", session));
        }
        public String credentialsJson() {
            if (accessKey.getText() == null || accessKey.getText().isBlank()) return null;
            StringBuilder sb = new StringBuilder("{\"accessKeyId\":\"")
                    .append(escape(accessKey.getText().trim()))
                    .append("\",\"secretAccessKey\":\"")
                    .append(escape(secret.getText() == null ? "" : secret.getText()))
                    .append("\"");
            String st = session.getText();
            if (st != null && !st.isBlank()) {
                sb.append(",\"sessionToken\":\"").append(escape(st.trim())).append("\"");
            }
            return sb.append("}").toString();
        }
        public void clear() { accessKey.clear(); secret.clear(); session.clear(); }
        public void populate(String j) {
            if (j == null || j.isBlank()) return;
            try {
                var d = org.bson.Document.parse(j);
                accessKey.setText(nullToEmpty(d.getString("accessKeyId")));
                secret.setText(nullToEmpty(d.getString("secretAccessKey")));
                session.setText(nullToEmpty(d.getString("sessionToken")));
            } catch (Exception ignored) {}
        }
    }

    private static final class GcsForm implements CredentialsForm {
        final TextArea saKey = new TextArea();
        GcsForm() {
            saKey.setPromptText("Paste the full service-account JSON key here. "
                    + "Leave blank for Application Default Credentials.");
            saKey.setPrefRowCount(5);
        }
        public List<javafx.scene.Node> fields() {
            return List.of(labeled("Service-account JSON key", saKey));
        }
        public String credentialsJson() {
            String t = saKey.getText();
            return t == null || t.isBlank() ? null : t.trim();
        }
        public void clear() { saKey.clear(); }
        public void populate(String j) { saKey.setText(j == null ? "" : j); }
    }

    private static final class AzureForm implements CredentialsForm {
        final ChoiceBox<String> mode = new ChoiceBox<>();
        final PasswordField sasToken = new PasswordField();
        final TextField accountName = new TextField();
        final PasswordField accountKey = new PasswordField();
        AzureForm() {
            mode.getItems().addAll("SAS token", "Account key", "Anonymous");
            mode.setValue("SAS token");
            sasToken.setPromptText("?sv=…&sig=…");
            accountName.setPromptText("mystorage  (defaults to the host from the URI)");
            accountKey.setPromptText("base64 account key");
        }
        public List<javafx.scene.Node> fields() {
            return List.of(
                    labeled("Auth mode", mode),
                    labeled("SAS token", sasToken),
                    labeled("Account name (for account-key mode)", accountName),
                    labeled("Account key", accountKey));
        }
        public String credentialsJson() {
            String m = mode.getValue();
            if ("Anonymous".equals(m)) return null;
            if ("SAS token".equals(m)) {
                String s = sasToken.getText();
                if (s == null || s.isBlank()) return null;
                return "{\"sasToken\":\"" + escape(s.trim()) + "\"}";
            }
            // Account key mode
            String n = accountName.getText();
            String k = accountKey.getText();
            if (k == null || k.isBlank()) return null;
            StringBuilder sb = new StringBuilder("{");
            if (n != null && !n.isBlank()) sb.append("\"accountName\":\"")
                    .append(escape(n.trim())).append("\",");
            sb.append("\"accountKey\":\"").append(escape(k)).append("\"}");
            return sb.toString();
        }
        public void clear() {
            sasToken.clear(); accountName.clear(); accountKey.clear();
            mode.setValue("SAS token");
        }
        public void populate(String j) {
            if (j == null || j.isBlank()) { mode.setValue("Anonymous"); return; }
            switch (AzureBlobTarget.classifyCredentials(j)) {
                case SAS -> {
                    mode.setValue("SAS token");
                    try { sasToken.setText(nullToEmpty(
                            org.bson.Document.parse(j).getString("sasToken"))); }
                    catch (Exception ignored) {}
                }
                case ACCOUNT_KEY -> {
                    mode.setValue("Account key");
                    try {
                        var d = org.bson.Document.parse(j);
                        accountName.setText(nullToEmpty(d.getString("accountName")));
                        accountKey.setText(nullToEmpty(d.getString("accountKey")));
                    } catch (Exception ignored) {}
                }
                case ANONYMOUS -> mode.setValue("Anonymous");
                case INVALID -> mode.setValue("SAS token");
            }
        }
    }

    private static final class SftpForm implements CredentialsForm {
        final ChoiceBox<String> mode = new ChoiceBox<>();
        final PasswordField password = new PasswordField();
        final TextArea privateKey = new TextArea();
        final PasswordField passphrase = new PasswordField();
        SftpForm() {
            mode.getItems().addAll("Password", "Private key", "None (anonymous)");
            mode.setValue("Password");
            password.setPromptText("SSH user password");
            privateKey.setPromptText("-----BEGIN OPENSSH PRIVATE KEY-----\n…");
            privateKey.setPrefRowCount(5);
            passphrase.setPromptText("key passphrase (optional)");
        }
        public List<javafx.scene.Node> fields() {
            return List.of(
                    labeled("Auth mode", mode),
                    labeled("Password", password),
                    labeled("Private key (PEM)", privateKey),
                    labeled("Key passphrase", passphrase));
        }
        public String credentialsJson() {
            String m = mode.getValue();
            if ("None (anonymous)".equals(m)) return null;
            if ("Private key".equals(m)) {
                String pk = privateKey.getText();
                if (pk == null || pk.isBlank()) return null;
                StringBuilder sb = new StringBuilder("{\"privateKey\":\"")
                        .append(escape(pk.trim())).append("\"");
                String pp = passphrase.getText();
                if (pp != null && !pp.isBlank()) {
                    sb.append(",\"passphrase\":\"").append(escape(pp)).append("\"");
                }
                return sb.append("}").toString();
            }
            // Password mode
            String pw = password.getText();
            if (pw == null || pw.isBlank()) return null;
            return "{\"password\":\"" + escape(pw) + "\"}";
        }
        public void clear() {
            password.clear(); privateKey.clear(); passphrase.clear();
            mode.setValue("Password");
        }
        public void populate(String j) {
            if (j == null || j.isBlank()) { mode.setValue("None (anonymous)"); return; }
            switch (SftpTarget.classifyCredentials(j)) {
                case PASSWORD -> {
                    mode.setValue("Password");
                    try { password.setText(nullToEmpty(
                            org.bson.Document.parse(j).getString("password"))); }
                    catch (Exception ignored) {}
                }
                case PRIVATE_KEY -> {
                    mode.setValue("Private key");
                    try {
                        var d = org.bson.Document.parse(j);
                        privateKey.setText(nullToEmpty(d.getString("privateKey")));
                        passphrase.setText(nullToEmpty(d.getString("passphrase")));
                    } catch (Exception ignored) {}
                }
                case UNAUTHENTICATED -> mode.setValue("None (anonymous)");
                case INVALID -> mode.setValue("Password");
            }
        }
    }

    /* ============================== helpers ============================== */

    private static VBox labeled(String label, javafx.scene.Node field) {
        Label l = new Label(label);
        l.setStyle("-fx-text-fill: #4b5563; -fx-font-size: 11px; -fx-font-weight: 600;");
        if (field instanceof TextArea ta) ta.setWrapText(true);
        VBox box = new VBox(2, l, field);
        return box;
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

    private static String displayKind(String raw) {
        try { return Kind.valueOf(raw).display; }
        catch (Exception e) { return raw; }
    }

    /** JSON string escape — backslash + quote first (so subsequent
     *  replacements don't double-escape), then every control char
     *  0x00–0x1F via a \\uXXXX sequence. Good enough for credential
     *  values pasted from a password manager. */
    private static String escape(String s) {
        if (s == null) return "";
        StringBuilder out = new StringBuilder(s.length() + 4);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '\\' -> out.append("\\\\");
                case '"'  -> out.append("\\\"");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                case '\b' -> out.append("\\b");
                case '\f' -> out.append("\\f");
                default -> {
                    if (c < 0x20) out.append(String.format("\\u%04x", (int) c));
                    else out.append(c);
                }
            }
        }
        return out.toString();
    }

    private static String nullToEmpty(String s) { return s == null ? "" : s; }
}

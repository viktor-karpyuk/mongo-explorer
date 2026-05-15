package com.kubrik.mex.ui;

import com.kubrik.mex.core.ConnectionManager;
import com.kubrik.mex.core.MongoService;
import com.kubrik.mex.shell.MongoShell;
import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.ProgressIndicator;
import javafx.scene.control.SplitPane;
import javafx.scene.control.TextArea;
import javafx.scene.input.KeyCode;
import javafx.scene.input.KeyCombination;
import javafx.scene.input.KeyCodeCombination;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import org.kordamp.ikonli.javafx.FontIcon;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Per-connection mongosh-shaped JavaScript shell. Top pane: editor.
 * Bottom pane: append-only output. Run on Cmd/Ctrl+Enter or via the
 * Run button; Cancel interrupts the in-flight statement via {@link
 * MongoShell#cancel()}.
 *
 * <p>The shell {@link MongoShell} instance is kept alive for the
 * lifetime of this view so user-defined variables and the active
 * database persist across statements.
 */
public class ShellView extends VBox {

    private final ConnectionManager manager;
    private final String connectionId;
    private final String connectionLabel;
    private MongoShell shell;
    private final JsonCodeArea editor;
    private final TextArea output;
    private final Label dbLabel;
    private final Button runBtn;
    private final Button cancelBtn;
    private final ProgressIndicator spinner;
    private final AtomicReference<Thread> running = new AtomicReference<>();

    public ShellView(ConnectionManager manager, String connectionId, String connectionLabel,
                     String defaultDbName) {
        this.manager = manager;
        this.connectionId = connectionId;
        this.connectionLabel = connectionLabel;

        // Output (constructed first so toolbar handlers can capture it)
        output = new TextArea();
        output.setEditable(false);
        output.setWrapText(false);
        output.setStyle("-fx-font-family: 'Menlo','Monaco',monospace; -fx-font-size: 12px; -fx-control-inner-background: #0b1020; -fx-text-fill: #d1d5db;");

        // Toolbar
        Label title = new Label("Shell · " + connectionLabel);
        title.setStyle("-fx-font-size: 13px; -fx-font-weight: bold;");
        dbLabel = new Label("db: " + (defaultDbName == null ? "test" : defaultDbName));
        dbLabel.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 12px;");

        FontIcon runIcon = new FontIcon("fth-play");
        runIcon.setIconSize(12);
        runIcon.setIconColor(javafx.scene.paint.Color.WHITE);
        runBtn = new Button(" Run", runIcon);
        runBtn.setStyle("-fx-background-color: #16a34a; -fx-text-fill: white; -fx-font-weight: bold; -fx-padding: 4 12 4 12; -fx-background-radius: 4;");
        runBtn.setOnAction(e -> runActive());

        FontIcon cancelIcon = new FontIcon("fth-x");
        cancelIcon.setIconSize(12);
        cancelIcon.setIconColor(javafx.scene.paint.Color.web("#374151"));
        cancelBtn = new Button(" Cancel", cancelIcon);
        cancelBtn.setStyle("-fx-background-color: white; -fx-text-fill: #374151; -fx-border-color: #d1d5db; -fx-border-radius: 4; -fx-background-radius: 4; -fx-padding: 4 12 4 12;");
        cancelBtn.setDisable(true);
        cancelBtn.setOnAction(e -> {
            if (shell != null) shell.cancel();
        });

        Button clearBtn = new Button("Clear");
        clearBtn.setStyle("-fx-background-color: white; -fx-text-fill: #374151; -fx-border-color: #d1d5db; -fx-border-radius: 4; -fx-background-radius: 4; -fx-padding: 4 12 4 12;");
        clearBtn.setOnAction(e -> output.clear());

        spinner = new ProgressIndicator();
        spinner.setPrefSize(16, 16);
        spinner.setVisible(false);

        Region sp = new Region();
        HBox.setHgrow(sp, Priority.ALWAYS);
        HBox toolbar = new HBox(8, title, dbLabel, sp, spinner, clearBtn, cancelBtn, runBtn);
        toolbar.setAlignment(Pos.CENTER_LEFT);
        toolbar.setPadding(new Insets(8, 12, 8, 12));
        toolbar.setStyle("-fx-background-color: #f9fafb; -fx-border-color: #e5e7eb; -fx-border-width: 0 0 1 0;");

        // Editor — JsonCodeArea highlights mongo tokens already.
        editor = new JsonCodeArea("// Run mongosh-style commands against " + connectionLabel + "\n"
                + "// Cmd/Ctrl+Enter to execute. Examples:\n"
                + "//   db.users.find({ active: true }).limit(5).toArray()\n"
                + "//   db.users.updateOne({ _id: ObjectId(\"…\") }, { $set: { tier: \"pro\" } })\n"
                + "//   db.runCommand({ ping: 1 })\n\n");
        org.fxmisc.flowless.VirtualizedScrollPane<JsonCodeArea> editorScroll =
                new org.fxmisc.flowless.VirtualizedScrollPane<>(editor);
        VBox.setVgrow(editorScroll, Priority.ALWAYS);

        SplitPane split = new SplitPane(editorScroll, output);
        split.setOrientation(javafx.geometry.Orientation.VERTICAL);
        split.setDividerPositions(0.55);
        VBox.setVgrow(split, Priority.ALWAYS);

        getChildren().addAll(toolbar, split);

        // Cmd/Ctrl+Enter to run
        editor.addEventFilter(javafx.scene.input.KeyEvent.KEY_PRESSED, e -> {
            KeyCombination cmdEnter = new KeyCodeCombination(KeyCode.ENTER, KeyCombination.SHORTCUT_DOWN);
            if (cmdEnter.match(e)) {
                runActive();
                e.consume();
            }
        });

        ensureShell(defaultDbName);
    }

    private void ensureShell(String defaultDbName) {
        MongoService svc = manager.service(connectionId);
        if (svc == null) {
            output.appendText("[shell] Not connected — open the connection first.\n");
            runBtn.setDisable(true);
            return;
        }
        if (shell == null) {
            shell = new MongoShell(svc, defaultDbName);
        }
    }

    private void runActive() {
        if (shell == null) {
            ensureShell(null);
            if (shell == null) return;
        }
        if (running.get() != null) return; // ignore double-fires
        String script = editor.getSelectedText();
        if (script == null || script.isBlank()) script = editor.getText();
        if (script.isBlank()) return;

        runBtn.setDisable(true);
        cancelBtn.setDisable(false);
        spinner.setVisible(true);
        long t0 = System.nanoTime();
        appendBanner("> " + summarize(script));

        String finalScript = script;
        Thread t = Thread.startVirtualThread(() -> {
            MongoShell.Result r = shell.eval(finalScript);
            long elapsedMs = (System.nanoTime() - t0) / 1_000_000;
            Platform.runLater(() -> {
                if (!r.stdout().isEmpty()) output.appendText(r.stdout());
                if (r.error()) {
                    output.appendText("ERROR: " + r.stderr() + "\n");
                } else if (!r.value().isBlank()) {
                    output.appendText(r.value() + "\n");
                }
                output.appendText("[" + elapsedMs + " ms · db=" + shell.currentDbName() + "]\n\n");
                dbLabel.setText("db: " + shell.currentDbName());
                runBtn.setDisable(false);
                cancelBtn.setDisable(true);
                spinner.setVisible(false);
                running.set(null);
                output.positionCaret(output.getLength());
            });
        });
        running.set(t);
    }

    private void appendBanner(String line) {
        output.appendText(line + "\n");
    }

    private static String summarize(String script) {
        String s = script.strip().replace('\n', ' ').replaceAll("\\s+", " ");
        return s.length() > 120 ? s.substring(0, 117) + "…" : s;
    }

    public String connectionId() { return connectionId; }

    /** Release the Graal {@link MongoShell} when the tab closes. */
    public void dispose() {
        if (shell != null) {
            try { shell.close(); } catch (Exception ignored) {}
        }
    }
}

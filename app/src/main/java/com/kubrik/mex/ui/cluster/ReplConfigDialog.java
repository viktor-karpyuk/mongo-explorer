package com.kubrik.mex.ui.cluster;

import com.kubrik.mex.core.MongoService;
import com.kubrik.mex.ui.JsonCodeArea;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.input.Clipboard;
import javafx.scene.input.ClipboardContent;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import javafx.stage.FileChooser;
import javafx.stage.Stage;
import javafx.stage.Window;
import org.bson.Document;
import org.bson.json.JsonWriterSettings;
import org.fxmisc.flowless.VirtualizedScrollPane;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * v2.4 RS-9 — read-only {@code replSetGetConfig} viewer. Opens as a separate
 * window so users can keep the cluster tab visible while inspecting the
 * current config. Copy + Export buttons cover the two most common follow-ups:
 * paste into chat / git commit, or capture a pre-reconfig baseline.
 *
 * <p>v2.5 polish: window is resizable with a sensible minimum so the JSON
 * pane can grow for large configs (mirrors DocumentEditorDialog). The code
 * area sits inside a {@link VirtualizedScrollPane} so scrolling stays smooth
 * on multi-thousand-line configs.</p>
 */
public final class ReplConfigDialog {

    private ReplConfigDialog() {}

    public static void show(Window owner, String connectionLabel, MongoService svc) {
        Stage stage = new Stage();
        if (owner != null) stage.initOwner(owner);
        stage.setTitle("rs.conf · " + connectionLabel);
        stage.setResizable(true);
        stage.setMinWidth(640);
        // Header (~50 px) + footer (~50 px) + vertical padding + breathing
        // room for the JSON pane — 540 keeps ≥ 400 px of scrollable config
        // content even at the floor.
        stage.setMinHeight(540);

        Label title = new Label("Replica set configuration");
        title.setStyle("-fx-font-weight: 700; -fx-font-size: 14px;");
        Label sub = new Label("Read-only. Editing lands with v2.7 guided reconfig.");
        sub.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px;");

        JsonCodeArea area = new JsonCodeArea("loading…");
        area.setEditable(false);
        VirtualizedScrollPane<JsonCodeArea> scroll = new VirtualizedScrollPane<>(area);
        VBox.setVgrow(scroll, Priority.ALWAYS);

        Button copyBtn = new Button("Copy JSON");
        Button exportBtn = new Button("Export…");
        copyBtn.setDisable(true);
        exportBtn.setDisable(true);
        Button closeBtn = new Button("Close");
        closeBtn.setOnAction(e -> stage.close());
        Region grow = new Region();
        HBox.setHgrow(grow, Priority.ALWAYS);
        HBox footer = new HBox(8, copyBtn, exportBtn, grow, closeBtn);
        footer.setAlignment(Pos.CENTER_RIGHT);
        footer.setPadding(new Insets(10, 14, 12, 14));

        VBox header = new VBox(2, title, sub);
        header.setPadding(new Insets(12, 14, 10, 14));

        VBox body = new VBox(scroll);
        body.setPadding(new Insets(0, 14, 0, 14));
        VBox.setVgrow(scroll, Priority.ALWAYS);
        VBox.setVgrow(body, Priority.ALWAYS);

        BorderPane root = new BorderPane(body);
        root.setTop(header);
        root.setBottom(footer);
        root.setStyle("-fx-background-color: white;");

        Scene scene = new Scene(root, 960, 640);
        stage.setScene(scene);
        stage.show();

        Thread.startVirtualThread(() -> {
            String payload;
            try {
                Document d = svc.database("admin").runCommand(new Document("replSetGetConfig", 1));
                Object cfg = d.get("config");
                String json = cfg instanceof Document c
                        ? c.toJson(JsonWriterSettings.builder().indent(true).build())
                        : d.toJson(JsonWriterSettings.builder().indent(true).build());
                payload = json;
            } catch (Exception e) {
                payload = "// replSetGetConfig failed: " + e.getMessage();
            }
            String finalPayload = payload;
            javafx.application.Platform.runLater(() -> {
                area.replaceText(0, area.getLength(), finalPayload);
                area.refreshHighlight();
                copyBtn.setDisable(false);
                exportBtn.setDisable(false);
                copyBtn.setOnAction(e -> {
                    ClipboardContent c = new ClipboardContent();
                    c.putString(finalPayload);
                    Clipboard.getSystemClipboard().setContent(c);
                });
                exportBtn.setOnAction(e -> {
                    FileChooser fc = new FileChooser();
                    fc.setTitle("Export rs.conf");
                    fc.setInitialFileName("rs-conf-" + connectionLabel.replaceAll("\\W+", "-") + ".json");
                    fc.getExtensionFilters().add(new FileChooser.ExtensionFilter("JSON", "*.json"));
                    java.io.File out = fc.showSaveDialog(stage);
                    if (out == null) return;
                    try {
                        Files.writeString(Path.of(out.getAbsolutePath()), finalPayload, StandardCharsets.UTF_8);
                    } catch (IOException io) {
                        // Surface via a small inline label rather than crashing the dialog.
                        sub.setText("Export failed: " + io.getMessage());
                        sub.setStyle("-fx-text-fill: #b91c1c; -fx-font-size: 11px;");
                    }
                });
            });
        });
    }
}

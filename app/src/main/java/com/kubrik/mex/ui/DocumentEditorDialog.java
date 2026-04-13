package com.kubrik.mex.ui;

import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.*;
import javafx.scene.layout.*;
import javafx.stage.Window;
import org.bson.BsonDocument;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.fxmisc.flowless.VirtualizedScrollPane;
import org.kordamp.ikonli.javafx.FontIcon;

import java.util.Optional;
import java.util.function.Consumer;

/**
 * Document editor dialog with two switchable views:
 * - Structured (Atlas-style field editor with type dropdowns)
 * - JSON (raw text editor with syntax highlighting)
 */
public class DocumentEditorDialog {

    private static final JsonWriterSettings PRETTY = JsonWriterSettings.builder()
            .outputMode(JsonMode.RELAXED).indent(true).build();

    private final Dialog<String> dialog;
    private final JsonCodeArea codeArea;
    private final BsonFieldEditor fieldEditor;
    private final StackPane editorPane;
    private boolean showingStructured = true;

    // Zoom state for JSON view
    private double fontSize = 13;

    public DocumentEditorDialog(Window owner, String title, String subtitle, String initialJson) {
        dialog = new Dialog<>();
        dialog.initOwner(owner);
        dialog.setTitle(title);
        dialog.setResizable(true);

        // Header
        Label header = new Label(title);
        header.setStyle("-fx-font-size: 16px; -fx-font-weight: bold;");
        Label sub = new Label(subtitle);
        sub.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 12px;");
        sub.setWrapText(true);

        // View toggle (Structured / JSON)
        ToggleGroup viewGroup = new ToggleGroup();
        ToggleButton structuredBtn = viewToggle("fth-list", "Structured", viewGroup);
        ToggleButton jsonBtn = viewToggle("fth-code", "JSON", viewGroup);
        structuredBtn.setSelected(true);

        HBox viewSwitcher = new HBox(0, structuredBtn, jsonBtn);
        viewSwitcher.setAlignment(Pos.CENTER);

        Region headerSpacer = new Region();
        HBox.setHgrow(headerSpacer, Priority.ALWAYS);

        HBox headerRow = new HBox(10, header, headerSpacer, viewSwitcher);
        headerRow.setAlignment(Pos.CENTER_LEFT);

        // JSON code editor
        codeArea = new JsonCodeArea(initialJson);
        VirtualizedScrollPane<JsonCodeArea> codeScroll = new VirtualizedScrollPane<>(codeArea);

        // Structured field editor
        fieldEditor = new BsonFieldEditor();

        // Load initial document into structured view
        try {
            BsonDocument doc = BsonDocument.parse(initialJson);
            fieldEditor.setDocument(doc);
        } catch (Exception ignored) {
            // If initial JSON is invalid, structured view starts empty
        }

        // Stack pane to switch between views
        editorPane = new StackPane(fieldEditor, codeScroll);
        VBox.setVgrow(editorPane, Priority.ALWAYS);
        codeScroll.setVisible(false);
        codeScroll.setManaged(false);

        // View switching logic
        viewGroup.selectedToggleProperty().addListener((o, oldToggle, newToggle) -> {
            if (newToggle == null) { oldToggle.setSelected(true); return; }
            boolean toStructured = newToggle == structuredBtn;
            switchView(toStructured, codeScroll);
        });

        // Validation status
        Label status = new Label("");
        status.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px;");
        Runnable validate = () -> {
            try {
                BsonDocument.parse(codeArea.getText());
                status.setStyle("-fx-text-fill: #16a34a; -fx-font-size: 11px;");
                status.setText("\u2713 valid JSON");
            } catch (Exception ex) {
                status.setStyle("-fx-text-fill: #dc2626; -fx-font-size: 11px;");
                status.setText("\u2717 " + ex.getMessage());
            }
        };
        codeArea.textProperty().addListener((obs, a, b) -> { if (!showingStructured) validate.run(); });

        // Toolbar: Format, Zoom, Add Field, status
        Button format = new Button("Format");
        format.setGraphic(new FontIcon("fth-align-left"));
        format.setOnAction(e -> {
            try {
                BsonDocument parsed = BsonDocument.parse(codeArea.getText());
                codeArea.replaceText(parsed.toJson(PRETTY));
                codeArea.refreshHighlight();
            } catch (Exception ex) {
                UiHelpers.error(owner, ex.getMessage());
            }
        });

        Button zoomIn = UiHelpers.iconButton("fth-maximize-2", "Zoom in");
        Button zoomOut = UiHelpers.iconButton("fth-minimize-2", "Zoom out");
        zoomIn.setOnAction(e -> { fontSize = Math.min(32, fontSize + 2); applyFontSize(); });
        zoomOut.setOnAction(e -> { fontSize = Math.max(8, fontSize - 2); applyFontSize(); });

        Button addFieldBtn = new Button("Add Field");
        addFieldBtn.setGraphic(new FontIcon("fth-plus"));
        addFieldBtn.setOnAction(e -> fieldEditor.addField());

        // JSON-only toolbar items
        HBox jsonToolbar = new HBox(8, format, zoomOut, zoomIn);
        jsonToolbar.setAlignment(Pos.CENTER_LEFT);

        // Structured-only toolbar items
        HBox structuredToolbar = new HBox(8, addFieldBtn);
        structuredToolbar.setAlignment(Pos.CENTER_LEFT);

        Region toolbarSpacer = new Region();
        HBox.setHgrow(toolbarSpacer, Priority.ALWAYS);

        HBox toolbar = new HBox(8, structuredToolbar, jsonToolbar, toolbarSpacer, status);
        toolbar.setAlignment(Pos.CENTER_LEFT);

        // Update toolbar visibility based on view
        Runnable updateToolbarVisibility = () -> {
            structuredToolbar.setVisible(showingStructured);
            structuredToolbar.setManaged(showingStructured);
            jsonToolbar.setVisible(!showingStructured);
            jsonToolbar.setManaged(!showingStructured);
            status.setVisible(!showingStructured);
            status.setManaged(!showingStructured);
        };
        updateToolbarVisibility.run();
        viewGroup.selectedToggleProperty().addListener((o, a, b) -> updateToolbarVisibility.run());

        // Layout
        VBox content = new VBox(10, headerRow, sub, new Separator(), editorPane, toolbar);
        content.setPadding(new Insets(16, 20, 12, 20));
        content.setFillWidth(true);
        VBox.setVgrow(content, Priority.ALWAYS);

        dialog.getDialogPane().setContent(content);
        dialog.getDialogPane().setPrefSize(860, 600);
        dialog.getDialogPane().setMinSize(500, 360);
        dialog.getDialogPane().getStylesheets().add(
                getClass().getResource("/json-editor.css").toExternalForm());
        dialog.getDialogPane().getButtonTypes().addAll(ButtonType.CANCEL, ButtonType.OK);

        Button ok = (Button) dialog.getDialogPane().lookupButton(ButtonType.OK);
        ok.setText("Save");
        ok.setStyle("-fx-base: #16a34a; -fx-text-fill: white; -fx-font-weight: bold;");

        dialog.setResultConverter(bt -> {
            if (bt != ButtonType.OK) return null;
            if (showingStructured) {
                // Convert structured view to JSON
                return fieldEditor.toDocument().toJson(PRETTY);
            } else {
                return codeArea.getText();
            }
        });
    }

    private void switchView(boolean toStructured, VirtualizedScrollPane<JsonCodeArea> codeScroll) {
        if (toStructured == showingStructured) return;

        if (toStructured) {
            // Sync: JSON → Structured
            try {
                BsonDocument doc = BsonDocument.parse(codeArea.getText());
                fieldEditor.setDocument(doc);
            } catch (Exception ex) {
                // If JSON is invalid, show error and stay on JSON view
                // (we'll allow the switch but the structured view may be stale)
            }
        } else {
            // Sync: Structured → JSON
            try {
                BsonDocument doc = fieldEditor.toDocument();
                codeArea.replaceText(doc.toJson(PRETTY));
                codeArea.refreshHighlight();
            } catch (Exception ignored) {}
        }

        showingStructured = toStructured;
        fieldEditor.setVisible(toStructured);
        fieldEditor.setManaged(toStructured);
        codeScroll.setVisible(!toStructured);
        codeScroll.setManaged(!toStructured);
    }

    private void applyFontSize() {
        codeArea.setStyle("-fx-font-size: " + (int) fontSize + "px;");
    }

    public Optional<String> showAndWait() {
        return dialog.showAndWait();
    }

    private static ToggleButton viewToggle(String iconLit, String text, ToggleGroup group) {
        FontIcon icon = new FontIcon(iconLit);
        icon.setIconSize(13);
        ToggleButton b = new ToggleButton(text);
        b.setGraphic(icon);
        b.setToggleGroup(group);
        b.setStyle("-fx-background-radius: 0; -fx-padding: 4 10 4 10; -fx-font-size: 11px;");
        return b;
    }
}

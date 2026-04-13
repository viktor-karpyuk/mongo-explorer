package com.kubrik.mex.ui;

import javafx.collections.FXCollections;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.*;
import javafx.scene.layout.*;
import javafx.scene.paint.Color;
import org.bson.*;
import org.bson.types.ObjectId;
import org.kordamp.ikonli.javafx.FontIcon;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Atlas-style structured BSON document editor.
 * Each field is a row: key | type dropdown | value editor.
 * Nested objects/arrays are collapsible sections.
 */
public class BsonFieldEditor extends ScrollPane {

    private final VBox rows = new VBox();
    private final List<FieldRow> topLevelRows = new ArrayList<>();
    private boolean readOnly = false;

    /** BSON types available in the dropdown. */
    enum BsonType {
        STRING("String", "#16a34a"),
        INT32("Int32", "#d97706"),
        INT64("Int64", "#d97706"),
        DOUBLE("Double", "#d97706"),
        BOOLEAN("Boolean", "#9333ea"),
        OBJECT_ID("ObjectId", "#0891b2"),
        DATE("Date", "#be185d"),
        NULL("Null", "#6b7280"),
        OBJECT("Object", "#2563eb"),
        ARRAY("Array", "#2563eb"),
        BINARY("Binary", "#6b7280"),
        REGEX("Regex", "#dc2626");

        final String label;
        final String color;
        BsonType(String label, String color) { this.label = label; this.color = color; }
        @Override public String toString() { return label; }
    }

    public BsonFieldEditor() {
        setFitToWidth(true);
        setHbarPolicy(ScrollBarPolicy.NEVER);
        rows.setPadding(new Insets(8));
        rows.setSpacing(0);
        setContent(rows);
        setStyle("-fx-background-color: white;");
    }

    /** Load a BsonDocument into the editor. */
    public void setDocument(BsonDocument doc) {
        rows.getChildren().clear();
        topLevelRows.clear();
        if (doc == null) return;
        for (Map.Entry<String, BsonValue> entry : doc.entrySet()) {
            FieldRow row = new FieldRow(entry.getKey(), entry.getValue(), 0, false);
            topLevelRows.add(row);
            rows.getChildren().add(row);
        }
    }

    /** Build a BsonDocument from the current editor state. */
    public BsonDocument toDocument() {
        BsonDocument doc = new BsonDocument();
        for (FieldRow row : topLevelRows) {
            if (row.isDeleted()) continue;
            doc.append(row.getKey(), row.toBsonValue());
        }
        return doc;
    }

    /** Add a new empty field at the top level. */
    public void addField() {
        FieldRow row = new FieldRow("", new BsonString(""), 0, false);
        topLevelRows.add(row);
        rows.getChildren().add(row);
        row.keyField.requestFocus();
    }

    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    // ─── FieldRow ──────────────────────────────────────────────────────

    class FieldRow extends VBox {
        private final TextField keyField;
        private final ComboBox<BsonType> typeBox;
        private final TextField valueField;
        private final VBox childContainer;
        private final List<FieldRow> childRows = new ArrayList<>();
        private final Button toggleBtn;
        private final Button deleteBtn;
        private final Button addChildBtn;
        private final int depth;
        private final boolean isArrayElement;
        private boolean expanded = true;
        private boolean deleted = false;

        FieldRow(String key, BsonValue value, int depth, boolean isArrayElement) {
            this.depth = depth;
            this.isArrayElement = isArrayElement;
            setSpacing(0);

            // Key field
            keyField = new TextField(key);
            keyField.setPrefWidth(140);
            keyField.setMinWidth(60);
            keyField.setStyle("-fx-font-family: 'Menlo','Monaco',monospace; -fx-font-size: 12px; "
                    + "-fx-font-weight: bold; -fx-text-fill: #2563eb; -fx-background-radius: 3;");
            if (isArrayElement) {
                keyField.setEditable(false);
                keyField.setStyle(keyField.getStyle() + " -fx-background-color: #f3f4f6; -fx-text-fill: #9ca3af;");
            }
            if ("_id".equals(key)) {
                keyField.setEditable(false);
            }

            // Type dropdown
            typeBox = new ComboBox<>(FXCollections.observableArrayList(BsonType.values()));
            typeBox.setPrefWidth(100);
            typeBox.setMinWidth(80);
            typeBox.setCellFactory(lv -> new TypeCell());
            typeBox.setButtonCell(new TypeCell());

            // Value field
            valueField = new TextField();
            valueField.setStyle("-fx-font-family: 'Menlo','Monaco',monospace; -fx-font-size: 12px; -fx-background-radius: 3;");
            HBox.setHgrow(valueField, Priority.ALWAYS);

            // Toggle button for objects/arrays
            toggleBtn = new Button();
            FontIcon chevron = new FontIcon("fth-chevron-right");
            chevron.setIconSize(12);
            toggleBtn.setGraphic(chevron);
            toggleBtn.setStyle("-fx-background-color: transparent; -fx-padding: 2 4 2 4;");
            toggleBtn.setPrefWidth(22);
            toggleBtn.setMinWidth(22);
            toggleBtn.setOnAction(e -> toggleExpand());

            // Delete button
            deleteBtn = UiHelpers.iconButton("fth-trash-2", "Remove field");
            deleteBtn.setOnAction(e -> markDeleted());

            // Add child button (for objects/arrays)
            addChildBtn = UiHelpers.iconButton("fth-plus", "Add field");
            addChildBtn.setOnAction(e -> addChild());

            // Child container for nested fields
            childContainer = new VBox();
            childContainer.setPadding(new Insets(0, 0, 0, 20));
            childContainer.setSpacing(0);

            // Set initial type and value
            BsonType detectedType = detectType(value);
            typeBox.setValue(detectedType);
            populateValue(value, detectedType);

            // Type change listener
            typeBox.valueProperty().addListener((o, oldType, newType) -> {
                if (oldType == newType) return;
                onTypeChange(oldType, newType);
            });

            // Build the main row
            HBox mainRow = buildRow();
            getChildren().add(mainRow);

            if (detectedType == BsonType.OBJECT || detectedType == BsonType.ARRAY) {
                getChildren().add(childContainer);
                updateChevron();
            }
        }

        private HBox buildRow() {
            Region indent = new Region();
            indent.setMinWidth(depth * 20);
            indent.setPrefWidth(depth * 20);

            boolean isContainer = typeBox.getValue() == BsonType.OBJECT || typeBox.getValue() == BsonType.ARRAY;

            HBox row = new HBox(4);
            row.setAlignment(Pos.CENTER_LEFT);
            row.setPadding(new Insets(3, 4, 3, 4));
            row.setStyle("-fx-border-color: transparent transparent #f3f4f6 transparent; -fx-border-width: 0 0 1 0;");

            row.setOnMouseEntered(e -> row.setStyle("-fx-background-color: #f9fafb; -fx-border-color: transparent transparent #f3f4f6 transparent; -fx-border-width: 0 0 1 0;"));
            row.setOnMouseExited(e -> row.setStyle("-fx-border-color: transparent transparent #f3f4f6 transparent; -fx-border-width: 0 0 1 0;"));

            if (isContainer) {
                row.getChildren().addAll(indent, toggleBtn, keyField, typeBox, addChildBtn, deleteBtn);
            } else {
                Region togglePlaceholder = new Region();
                togglePlaceholder.setMinWidth(22);
                togglePlaceholder.setPrefWidth(22);
                row.getChildren().addAll(indent, togglePlaceholder, keyField, typeBox, valueField, deleteBtn);
            }
            return row;
        }

        private void rebuildRow() {
            getChildren().clear();
            HBox mainRow = buildRow();
            getChildren().add(mainRow);
            boolean isContainer = typeBox.getValue() == BsonType.OBJECT || typeBox.getValue() == BsonType.ARRAY;
            if (isContainer) {
                getChildren().add(childContainer);
                updateChevron();
            }
        }

        private void onTypeChange(BsonType oldType, BsonType newType) {
            boolean wasContainer = oldType == BsonType.OBJECT || oldType == BsonType.ARRAY;
            boolean isContainer = newType == BsonType.OBJECT || newType == BsonType.ARRAY;

            if (isContainer && !wasContainer) {
                childRows.clear();
                childContainer.getChildren().clear();
                valueField.clear();
                expanded = true;
            } else if (!isContainer && wasContainer) {
                childRows.clear();
                childContainer.getChildren().clear();
            }

            // Convert value if possible
            if (!isContainer) {
                String current = valueField.getText();
                valueField.setPromptText(promptForType(newType));
                colorizeValue(newType);
            }
            rebuildRow();
        }

        private void toggleExpand() {
            expanded = !expanded;
            childContainer.setVisible(expanded);
            childContainer.setManaged(expanded);
            updateChevron();
        }

        private void updateChevron() {
            FontIcon icon = (FontIcon) toggleBtn.getGraphic();
            icon.setIconLiteral(expanded ? "fth-chevron-down" : "fth-chevron-right");
        }

        void addChild() {
            BsonType type = typeBox.getValue();
            String childKey = type == BsonType.ARRAY ? String.valueOf(childRows.size()) : "";
            FieldRow child = new FieldRow(childKey, new BsonString(""), depth + 1, type == BsonType.ARRAY);
            childRows.add(child);
            childContainer.getChildren().add(child);
            if (!expanded) toggleExpand();
            child.keyField.requestFocus();
        }

        void markDeleted() {
            deleted = true;
            setVisible(false);
            setManaged(false);
            // Also remove from parent's childRows if applicable
        }

        boolean isDeleted() { return deleted; }

        String getKey() { return keyField.getText(); }

        private void populateValue(BsonValue value, BsonType type) {
            switch (type) {
                case STRING -> valueField.setText(value.isString() ? value.asString().getValue() : "");
                case INT32 -> valueField.setText(value.isInt32() ? String.valueOf(value.asInt32().getValue()) : "0");
                case INT64 -> valueField.setText(value.isInt64() ? String.valueOf(value.asInt64().getValue()) : "0");
                case DOUBLE -> valueField.setText(value.isDouble() ? String.valueOf(value.asDouble().getValue()) : "0.0");
                case BOOLEAN -> valueField.setText(value.isBoolean() ? String.valueOf(value.asBoolean().getValue()) : "false");
                case OBJECT_ID -> valueField.setText(value.isObjectId() ? value.asObjectId().getValue().toHexString() : new ObjectId().toHexString());
                case DATE -> valueField.setText(value.isDateTime() ? Instant.ofEpochMilli(value.asDateTime().getValue()).toString() : Instant.now().toString());
                case NULL -> valueField.setText("null");
                case BINARY -> valueField.setText("(binary)");
                case REGEX -> valueField.setText(value.isRegularExpression() ? value.asRegularExpression().getPattern() : "");
                case OBJECT -> populateChildren(value.isDocument() ? value.asDocument() : new BsonDocument());
                case ARRAY -> populateArrayChildren(value.isArray() ? value.asArray() : new BsonArray());
            }
            valueField.setPromptText(promptForType(type));
            colorizeValue(type);
        }

        private void populateChildren(BsonDocument doc) {
            childRows.clear();
            childContainer.getChildren().clear();
            for (Map.Entry<String, BsonValue> entry : doc.entrySet()) {
                FieldRow child = new FieldRow(entry.getKey(), entry.getValue(), depth + 1, false);
                childRows.add(child);
                childContainer.getChildren().add(child);
            }
        }

        private void populateArrayChildren(BsonArray arr) {
            childRows.clear();
            childContainer.getChildren().clear();
            for (int i = 0; i < arr.size(); i++) {
                FieldRow child = new FieldRow(String.valueOf(i), arr.get(i), depth + 1, true);
                childRows.add(child);
                childContainer.getChildren().add(child);
            }
        }

        BsonValue toBsonValue() {
            BsonType type = typeBox.getValue();
            String val = valueField.getText().trim();
            try {
                return switch (type) {
                    case STRING -> new BsonString(val);
                    case INT32 -> new BsonInt32(Integer.parseInt(val));
                    case INT64 -> new BsonInt64(Long.parseLong(val));
                    case DOUBLE -> new BsonDouble(Double.parseDouble(val));
                    case BOOLEAN -> new BsonBoolean(Boolean.parseBoolean(val));
                    case OBJECT_ID -> new BsonObjectId(new ObjectId(val));
                    case DATE -> new BsonDateTime(Instant.parse(val).toEpochMilli());
                    case NULL -> new BsonNull();
                    case BINARY -> new BsonBinary(new byte[0]);
                    case REGEX -> new BsonRegularExpression(val);
                    case OBJECT -> {
                        BsonDocument doc = new BsonDocument();
                        for (FieldRow child : childRows) {
                            if (!child.isDeleted()) doc.append(child.getKey(), child.toBsonValue());
                        }
                        yield doc;
                    }
                    case ARRAY -> {
                        BsonArray arr = new BsonArray();
                        for (FieldRow child : childRows) {
                            if (!child.isDeleted()) arr.add(child.toBsonValue());
                        }
                        yield arr;
                    }
                };
            } catch (Exception e) {
                // Fallback: return as string if conversion fails
                return new BsonString(val);
            }
        }

        private void colorizeValue(BsonType type) {
            String color = type.color;
            valueField.setStyle("-fx-font-family: 'Menlo','Monaco',monospace; -fx-font-size: 12px; "
                    + "-fx-text-fill: " + color + "; -fx-background-radius: 3;");
        }

        private String promptForType(BsonType type) {
            return switch (type) {
                case STRING -> "text value";
                case INT32, INT64 -> "0";
                case DOUBLE -> "0.0";
                case BOOLEAN -> "true / false";
                case OBJECT_ID -> "hex string";
                case DATE -> "2024-01-01T00:00:00Z";
                case NULL -> "null";
                case BINARY -> "(binary data)";
                case REGEX -> "pattern";
                default -> "";
            };
        }
    }

    // ─── Helpers ───────────────────────────────────────────────────────

    static BsonType detectType(BsonValue value) {
        if (value == null || value.isNull()) return BsonType.NULL;
        return switch (value.getBsonType()) {
            case STRING -> BsonType.STRING;
            case INT32 -> BsonType.INT32;
            case INT64 -> BsonType.INT64;
            case DOUBLE -> BsonType.DOUBLE;
            case BOOLEAN -> BsonType.BOOLEAN;
            case OBJECT_ID -> BsonType.OBJECT_ID;
            case DATE_TIME -> BsonType.DATE;
            case DOCUMENT -> BsonType.OBJECT;
            case ARRAY -> BsonType.ARRAY;
            case BINARY -> BsonType.BINARY;
            case REGULAR_EXPRESSION -> BsonType.REGEX;
            default -> BsonType.STRING;
        };
    }

    /** Custom cell that shows the type label with its color. */
    static class TypeCell extends ListCell<BsonType> {
        @Override
        protected void updateItem(BsonType item, boolean empty) {
            super.updateItem(item, empty);
            if (empty || item == null) {
                setText(null);
                setGraphic(null);
            } else {
                setText(item.label);
                setStyle("-fx-font-size: 11px; -fx-font-weight: bold; -fx-text-fill: " + item.color + ";");
            }
        }
    }
}

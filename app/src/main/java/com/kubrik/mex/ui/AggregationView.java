package com.kubrik.mex.ui;

import javafx.collections.FXCollections;
import javafx.geometry.Insets;
import javafx.geometry.Orientation;
import javafx.geometry.Pos;
import javafx.scene.control.Button;
import javafx.scene.control.CheckBox;
import javafx.scene.control.ChoiceBox;
import javafx.scene.control.MenuButton;
import javafx.scene.control.MenuItem;
import javafx.scene.control.ScrollPane;
import javafx.scene.control.Separator;
import javafx.scene.control.TextArea;
import javafx.scene.input.Clipboard;
import javafx.scene.input.ClipboardContent;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Studio-3T-style aggregation pipeline editor:
 * stage-by-stage cards (operator + body), enable / reorder / delete,
 * run-up-to-this-stage, sample pipelines, copy-as-mongosh.
 */
public class AggregationView extends VBox {

    public interface RunHandler { void run(String pipelineJson); }

    /* -------- snippets per operator -------- */
    static final LinkedHashMap<String, String> SNIPPETS = new LinkedHashMap<>();
    static {
        SNIPPETS.put("$match",       "{ \"field\": \"value\" }");
        SNIPPETS.put("$project",     "{ \"field1\": 1, \"field2\": 1, \"_id\": 0 }");
        SNIPPETS.put("$group",       "{ \"_id\": \"$field\", \"count\": { \"$sum\": 1 } }");
        SNIPPETS.put("$sort",        "{ \"field\": -1 }");
        SNIPPETS.put("$limit",       "10");
        SNIPPETS.put("$skip",        "0");
        SNIPPETS.put("$unwind",      "\"$arrayField\"");
        SNIPPETS.put("$lookup",      "{\n  \"from\": \"other\",\n  \"localField\": \"x\",\n  \"foreignField\": \"y\",\n  \"as\": \"joined\"\n}");
        SNIPPETS.put("$addFields",   "{ \"newField\": { \"$add\": [\"$a\", \"$b\"] } }");
        SNIPPETS.put("$set",         "{ \"field\": \"value\" }");
        SNIPPETS.put("$unset",       "\"field\"");
        SNIPPETS.put("$count",       "\"total\"");
        SNIPPETS.put("$sample",      "{ \"size\": 100 }");
        SNIPPETS.put("$facet",       "{\n  \"counts\": [ { \"$sortByCount\": \"$field\" } ],\n  \"total\":  [ { \"$count\": \"n\" } ]\n}");
        SNIPPETS.put("$bucket",      "{ \"groupBy\": \"$field\", \"boundaries\": [0, 100, 200], \"default\": \"Other\" }");
        SNIPPETS.put("$bucketAuto",  "{ \"groupBy\": \"$field\", \"buckets\": 5 }");
        SNIPPETS.put("$replaceRoot", "{ \"newRoot\": \"$subdoc\" }");
        SNIPPETS.put("$replaceWith", "\"$subdoc\"");
        SNIPPETS.put("$sortByCount", "\"$field\"");
        SNIPPETS.put("$graphLookup", "{ \"from\": \"coll\", \"startWith\": \"$x\", \"connectFromField\": \"x\", \"connectToField\": \"y\", \"as\": \"tree\" }");
        SNIPPETS.put("$geoNear",     "{ \"near\": { \"type\": \"Point\", \"coordinates\": [0, 0] }, \"distanceField\": \"dist\" }");
        SNIPPETS.put("$merge",       "{ \"into\": \"target\" }");
        SNIPPETS.put("$out",         "\"target\"");
    }

    /* -------- ready-made sample pipelines -------- */
    private record Sample(String name, List<String[]> stages) {}
    private static final List<Sample> SAMPLES = List.of(
            new Sample("Count by field", List.of(
                    new String[]{"$group", "{ \"_id\": \"$field\", \"count\": { \"$sum\": 1 } }"},
                    new String[]{"$sort",  "{ \"count\": -1 }"})),
            new Sample("Top 10 by field", List.of(
                    new String[]{"$sort",  "{ \"field\": -1 }"},
                    new String[]{"$limit", "10"})),
            new Sample("Join with lookup", List.of(
                    new String[]{"$lookup", "{\n  \"from\": \"other\",\n  \"localField\": \"x\",\n  \"foreignField\": \"y\",\n  \"as\": \"joined\"\n}"},
                    new String[]{"$unwind", "\"$joined\""})),
            new Sample("Faceted search", List.<String[]>of(
                    new String[]{"$facet", "{\n  \"byField\": [ { \"$sortByCount\": \"$field\" } ],\n  \"total\": [ { \"$count\": \"n\" } ]\n}"})),
            new Sample("Average per group", List.of(
                    new String[]{"$group", "{ \"_id\": \"$category\", \"avgValue\": { \"$avg\": \"$value\" }, \"n\": { \"$sum\": 1 } }"},
                    new String[]{"$sort",  "{ \"avgValue\": -1 }"})),
            new Sample("Distinct values", List.of(
                    new String[]{"$group", "{ \"_id\": \"$field\" }"},
                    new String[]{"$sort",  "{ \"_id\": 1 }"})),
            new Sample("Time bucket (day)", List.of(
                    new String[]{"$group", "{ \"_id\": { \"$dateTrunc\": { \"date\": \"$createdAt\", \"unit\": \"day\" } }, \"count\": { \"$sum\": 1 } }"},
                    new String[]{"$sort",  "{ \"_id\": 1 }"})),
            new Sample("Unwind + group tags", List.of(
                    new String[]{"$unwind", "\"$tags\""},
                    new String[]{"$group",  "{ \"_id\": \"$tags\", \"count\": { \"$sum\": 1 } }"},
                    new String[]{"$sort",   "{ \"count\": -1 }"})),
            new Sample("Filter, project, sort, limit", List.of(
                    new String[]{"$match",   "{ \"status\": \"active\" }"},
                    new String[]{"$project", "{ \"name\": 1, \"createdAt\": 1, \"_id\": 0 }"},
                    new String[]{"$sort",    "{ \"createdAt\": -1 }"},
                    new String[]{"$limit",   "50"})),
            new Sample("Random sample", List.<String[]>of(
                    new String[]{"$sample",  "{ \"size\": 20 }"}))
    );

    private final VBox stagesBox = new VBox(8);
    private final List<StageCard> cards = new ArrayList<>();
    private RunHandler onRun = json -> {};
    private Supplier<String> collectionNameSupplier = () -> "collection";

    public AggregationView() {
        setSpacing(8);
        setPadding(new Insets(10));

        Button addStage = new Button("Add stage");
        addStage.setGraphic(new org.kordamp.ikonli.javafx.FontIcon("fth-plus"));
        addStage.setOnAction(e -> addStage("$match", null));

        MenuButton samples = new MenuButton("Load sample…");
        samples.setGraphic(new org.kordamp.ikonli.javafx.FontIcon("fth-book"));
        for (Sample s : SAMPLES) {
            MenuItem mi = new MenuItem(s.name());
            mi.setOnAction(e -> loadSample(s));
            samples.getItems().add(mi);
        }

        Button run = new Button("Run pipeline");
        run.setGraphic(new org.kordamp.ikonli.javafx.FontIcon("fth-play"));
        run.setStyle("-fx-base: #16a34a;");
        run.setOnAction(e -> runAll());

        Button runTo = new Button("Run to focused stage");
        runTo.setOnAction(e -> runToFocused());

        Button clear = new Button("Clear");
        clear.setOnAction(e -> { cards.clear(); stagesBox.getChildren().clear(); });

        Button copy = new Button("Copy as mongosh");
        copy.setGraphic(new org.kordamp.ikonli.javafx.FontIcon("fth-copy"));
        copy.setOnAction(e -> {
            String pipeline = buildJson(-1);
            String code = "db.getCollection(\"" + collectionNameSupplier.get() + "\").aggregate(" + pipeline + ")";
            ClipboardContent cc = new ClipboardContent();
            cc.putString(code);
            Clipboard.getSystemClipboard().setContent(cc);
        });

        Region sp = new Region();
        HBox.setHgrow(sp, Priority.ALWAYS);
        HBox toolbar = new HBox(8, addStage, samples,
                new Separator(Orientation.VERTICAL),
                run, runTo,
                new Separator(Orientation.VERTICAL),
                clear, copy);
        toolbar.setAlignment(Pos.CENTER_LEFT);

        ScrollPane scroll = new ScrollPane(stagesBox);
        scroll.setFitToWidth(true);
        scroll.setStyle("-fx-background-color: transparent;");
        VBox.setVgrow(scroll, Priority.ALWAYS);

        getChildren().addAll(toolbar, scroll);
        addStage("$match", null);
    }

    public void setOnRun(RunHandler h) { this.onRun = h; }
    public void setCollectionNameSupplier(Supplier<String> s) { this.collectionNameSupplier = s; }

    /** Run the entire enabled pipeline. Used by an external Run button. */
    public void run() { runAll(); }

    /* -------- card management -------- */

    private void addStage(String op, String body) {
        StageCard card = new StageCard(op, body == null ? SNIPPETS.getOrDefault(op, "{}") : body);
        card.upBtn.setOnAction(e -> moveCard(card, -1));
        card.downBtn.setOnAction(e -> moveCard(card, 1));
        card.deleteBtn.setOnAction(e -> {
            cards.remove(card);
            stagesBox.getChildren().remove(card);
            renumber();
        });
        card.runHereBtn.setOnAction(e -> {
            int idx = cards.indexOf(card);
            if (idx >= 0) safeRun(idx);
        });
        cards.add(card);
        stagesBox.getChildren().add(card);
        renumber();
    }

    private void moveCard(StageCard card, int delta) {
        int i = cards.indexOf(card);
        int j = i + delta;
        if (i < 0 || j < 0 || j >= cards.size()) return;
        cards.remove(i); cards.add(j, card);
        stagesBox.getChildren().remove(card); stagesBox.getChildren().add(j, card);
        renumber();
    }

    private void renumber() {
        for (int i = 0; i < cards.size(); i++) cards.get(i).setIndex(i);
    }

    private void loadSample(Sample sample) {
        cards.clear();
        stagesBox.getChildren().clear();
        for (String[] s : sample.stages()) addStage(s[0], s[1]);
    }

    /* -------- run -------- */

    private String buildJson(int upToInclusive) {
        StringBuilder sb = new StringBuilder("[");
        boolean first = true;
        for (int i = 0; i < cards.size(); i++) {
            if (upToInclusive >= 0 && i > upToInclusive) break;
            StageCard c = cards.get(i);
            if (!c.enabled.isSelected()) continue;
            if (!first) sb.append(",\n  ");
            first = false;
            sb.append("{ \"").append(c.operator.getValue()).append("\": ").append(c.body.getText()).append(" }");
        }
        sb.append("]");
        return sb.toString();
    }

    private void runAll() { safeRun(-1); }

    private void runToFocused() {
        int idx = -1;
        for (int i = 0; i < cards.size(); i++) {
            if (cards.get(i).body.isFocused() || cards.get(i).operator.isFocused()) { idx = i; break; }
        }
        if (idx < 0) idx = cards.size() - 1;
        safeRun(idx);
    }

    private void safeRun(int upTo) {
        try {
            String json = buildJson(upTo);
            // Validate by wrapping into a doc the BSON parser accepts.
            org.bson.BsonDocument.parse("{ p: " + json + " }");
            onRun.run(json);
        } catch (Exception ex) {
            UiHelpers.error(getScene().getWindow(), "Invalid pipeline: " + ex.getMessage());
        }
    }

    /* -------- one stage card -------- */

    private static class StageCard extends VBox {
        final ChoiceBox<String> operator = new ChoiceBox<>();
        final CheckBox enabled = new CheckBox("Enabled");
        final TextArea body = new TextArea();
        final javafx.scene.control.Label index = new javafx.scene.control.Label();
        final Button upBtn = UiHelpers.iconButton("fth-arrow-up", "Move up");
        final Button downBtn = UiHelpers.iconButton("fth-arrow-down", "Move down");
        final Button deleteBtn = UiHelpers.iconButton("fth-trash-2", "Delete stage");
        final Button runHereBtn = UiHelpers.iconButton("fth-play", "Run pipeline up to this stage");

        StageCard(String op, String bodyText) {
            setSpacing(6);
            setPadding(new Insets(10));
            setStyle("-fx-background-color: #f9fafb; "
                    + "-fx-border-color: #e5e7eb; "
                    + "-fx-border-radius: 6; "
                    + "-fx-background-radius: 6;");

            operator.setItems(FXCollections.observableArrayList(SNIPPETS.keySet()));
            operator.setValue(op);
            operator.valueProperty().addListener((o, prev, next) -> {
                // If user hasn't customized, swap to the snippet for the new operator.
                String prevSnippet = SNIPPETS.get(prev);
                if (next != null && (body.getText().isBlank() || body.getText().equals(prevSnippet))) {
                    body.setText(SNIPPETS.getOrDefault(next, "{}"));
                }
            });
            enabled.setSelected(true);

            index.setStyle("-fx-text-fill: #6b7280; -fx-font-family: 'Menlo','Monaco',monospace;");

            Region sp = new Region();
            HBox.setHgrow(sp, Priority.ALWAYS);
            HBox header = new HBox(8, index, operator, enabled, sp, runHereBtn, upBtn, downBtn, deleteBtn);
            header.setAlignment(Pos.CENTER_LEFT);

            body.setText(bodyText);
            body.setStyle("-fx-font-family: 'Menlo','Monaco',monospace; -fx-font-size: 12px;");
            body.setPrefRowCount(4);

            getChildren().addAll(header, body);
        }

        void setIndex(int i) { index.setText(String.format("#%d", i + 1)); }
    }
}

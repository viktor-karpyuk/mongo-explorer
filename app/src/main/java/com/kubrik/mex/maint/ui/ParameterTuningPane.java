package com.kubrik.mex.maint.ui;

import com.kubrik.mex.maint.model.ClusterShape;
import com.kubrik.mex.maint.model.ParamProposal;
import com.kubrik.mex.maint.param.ParamCatalogue;
import com.kubrik.mex.maint.param.ParamRunner;
import com.kubrik.mex.maint.param.Recommender;
import com.mongodb.client.MongoClient;
import javafx.application.Platform;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.ChoiceBox;
import javafx.scene.control.Label;
import javafx.scene.control.Spinner;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextArea;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * v2.7 PARAM-* UI — Recommender pane: enter cluster-shape inputs +
 * pull current values, see the proposal table with severity + rationale
 * drawer, Apply a single row via ParamRunner.
 */
public final class ParameterTuningPane extends BorderPane {

    private final Recommender recommender = new Recommender();
    private final ParamRunner paramRunner = new ParamRunner();

    private final java.util.function.Supplier<MongoClient> clientSupplier;

    private final ChoiceBox<String> enginePicker = new ChoiceBox<>();
    private final Spinner<Integer> ramGbSpinner = new Spinner<>(1, 4_096, 32);
    private final Spinner<Integer> cpuSpinner = new Spinner<>(1, 256, 8);
    private final ChoiceBox<ClusterShape.Workload> workloadPicker = new ChoiceBox<>();
    private final Spinner<Integer> verSpinner = new Spinner<>(4, 8, 7);

    private final ObservableList<ParamProposal> proposals =
            FXCollections.observableArrayList();
    private final TableView<ParamProposal> table = new TableView<>(proposals);
    private final TextArea rationaleArea = new TextArea();
    private final Label statusLabel = new Label("—");

    public ParameterTuningPane(java.util.function.Supplier<MongoClient> clientSupplier) {
        this.clientSupplier = clientSupplier;
        setStyle("-fx-background-color: white;");
        setPadding(new Insets(14, 16, 14, 16));
        enginePicker.getItems().addAll("wiredTiger", "inMemory");
        enginePicker.setValue("wiredTiger");
        workloadPicker.getItems().addAll(ClusterShape.Workload.values());
        workloadPicker.setValue(ClusterShape.Workload.MIXED);

        setTop(buildHeader());
        setCenter(buildCenter());
        setBottom(buildActions());
    }

    private Region buildHeader() {
        Label title = new Label("Parameter tuning");
        title.setStyle("-fx-font-size: 14px; -fx-font-weight: 700;");
        Label hint = new Label(
                "Curated setParameter catalogue. Enter the cluster shape "
                + "below + Refresh to see recommendations with rationale. "
                + "Severity: ACT (red) / CONSIDER (amber) / INFO (already "
                + "tuned).");
        hint.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px;");
        hint.setWrapText(true);

        GridPane g = new GridPane();
        g.setHgap(8); g.setVgap(6);
        int row = 0;
        g.add(small("Engine"), 0, row); g.add(enginePicker, 1, row);
        g.add(small("RAM (GB)"), 2, row); g.add(ramGbSpinner, 3, row++);
        g.add(small("CPU cores"), 0, row); g.add(cpuSpinner, 1, row);
        g.add(small("Workload"), 2, row); g.add(workloadPicker, 3, row++);
        g.add(small("Server major version"), 0, row); g.add(verSpinner, 1, row++);

        VBox v = new VBox(6, title, hint, g);
        v.setPadding(new Insets(0, 0, 10, 0));
        return v;
    }

    private Region buildCenter() {
        table.setPlaceholder(new Label("Click Refresh to compute proposals."));
        table.getColumns().setAll(
                col("Parameter", 260, ParamProposal::param),
                col("Current", 90, ParamProposal::currentValue),
                col("Proposed", 90, ParamProposal::proposedValue),
                col("Severity", 90, p -> p.severity().name()));
        table.setRowFactory(tv -> new javafx.scene.control.TableRow<>() {
            @Override
            protected void updateItem(ParamProposal item, boolean empty) {
                super.updateItem(item, empty);
                if (empty || item == null) { setStyle(""); return; }
                setStyle(switch (item.severity()) {
                    case ACT -> "-fx-background-color: #fef2f2;";
                    case CONSIDER -> "-fx-background-color: #fffbeb;";
                    case INFO -> "";
                });
            }
        });
        table.getSelectionModel().selectedItemProperty().addListener((o, a, r) ->
                rationaleArea.setText(r == null ? "" : r.rationale()));

        rationaleArea.setEditable(false);
        rationaleArea.setWrapText(true);
        rationaleArea.setPrefRowCount(4);

        Label rationaleLabel = new Label("Rationale");
        rationaleLabel.setStyle("-fx-text-fill: #4b5563; -fx-font-size: 11px; -fx-font-weight: 600;");
        VBox v = new VBox(6, table, rationaleLabel, rationaleArea);
        VBox.setVgrow(table, Priority.ALWAYS);
        return v;
    }

    private Region buildActions() {
        Button refreshBtn = new Button("Refresh");
        refreshBtn.setOnAction(e -> onRefresh());
        Button applyBtn = new Button("Apply selected…");
        applyBtn.setOnAction(e -> onApplySelected());

        Region grow = new Region();
        HBox.setHgrow(grow, Priority.ALWAYS);
        HBox actions = new HBox(8, refreshBtn, grow, applyBtn);
        actions.setPadding(new Insets(10, 0, 0, 0));

        statusLabel.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px;");
        statusLabel.setWrapText(true);
        return new VBox(6, actions, statusLabel);
    }

    private void onRefresh() {
        MongoClient client = clientSupplier.get();
        if (client == null) { fail("No active connection."); return; }
        ClusterShape shape = new ClusterShape(
                enginePicker.getValue(),
                (long) ramGbSpinner.getValue() * 1024L * 1024L * 1024L,
                cpuSpinner.getValue(),
                /*docCountApprox=*/1_000_000L,
                workloadPicker.getValue(),
                verSpinner.getValue());

        Thread.startVirtualThread(() -> {
            Map<String, String> current = new LinkedHashMap<>();
            for (ParamCatalogue.Entry entry : ParamCatalogue.all()) {
                paramRunner.get(client, entry.name())
                        .ifPresent(v -> current.put(entry.name(), v.toString()));
            }
            var out = recommender.recommend(shape, current);
            Platform.runLater(() -> {
                proposals.setAll(out);
                ok(out.size() + " proposals computed.");
            });
        });
    }

    private void onApplySelected() {
        ParamProposal sel = table.getSelectionModel().getSelectedItem();
        if (sel == null) { fail("Pick a row first."); return; }
        if (!sel.isActionable()) {
            fail("Selected row is already at the recommended value.");
            return;
        }
        Alert confirm = new Alert(Alert.AlertType.CONFIRMATION,
                "Apply " + sel.param() + " = " + sel.proposedValue() + "?",
                javafx.scene.control.ButtonType.OK,
                javafx.scene.control.ButtonType.CANCEL);
        confirm.showAndWait().ifPresent(b -> {
            if (b != javafx.scene.control.ButtonType.OK) return;
            MongoClient client = clientSupplier.get();
            if (client == null) { fail("No active connection."); return; }
            Thread.startVirtualThread(() -> {
                var outcome = paramRunner.set(client, sel.param(),
                        parseValue(sel.proposedValue()));
                Platform.runLater(() -> {
                    if (outcome instanceof ParamRunner.Outcome.Ok ok) {
                        ok("Applied — was " + ok.wasValue()
                                + ", now " + ok.nowValue() + ".");
                        onRefresh();
                    } else if (outcome instanceof ParamRunner.Outcome.Failed f) {
                        fail("setParameter failed: " + f.code() + " — " + f.message());
                    }
                });
            });
        });
    }

    /** setParameter rejects type-mismatched values — the catalogue's
     *  recommendedValue is the source of truth for the intended type. */
    private static Object parseValue(String s) {
        if ("true".equalsIgnoreCase(s)) return Boolean.TRUE;
        if ("false".equalsIgnoreCase(s)) return Boolean.FALSE;
        try { return Long.parseLong(s); }
        catch (NumberFormatException ignored) {}
        return s;
    }

    private void ok(String msg) {
        statusLabel.setText(msg);
        statusLabel.setStyle("-fx-text-fill: #166534; -fx-font-size: 11px; -fx-font-weight: 600;");
    }
    private void fail(String msg) {
        statusLabel.setText(msg);
        statusLabel.setStyle("-fx-text-fill: #b91c1c; -fx-font-size: 11px; -fx-font-weight: 600;");
    }
    private static Label small(String text) {
        Label l = new Label(text);
        l.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px;");
        return l;
    }
    private static <T> TableColumn<T, String> col(String title, int width,
                                                   java.util.function.Function<T, String> getter) {
        TableColumn<T, String> c = new TableColumn<>(title);
        c.setPrefWidth(width);
        c.setCellValueFactory(cd -> new SimpleStringProperty(getter.apply(cd.getValue())));
        return c;
    }
}

package com.kubrik.mex.ui.migration;

import com.kubrik.mex.migration.schedule.CronExpression;
import com.kubrik.mex.migration.schedule.Schedule;
import com.kubrik.mex.migration.schedule.ScheduleStore;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.CheckBox;
import javafx.scene.control.Label;
import javafx.scene.control.TableCell;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextField;
import javafx.scene.control.Tooltip;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import javafx.stage.Modality;
import javafx.stage.Stage;
import javafx.stage.Window;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

/** UX-7 — modal that lists the schedules attached to a single migration profile and lets the
 *  user add, enable/disable, and delete them. Opened from the Profiles sub-tab's context menu.
 *  <p>
 *  One dialog per profile. The "Add" row parses the cron expression on the fly and shows a
 *  preview of the next three fire times so the user can sanity-check before saving. */
public final class ScheduleDialog {

    private static final DateTimeFormatter PREVIEW_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm z");

    private final ScheduleStore store;
    private final String profileId;
    private final String profileName;
    private final ObservableList<ScheduleRow> rows = FXCollections.observableArrayList();

    public ScheduleDialog(ScheduleStore store, String profileId, String profileName) {
        this.store = store;
        this.profileId = profileId;
        this.profileName = profileName;
    }

    /** Opens the dialog modal to {@code owner}. Blocks until the user closes it. */
    public void showAndWait(Window owner) {
        Stage stage = new Stage();
        stage.initOwner(owner);
        stage.initModality(Modality.WINDOW_MODAL);
        stage.setTitle("Schedules — " + profileName);

        TableView<ScheduleRow> table = buildTable();
        table.setItems(rows);
        table.setPlaceholder(new Label(
                "No schedules yet. Add a cron expression below."));

        HBox addRow = buildAddRow();

        VBox root = new VBox(12, table, addRow);
        root.setPadding(new Insets(16));
        VBox.setVgrow(table, Priority.ALWAYS);

        stage.setScene(new Scene(root, 720, 420));
        refresh();
        stage.showAndWait();
    }

    // --- layout ------------------------------------------------------------------

    private TableView<ScheduleRow> buildTable() {
        TableView<ScheduleRow> table = new TableView<>();

        TableColumn<ScheduleRow, String> cronCol = new TableColumn<>("Cron");
        cronCol.setCellValueFactory(new PropertyValueFactory<>("cron"));
        cronCol.setPrefWidth(180);

        TableColumn<ScheduleRow, String> enabledCol = new TableColumn<>("Enabled");
        enabledCol.setCellValueFactory(new PropertyValueFactory<>("enabledLabel"));
        enabledCol.setPrefWidth(90);

        TableColumn<ScheduleRow, String> lastCol = new TableColumn<>("Last run");
        lastCol.setCellValueFactory(new PropertyValueFactory<>("lastRun"));
        lastCol.setPrefWidth(170);

        TableColumn<ScheduleRow, String> nextCol = new TableColumn<>("Next run");
        nextCol.setCellValueFactory(new PropertyValueFactory<>("nextRun"));
        nextCol.setPrefWidth(170);

        TableColumn<ScheduleRow, Void> actionsCol = new TableColumn<>("Actions");
        actionsCol.setPrefWidth(190);
        actionsCol.setCellFactory(col -> new TableCell<>() {
            private final Button toggle = new Button();
            private final Button delete = new Button("Delete");
            private final HBox box = new HBox(6, toggle, delete);
            {
                box.setAlignment(Pos.CENTER_LEFT);
                toggle.setOnAction(e -> {
                    ScheduleRow row = getTableView().getItems().get(getIndex());
                    store.setEnabled(row.id, !row.enabled);
                    refresh();
                });
                delete.setOnAction(e -> {
                    ScheduleRow row = getTableView().getItems().get(getIndex());
                    store.delete(row.id);
                    refresh();
                });
            }
            @Override protected void updateItem(Void v, boolean empty) {
                super.updateItem(v, empty);
                if (empty || getIndex() < 0 || getIndex() >= getTableView().getItems().size()) {
                    setGraphic(null);
                    return;
                }
                ScheduleRow row = getTableView().getItems().get(getIndex());
                toggle.setText(row.enabled ? "Disable" : "Enable");
                setGraphic(box);
            }
        });

        table.getColumns().addAll(List.of(cronCol, enabledCol, lastCol, nextCol, actionsCol));
        return table;
    }

    private HBox buildAddRow() {
        TextField cronField = new TextField();
        cronField.setPromptText("e.g. 0 2 * * *  (daily at 02:00)");
        cronField.setPrefWidth(220);
        cronField.setTooltip(new Tooltip(
                "5-field cron: minute hour day-of-month month day-of-week. "
              + "Supports *, literal, a-b range, a,b,c list, */step."));

        Label preview = new Label();
        preview.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px;");
        preview.setWrapText(true);

        CheckBox enabled = new CheckBox("Enabled");
        enabled.setSelected(true);

        Button add = new Button("Add schedule");
        add.setDisable(true);

        cronField.textProperty().addListener((obs, a, b) -> {
            String parsed = validateAndPreview(b);
            if (parsed.startsWith("✓")) {
                preview.setStyle("-fx-text-fill: #065f46; -fx-font-size: 11px;");
                add.setDisable(false);
            } else {
                preview.setStyle("-fx-text-fill: #b91c1c; -fx-font-size: 11px;");
                add.setDisable(true);
            }
            preview.setText(parsed);
        });

        add.setOnAction(e -> {
            String cron = cronField.getText().trim();
            if (cron.isEmpty()) return;
            try {
                Instant next = CronExpression.parse(cron)
                        .nextFireAfter(Instant.now(), ZoneId.systemDefault());
                store.create(profileId, cron, enabled.isSelected(), next);
                cronField.clear();
                preview.setText("");
                add.setDisable(true);
                refresh();
            } catch (Exception ex) {
                preview.setText("✗ " + ex.getMessage());
                preview.setStyle("-fx-text-fill: #b91c1c; -fx-font-size: 11px;");
            }
        });

        VBox fieldAndPreview = new VBox(4, cronField, preview);
        fieldAndPreview.setMinWidth(240);

        HBox row = new HBox(12, new Label("New schedule:"), fieldAndPreview, enabled, add);
        row.setAlignment(Pos.TOP_LEFT);
        HBox.setHgrow(fieldAndPreview, Priority.ALWAYS);
        return row;
    }

    /** Parses {@code expr} and, on success, returns a "✓ next: …" preview string. On failure,
     *  returns an "✗ …" explanation so the UI can highlight it in red. */
    private static String validateAndPreview(String expr) {
        if (expr == null || expr.isBlank()) return "";
        try {
            CronExpression cron = CronExpression.parse(expr);
            Instant now = Instant.now();
            StringBuilder sb = new StringBuilder("✓ next: ");
            Instant at = now;
            for (int i = 0; i < 3; i++) {
                at = cron.nextFireAfter(at, ZoneId.systemDefault());
                if (i > 0) sb.append(", ");
                sb.append(PREVIEW_FMT.format(ZonedDateTime.ofInstant(at, ZoneId.systemDefault())));
            }
            return sb.toString();
        } catch (Exception e) {
            return "✗ " + e.getMessage();
        }
    }

    private void refresh() {
        List<Schedule> mine = store.list().stream()
                .filter(s -> profileId.equals(s.profileId()))
                .toList();
        rows.setAll(mine.stream().map(ScheduleRow::from).toList());
    }

    // --- row type ----------------------------------------------------------------

    /** JavaFX TableView expects JavaBean-style getters — these are filled in once at
     *  construction since schedules don't live-update in this dialog. */
    public static final class ScheduleRow {
        private final String id;
        private final boolean enabled;
        private final SimpleStringProperty cron;
        private final SimpleStringProperty enabledLabel;
        private final SimpleStringProperty lastRun;
        private final SimpleStringProperty nextRun;

        private ScheduleRow(String id, String cron, boolean enabled,
                            String lastRun, String nextRun) {
            this.id = id;
            this.enabled = enabled;
            this.cron = new SimpleStringProperty(cron);
            this.enabledLabel = new SimpleStringProperty(enabled ? "Yes" : "No");
            this.lastRun = new SimpleStringProperty(lastRun);
            this.nextRun = new SimpleStringProperty(nextRun);
        }

        static ScheduleRow from(Schedule s) {
            return new ScheduleRow(
                    s.id(),
                    s.cron(),
                    s.enabled(),
                    fmt(s.lastRunAt()),
                    fmt(s.nextRunAt()));
        }

        private static String fmt(Instant t) {
            if (t == null) return "—";
            return PREVIEW_FMT.format(ZonedDateTime.ofInstant(t, ZoneId.systemDefault()));
        }

        // JavaFX PropertyValueFactory reflection requires JavaBean-style getters.
        public String getCron()         { return cron.get(); }
        public String getEnabledLabel() { return enabledLabel.get(); }
        public String getLastRun()      { return lastRun.get(); }
        public String getNextRun()      { return nextRun.get(); }
    }

    /** Convenience alias so the MigrationsTab can open the dialog without importing Region. */
    public static void open(Window owner, ScheduleStore store, String profileId, String profileName) {
        new ScheduleDialog(store, profileId, profileName).showAndWait(owner);
    }
}

package com.kubrik.mex.ui.migration;

import com.kubrik.mex.core.ConnectionManager;
import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.migration.JobHistoryQuery;
import com.kubrik.mex.migration.JobId;
import com.kubrik.mex.migration.MigrationJobRecord;
import com.kubrik.mex.migration.MigrationService;
import com.kubrik.mex.migration.events.JobEvent;
import com.kubrik.mex.migration.events.JobStatus;
import com.kubrik.mex.migration.store.ProfileStore;
import com.kubrik.mex.model.MongoConnection;
import com.kubrik.mex.store.ConnectionStore;
import com.kubrik.mex.ui.util.DurationFormat;
import javafx.animation.KeyFrame;
import javafx.animation.Timeline;
import javafx.application.Platform;
import javafx.beans.property.ReadOnlyObjectWrapper;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Button;
import javafx.scene.control.ContextMenu;
import javafx.scene.control.Label;
import javafx.scene.control.MenuItem;
import javafx.scene.control.Tab;
import javafx.scene.control.TabPane;
import javafx.scene.control.TableCell;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.Tooltip;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import javafx.util.Duration;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/** The top-level Migrations tab (`UX-1`). Has two sub-tabs — History and Profiles — plus a
 *  toolbar with the primary action "New Migration". */
public final class MigrationsTab extends BorderPane {

    private final MigrationService service;
    private final ConnectionStore connectionStore;
    private final ConnectionManager manager;
    private final EventBus bus;

    private final ObservableList<HistoryRow> historyRows = FXCollections.observableArrayList();
    private final ObservableList<ProfileRow> profileRows = FXCollections.observableArrayList();
    private final VBox recoveryPanel = new VBox(6);

    public MigrationsTab(MigrationService service,
                         ConnectionStore connectionStore,
                         ConnectionManager manager,
                         EventBus bus) {
        this.service = service;
        this.connectionStore = connectionStore;
        this.manager = manager;
        this.bus = bus;

        Button newBtn = new Button("New migration…");
        newBtn.setOnAction(e -> new MigrationWizard(getScene().getWindow(), service,
                connectionStore, manager, bus).show());

        Button refreshBtn = new Button("Refresh");
        refreshBtn.setOnAction(e -> refresh());

        HBox toolbar = new HBox(8, newBtn, refreshBtn);
        toolbar.setPadding(new Insets(10));
        toolbar.setAlignment(Pos.CENTER_LEFT);

        TabPane inner = new TabPane();
        inner.getTabs().addAll(buildHistoryTab(), buildProfilesTab());

        VBox header = new VBox(toolbar, recoveryPanel);
        recoveryPanel.setPadding(new Insets(0, 10, 10, 10));

        setTop(header);
        setCenter(inner);

        bus.onJob(this::onJob);
        refresh();
        refreshRecoveryPanel();
    }

    private void refreshRecoveryPanel() {
        recoveryPanel.getChildren().clear();
        var unfinished = service.unfinishedOnStartup();
        if (unfinished.isEmpty()) return;

        Label header = new Label("Unfinished migrations from a previous session — "
                + unfinished.size() + " job(s):");
        header.setStyle("-fx-font-weight: bold; -fx-text-fill: #b45309;");
        recoveryPanel.getChildren().add(header);

        for (MigrationJobRecord rec : unfinished) {
            Label summary = new Label(rec.id().value() + "  ·  " + rec.status()
                    + "  ·  " + rec.docsCopied() + " docs");
            Button resumeBtn = new Button("Resume");
            resumeBtn.setOnAction(e -> {
                try {
                    service.resume(rec.id());
                    refreshRecoveryPanel();
                    refresh();
                } catch (Exception ex) {
                    javafx.scene.control.Alert a = new javafx.scene.control.Alert(
                            javafx.scene.control.Alert.AlertType.ERROR, ex.getMessage());
                    a.setHeaderText("Resume blocked");
                    a.initOwner(getScene().getWindow());
                    a.showAndWait();
                }
            });
            Region spacer = new Region();
            HBox.setHgrow(spacer, Priority.ALWAYS);
            HBox row = new HBox(8, summary, spacer, resumeBtn);
            row.setStyle("-fx-background-color: #fffbeb; -fx-border-color: #fde68a; "
                    + "-fx-border-width: 1; -fx-padding: 8;");
            recoveryPanel.getChildren().add(row);
        }

        Button dismiss = new Button("Dismiss");
        dismiss.setOnAction(e -> recoveryPanel.getChildren().clear());
        HBox dismissRow = new HBox(dismiss);
        dismissRow.setAlignment(Pos.CENTER_RIGHT);
        recoveryPanel.getChildren().add(dismissRow);
    }

    private static final DateTimeFormatter STARTED_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());

    private TableView<HistoryRow> historyTable;

    // Pagination state for the history table. pageSize is user-selectable; totalRows is
    // updated on every refresh so prev/next can disable themselves at the boundaries.
    private static final int[] PAGE_SIZES = { 25, 50, 100, 200 };
    private int pageIndex = 0;
    private int pageSize = 50;
    private long totalRows = 0;
    private Label pageStatusLabel;
    private Button prevPageBtn;
    private Button nextPageBtn;

    private Tab buildHistoryTab() {
        TableView<HistoryRow> table = new TableView<>();
        this.historyTable = table;

        TableColumn<HistoryRow, String> idCol = new TableColumn<>("Job");
        idCol.setCellValueFactory(new PropertyValueFactory<>("id"));
        idCol.setPrefWidth(220);

        TableColumn<HistoryRow, String> kindCol = new TableColumn<>("Kind");
        kindCol.setCellValueFactory(new PropertyValueFactory<>("kind"));
        kindCol.setPrefWidth(110);

        // UX-11 Source column — resolves id → name, falling back to persisted name when the
        // connection has since been deleted.
        TableColumn<HistoryRow, HistoryRow> srcCol = new TableColumn<>("Source");
        srcCol.setCellValueFactory(r -> new ReadOnlyObjectWrapper<>(r.getValue()));
        srcCol.setCellFactory(c -> new ConnectionLabelCell(true));
        srcCol.setPrefWidth(240);

        TableColumn<HistoryRow, HistoryRow> tgtCol = new TableColumn<>("Target");
        tgtCol.setCellValueFactory(r -> new ReadOnlyObjectWrapper<>(r.getValue()));
        tgtCol.setCellFactory(c -> new ConnectionLabelCell(false));
        tgtCol.setPrefWidth(240);

        // UX-10 colored pill for JobStatus.
        TableColumn<HistoryRow, JobStatus> statusCol = new TableColumn<>("Status");
        statusCol.setCellValueFactory(r -> new ReadOnlyObjectWrapper<>(r.getValue().statusEnum));
        statusCol.setCellFactory(c -> new StatusPillCell());
        statusCol.setPrefWidth(160);

        TableColumn<HistoryRow, Long> docsCol = new TableColumn<>("Docs");
        docsCol.setCellValueFactory(new PropertyValueFactory<>("docs"));
        docsCol.setPrefWidth(100);

        // OBS-7 Started + Duration columns.
        TableColumn<HistoryRow, String> startedCol = new TableColumn<>("Started");
        startedCol.setCellValueFactory(r -> new ReadOnlyObjectWrapper<>(
                r.getValue().startedAt == null ? "—" : STARTED_FMT.format(r.getValue().startedAt)));
        startedCol.setPrefWidth(160);

        TableColumn<HistoryRow, HistoryRow> durationCol = new TableColumn<>("Duration");
        durationCol.setCellValueFactory(r -> new ReadOnlyObjectWrapper<>(r.getValue()));
        durationCol.setCellFactory(c -> new DurationCell());
        durationCol.setPrefWidth(140);

        table.getColumns().addAll(java.util.List.of(
                idCol, kindCol, srcCol, tgtCol, statusCol, docsCol, startedCol, durationCol));
        table.setItems(historyRows);
        table.setPlaceholder(new Label("No migrations yet. Click \"New migration…\" to start one."));

        // UX-12 row actions — double-click opens details; right-click exposes the same.
        table.setRowFactory(tv -> {
            javafx.scene.control.TableRow<HistoryRow> row = new javafx.scene.control.TableRow<>();
            row.setOnMouseClicked(ev -> {
                if (ev.getClickCount() == 2 && !row.isEmpty()) {
                    openJob(JobId.of(row.getItem().id));
                }
            });
            ContextMenu rowMenu = new ContextMenu();
            MenuItem open = new MenuItem("Open details");
            open.setOnAction(e -> {
                if (!row.isEmpty()) openJob(JobId.of(row.getItem().id));
            });
            MenuItem copyId = new MenuItem("Copy job id");
            copyId.setOnAction(e -> {
                if (row.isEmpty()) return;
                javafx.scene.input.ClipboardContent content = new javafx.scene.input.ClipboardContent();
                content.putString(row.getItem().id);
                javafx.scene.input.Clipboard.getSystemClipboard().setContent(content);
            });
            rowMenu.getItems().addAll(open, copyId);
            row.contextMenuProperty().bind(javafx.beans.binding.Bindings
                    .when(row.emptyProperty()).then((ContextMenu) null).otherwise(rowMenu));
            return row;
        });

        // Live tick: refresh the table every second so Duration cells on running rows advance.
        // Cheap on <100 rows; auto-stops when there are no non-terminal jobs.
        Timeline ticker = new Timeline(new KeyFrame(Duration.seconds(1), e -> {
            boolean anyLive = historyRows.stream().anyMatch(r -> !r.statusEnum.isTerminal());
            if (anyLive) table.refresh();
        }));
        ticker.setCycleCount(Timeline.INDEFINITE);
        ticker.play();

        Tab t = new Tab("History");
        t.setClosable(false);
        t.setContent(new VBox(table, buildHistoryPager()));
        VBox.setVgrow(table, Priority.ALWAYS);
        return t;
    }

    /** Footer with page-size picker, prev/next, and the "showing X–Y of N" status. */
    private HBox buildHistoryPager() {
        javafx.scene.control.ComboBox<Integer> sizeBox = new javafx.scene.control.ComboBox<>(
                FXCollections.observableArrayList(PAGE_SIZES[0], PAGE_SIZES[1], PAGE_SIZES[2], PAGE_SIZES[3]));
        sizeBox.setValue(pageSize);
        sizeBox.valueProperty().addListener((o, a, b) -> {
            if (b == null || b.equals(pageSize)) return;
            pageSize = b;
            pageIndex = 0;  // larger page size may make the current page invalid; simplest is reset
            refreshHistory();
        });

        prevPageBtn = new Button("‹ Prev");
        prevPageBtn.setOnAction(e -> {
            if (pageIndex > 0) {
                pageIndex--;
                refreshHistory();
            }
        });
        nextPageBtn = new Button("Next ›");
        nextPageBtn.setOnAction(e -> {
            if ((long) (pageIndex + 1) * pageSize < totalRows) {
                pageIndex++;
                refreshHistory();
            }
        });

        pageStatusLabel = new Label("");
        pageStatusLabel.setStyle("-fx-text-fill: #6b7280;");

        Region spacer = new Region();
        HBox.setHgrow(spacer, Priority.ALWAYS);

        HBox pager = new HBox(8,
                new Label("Rows per page:"), sizeBox,
                spacer,
                pageStatusLabel, prevPageBtn, nextPageBtn);
        pager.setPadding(new Insets(8, 10, 8, 10));
        pager.setAlignment(Pos.CENTER_LEFT);
        return pager;
    }

    /** Refresh the pager labels + button states for the current {@link #pageIndex} / {@link #pageSize}. */
    private void syncPagerChrome() {
        if (pageStatusLabel == null) return;
        if (totalRows == 0) {
            pageStatusLabel.setText("No rows");
        } else {
            long from = (long) pageIndex * pageSize + 1;
            long to = Math.min(totalRows, (long) (pageIndex + 1) * pageSize);
            pageStatusLabel.setText(from + "–" + to + " of " + totalRows);
        }
        prevPageBtn.setDisable(pageIndex == 0);
        nextPageBtn.setDisable((long) (pageIndex + 1) * pageSize >= totalRows);
    }

    /** UX-11: render {@code <Connection Name> (<id>)} with a muted suffix when the
     *  connection has since been deleted. Falls back to raw id for pre-upgrade rows. */
    private final class ConnectionLabelCell extends TableCell<HistoryRow, HistoryRow> {
        private final boolean source;
        ConnectionLabelCell(boolean source) { this.source = source; }
        @Override protected void updateItem(HistoryRow row, boolean empty) {
            super.updateItem(row, empty);
            if (empty || row == null) { setText(null); setStyle(""); setTooltip(null); return; }
            String id = source ? row.sourceId : row.targetId;
            String persisted = source ? row.sourcePersistedName : row.targetPersistedName;
            if (id == null || id.isBlank()) { setText(""); setStyle(""); setTooltip(null); return; }

            MongoConnection live;
            try { live = connectionStore.get(id); } catch (Exception ex) { live = null; }

            if (live != null) {
                setText(live.name() + " (" + id + ")");
                setStyle("");
                setTooltip(new Tooltip(live.name() + "\n" + id));
            } else if (persisted != null && !persisted.isBlank()) {
                setText(persisted + " (" + id + ") — deleted");
                setStyle("-fx-text-fill: #6b7280; -fx-font-style: italic;");
                setTooltip(new Tooltip("Connection has been deleted.\nLast known name: " + persisted));
            } else {
                setText(id);
                setStyle("-fx-text-fill: #6b7280;");
                setTooltip(new Tooltip("Connection id (pre-upgrade row — name not captured)"));
            }
        }
    }

    /** UX-10: colored pill TableCell for JobStatus. Inline CSS keeps the change
     *  self-contained; a later polish pass can move this to a stylesheet. */
    private static final class StatusPillCell extends TableCell<HistoryRow, JobStatus> {
        @Override protected void updateItem(JobStatus status, boolean empty) {
            super.updateItem(status, empty);
            if (empty || status == null) { setText(null); setGraphic(null); return; }
            Label pill = new Label(status.name());
            String base = "-fx-padding: 2 10 2 10; -fx-background-radius: 10; "
                    + "-fx-font-size: 11px; -fx-font-weight: bold; ";
            String theme = switch (status) {
                case COMPLETED               -> "-fx-background-color: #dcfce7; -fx-text-fill: #166534;";
                case COMPLETED_WITH_WARNINGS -> "-fx-background-color: #fef9c3; -fx-text-fill: #854d0e;";
                case FAILED                  -> "-fx-background-color: #fee2e2; -fx-text-fill: #991b1b;";
                case CANCELLED               -> "-fx-background-color: #e5e7eb; -fx-text-fill: #374151;";
                case RUNNING                 -> "-fx-background-color: #dbeafe; -fx-text-fill: #1e40af;";
                case PAUSED, PAUSING, CANCELLING -> "-fx-background-color: #e0f2fe; -fx-text-fill: #075985;";
                case PENDING                 -> "-fx-background-color: #f3f4f6; -fx-text-fill: #4b5563;";
            };
            pill.setStyle(base + theme);
            setGraphic(pill);
            setText(null);
        }
    }

    /** OBS-7: formatted Duration cell. Terminal rows show wall-clock (endedAt-startedAt).
     *  Non-terminal rows live-tick via the table's 1 s refresh timeline. Tooltip for
     *  terminal rows breaks down wall / active / paused. */
    private static final class DurationCell extends TableCell<HistoryRow, HistoryRow> {
        @Override protected void updateItem(HistoryRow row, boolean empty) {
            super.updateItem(row, empty);
            if (empty || row == null || row.startedAt == null) {
                setText(null); setTooltip(null); return;
            }
            Instant end = row.endedAt != null ? row.endedAt : Instant.now();
            long wallMillis = java.time.Duration.between(row.startedAt, end).toMillis();
            setText(DurationFormat.format(java.time.Duration.ofMillis(Math.max(0L, wallMillis))));
            if (row.endedAt != null) {
                setTooltip(new Tooltip(DurationFormat.breakdown(wallMillis, row.activeMillis)));
            } else {
                setTooltip(new Tooltip("running — " + DurationFormat.format(
                        java.time.Duration.ofMillis(Math.max(0L, wallMillis)))));
            }
        }
    }

    private Tab buildProfilesTab() {
        TableView<ProfileRow> table = new TableView<>();
        TableColumn<ProfileRow, String> nameCol = new TableColumn<>("Name");
        nameCol.setCellValueFactory(new PropertyValueFactory<>("name"));
        TableColumn<ProfileRow, String> kindCol = new TableColumn<>("Kind");
        kindCol.setCellValueFactory(new PropertyValueFactory<>("kind"));
        TableColumn<ProfileRow, String> srcCol = new TableColumn<>("Source → Target");
        srcCol.setCellValueFactory(new PropertyValueFactory<>("summary"));
        table.getColumns().addAll(java.util.List.of(nameCol, kindCol, srcCol));
        table.setItems(profileRows);
        table.setPlaceholder(new Label(
                "No saved profiles. Save a spec at Step 5 of the wizard to reuse it later."));

        ContextMenu menu = new ContextMenu();
        MenuItem runItem = new MenuItem("Run");
        runItem.setOnAction(e -> {
            ProfileRow row = table.getSelectionModel().getSelectedItem();
            if (row == null) return;
            service.profiles().get(row.id).ifPresent(p ->
                    MigrationWizard.openPrefilled(
                            getScene().getWindow(), service, connectionStore, manager, bus, p.spec()));
        });
        MenuItem exportItem = new MenuItem("Export…");
        exportItem.setOnAction(e -> {
            ProfileRow row = table.getSelectionModel().getSelectedItem();
            if (row == null) return;
            service.profiles().get(row.id).ifPresent(p -> exportProfile(p.name(), p.spec()));
        });
        MenuItem scheduleItem = new MenuItem("Schedule…");
        scheduleItem.setOnAction(e -> {
            ProfileRow row = table.getSelectionModel().getSelectedItem();
            if (row == null) return;
            service.profiles().get(row.id).ifPresent(p ->
                    ScheduleDialog.open(getScene().getWindow(),
                            service.schedules(), p.id(), p.name()));
        });
        MenuItem deleteItem = new MenuItem("Delete");
        deleteItem.setOnAction(e -> {
            ProfileRow row = table.getSelectionModel().getSelectedItem();
            if (row == null) return;
            service.profiles().delete(row.id);
            refreshProfiles();
        });
        menu.getItems().addAll(runItem, exportItem, scheduleItem, deleteItem);
        table.setContextMenu(menu);

        Tab t = new Tab("Profiles");
        t.setClosable(false);
        t.setContent(table);
        return t;
    }

    private void refresh() {
        refreshHistory();
        refreshProfiles();
    }

    private void refreshHistory() {
        try {
            // Compute total first so we can clamp the page index: if the user was on page 5
            // and rows were deleted down to 2 pages' worth, show the last valid page instead
            // of an empty table.
            JobHistoryQuery filters = JobHistoryQuery.all();
            long total = service.count(filters);
            int maxPage = total == 0 ? 0 : (int) ((total - 1) / pageSize);
            if (pageIndex > maxPage) pageIndex = maxPage;

            JobHistoryQuery pageQuery = filters.withPage(pageIndex, pageSize);
            var rows = service.list(pageQuery).stream()
                    .map(HistoryRow::from).toList();
            Platform.runLater(() -> {
                totalRows = total;
                historyRows.clear();
                historyRows.addAll(rows);
                syncPagerChrome();
            });
        } catch (Exception ignored) {}
    }

    private void refreshProfiles() {
        try {
            var rows = service.profiles().list().stream().map(ProfileRow::from).toList();
            Platform.runLater(() -> {
                profileRows.clear();
                profileRows.addAll(rows);
            });
        } catch (Exception ignored) {}
    }

    private void onJob(JobEvent e) {
        // Progress events fire frequently; only refresh history on status/start/end events.
        if (e instanceof JobEvent.StatusChanged
                || e instanceof JobEvent.Started
                || e instanceof JobEvent.Completed
                || e instanceof JobEvent.Failed
                || e instanceof JobEvent.Cancelled) {
            refreshHistory();
        }
    }

    // --- row types ---------------------------------------------------------------

    public static final class HistoryRow {
        private final String id, kind;
        final String sourceId, targetId;
        final String sourcePersistedName, targetPersistedName;
        final JobStatus statusEnum;
        private final long docs;
        final Instant startedAt, endedAt;
        final long activeMillis;

        HistoryRow(String id, String kind,
                   String sourceId, String targetId,
                   String sourcePersistedName, String targetPersistedName,
                   JobStatus statusEnum, long docs,
                   Instant startedAt, Instant endedAt, long activeMillis) {
            this.id = id; this.kind = kind;
            this.sourceId = sourceId; this.targetId = targetId;
            this.sourcePersistedName = sourcePersistedName;
            this.targetPersistedName = targetPersistedName;
            this.statusEnum = statusEnum; this.docs = docs;
            this.startedAt = startedAt; this.endedAt = endedAt;
            this.activeMillis = activeMillis;
        }
        static HistoryRow from(MigrationJobRecord r) {
            return new HistoryRow(
                    r.id().value(),
                    r.kind().name(),
                    r.sourceConnectionId() == null ? "" : r.sourceConnectionId(),
                    r.targetConnectionId() == null ? "" : r.targetConnectionId(),
                    r.sourceConnectionName(),
                    r.targetConnectionName(),
                    r.status(),
                    r.docsCopied(),
                    r.startedAt(),
                    r.endedAt(),
                    r.activeMillis());
        }
        public String getId() { return id; }
        public String getKind() { return kind; }
        public long getDocs() { return docs; }
    }

    public static final class ProfileRow {
        private final String id, name, kind, summary;
        ProfileRow(String id, String name, String kind, String summary) {
            this.id = id; this.name = name; this.kind = kind; this.summary = summary;
        }
        static ProfileRow from(ProfileStore.Profile p) {
            String tgtDb = p.spec().target().database();
            String summary = p.spec().source().connectionId()
                    + "  →  " + p.spec().target().connectionId()
                    + (tgtDb != null ? " / " + tgtDb : "");
            return new ProfileRow(p.id(), p.name(), p.kind().name(), summary);
        }
        public String getId() { return id; }
        public String getName() { return name; }
        public String getKind() { return kind; }
        public String getSummary() { return summary; }
    }

    private void exportProfile(String name, com.kubrik.mex.migration.spec.MigrationSpec spec) {
        javafx.stage.FileChooser fc = new javafx.stage.FileChooser();
        fc.setTitle("Export profile");
        fc.setInitialFileName(name.replaceAll("[^A-Za-z0-9._-]", "_") + ".yaml");
        fc.getExtensionFilters().addAll(
                new javafx.stage.FileChooser.ExtensionFilter("YAML", "*.yaml", "*.yml"),
                new javafx.stage.FileChooser.ExtensionFilter("JSON", "*.json"));
        java.io.File file = fc.showSaveDialog(getScene().getWindow());
        if (file == null) return;
        try {
            String body = file.getName().toLowerCase().endsWith(".json")
                    ? service.codec().toJson(spec)
                    : service.codec().toYaml(spec);
            java.nio.file.Files.writeString(file.toPath(), body);
        } catch (Exception ex) {
            javafx.scene.control.Alert a = new javafx.scene.control.Alert(
                    javafx.scene.control.Alert.AlertType.ERROR, ex.getMessage());
            a.setHeaderText("Export failed");
            a.initOwner(getScene().getWindow());
            a.showAndWait();
        }
    }

    /** UX-12 — open the Job Details view for {@code id}. Opens a modal stage by default;
     *  right-click "Open in tab" on the history row re-hosts the same node in a tab. */
    public void openJob(JobId id) {
        MigrationJobRecord rec = service.get(id).orElse(null);
        if (rec == null) return;
        JobDetailsView view = new JobDetailsView(service, bus, rec);
        javafx.stage.Stage stage = new javafx.stage.Stage();
        stage.setTitle("Migration — " + id.value());
        stage.initOwner(getScene() == null ? null : getScene().getWindow());
        stage.initModality(javafx.stage.Modality.NONE);
        stage.setScene(new javafx.scene.Scene(view, 860, 640));
        stage.show();
    }
}

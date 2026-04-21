package com.kubrik.mex.ui.backup;

import com.kubrik.mex.backup.event.BackupEvent;
import com.kubrik.mex.backup.pitr.PitrPlanner;
import com.kubrik.mex.backup.rehearse.DrRehearsalReport;
import com.kubrik.mex.backup.runner.RestoreService;
import com.kubrik.mex.backup.store.BackupCatalogDao;
import com.kubrik.mex.backup.store.BackupCatalogRow;
import com.kubrik.mex.backup.store.BackupFileDao;
import com.kubrik.mex.backup.store.BackupStatus;
import com.kubrik.mex.backup.verify.CatalogVerifier;
import com.kubrik.mex.events.EventBus;
import javafx.application.Platform;
import javafx.beans.property.SimpleObjectProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.collections.transformation.FilteredList;
import javafx.collections.transformation.SortedList;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Button;
import javafx.scene.control.ChoiceBox;
import javafx.scene.control.ContextMenu;
import javafx.scene.control.Label;
import javafx.scene.control.MenuItem;
import javafx.scene.control.TableCell;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextField;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import javafx.stage.FileChooser;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * v2.5 UI-BKP-4 — backup history / catalog pane. Loads the most-recent
 * {@link #PAGE_SIZE} rows for the selected connection on mount and
 * live-prepends rows arriving on {@link EventBus#onBackup}; filters
 * (status + free-text over policy / notes) drive a {@link FilteredList}
 * so narrowing the view doesn't hit the DAO.
 *
 * <p>Row drawer (verify / restore / manifest export) lands with Q2.5-D
 * once the {@code CatalogVerifier} exists.</p>
 */
public final class BackupHistoryPane extends BorderPane implements AutoCloseable {

    private static final int PAGE_SIZE = 500;
    private static final DateTimeFormatter TS_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());

    private final BackupCatalogDao catalog;
    private final BackupFileDao files;
    private final CatalogVerifier verifier;
    private final RestoreService restoreService;
    private final PitrPlanner pitrPlanner;
    private final DrRehearsalReport rehearsalReport;
    private final String callerUser;
    private final String callerHost;
    private final EventBus bus;
    private final EventBus.Subscription sub;
    private final SimpleObjectProperty<String> connection = new SimpleObjectProperty<>();

    private final ObservableList<BackupCatalogRow> rows = FXCollections.observableArrayList();
    private final FilteredList<BackupCatalogRow> filtered = new FilteredList<>(rows, r -> true);
    private final SortedList<BackupCatalogRow> sorted = new SortedList<>(filtered);
    private final TableView<BackupCatalogRow> table = new TableView<>(sorted);
    private final ChoiceBox<String> statusFilter = new ChoiceBox<>();
    private final TextField searchField = new TextField();
    private final Label footer = new Label("—");

    public BackupHistoryPane(BackupCatalogDao catalog, BackupFileDao files,
                             CatalogVerifier verifier, RestoreService restoreService,
                             PitrPlanner pitrPlanner, DrRehearsalReport rehearsalReport,
                             String callerUser, String callerHost, EventBus bus) {
        this.catalog = catalog;
        this.files = files;
        this.verifier = verifier;
        this.restoreService = restoreService;
        this.pitrPlanner = pitrPlanner;
        this.rehearsalReport = rehearsalReport;
        this.callerUser = callerUser;
        this.callerHost = callerHost;
        this.bus = bus;

        setStyle("-fx-background-color: white;");
        setPadding(new Insets(14, 16, 14, 16));
        setTop(buildFilterBar());
        setCenter(buildTable());
        HBox foot = new HBox(footer);
        foot.setPadding(new Insets(6, 0, 0, 0));
        setBottom(foot);

        connection.addListener((obs, o, n) -> reload());
        sub = bus.onBackup(this::onBackupEvent);
    }

    public SimpleObjectProperty<String> connectionProperty() { return connection; }

    @Override
    public void close() {
        try { sub.close(); } catch (Exception ignored) {}
    }

    /* ============================= filter bar ============================= */

    private Region buildFilterBar() {
        statusFilter.getItems().addAll("all statuses", "OK", "FAILED", "CANCELLED",
                "MISSED", "RUNNING");
        statusFilter.setValue("all statuses");
        statusFilter.valueProperty().addListener((o, a, b) -> reapplyFilter());

        searchField.setPromptText("search policy / notes");
        searchField.setPrefWidth(240);
        searchField.textProperty().addListener((o, a, b) -> reapplyFilter());

        Region grow = new Region();
        HBox.setHgrow(grow, Priority.ALWAYS);
        Button pitrBtn = new Button("PITR…");
        pitrBtn.setDisable(pitrPlanner == null);
        pitrBtn.setOnAction(e -> {
            String cx = connection.get();
            if (cx == null || pitrPlanner == null) return;
            PitrPickerDialog.show(getScene() == null ? null : getScene().getWindow(),
                    cx, pitrPlanner, restoreService, callerUser, callerHost);
        });
        Button reportBtn = new Button("Rehearsal report…");
        reportBtn.setDisable(rehearsalReport == null);
        reportBtn.setOnAction(e -> exportRehearsalReport());
        HBox row = new HBox(10, small("status"), statusFilter,
                small("search"), searchField, grow, reportBtn, pitrBtn);
        row.setAlignment(Pos.CENTER_LEFT);
        row.setPadding(new Insets(0, 0, 10, 0));
        return row;
    }

    private void reapplyFilter() {
        String st = statusFilter.getValue();
        String q = searchField.getText() == null ? "" : searchField.getText().trim().toLowerCase();
        filtered.setPredicate(r -> {
            if (st != null && !"all statuses".equals(st) && !r.status().name().equals(st))
                return false;
            if (!q.isEmpty()) {
                String blob = ((r.notes() == null ? "" : r.notes()) + " "
                        + (r.sinkPath() == null ? "" : r.sinkPath())).toLowerCase();
                if (!blob.contains(q)) return false;
            }
            return true;
        });
        updateFooter();
    }

    /* =============================== table =============================== */

    private Region buildTable() {
        table.setPlaceholder(new Label(
                "No backups recorded for this connection yet."));
        sorted.comparatorProperty().bind(table.comparatorProperty());
        table.setRowFactory(tv -> {
            var r = new javafx.scene.control.TableRow<BackupCatalogRow>();
            r.setOnMouseClicked(e -> {
                if (e.getClickCount() == 2 && !r.isEmpty()) openArtefactExplorer(r.getItem());
            });
            r.itemProperty().addListener((obs, old, item) -> {
                if (item == null) { r.setContextMenu(null); return; }
                r.setContextMenu(buildRowMenu(item));
            });
            return r;
        });

        table.getColumns().setAll(
                tsCol("started", 170, BackupCatalogRow::startedAt),
                statusCol(),
                textCol("duration", 100, r -> formatDuration(r.startedAt(), r.finishedAt())),
                textCol("bytes", 110, r -> r.totalBytes() == null ? "—"
                        : formatBytes(r.totalBytes())),
                textCol("docs", 90, r -> r.docCount() == null ? "—"
                        : String.format("%,d", r.docCount())),
                textCol("sink", 120, r -> "#" + r.sinkId()),
                textCol("path", 240, BackupCatalogRow::sinkPath),
                textCol("notes", 260, r -> r.notes() == null ? "" : r.notes())
        );
        VBox.setVgrow(table, Priority.ALWAYS);
        return table;
    }

    /* ============================== reload =============================== */

    private void reload() {
        String cx = connection.get();
        if (cx == null) { rows.clear(); updateFooter(); return; }
        Thread.startVirtualThread(() -> {
            List<BackupCatalogRow> page = catalog.listForConnection(cx, PAGE_SIZE);
            Platform.runLater(() -> {
                rows.setAll(page);
                reapplyFilter();
            });
        });
    }

    private void onBackupEvent(BackupEvent e) {
        // Load-or-upsert the matching catalog row on Started and Ended so
        // in-progress backups show up in the history with a RUNNING pill
        // instead of appearing only after finalisation. Progress is
        // frame-volume heavy and doesn't change row identity; skip it here.
        long catalogId;
        String eventCx;
        if (e instanceof BackupEvent.Started s) {
            catalogId = s.catalogId();
            eventCx = s.connectionId();
        } else if (e instanceof BackupEvent.Ended end) {
            catalogId = end.catalogId();
            eventCx = end.connectionId();
        } else {
            return;
        }
        String cx = connection.get();
        if (cx == null || !cx.equals(eventCx)) return;
        Platform.runLater(() -> catalog.byId(catalogId).ifPresent(row -> {
            int idx = -1;
            for (int i = 0; i < rows.size(); i++) {
                if (rows.get(i).id() == row.id()) { idx = i; break; }
            }
            if (idx < 0) rows.add(0, row);
            else rows.set(idx, row);
            reapplyFilter();
        }));
    }

    private void updateFooter() {
        footer.setText(rows.size() + " rows loaded · " + filtered.size()
                + " visible  ·  newest first");
    }

    private void openArtefactExplorer(BackupCatalogRow row) {
        javafx.stage.Window win = getScene() == null ? null : getScene().getWindow();
        ArtefactExplorerDialog.show(win, row, files, verifier, report -> {
            // Refresh the table row with the new verify_outcome after verify.
            catalog.byId(row.id()).ifPresent(updated -> Platform.runLater(() -> {
                for (int i = 0; i < rows.size(); i++) {
                    if (rows.get(i).id() == updated.id()) {
                        rows.set(i, updated);
                        break;
                    }
                }
            }));
        });
    }

    private void exportRehearsalReport() {
        if (rehearsalReport == null) return;
        javafx.stage.Window win = getScene() == null ? null : getScene().getWindow();
        FileChooser fc = new FileChooser();
        fc.setTitle("Export DR rehearsal report");
        fc.setInitialFileName("dr-rehearsal-report-" + Instant.now().getEpochSecond() + ".html");
        fc.getExtensionFilters().addAll(
                new FileChooser.ExtensionFilter("HTML", "*.html"),
                new FileChooser.ExtensionFilter("JSON", "*.json"));
        java.io.File out = fc.showSaveDialog(win);
        if (out == null) return;
        // 30-day window by default; the picker doesn't expose a window editor
        // in v2.5.0 — a dedicated window-picker dialog is a follow-up.
        long sinceMs = System.currentTimeMillis() - 30L * 86_400_000L;
        Thread.startVirtualThread(() -> {
            try {
                DrRehearsalReport.Bundle b = rehearsalReport.build(sinceMs, 5_000);
                java.nio.file.Path target = java.nio.file.Path.of(out.getAbsolutePath());
                if (out.getName().toLowerCase().endsWith(".json")) rehearsalReport.writeJson(b, target);
                else rehearsalReport.writeHtml(b, target);
                Platform.runLater(() -> footer.setText(
                        "Exported " + b.rowCount() + " rehearsal rows → " + out.getName()));
            } catch (Exception ex) {
                Platform.runLater(() -> footer.setText(
                        "Report export failed: " + ex.getClass().getSimpleName()
                                + " — " + ex.getMessage()));
            }
        });
    }

    private ContextMenu buildRowMenu(BackupCatalogRow row) {
        ContextMenu m = new ContextMenu();
        MenuItem verify = new MenuItem("Artefacts + verify…");
        verify.setOnAction(e -> openArtefactExplorer(row));
        MenuItem restore = new MenuItem("Restore…");
        restore.setDisable(restoreService == null || row.status() != BackupStatus.OK);
        if (row.status() != BackupStatus.OK) {
            restore.setText("Restore…  (only available for OK backups)");
        }
        restore.setOnAction(e -> {
            javafx.stage.Window win = getScene() == null ? null : getScene().getWindow();
            RestoreWizardDialog.show(win, row, restoreService, callerUser, callerHost);
        });
        m.getItems().addAll(verify, restore);
        return m;
    }

    /* ============================= helpers =============================== */

    private static TableColumn<BackupCatalogRow, String> textCol(String title, int width,
                                                                  java.util.function.Function<BackupCatalogRow, String> getter) {
        TableColumn<BackupCatalogRow, String> c = new TableColumn<>(title);
        c.setPrefWidth(width);
        c.setCellValueFactory(cd -> new SimpleStringProperty(getter.apply(cd.getValue())));
        return c;
    }

    private static TableColumn<BackupCatalogRow, String> tsCol(String title, int width,
                                                                java.util.function.ToLongFunction<BackupCatalogRow> getter) {
        TableColumn<BackupCatalogRow, String> c = new TableColumn<>(title);
        c.setPrefWidth(width);
        c.setCellValueFactory(cd -> new SimpleStringProperty(
                TS_FMT.format(Instant.ofEpochMilli(getter.applyAsLong(cd.getValue())))));
        c.setSortType(TableColumn.SortType.DESCENDING);
        return c;
    }

    private static TableColumn<BackupCatalogRow, BackupStatus> statusCol() {
        TableColumn<BackupCatalogRow, BackupStatus> c = new TableColumn<>("status");
        c.setPrefWidth(110);
        c.setCellValueFactory(cd ->
                new javafx.beans.property.SimpleObjectProperty<>(cd.getValue().status()));
        c.setCellFactory(col -> new TableCell<>() {
            @Override protected void updateItem(BackupStatus s, boolean empty) {
                super.updateItem(s, empty);
                if (empty || s == null) { setText(""); setStyle(""); return; }
                setText(s.name());
                setStyle(statusStyle(s));
            }
        });
        return c;
    }

    private static String statusStyle(BackupStatus s) {
        String fg = switch (s) {
            case OK -> "#166534";
            case FAILED -> "#991b1b";
            case CANCELLED -> "#374151";
            case MISSED -> "#92400e";
            case RUNNING -> "#2563eb";
        };
        return "-fx-text-fill: " + fg + "; -fx-font-weight: 700;";
    }

    private static Label small(String s) {
        Label l = new Label(s);
        l.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px;");
        return l;
    }

    private static String formatDuration(long startedAt, Long finishedAt) {
        if (finishedAt == null) return "—";
        Duration d = Duration.ofMillis(finishedAt - startedAt);
        if (d.isNegative()) return "—";
        long secs = d.getSeconds();
        if (secs < 60) return secs + " s";
        if (secs < 3600) return (secs / 60) + " m " + (secs % 60) + " s";
        return (secs / 3600) + " h " + ((secs % 3600) / 60) + " m";
    }

    private static String formatBytes(long b) {
        double gb = b / (1024.0 * 1024 * 1024);
        if (gb >= 1) return String.format("%.1f GB", gb);
        double mb = b / (1024.0 * 1024);
        if (mb >= 1) return String.format("%.1f MB", mb);
        return b + " B";
    }
}

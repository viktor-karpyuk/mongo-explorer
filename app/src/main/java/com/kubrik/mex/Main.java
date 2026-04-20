package com.kubrik.mex;

import atlantafx.base.theme.PrimerLight;
import com.kubrik.mex.cluster.ClusterWiring;
import com.kubrik.mex.cluster.service.ClusterTopologyService;
import com.kubrik.mex.cluster.store.TopologySnapshotDao;
import com.kubrik.mex.core.ConnectionManager;
import com.kubrik.mex.core.Crypto;
import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.migration.MigrationService;
import com.kubrik.mex.migration.gate.PreconditionGate;
import com.kubrik.mex.migration.schedule.MigrationScheduler;
import com.kubrik.mex.migration.sink.PluginLoader;
import com.kubrik.mex.store.AppPaths;
import com.kubrik.mex.monitoring.MonitoringService;
import com.kubrik.mex.monitoring.MonitoringWiring;
import com.kubrik.mex.monitoring.recording.RecordingCaptureSubscriber;
import com.kubrik.mex.monitoring.recording.RecordingService;
import com.kubrik.mex.monitoring.recording.store.RecordingProfileSampleDao;
import com.kubrik.mex.monitoring.recording.store.RecordingSampleDao;
import com.kubrik.mex.store.ConnectionStore;
import com.kubrik.mex.store.Database;
import com.kubrik.mex.store.HistoryStore;
import com.kubrik.mex.ui.MainView;
import javafx.application.Application;
import javafx.application.Platform;
import javafx.scene.Scene;
import javafx.stage.Stage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;

public class Main extends Application {

    static {
        String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        String home = System.getProperty("user.home");
        String dir;
        if (os.contains("mac")) {
            dir = home + "/Library/Logs/MongoExplorer";
        } else if (os.contains("win")) {
            String local = System.getenv("LOCALAPPDATA");
            dir = (local != null ? local : home) + "/MongoExplorer/logs";
        } else {
            String xdg = System.getenv("XDG_STATE_HOME");
            dir = (xdg != null ? xdg : home + "/.local/state") + "/mongo-explorer";
        }
        System.setProperty("LOG_DIR", dir);
    }

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private Database db;
    private ConnectionManager connectionManager;
    private MigrationService migrationService;
    private MigrationScheduler migrationScheduler;
    private MonitoringService monitoringService;
    private MonitoringWiring monitoringWiring;
    private RecordingService recordingService;
    private RecordingCaptureSubscriber recordingCapture;
    private ClusterTopologyService clusterTopologyService;
    private ClusterWiring clusterWiring;

    @Override
    public void start(Stage stage) throws Exception {
        Application.setUserAgentStylesheet(new PrimerLight().getUserAgentStylesheet());

        db = new Database();

        // EXT-1 — load sink plugins once, before anything touches the sink registry.
        // Missing plugins dir is a no-op; loader failures are logged, not fatal.
        PluginLoader.loadFrom(AppPaths.pluginsDir());

        ConnectionStore connectionStore = new ConnectionStore(db);
        HistoryStore historyStore = new HistoryStore(db);
        EventBus eventBus = new EventBus();
        Crypto crypto = new Crypto();
        connectionManager = new ConnectionManager(connectionStore, eventBus, crypto);

        PreconditionGate preconditionGate = new PreconditionGate(connectionStore, connectionManager);
        migrationService = new MigrationService(connectionManager, connectionStore, db, eventBus, preconditionGate);

        // UX-7 — local scheduler. Wakes every 30s, dispatches due profiles via
        // MigrationService.start. Local-only — the app must be running for schedules to fire.
        migrationScheduler = new MigrationScheduler(
                migrationService.schedules(), migrationService.profiles(),
                spec -> migrationService.start(spec));
        migrationScheduler.start();

        // v2.1.0 — monitoring subsystem. Lifecycle is managed here so samplers stop
        // cleanly before the Mongo client / Database close in stop().
        monitoringService = new MonitoringService(db, eventBus);
        monitoringService.startBackgroundWorkers();
        monitoringWiring = new MonitoringWiring(monitoringService, connectionManager, eventBus);

        // v2.3.0 — recording subsystem. RecordingService owns lifecycle + auto-stop tick;
        // RecordingCaptureSubscriber double-writes EventBus metric/profiler samples into
        // the recording tables for any active recording. Auto-stop on disconnect uses the
        // ConnectionManager so REC-AUTO-2 fires accurately in production.
        recordingService = new RecordingService(db, eventBus, java.time.Clock.systemUTC(),
                id -> connectionManager.service(id) != null,
                () -> RecordingService.DEFAULT_STORAGE_CAP_BYTES);
        recordingService.init();
        recordingCapture = new RecordingCaptureSubscriber(
                new RecordingSampleDao(db.connection()),
                new RecordingProfileSampleDao(db.connection()),
                eventBus);

        // v2.4 — cluster topology sampler. Lives outside MonitoringWiring because
        // its cadence (300 ms heartbeat / 2 s visible) and consumers (Cluster tab,
        // connection card health pill) are orthogonal to monitoring samplers.
        clusterTopologyService = new ClusterTopologyService(
                connectionManager::service,
                new TopologySnapshotDao(db),
                eventBus,
                java.time.Clock.systemUTC());
        clusterWiring = new ClusterWiring(clusterTopologyService, connectionManager, eventBus);

        MainView root = new MainView(connectionManager, connectionStore, historyStore, eventBus,
                migrationService, monitoringService, db);

        // If a previous session left unfinished migrations behind, surface the recovery panel
        // as soon as the UI is up. See docs/mvp-functional-spec.md §4.6.
        if (!migrationService.unfinishedOnStartup().isEmpty()) {
            Platform.runLater(root::openMigrationsTabPublic);
        }
        Scene scene = new Scene(root, 1200, 750);
        stage.setTitle("Mongo Explorer");
        try {
            stage.getIcons().add(new javafx.scene.image.Image(
                    getClass().getResourceAsStream("/icons/app_1024.png")));
        } catch (Exception ignored) {}
        stage.setScene(scene);

        stage.setOnCloseRequest(e -> {
            e.consume();
            requestQuit(stage);
        });
        stage.show();
    }

    private void requestQuit(Stage stage) {
        javafx.scene.control.Dialog<String> d = new javafx.scene.control.Dialog<>();
        d.initOwner(stage);
        d.initModality(javafx.stage.Modality.APPLICATION_MODAL);
        d.setTitle("Mongo Explorer");

        org.kordamp.ikonli.javafx.FontIcon icon = new org.kordamp.ikonli.javafx.FontIcon("fth-power");
        icon.setIconSize(36);
        icon.setIconColor(javafx.scene.paint.Color.web("#dc2626"));

        javafx.scene.control.Label title = new javafx.scene.control.Label("Quit Mongo Explorer?");
        title.setStyle("-fx-font-size: 16px; -fx-font-weight: bold;");
        javafx.scene.control.Label sub = new javafx.scene.control.Label(
                "All open Mongo connections will be closed.");
        sub.setStyle("-fx-text-fill: #6b7280;");
        sub.setWrapText(true);

        javafx.scene.layout.VBox texts = new javafx.scene.layout.VBox(4, title, sub);
        javafx.scene.layout.HBox header = new javafx.scene.layout.HBox(16, icon, texts);
        header.setAlignment(javafx.geometry.Pos.CENTER_LEFT);
        header.setPadding(new javafx.geometry.Insets(20, 24, 16, 24));
        header.setPrefWidth(440);

        d.getDialogPane().setContent(header);
        d.getDialogPane().setStyle("-fx-background-color: white;");

        javafx.scene.control.ButtonType quit =
                new javafx.scene.control.ButtonType("Quit", javafx.scene.control.ButtonBar.ButtonData.OK_DONE);
        javafx.scene.control.ButtonType cancel =
                new javafx.scene.control.ButtonType("Cancel", javafx.scene.control.ButtonBar.ButtonData.CANCEL_CLOSE);
        d.getDialogPane().getButtonTypes().setAll(cancel, quit);

        javafx.scene.control.Button quitBtn =
                (javafx.scene.control.Button) d.getDialogPane().lookupButton(quit);
        quitBtn.setStyle("-fx-background-color: #dc2626; -fx-text-fill: white; -fx-font-weight: bold;");

        d.setResultConverter(bt -> bt == null ? null : bt.getText());
        d.showAndWait().ifPresent(r -> {
            if ("Quit".equals(r)) Platform.exit();
        });
    }

    @Override
    public void stop() {
        log.info("shutting down");
        // Halt immediately — cleanup happens in the background.
        // MongoClient.close() and driver shutdown hooks can stall for seconds;
        // halt(0) bypasses all of that so the app exits instantly.
        Thread cleanup = new Thread(() -> {
            try { if (migrationScheduler != null) migrationScheduler.close(); } catch (Exception ignored) {}
            try { if (recordingCapture != null) recordingCapture.close(); } catch (Exception ignored) {}
            try { if (recordingService != null) recordingService.close(); } catch (Exception ignored) {}
            try { if (clusterWiring != null) clusterWiring.close(); } catch (Exception ignored) {}
            try { if (clusterTopologyService != null) clusterTopologyService.close(); } catch (Exception ignored) {}
            try { if (monitoringWiring != null) monitoringWiring.close(); } catch (Exception ignored) {}
            try { if (monitoringService != null) monitoringService.close(); } catch (Exception ignored) {}
            try { if (connectionManager != null) connectionManager.closeAll(); } catch (Exception ignored) {}
            try { if (db != null) db.close(); } catch (Exception ignored) {}
        }, "shutdown-cleanup");
        cleanup.setDaemon(true);
        cleanup.start();
        Runtime.getRuntime().halt(0);
    }

    public static void main(String[] args) { launch(args); }
}

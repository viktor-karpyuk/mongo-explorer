package com.kubrik.mex;

import atlantafx.base.theme.PrimerLight;
import com.kubrik.mex.core.ConnectionManager;
import com.kubrik.mex.core.Crypto;
import com.kubrik.mex.events.EventBus;
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

    @Override
    public void start(Stage stage) throws Exception {
        Application.setUserAgentStylesheet(new PrimerLight().getUserAgentStylesheet());

        db = new Database();
        ConnectionStore connectionStore = new ConnectionStore(db);
        HistoryStore historyStore = new HistoryStore(db);
        EventBus eventBus = new EventBus();
        Crypto crypto = new Crypto();
        connectionManager = new ConnectionManager(connectionStore, eventBus, crypto);

        MainView root = new MainView(connectionManager, connectionStore, historyStore, eventBus);
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
            try { if (connectionManager != null) connectionManager.closeAll(); } catch (Exception ignored) {}
            try { if (db != null) db.close(); } catch (Exception ignored) {}
        }, "shutdown-cleanup");
        cleanup.setDaemon(true);
        cleanup.start();
        Runtime.getRuntime().halt(0);
    }

    public static void main(String[] args) { launch(args); }
}

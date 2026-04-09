package com.kubrik.mex.ui;

import com.kubrik.mex.events.EventBus;
import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TextArea;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

/** Tail of EventBus state + log events. */
public class LogsView extends VBox {

    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    private final TextArea area = new TextArea();

    public LogsView(EventBus events) {
        setPadding(new Insets(12));
        setSpacing(8);

        Label title = new Label("Connection log");
        title.setStyle("-fx-font-size: 16px; -fx-font-weight: bold;");
        Button clear = new Button("Clear");
        clear.setOnAction(e -> area.clear());
        Region sp = new Region();
        HBox.setHgrow(sp, Priority.ALWAYS);
        HBox toolbar = new HBox(8, title, sp, clear);

        area.setEditable(false);
        area.setWrapText(true);
        area.setStyle("-fx-font-family: 'Menlo','Monaco',monospace; -fx-font-size: 12px;");
        VBox.setVgrow(area, Priority.ALWAYS);

        getChildren().addAll(toolbar, area);

        events.onLog((id, line) -> append(id + "  " + line));
        events.onState(s -> append(s.connectionId() + "  state=" + s.status()
                + (s.serverVersion() != null ? " v=" + s.serverVersion() : "")
                + (s.lastError() != null ? "  error=" + s.lastError() : "")));
    }

    private void append(String line) {
        String stamp = FMT.format(LocalTime.now());
        Platform.runLater(() -> area.appendText(stamp + "  " + line + "\n"));
    }
}

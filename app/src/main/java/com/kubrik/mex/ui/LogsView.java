package com.kubrik.mex.ui;

import com.kubrik.mex.events.EventBus;
import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TextArea;
import javafx.scene.control.ToggleButton;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * Tail of EventBus state + log + v2.4 ops-audit events. The "Audit only" toggle
 * narrows the view to destructive-action rows without dropping the feed — the
 * underlying buffer keeps everything, so flipping the toggle off restores the
 * full history in place.
 */
public class LogsView extends VBox {

    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    /** Tag prepended to ops-audit lines in the buffer. Used by the filter toggle. */
    private static final String AUDIT_TAG = "audit";

    private final TextArea area = new TextArea();
    private final ToggleButton auditOnly = new ToggleButton("Audit only");
    private final List<String> buffer = new ArrayList<>();

    public LogsView(EventBus events) {
        setPadding(new Insets(12));
        setSpacing(8);

        Label title = new Label("Connection log");
        title.setStyle("-fx-font-size: 16px; -fx-font-weight: bold;");
        Button clear = new Button("Clear");
        clear.setOnAction(e -> { buffer.clear(); area.clear(); });
        auditOnly.setOnAction(e -> refresh());
        Region sp = new Region();
        HBox.setHgrow(sp, Priority.ALWAYS);
        HBox toolbar = new HBox(8, title, sp, auditOnly, clear);

        area.setEditable(false);
        area.setWrapText(true);
        area.setStyle("-fx-font-family: 'Menlo','Monaco',monospace; -fx-font-size: 12px;");
        VBox.setVgrow(area, Priority.ALWAYS);

        getChildren().addAll(toolbar, area);

        events.onLog((id, line) -> append(id + "  " + line));
        events.onState(s -> append(s.connectionId() + "  state=" + s.status()
                + (s.serverVersion() != null ? " v=" + s.serverVersion() : "")
                + (s.lastError() != null ? "  error=" + s.lastError() : "")));
        events.onOpsAudit(r -> append(r.connectionId() + "  " + AUDIT_TAG
                + "  " + r.commandName()
                + "  " + r.outcome()
                + (r.serverMessage() == null ? "" : "  — " + r.serverMessage())));
    }

    /** v2.4 UI-OPS-8 — engage the "Audit only" toggle from the keyboard
     *  accelerator. No-op when already engaged. */
    public void filterToAudit() {
        if (!auditOnly.isSelected()) {
            auditOnly.setSelected(true);
            refresh();
        }
    }

    private void append(String line) {
        String stamp = FMT.format(LocalTime.now());
        String stamped = stamp + "  " + line;
        synchronized (buffer) { buffer.add(stamped); }
        Platform.runLater(() -> {
            if (!auditOnly.isSelected() || line.contains("  " + AUDIT_TAG + "  ")) {
                area.appendText(stamped + "\n");
            }
        });
    }

    private void refresh() {
        Platform.runLater(() -> {
            StringBuilder sb = new StringBuilder();
            synchronized (buffer) {
                for (String line : buffer) {
                    if (!auditOnly.isSelected() || line.contains("  " + AUDIT_TAG + "  ")) {
                        sb.append(line).append('\n');
                    }
                }
            }
            area.setText(sb.toString());
            area.positionCaret(area.getLength());
        });
    }
}

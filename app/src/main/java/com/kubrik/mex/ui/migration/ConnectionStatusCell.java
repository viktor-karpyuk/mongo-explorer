package com.kubrik.mex.ui.migration;

import com.kubrik.mex.core.ConnectionManager;
import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.model.ConnectionState;
import com.kubrik.mex.model.MongoConnection;
import javafx.application.Platform;
import javafx.scene.control.ListCell;
import javafx.scene.control.Tooltip;
import javafx.scene.layout.HBox;
import javafx.scene.paint.Color;
import javafx.scene.shape.Circle;
import javafx.scene.text.Text;

/** UX-9 — renders a MongoConnection row as a colored status dot + plain name.
 *  Subscribes to the {@link EventBus} for {@link ConnectionState} changes and repaints the
 *  dot live when the currently-shown connection's state changes. */
public final class ConnectionStatusCell extends ListCell<MongoConnection> {

    private final ConnectionManager manager;
    private final Circle dot = new Circle(5);
    private final Text nameLabel = new Text();
    private final HBox row = new HBox(8, dot, nameLabel);
    private final Tooltip tip = new Tooltip();

    public ConnectionStatusCell(ConnectionManager manager, EventBus bus) {
        this.manager = manager;
        row.setStyle("-fx-alignment: center-left;");
        setTooltip(tip);

        bus.onState(s -> {
            MongoConnection cur = getItem();
            if (cur == null || !cur.id().equals(s.connectionId())) return;
            Platform.runLater(this::refreshStatus);
        });
    }

    @Override
    protected void updateItem(MongoConnection item, boolean empty) {
        super.updateItem(item, empty);
        if (empty || item == null) { setGraphic(null); setText(null); return; }
        nameLabel.setText(item.name());
        refreshStatus();
        setGraphic(row);
        setText(null);
    }

    private void refreshStatus() {
        MongoConnection c = getItem();
        if (c == null) return;
        ConnectionState state = manager.state(c.id());
        dot.setFill(colorFor(state.status()));
        String tipText = switch (state.status()) {
            case CONNECTED -> "connected (mongo " + state.serverVersion() + ")";
            case CONNECTING -> "connecting…";
            case ERROR -> "error: " + (state.lastError() == null ? "unknown" : state.lastError());
            case DISCONNECTED -> "not connected";
        };
        tip.setText(tipText);
    }

    private static Color colorFor(ConnectionState.Status status) {
        return switch (status) {
            case CONNECTED -> Color.web("#16a34a");
            case CONNECTING -> Color.web("#d97706");
            case ERROR -> Color.web("#dc2626");
            case DISCONNECTED -> Color.web("#9ca3af");
        };
    }
}

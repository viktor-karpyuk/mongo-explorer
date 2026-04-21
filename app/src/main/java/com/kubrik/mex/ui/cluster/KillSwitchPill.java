package com.kubrik.mex.ui.cluster;

import com.kubrik.mex.cluster.safety.KillSwitch;
import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Alert;
import javafx.scene.control.ButtonType;
import javafx.scene.control.Label;
import javafx.scene.control.Tooltip;
import javafx.scene.layout.HBox;
import org.kordamp.ikonli.javafx.FontIcon;

/**
 * v2.4 SAFE-OPS-8 — top-level UI toggle for the process-wide kill-switch.
 * Lives in the status bar so it's visible on every tab. Engaging requires a
 * confirmation dialog (the spec's explicit no-accidents rule); disengaging
 * is free. Subscribes to {@link KillSwitch#onChange} so the pill updates
 * regardless of how the flag flipped (tests, future automation, etc.).
 */
public final class KillSwitchPill extends HBox implements AutoCloseable {

    private final KillSwitch killSwitch;
    private final Label text = new Label("kill-switch off");
    private final FontIcon icon = new FontIcon("fth-shield");
    private final AutoCloseable sub;

    public KillSwitchPill(KillSwitch killSwitch) {
        this.killSwitch = killSwitch;

        icon.setIconSize(12);
        getChildren().setAll(icon, text);
        setAlignment(Pos.CENTER);
        setSpacing(6);
        setPadding(new Insets(2, 10, 2, 10));

        setOnMouseClicked(e -> onClick());
        setCursor(javafx.scene.Cursor.HAND);

        this.sub = killSwitch.onChange(engaged ->
                Platform.runLater(() -> render(engaged)));
        Tooltip.install(this, new Tooltip(
                "Kill-switch: hides every destructive action across the app.\n"
                        + "Engaging requires confirmation; disengaging is free."));
    }

    @Override
    public void close() {
        try { sub.close(); } catch (Exception ignored) {}
    }

    private void onClick() {
        if (killSwitch.isEngaged()) {
            killSwitch.disengage();
            return;
        }
        Alert a = new Alert(Alert.AlertType.CONFIRMATION);
        a.initOwner(getScene() == null ? null : getScene().getWindow());
        a.setTitle("Engage kill-switch");
        a.setHeaderText("Block every destructive cluster action?");
        a.setContentText("Step-down, freeze, killOp, moveChunk, balancer commands, "
                + "and tag-range edits are refused while engaged. Existing audit "
                + "history is unaffected. You can flip it off any time.");
        a.getButtonTypes().setAll(ButtonType.CANCEL,
                new ButtonType("Engage", javafx.scene.control.ButtonBar.ButtonData.OK_DONE));
        a.showAndWait().ifPresent(bt -> {
            if (bt.getButtonData() == javafx.scene.control.ButtonBar.ButtonData.OK_DONE) {
                killSwitch.engage();
            }
        });
    }

    private void render(boolean engaged) {
        text.setText(engaged ? "kill-switch ENGAGED" : "kill-switch off");
        if (engaged) {
            icon.setIconColor(javafx.scene.paint.Color.web("#991b1b"));
            setStyle("-fx-background-color: #fee2e2; -fx-background-radius: 999; "
                    + "-fx-border-color: #fca5a5; -fx-border-radius: 999; "
                    + "-fx-border-width: 1;");
            text.setStyle("-fx-text-fill: #991b1b; -fx-font-weight: 700; -fx-font-size: 11px;");
        } else {
            icon.setIconColor(javafx.scene.paint.Color.web("#6b7280"));
            setStyle("-fx-background-color: transparent; -fx-background-radius: 999; "
                    + "-fx-border-color: #d1d5db; -fx-border-radius: 999; "
                    + "-fx-border-width: 1;");
            text.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px;");
        }
    }
}

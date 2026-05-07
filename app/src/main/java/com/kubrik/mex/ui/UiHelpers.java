package com.kubrik.mex.ui;

import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.ButtonType;
import javafx.scene.control.Label;
import javafx.scene.control.PasswordField;
import javafx.scene.control.TextField;
import javafx.scene.control.TextInputDialog;
import javafx.scene.control.Tooltip;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.stage.Window;
import org.kordamp.ikonli.javafx.FontIcon;

import java.util.Optional;

public final class UiHelpers {
    private UiHelpers() {}

    /**
     * Decorated, window-modal "Connecting…" dialog with a spinner, a
     * status label, and a Cancel button. Uses {@code Stage.show()} (NOT
     * showAndWait) so the FX thread continues to dispatch — the spinner
     * animates and the Cancel click fires while the underlying connect
     * runs on a virtual thread.
     *
     * <p>The caller closes the returned {@link ConnectingHandle} when
     * the connect terminates (success or failure); the Cancel button
     * invokes the {@code onCancel} runnable then closes the stage.
     */
    public static ConnectingHandle connectingDialog(Window owner, String label, Runnable onCancel) {
        javafx.scene.control.ProgressIndicator pi = new javafx.scene.control.ProgressIndicator();
        pi.setPrefSize(28, 28);
        Label header = new Label("Connecting to " + label + "…");
        header.setStyle("-fx-font-size: 14px; -fx-font-weight: bold; -fx-text-fill: #1f2937;");
        Label status = new Label("Resolving server and negotiating…");
        status.setStyle("-fx-font-size: 12px; -fx-text-fill: #6b7280;");
        status.setWrapText(true);
        status.setMaxWidth(360);
        javafx.scene.layout.VBox texts = new javafx.scene.layout.VBox(4, header, status);

        HBox row = new HBox(14, pi, texts);
        row.setAlignment(javafx.geometry.Pos.CENTER_LEFT);

        javafx.scene.control.Button cancel = new javafx.scene.control.Button("Cancel");
        cancel.setStyle("-fx-background-color: white; -fx-text-fill: #1f2937; -fx-border-color: #d1d5db; "
                + "-fx-border-radius: 4; -fx-background-radius: 4; -fx-padding: 6 14 6 14;");
        Region sp = new Region();
        HBox.setHgrow(sp, Priority.ALWAYS);
        HBox footer = new HBox(8, sp, cancel);

        javafx.scene.layout.VBox content = new javafx.scene.layout.VBox(16, row, footer);
        content.setPadding(new javafx.geometry.Insets(20, 22, 16, 22));
        content.setStyle("-fx-background-color: white;");
        content.setMinWidth(420);

        javafx.scene.Scene sc = new javafx.scene.Scene(content);
        javafx.stage.Stage st = new javafx.stage.Stage(javafx.stage.StageStyle.UTILITY);
        st.setTitle("Connecting");
        st.initModality(javafx.stage.Modality.WINDOW_MODAL);
        if (owner != null) st.initOwner(owner);
        st.setScene(sc);
        st.setResizable(false);

        ConnectingHandle handle = new ConnectingHandle(st, status);
        cancel.setOnAction(e -> {
            if (handle.closed.compareAndSet(false, true)) {
                if (onCancel != null) {
                    try { onCancel.run(); } catch (Exception ignored) {}
                }
                st.close();
            }
        });
        // Closing the window via OS chrome (× / ESC) behaves like Cancel.
        st.setOnCloseRequest(e -> {
            if (handle.closed.compareAndSet(false, true)) {
                if (onCancel != null) {
                    try { onCancel.run(); } catch (Exception ignored) {}
                }
            }
        });
        return handle;
    }

    /** Returned by {@link #connectingDialog}; lets the caller close the
     *  dialog cleanly when the connect terminates. {@link #close()} is
     *  idempotent and safe to call from any thread (re-posts to FX). */
    public static final class ConnectingHandle {
        private final javafx.stage.Stage stage;
        private final Label statusLabel;
        private final java.util.concurrent.atomic.AtomicBoolean closed =
                new java.util.concurrent.atomic.AtomicBoolean(false);
        private ConnectingHandle(javafx.stage.Stage stage, Label statusLabel) {
            this.stage = stage;
            this.statusLabel = statusLabel;
        }
        public void show() {
            if (javafx.application.Platform.isFxApplicationThread()) stage.show();
            else javafx.application.Platform.runLater(stage::show);
        }
        public void setStatus(String text) {
            javafx.application.Platform.runLater(() -> statusLabel.setText(text));
        }
        public void close() {
            if (!closed.compareAndSet(false, true)) return;
            javafx.application.Platform.runLater(() -> {
                if (stage.isShowing()) stage.close();
            });
        }
    }

    /**
     * Bottom-right transient notification that auto-dismisses after
     * {@code millis}. Non-modal; doesn't steal focus. Safe no-op if the
     * owner is null or has no scene yet.
     */
    public static void toast(Window owner, String message, long millis) {
        if (owner == null) return;
        Label l = new Label(message);
        l.setStyle("-fx-text-fill: white; -fx-font-size: 13px; -fx-padding: 10 16 10 16; "
                + "-fx-background-color: #16a34a; -fx-background-radius: 6;");
        javafx.stage.Popup p = new javafx.stage.Popup();
        p.setAutoFix(true);
        p.getContent().add(l);
        // Anchor to bottom-right of the owner window with a small margin.
        double x = owner.getX() + owner.getWidth() - 360;
        double y = owner.getY() + owner.getHeight() - 80;
        p.show(owner, Math.max(0, x), Math.max(0, y));
        javafx.animation.PauseTransition pt =
                new javafx.animation.PauseTransition(javafx.util.Duration.millis(millis));
        pt.setOnFinished(e -> p.hide());
        pt.play();
    }

    public static Button iconButton(String iconLiteral, String tooltip) {
        FontIcon icon = new FontIcon(iconLiteral);
        icon.setIconSize(16);
        icon.setIconColor(javafx.scene.paint.Color.web("#374151"));
        Button b = new Button();
        b.setGraphic(icon);
        b.setTooltip(new Tooltip(tooltip));
        String idle = "-fx-background-color: transparent; -fx-padding: 4 8 4 8; -fx-background-radius: 4;";
        String hover = "-fx-background-color: #e5e7eb; -fx-padding: 4 8 4 8; -fx-background-radius: 4;";
        b.setStyle(idle);
        b.setOnMouseEntered(e -> b.setStyle(hover));
        b.setOnMouseExited(e -> b.setStyle(idle));
        return b;
    }

    public static Label statusDot(String color) {
        Label l = new Label("●");
        l.setStyle("-fx-text-fill: " + color + "; -fx-font-size: 14px;");
        return l;
    }

    public static String colorFor(com.kubrik.mex.model.ConnectionState.Status s) {
        return switch (s) {
            case CONNECTED -> "#16a34a";
            case CONNECTING -> "#d97706";
            case ERROR -> "#dc2626";
            case DISCONNECTED -> "#9ca3af";
        };
    }

    public static boolean confirm(Window owner, String message) {
        Alert a = new Alert(Alert.AlertType.CONFIRMATION, message, ButtonType.OK, ButtonType.CANCEL);
        a.initOwner(owner);
        return a.showAndWait().filter(b -> b == ButtonType.OK).isPresent();
    }

    public static boolean confirmTyped(Window owner, String name) {
        TextInputDialog d = new TextInputDialog();
        d.initOwner(owner);
        d.setTitle("Confirm destructive action");
        d.setHeaderText("Type \"" + name + "\" to confirm");
        d.setContentText("Name:");
        Optional<String> r = d.showAndWait();
        return r.isPresent() && name.equals(r.get());
    }

    public static void error(Window owner, String message) {
        Alert a = new Alert(Alert.AlertType.ERROR, message, ButtonType.OK);
        a.initOwner(owner);
        a.showAndWait();
    }

    /** Wraps a PasswordField with a sibling TextField and an eye toggle to preview the value. */
    public static HBox passwordWithEye(PasswordField pf) {
        TextField visible = new TextField();
        visible.setManaged(false);
        visible.setVisible(false);
        visible.promptTextProperty().bind(pf.promptTextProperty());
        // Two-way text binding
        visible.textProperty().bindBidirectional(pf.textProperty());

        FontIcon icon = new FontIcon("fth-eye");
        icon.setIconSize(14);
        icon.setIconColor(javafx.scene.paint.Color.web("#374151"));
        Button toggle = new Button();
        toggle.setGraphic(icon);
        toggle.setTooltip(new Tooltip("Show password"));
        toggle.setStyle("-fx-background-color: transparent; -fx-padding: 4 8 4 8;");
        toggle.setFocusTraversable(false);
        toggle.setOnAction(e -> {
            boolean show = !visible.isVisible();
            visible.setVisible(show);
            visible.setManaged(show);
            pf.setVisible(!show);
            pf.setManaged(!show);
            icon.setIconLiteral(show ? "fth-eye-off" : "fth-eye");
            toggle.getTooltip().setText(show ? "Hide password" : "Show password");
        });

        HBox box = new HBox(4, pf, visible, toggle);
        HBox.setHgrow(pf, Priority.ALWAYS);
        HBox.setHgrow(visible, Priority.ALWAYS);
        pf.setMaxWidth(Double.MAX_VALUE);
        visible.setMaxWidth(Double.MAX_VALUE);
        return box;
    }

    public static Optional<String> styledInput(Window owner, String title, String subtitle,
                                                String fieldLabel, String initialValue) {
        javafx.scene.control.Dialog<String> d = new javafx.scene.control.Dialog<>();
        d.initOwner(owner);
        d.setTitle(title);

        Label header = new Label(title);
        header.setStyle("-fx-font-size: 16px; -fx-font-weight: bold;");
        Label sub = new Label(subtitle);
        sub.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 12px;");
        sub.setWrapText(true);

        Label lbl = new Label(fieldLabel);
        lbl.setStyle("-fx-text-fill: #374151; -fx-font-size: 13px;");
        TextField input = new TextField(initialValue == null ? "" : initialValue);
        input.setPromptText(fieldLabel);
        input.setStyle("-fx-font-size: 13px;");
        HBox.setHgrow(input, Priority.ALWAYS);

        javafx.scene.layout.VBox content = new javafx.scene.layout.VBox(12,
                header, sub, new javafx.scene.control.Separator(), lbl, input);
        content.setPadding(new javafx.geometry.Insets(16, 20, 12, 20));
        content.setPrefWidth(400);

        d.getDialogPane().setContent(content);
        d.getDialogPane().getButtonTypes().addAll(
                javafx.scene.control.ButtonType.CANCEL, javafx.scene.control.ButtonType.OK);
        Button ok = (Button) d.getDialogPane().lookupButton(javafx.scene.control.ButtonType.OK);
        ok.setText("Create");
        ok.setStyle("-fx-base: #2563eb; -fx-text-fill: white; -fx-font-weight: bold;");
        ok.setDisable(input.getText().isBlank());
        input.textProperty().addListener((o, a, b) -> ok.setDisable(b.trim().isEmpty()));

        javafx.application.Platform.runLater(input::requestFocus);
        d.setResultConverter(bt -> bt == javafx.scene.control.ButtonType.OK ? input.getText().trim() : null);
        return d.showAndWait();
    }

    public static void info(Window owner, String title, String message) {
        Alert a = new Alert(Alert.AlertType.INFORMATION, message, ButtonType.OK);
        a.initOwner(owner);
        a.setHeaderText(title);
        a.showAndWait();
    }
}

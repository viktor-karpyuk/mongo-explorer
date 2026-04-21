package com.kubrik.mex.ui.security;

import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.Tooltip;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import javafx.util.Duration;

/**
 * v2.6 Q2.6-J — factories the seven Security sub-panes share so header
 * styling, small-text treatment, tooltip dwell + duration, and empty-
 * state copy stay consistent. Every pane used to re-invent these inline
 * with slightly different hex colours and font sizes; centralising
 * them also makes the dark-mode pass in Q2.6-J2 a one-file change.
 */
public final class SecurityPaneHelpers {

    private SecurityPaneHelpers() {}

    /** Foreground + size for small, neutral-grey meta text. */
    public static final String SMALL_STYLE =
            "-fx-text-fill: #6b7280; -fx-font-size: 11px;";

    /** Footer line style — shared between every pane's status label. */
    public static final String FOOTER_STYLE =
            "-fx-text-fill: #6b7280; -fx-font-size: 11px;";

    /** Title inside the pane's top row (14 px bold near-black). */
    public static Label paneTitle(String text) {
        Label l = new Label(text);
        l.setStyle("-fx-font-size: 14px; -fx-font-weight: 700;");
        return l;
    }

    /** One-line subtitle under the title — explains what the pane does. */
    public static Label paneSubtitle(String text) {
        Label l = new Label(text);
        l.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 12px;");
        l.setWrapText(true);
        l.setMaxWidth(Double.MAX_VALUE);
        return l;
    }

    /** Section heading inside a detail drawer (12 px bold). */
    public static Label heading(String text) {
        Label l = new Label(text);
        l.setStyle("-fx-font-size: 12px; -fx-font-weight: 700; -fx-text-fill: #374151;");
        return l;
    }

    /** Small neutral label (form field captions, status line fragments). */
    public static Label small(String text) {
        Label l = new Label(text);
        l.setStyle(SMALL_STYLE);
        return l;
    }

    /** Footer label — single-line status text at the bottom of a pane. */
    public static Label footer(String initialText) {
        Label l = new Label(initialText == null ? "—" : initialText);
        l.setStyle(FOOTER_STYLE);
        return l;
    }

    /**
     * Standard placeholder for a TableView that hasn't loaded yet. Two
     * lines: a heading explaining what'll appear, a subtitle naming
     * the action that fills it.
     */
    public static VBox emptyState(String headline, String callToAction) {
        Label head = new Label(headline);
        head.setStyle("-fx-text-fill: #374151; -fx-font-size: 13px; -fx-font-weight: 600;");
        head.setWrapText(true);
        Label sub = new Label(callToAction);
        sub.setStyle(SMALL_STYLE);
        sub.setWrapText(true);
        VBox box = new VBox(6, head, sub);
        box.setAlignment(Pos.CENTER);
        box.setPadding(new Insets(24));
        box.setMaxWidth(440);
        return box;
    }

    /** Tooltip with the security-pane standard dwell + wrap + max width. */
    public static Tooltip tip(String body) {
        Tooltip t = new Tooltip(body);
        t.setShowDelay(Duration.millis(250));
        t.setShowDuration(Duration.seconds(20));
        t.setWrapText(true);
        t.setMaxWidth(340);
        return t;
    }

    /** Install a help tooltip on a control AND mirror the body into the
     *  JavaFX accessibility tree ({@code accessibleHelp}) so screen
     *  readers / VoiceOver pick up the same explanation sighted users
     *  see on hover. Idempotent. */
    public static <T extends javafx.scene.control.Control> T withTip(T control, String body) {
        control.setTooltip(tip(body));
        if (body != null && !body.isBlank()) control.setAccessibleHelp(body);
        return control;
    }

    /** Top-bar row with title on the left, a grow-spacer, then the given
     *  trailing controls (typically a refresh button). */
    public static HBox topBar(Label title, Node... trailing) {
        Region grow = new Region();
        HBox.setHgrow(grow, Priority.ALWAYS);
        HBox row = new HBox(10, title, grow);
        row.getChildren().addAll(trailing);
        row.setAlignment(Pos.CENTER_LEFT);
        row.setPadding(new Insets(0, 0, 10, 0));
        return row;
    }

    /** Factory for the standard "Refresh" button with a consistent tip.
     *  Tooltip body is also surfaced as {@code accessibleHelp} so screen
     *  readers announce the same explanation. */
    public static Button refreshButton(String tip) {
        Button b = new Button("Refresh");
        String body = tip == null ? "Reload this pane's data." : tip;
        b.setTooltip(tip(body));
        b.setAccessibleHelp(body);
        return b;
    }

    /** Install an accessible description on a Region / Control (tables,
     *  TextFields) — JavaFX's accessibility tree picks this up for
     *  screen readers in addition to the visual label. */
    public static <T extends javafx.scene.Node> T describe(T node, String description) {
        if (description != null && !description.isBlank()) {
            node.setAccessibleText(description);
            if (node instanceof javafx.scene.control.Control c) {
                c.setAccessibleHelp(description);
            }
        }
        return node;
    }
}

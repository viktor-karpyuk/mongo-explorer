package com.kubrik.mex.ui;

import javafx.animation.PauseTransition;
import javafx.application.Platform;
import javafx.util.Duration;
import org.fxmisc.richtext.CodeArea;
import org.fxmisc.richtext.LineNumberFactory;
import org.fxmisc.richtext.model.StyleSpans;
import org.fxmisc.richtext.model.StyleSpansBuilder;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** A {@link CodeArea} with JSON syntax highlighting and line numbers. */
public class JsonCodeArea extends CodeArea {

    private static final Pattern PATTERN = Pattern.compile(
            "(?<KEY>\"(?:\\\\.|[^\"\\\\])*\"\\s*:)"
                    + "|(?<STRING>\"(?:\\\\.|[^\"\\\\])*\"|'(?:\\\\.|[^'\\\\])*')"
                    + "|(?<MONGOFN>\\b(?:ObjectId|ISODate|NumberLong|NumberDecimal|NumberInt|BinData|UUID|Timestamp|Date|MinKey|MaxKey|DBRef|RegExp)\\b)"
                    + "|(?<MONGOOP>\\$[a-zA-Z][a-zA-Z0-9]*)"
                    + "|(?<UKEY>[A-Za-z_$][A-Za-z_$0-9]*\\s*(?=:))"
                    + "|(?<NUMBER>-?\\b\\d+(?:\\.\\d+)?(?:[eE][+-]?\\d+)?\\b)"
                    + "|(?<BOOL>\\btrue\\b|\\bfalse\\b)"
                    + "|(?<NULL>\\bnull\\b)"
                    + "|(?<PUNCT>[\\{\\}\\[\\],:])");

    /** Debounce: per-char highlighting was the dominant typing-lag
     *  source app-wide (regex over the full buffer on every key).
     *  150 ms after the last edit we kick a virtual thread to compute
     *  spans and apply them on the FX thread. */
    private static final Duration HIGHLIGHT_DELAY = Duration.millis(150);
    private final PauseTransition debounce = new PauseTransition(HIGHLIGHT_DELAY);
    /** Generation counter so a stale highlight result (computed against
     *  text that has since been edited again) is discarded. */
    private final AtomicLong gen = new AtomicLong();

    public JsonCodeArea(String initial) {
        getStyleClass().add("json-code-area");
        setParagraphGraphicFactory(LineNumberFactory.get(this));
        debounce.setOnFinished(e -> kickHighlight());
        textProperty().addListener((obs, ov, nv) -> debounce.playFromStart());
        if (initial != null) {
            replaceText(0, 0, initial);
            // Initial render is cheap and important for first paint; apply synchronously.
            setStyleSpans(0, computeHighlighting(initial));
        }
    }

    private void kickHighlight() {
        final long mine = gen.incrementAndGet();
        final String text = getText();
        Thread.startVirtualThread(() -> {
            StyleSpans<Collection<String>> spans = computeHighlighting(text);
            Platform.runLater(() -> {
                // Drop if a newer edit has already scheduled another pass.
                if (gen.get() != mine) return;
                try { setStyleSpans(0, spans); } catch (Exception ignored) {
                    // Defensive: in flight when text was wholesale-replaced
                    // to a shorter buffer; the next debounce will resync.
                }
            });
        });
    }

    /** Re-apply highlighting synchronously (call after replaceText when
     *  you need an immediate refresh, e.g. before a screenshot or after
     *  a programmatic load). */
    public void refreshHighlight() {
        gen.incrementAndGet(); // invalidate any in-flight virtual-thread highlight
        setStyleSpans(0, computeHighlighting(getText()));
    }

    private static StyleSpans<Collection<String>> computeHighlighting(String text) {
        Matcher m = PATTERN.matcher(text);
        int last = 0;
        StyleSpansBuilder<Collection<String>> sb = new StyleSpansBuilder<>();
        while (m.find()) {
            String style = m.group("KEY") != null ? "json-key"
                    : m.group("STRING") != null ? "json-string"
                    : m.group("MONGOFN") != null ? "json-mongo-fn"
                    : m.group("MONGOOP") != null ? "json-mongo-op"
                    : m.group("UKEY") != null ? "json-key"
                    : m.group("NUMBER") != null ? "json-number"
                    : m.group("BOOL") != null ? "json-boolean"
                    : m.group("NULL") != null ? "json-null"
                    : m.group("PUNCT") != null ? "json-punct" : null;
            sb.add(Collections.emptyList(), m.start() - last);
            sb.add(style == null ? Collections.emptyList() : Collections.singleton(style), m.end() - m.start());
            last = m.end();
        }
        sb.add(Collections.emptyList(), text.length() - last);
        return sb.create();
    }
}

package com.kubrik.mex.ui;

import org.fxmisc.richtext.CodeArea;
import org.fxmisc.richtext.LineNumberFactory;
import org.fxmisc.richtext.model.StyleSpans;
import org.fxmisc.richtext.model.StyleSpansBuilder;

import java.util.Collection;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** A {@link CodeArea} with JSON syntax highlighting and line numbers. */
public class JsonCodeArea extends CodeArea {

    private static final Pattern PATTERN = Pattern.compile(
            "(?<KEY>\"(?:\\\\.|[^\"\\\\])*\"\\s*:)"
                    + "|(?<STRING>\"(?:\\\\.|[^\"\\\\])*\")"
                    + "|(?<NUMBER>-?\\b\\d+(?:\\.\\d+)?(?:[eE][+-]?\\d+)?\\b)"
                    + "|(?<BOOL>\\btrue\\b|\\bfalse\\b)"
                    + "|(?<NULL>\\bnull\\b)"
                    + "|(?<PUNCT>[\\{\\}\\[\\],:])");

    public JsonCodeArea(String initial) {
        getStyleClass().add("json-code-area");
        setParagraphGraphicFactory(LineNumberFactory.get(this));
        textProperty().addListener((obs, ov, nv) -> setStyleSpans(0, computeHighlighting(nv)));
        if (initial != null) {
            replaceText(0, 0, initial);
            setStyleSpans(0, computeHighlighting(initial));
        }
    }

    /** Re-apply highlighting (call after replaceText if the listener didn't run yet). */
    public void refreshHighlight() {
        setStyleSpans(0, computeHighlighting(getText()));
    }

    private static StyleSpans<Collection<String>> computeHighlighting(String text) {
        Matcher m = PATTERN.matcher(text);
        int last = 0;
        StyleSpansBuilder<Collection<String>> sb = new StyleSpansBuilder<>();
        while (m.find()) {
            String style = m.group("KEY") != null ? "json-key"
                    : m.group("STRING") != null ? "json-string"
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

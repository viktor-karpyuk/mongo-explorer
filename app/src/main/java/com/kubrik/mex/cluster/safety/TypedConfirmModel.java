package com.kubrik.mex.cluster.safety;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

/**
 * v2.4 SAFE-OPS-5..7 — headless state for the typed-confirm dialog. The JavaFX
 * dialog is a thin view that binds to this model. Keeping the logic non-UI
 * makes it unit-testable without a JavaFX runtime.
 *
 * <p>Rules:
 * <ul>
 *   <li>The <em>Execute</em> button is enabled iff
 *       {@code input.trim().equals(expected.trim())}.</li>
 *   <li>Paste events set {@link #paste()} to {@code true}; the flag is audit-
 *       visible but does not block the match.</li>
 *   <li>{@link Outcome#CANCELLED} is produced by explicit cancel (ESC or the
 *       <em>Cancel</em> button).</li>
 * </ul>
 */
public final class TypedConfirmModel {

    public enum Outcome { CONFIRMED, CANCELLED }

    private final String expected;
    private final String expectedNormalised;
    private final DryRunResult preview;

    private volatile String input = "";
    private volatile boolean paste = false;
    private volatile Outcome outcome = null;

    private final ConcurrentMap<Object, Consumer<Boolean>> matchListeners = new ConcurrentHashMap<>();

    public TypedConfirmModel(String expected, DryRunResult preview) {
        if (expected == null) throw new IllegalArgumentException("expected");
        if (preview == null) throw new IllegalArgumentException("preview");
        this.expected = expected;
        this.expectedNormalised = expected.trim();
        this.preview = preview;
    }

    public String expected() { return expected; }
    public DryRunResult preview() { return preview; }

    public String input() { return input; }
    public boolean paste() { return paste; }
    public Outcome outcome() { return outcome; }

    /** Update the typed text. Fires match listeners on every change. */
    public void setInput(String next) {
        this.input = next == null ? "" : next;
        boolean match = matches();
        for (Consumer<Boolean> l : matchListeners.values()) {
            try { l.accept(match); } catch (Exception ignored) {}
        }
    }

    /** Mark that the last input change came from a paste event (SAFE-OPS-6). */
    public void markPaste() { this.paste = true; }

    public boolean matches() {
        return expectedNormalised.equals(input == null ? "" : input.trim());
    }

    public void confirm() {
        if (matches()) outcome = Outcome.CONFIRMED;
    }

    public void cancel() {
        outcome = Outcome.CANCELLED;
    }

    public AutoCloseable onMatchChanged(Consumer<Boolean> l) {
        Object key = new Object();
        matchListeners.put(key, l);
        try { l.accept(matches()); } catch (Exception ignored) {}
        return () -> matchListeners.remove(key);
    }
}

package com.kubrik.mex.maint.ui;

import javafx.application.Platform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

/**
 * v2.7 — Helper that runs a blocking body on a virtual thread and
 * funnels uncaught exceptions to a UI status callback + the logger.
 *
 * <p>Without this, every {@code Thread.startVirtualThread(() -> …)}
 * in the maintenance panes is a foot-gun: a NullPointerException, a
 * MongoSocketException from SDAM, or any other uncaught throw
 * vanishes to stderr and the UI hangs on "Running…" indefinitely.
 * The helper guarantees an error message reaches the status label
 * even when the pane author forgot a catch.</p>
 */
public final class FxOffThread {

    private static final Logger log = LoggerFactory.getLogger(FxOffThread.class);

    @FunctionalInterface
    public interface FallibleRunnable {
        void run() throws Exception;
    }

    /**
     * Run {@code body} on a virtual thread. Uncaught exceptions are
     * reported via {@code onError} on the FX thread.
     */
    public static void run(FallibleRunnable body, Consumer<String> onError) {
        Thread.startVirtualThread(() -> {
            try {
                body.run();
            } catch (InterruptedException ie) {
                // Preserve the interrupt flag so any joiner upstream
                // notices, then report. Don't consume the interrupt.
                Thread.currentThread().interrupt();
                Platform.runLater(() -> onError.accept("interrupted"));
            } catch (Error err) {
                // OutOfMemoryError / StackOverflowError / VirtualMachineError
                // must NOT be swallowed — let them propagate so the app
                // crashes cleanly under resource exhaustion instead of
                // limping along with corrupted state.
                log.error("fatal Error on virtual thread", err);
                throw err;
            } catch (Throwable t) {
                log.warn("background body threw on virtual thread", t);
                String msg = t.getClass().getSimpleName()
                        + (t.getMessage() == null ? "" : ": " + t.getMessage());
                Platform.runLater(() -> onError.accept(msg));
            }
        });
    }

    private FxOffThread() {}
}

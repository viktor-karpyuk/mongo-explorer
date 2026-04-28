package com.kubrik.mex.ui;

import javafx.application.Platform;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/** Shared bootstrapper for UI tests. Starts the JavaFX toolkit at most once per JVM and
 *  provides {@link #onFx} to run code on the FX application thread and wait for the result.
 *
 *  <p>v2.8.4 — Two robustness fixes for the prior flake where the
 *  WizardStepScopeValidatorTest passed in isolation but failed in the
 *  full suite:</p>
 *  <ul>
 *    <li>{@link #start} catches {@code IllegalStateException("Toolkit
 *        already initialized")} and treats it as success — covers the
 *        case where another test in the same JVM started JavaFX
 *        directly.</li>
 *    <li>{@link Platform#setImplicitExit}{@code (false)} is set
 *        unconditionally, so an earlier test that stowed the
 *        toolkit's last window can't trigger {@code Platform.exit}
 *        and leave subsequent tests staring at a dead toolkit.</li>
 *  </ul>
 *
 *  <p>On a display-less CI you must add Monocle headless (not wired yet).</p>
 */
public final class Fx {

    private static volatile boolean started = false;

    private Fx() {}

    public static synchronized void start() {
        if (started) return;
        CountDownLatch latch = new CountDownLatch(1);
        try {
            Platform.startup(latch::countDown);
            try {
                if (!latch.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("JavaFX platform did not start within 5 s");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        } catch (IllegalStateException ise) {
            // Some other test in the same JVM already started the
            // toolkit. That's fine — Platform is alive, we just have
            // to skip the latch wait. Re-throw anything else.
            String msg = ise.getMessage() == null ? "" : ise.getMessage();
            if (!msg.contains("Toolkit already initialized")
                    && !msg.contains("already started")) {
                throw ise;
            }
        }
        Platform.setImplicitExit(false);
        started = true;
    }

    public static <T> T onFx(Supplier<T> task) {
        start();
        AtomicReference<T> out = new AtomicReference<>();
        AtomicReference<Throwable> err = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        Platform.runLater(() -> {
            try { out.set(task.get()); }
            catch (Throwable t) { err.set(t); }
            finally { latch.countDown(); }
        });
        // 30 s is generous on first-render — JavaFX font loading can
        // take a few seconds on cold CI runs after another test
        // class has already run.
        try {
            if (!latch.await(30, TimeUnit.SECONDS)) {
                throw new IllegalStateException(
                        "FX task did not complete within 30 s — toolkit may be wedged");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        if (err.get() != null) throw new RuntimeException(err.get());
        return out.get();
    }

    public static void runFx(Runnable task) {
        onFx(() -> { task.run(); return null; });
    }
}

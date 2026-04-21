package com.kubrik.mex.ui;

import javafx.application.Platform;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/** Shared bootstrapper for UI tests. Starts the JavaFX toolkit at most once per JVM and
 *  provides {@link #onFx} to run code on the FX application thread and wait for the result.
 *  On a display-less CI you must add Monocle headless (not wired yet). */
public final class Fx {

    private static volatile boolean started = false;

    private Fx() {}

    public static synchronized void start() {
        if (started) return;
        CountDownLatch latch = new CountDownLatch(1);
        Platform.startup(latch::countDown);
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("JavaFX platform did not start within 5 s");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
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
        try { latch.await(5, TimeUnit.SECONDS); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new RuntimeException(e); }
        if (err.get() != null) throw new RuntimeException(err.get());
        return out.get();
    }

    public static void runFx(Runnable task) {
        onFx(() -> { task.run(); return null; });
    }
}

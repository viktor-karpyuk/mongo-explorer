package com.kubrik.mex.cluster.safety;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * v2.4 SAFE-OPS-8..10 — process-wide toggle that hides every destructive
 * dispatcher. In-memory only; on restart it defaults OFF.
 *
 * <p>Callers that own destructive buttons subscribe via {@link #onChange} and
 * drive a {@code disableProperty}. Callers that dispatch destructive commands
 * must invoke {@link #requireDisengaged} before sending.</p>
 *
 * <p>Engage requires an already-confirmed call from the UI (the confirm
 * dialog lives in the UI layer). Disengage is free.</p>
 */
public final class KillSwitch {

    private final AtomicBoolean engaged = new AtomicBoolean(false);
    private final ConcurrentMap<Object, Consumer<Boolean>> listeners = new ConcurrentHashMap<>();

    public boolean isEngaged() { return engaged.get(); }

    /** Idempotent — re-engaging a currently-engaged switch does not fire listeners. */
    public void engage() {
        if (engaged.compareAndSet(false, true)) fire(true);
    }

    public void disengage() {
        if (engaged.compareAndSet(true, false)) fire(false);
    }

    /** Register a listener; returns a close() handle. Fires once with the current state. */
    public AutoCloseable onChange(Consumer<Boolean> l) {
        Object key = new Object();
        listeners.put(key, l);
        try { l.accept(engaged.get()); } catch (Exception ignored) {}
        return () -> listeners.remove(key);
    }

    /** Throws when the switch is engaged; callers should abort + audit as CANCELLED. */
    public void requireDisengaged() {
        if (engaged.get()) throw new KillSwitchEngagedException();
    }

    public static final class KillSwitchEngagedException extends RuntimeException {
        public KillSwitchEngagedException() { super("kill_switch_engaged"); }
    }

    private void fire(boolean state) {
        for (Consumer<Boolean> l : listeners.values()) {
            try { l.accept(state); } catch (Exception ignored) {}
        }
    }
}

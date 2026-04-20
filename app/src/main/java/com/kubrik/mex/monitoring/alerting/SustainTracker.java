package com.kubrik.mex.monitoring.alerting;

import com.kubrik.mex.monitoring.model.Severity;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Per-(rule, labels) state machine: a breach must persist for {@code rule.sustain()}
 * before the tracker reports a fired state. Implemented in terms of wall-clock
 * timestamps; a monotonic clock would be more defensive but not needed at this
 * cadence.
 *
 * <p>Not thread-safe; the {@link Alerter} is single-threaded.
 */
public final class SustainTracker {

    public enum State { OK, WARN_PENDING, CRIT_PENDING, WARN, CRIT }

    private static final class Entry {
        State state = State.OK;
        long enteredAtMs = 0;
    }

    private final Map<Key, Entry> table = new HashMap<>();

    /**
     * Update the state given the current observed severity. Returns the new
     * "visible" severity transition if any, otherwise null.
     */
    public Transition observe(AlertRule rule, Map<String, String> labels,
                              Severity observed, long nowMs) {
        Key k = new Key(rule.id(), labels);
        Entry e = table.computeIfAbsent(k, x -> new Entry());
        State prev = e.state;
        State next = project(prev, observed, e, rule.sustain(), nowMs);
        if (next != prev) {
            e.state = next;
            if (next == State.OK) e.enteredAtMs = 0;
            return new Transition(prev, next);
        }
        return null;
    }

    /** Clear all state for tests. */
    public void reset() { table.clear(); }

    private State project(State prev, Severity observed, Entry e, Duration sustain, long nowMs) {
        long sustainMs = sustain.toMillis();
        return switch (observed) {
            case CRIT -> {
                if (prev != State.CRIT_PENDING && prev != State.CRIT) {
                    e.enteredAtMs = nowMs;
                    // Zero-sustain: fire immediately on the first breach.
                    yield sustainMs == 0 ? State.CRIT : State.CRIT_PENDING;
                }
                if (prev == State.CRIT_PENDING && nowMs - e.enteredAtMs >= sustainMs) yield State.CRIT;
                yield prev;
            }
            case WARN -> {
                if (prev == State.CRIT || prev == State.CRIT_PENDING) {
                    e.enteredAtMs = nowMs;
                    yield sustainMs == 0 ? State.WARN : State.WARN_PENDING;
                }
                if (prev != State.WARN_PENDING && prev != State.WARN) {
                    e.enteredAtMs = nowMs;
                    yield sustainMs == 0 ? State.WARN : State.WARN_PENDING;
                }
                if (prev == State.WARN_PENDING && nowMs - e.enteredAtMs >= sustainMs) yield State.WARN;
                yield prev;
            }
            case OK -> State.OK;
        };
    }

    public record Transition(State from, State to) {
        public boolean fired() { return (from != State.WARN && from != State.CRIT)
                                      && (to   == State.WARN || to   == State.CRIT); }
        public boolean cleared() { return (from == State.WARN || from == State.CRIT) && to == State.OK; }
    }

    private record Key(String ruleId, Map<String, String> labels) {}
}

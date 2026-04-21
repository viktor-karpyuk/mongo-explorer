package com.kubrik.mex.backup.runner;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * v2.5 BKP-RUN-8 — bounded ring buffer for mongodump's stderr tail.
 *
 * <p>Default capacity 1000 lines (per technical-spec §4.2). When a run ends
 * with a non-zero exit code the last N lines are persisted to
 * {@code backup_catalog.notes}; a healthy run discards the buffer.</p>
 */
public final class RunLog {

    public static final int DEFAULT_CAPACITY = 1000;

    private final Deque<String> buffer = new ArrayDeque<>();
    private final int capacity;

    public RunLog() { this(DEFAULT_CAPACITY); }

    public RunLog(int capacity) {
        if (capacity < 1) throw new IllegalArgumentException("capacity");
        this.capacity = capacity;
    }

    public synchronized void append(String line) {
        if (line == null) return;
        while (buffer.size() >= capacity) buffer.pollFirst();
        buffer.addLast(line);
    }

    public synchronized List<String> snapshot() {
        return new ArrayList<>(buffer);
    }

    /** Last N lines joined with newlines; used for
     *  {@code backup_catalog.notes} on failure. */
    public synchronized String tail(int n) {
        if (n <= 0) return "";
        List<String> snap = snapshot();
        int start = Math.max(0, snap.size() - n);
        return String.join("\n", snap.subList(start, snap.size()));
    }

    public synchronized int size() { return buffer.size(); }
}

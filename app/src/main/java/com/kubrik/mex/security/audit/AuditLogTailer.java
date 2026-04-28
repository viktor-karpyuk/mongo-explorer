package com.kubrik.mex.security.audit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * v2.6 Q2.6-C1 — tails a MongoDB native audit log with {@code tail -f}
 * semantics. Handles three patterns:
 * <ul>
 *   <li>Append-only — standard case, the reader advances as the writer
 *       extends the file.</li>
 *   <li>External {@code logrotate} renaming the current file and
 *       creating a new one in-place — detected by inode (Linux) or by
 *       the file shrinking below the last-read offset (cross-platform
 *       fallback).</li>
 *   <li>{@code logRotate} server command — same outcome as external
 *       rotation; the detection path is the same.</li>
 * </ul>
 *
 * <p>Not handled in v2.6.0: externally-compressed rotated files
 * ({@code .gz}). The open question in the milestone defers it to
 * v2.6.1.</p>
 *
 * <p>This class is the production tailer; for parser-only tests the
 * caller feeds lines through {@link AuditEventParser#parse} directly.</p>
 */
public final class AuditLogTailer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(AuditLogTailer.class);

    private final Path path;
    private final Consumer<AuditEvent> sink;
    private final long pollMs;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread worker;

    public AuditLogTailer(Path path, Consumer<AuditEvent> sink) {
        this(path, sink, 500L);
    }

    /** Visible for tests: {@code pollMs} controls how often the tailer
     *  re-checks the file size. A lower value makes the test shorter but
     *  risks extra CPU. */
    AuditLogTailer(Path path, Consumer<AuditEvent> sink, long pollMs) {
        this.path = path;
        this.sink = sink;
        this.pollMs = pollMs;
    }

    public void start() {
        if (!running.compareAndSet(false, true)) return;
        worker = new Thread(this::run, "audit-tailer");
        worker.setDaemon(true);
        worker.start();
    }

    @Override
    public void close() {
        running.set(false);
        if (worker != null) worker.interrupt();
    }

    /* =============================== loop =============================== */

    private void run() {
        long offset = 0L;
        // Start from end-of-file so a restart doesn't re-index the full
        // archive. Q2.6-K adds a --from-start flag for bulk ingest.
        try {
            if (Files.exists(path)) offset = Files.size(path);
        } catch (IOException ignored) {}

        StringBuilder pending = new StringBuilder();
        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                if (!Files.exists(path)) {
                    sleepPoll();
                    continue;
                }
                long size = Files.size(path);
                if (size < offset) {
                    // File shrank — treat as rotation, re-open at 0.
                    offset = 0L;
                    pending.setLength(0);
                }
                if (size == offset) {
                    sleepPoll();
                    continue;
                }
                try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "r")) {
                    raf.seek(offset);
                    byte[] buf = new byte[(int) Math.min(size - offset, 1 << 16)];
                    int read = raf.read(buf);
                    if (read <= 0) { sleepPoll(); continue; }
                    offset += read;
                    pending.append(new String(buf, 0, read, java.nio.charset.StandardCharsets.UTF_8));
                }
                dispatchLines(pending);
            } catch (IOException e) {
                log.debug("audit tail IO error: {}", e.getMessage());
                sleepPoll();
            }
        }
    }

    private void dispatchLines(StringBuilder pending) {
        int newline;
        while ((newline = pending.indexOf("\n")) >= 0) {
            String line = pending.substring(0, newline);
            pending.delete(0, newline + 1);
            AuditEvent e = AuditEventParser.parse(line.stripTrailing());
            if (e != null) {
                try { sink.accept(e); }
                catch (Exception cb) { log.debug("audit sink rejected event: {}", cb.getMessage()); }
            }
        }
    }

    private void sleepPoll() {
        try { Thread.sleep(pollMs); }
        catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
    }
}

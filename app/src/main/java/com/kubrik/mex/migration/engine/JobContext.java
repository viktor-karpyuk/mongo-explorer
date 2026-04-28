package com.kubrik.mex.migration.engine;

import com.kubrik.mex.migration.JobId;
import com.kubrik.mex.migration.resume.ResumeManager;
import com.kubrik.mex.migration.spec.MigrationSpec;
import org.bson.BsonValue;

import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** Shared runtime state for a single job: spec, metrics, resume, stop flag, pause flag.
 *  <p>
 *  Every pipeline worker holds a reference and polls the stop/pause flags between units of
 *  work. See {@link #stopping()} and {@link #paused()}. */
public final class JobContext {

    private final JobId jobId;
    private final MigrationSpec spec;
    private final Path jobDir;
    private final ResumeManager resume;
    private final Metrics metrics = new Metrics();
    private final RateLimiter rateLimiter;

    private final AtomicBoolean stopping = new AtomicBoolean(false);
    private final AtomicBoolean paused = new AtomicBoolean(false);
    private final AtomicReference<String> cancellationReason = new AtomicReference<>();

    /** Last checkpointed _id per source namespace. Populated by the Writer's checkpoint
     *  callback; read by the JobRunner's resume flusher (M-1.2). */
    private final ConcurrentHashMap<String, BsonValue> lastIdByNs = new ConcurrentHashMap<>();
    private final AtomicReference<String> inProgressNs = new AtomicReference<>();

    public JobContext(JobId jobId, MigrationSpec spec, Path jobDir, ResumeManager resume) {
        this.jobId = jobId;
        this.spec = spec;
        this.jobDir = jobDir;
        this.resume = resume;
        this.rateLimiter = RateLimiter.perSecond(spec.options().performance().rateLimitDocsPerSec());
    }

    public JobId jobId() { return jobId; }
    public MigrationSpec spec() { return spec; }
    public Path jobDir() { return jobDir; }
    public ResumeManager resume() { return resume; }
    public Metrics metrics() { return metrics; }
    public RateLimiter rateLimiter() { return rateLimiter; }

    public void stop(String reason) {
        cancellationReason.compareAndSet(null, reason);
        stopping.set(true);
    }

    public boolean stopping() { return stopping.get(); }
    public String cancellationReason() { return cancellationReason.get(); }

    public void pause() { paused.set(true); }
    public void resumeRun() { paused.set(false); }
    public boolean paused() { return paused.get(); }

    // --- resume plumbing -------------------------------------------------------------

    public void setInProgressCollection(String sourceNs) { inProgressNs.set(sourceNs); }
    public String inProgressCollection() { return inProgressNs.get(); }
    public void clearInProgressCollection(String sourceNs) { inProgressNs.compareAndSet(sourceNs, null); }

    /** Records the most recent successfully-written {@code _id} for a namespace. Updated
     *  after every bulk-write ack; stored atomically per namespace. */
    public void setLastId(String sourceNs, BsonValue id) {
        if (id != null) lastIdByNs.put(sourceNs, id);
    }

    public BsonValue lastId(String sourceNs) { return lastIdByNs.get(sourceNs); }
}

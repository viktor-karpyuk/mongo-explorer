package com.kubrik.mex.migration.store;

import com.kubrik.mex.migration.JobHistoryQuery;
import com.kubrik.mex.migration.JobId;
import com.kubrik.mex.migration.MigrationJobRecord;
import com.kubrik.mex.migration.events.JobStatus;
import com.kubrik.mex.migration.profile.ProfileCodec;
import com.kubrik.mex.migration.spec.ExecutionMode;
import com.kubrik.mex.migration.spec.MigrationKind;
import com.kubrik.mex.migration.spec.MigrationSpec;
import com.kubrik.mex.store.Database;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** CRUD over the {@code migration_jobs} table. Thread-safe per SQLite connection;
 *  a single instance is shared across the app. */
public final class MigrationJobDao {

    private final Database db;
    private final ProfileCodec codec;

    public MigrationJobDao(Database db, ProfileCodec codec) {
        this.db = db;
        this.codec = codec;
    }

    public void insert(MigrationJobRecord r) {
        String sql = """
            INSERT INTO migration_jobs
                (id, kind, source_conn_id, target_conn_id, spec_json, spec_hash,
                 status, execution_mode, started_at, ended_at, docs_copied, bytes_copied,
                 errors, error_message, resume_path, artifact_dir, created_at, updated_at,
                 source_connection_name, target_connection_name, docs_processed, active_millis)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;
        synchronized (db.writeLock()) {
            try (PreparedStatement ps = db.connection().prepareStatement(sql)) {
                ps.setString(1, r.id().value());
                ps.setString(2, r.kind().name());
                ps.setString(3, r.sourceConnectionId());
                ps.setString(4, r.targetConnectionId());
                ps.setString(5, codec.toJson(r.spec()));
                ps.setString(6, r.specHash());
                ps.setString(7, r.status().name());
                ps.setString(8, r.executionMode().name());
                setInstant(ps, 9, r.startedAt());
                setInstant(ps, 10, r.endedAt());
                ps.setLong(11, r.docsCopied());
                ps.setLong(12, r.bytesCopied());
                ps.setLong(13, r.errors());
                ps.setString(14, r.errorMessage());
                ps.setString(15, r.resumePath() == null ? null : r.resumePath().toString());
                ps.setString(16, r.artifactDir() == null ? null : r.artifactDir().toString());
                setInstant(ps, 17, r.createdAt());
                setInstant(ps, 18, r.updatedAt());
                ps.setString(19, r.sourceConnectionName());
                ps.setString(20, r.targetConnectionName());
                ps.setLong(21, r.docsProcessed());
                ps.setLong(22, r.activeMillis());
                ps.executeUpdate();
            } catch (Exception e) {
                throw new RuntimeException("insert migration_jobs failed: " + e.getMessage(), e);
            }
        }
    }

    public void updateStatus(JobId id, JobStatus status, String error) {
        String sql = """
            UPDATE migration_jobs
               SET status = ?, error_message = ?, updated_at = ?
             WHERE id = ?
            """;
        synchronized (db.writeLock()) {
            try (PreparedStatement ps = db.connection().prepareStatement(sql)) {
                ps.setString(1, status.name());
                ps.setString(2, error);
                setInstant(ps, 3, Instant.now());
                ps.setString(4, id.value());
                ps.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException("updateStatus failed: " + e.getMessage(), e);
            }
        }
    }

    /** Stamp {@code started_at} when the job actually begins running. Uses
     *  {@code COALESCE} so a resumed job keeps its original start time. */
    public void markStarted(JobId id, Instant startedAt) {
        String sql = """
            UPDATE migration_jobs
               SET started_at = COALESCE(started_at, ?),
                   updated_at = ?
             WHERE id = ?
            """;
        synchronized (db.writeLock()) {
            try (PreparedStatement ps = db.connection().prepareStatement(sql)) {
                setInstant(ps, 1, startedAt);
                setInstant(ps, 2, Instant.now());
                ps.setString(3, id.value());
                ps.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException("markStarted failed: " + e.getMessage(), e);
            }
        }
    }

    /** Throttled progress update (T-14). Callers decide cadence. */
    public void updateProgress(JobId id, long docsCopied, long bytesCopied, long errors,
                               long docsProcessed, long activeMillis) {
        String sql = """
            UPDATE migration_jobs
               SET docs_copied = ?, bytes_copied = ?, errors = ?,
                   docs_processed = ?, active_millis = ?, updated_at = ?
             WHERE id = ?
            """;
        synchronized (db.writeLock()) {
            try (PreparedStatement ps = db.connection().prepareStatement(sql)) {
                ps.setLong(1, docsCopied);
                ps.setLong(2, bytesCopied);
                ps.setLong(3, errors);
                ps.setLong(4, docsProcessed);
                ps.setLong(5, activeMillis);
                setInstant(ps, 6, Instant.now());
                ps.setString(7, id.value());
                ps.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException("updateProgress failed: " + e.getMessage(), e);
            }
        }
    }

    public void markEnded(JobId id, JobStatus status, String error,
                          long docsCopied, long bytesCopied,
                          long docsProcessed, long activeMillis) {
        String sql = """
            UPDATE migration_jobs
               SET status = ?, error_message = ?, docs_copied = ?, bytes_copied = ?,
                   docs_processed = ?, active_millis = ?,
                   ended_at = ?, updated_at = ?,
                   owner_pid = NULL, last_heartbeat_at = NULL
             WHERE id = ?
            """;
        synchronized (db.writeLock()) {
            try (PreparedStatement ps = db.connection().prepareStatement(sql)) {
                Instant now = Instant.now();
                ps.setString(1, status.name());
                ps.setString(2, error);
                ps.setLong(3, docsCopied);
                ps.setLong(4, bytesCopied);
                ps.setLong(5, docsProcessed);
                ps.setLong(6, activeMillis);
                setInstant(ps, 7, now);
                setInstant(ps, 8, now);
                ps.setString(9, id.value());
                ps.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException("markEnded failed: " + e.getMessage(), e);
            }
        }
    }

    /** Legacy 5-arg overload kept for tests and for the migrate-ended path where
     *  active-millis / docs-processed are not tracked. Maps to the 7-arg form with zeros. */
    public void markEnded(JobId id, JobStatus status, String error,
                          long docsCopied, long bytesCopied) {
        markEnded(id, status, error, docsCopied, bytesCopied, docsCopied, 0L);
    }

    // --- OBS-7 per-collection timings ----------------------------------------------------

    /** Stamps a {@code started_at} for the given source namespace. No-op if one already
     *  exists (the first start wins — resumes after a crash reuse the original timestamp). */
    public void recordCollectionStart(JobId jobId, String sourceNs) {
        String sql = """
            INSERT INTO migration_collection_timings (job_id, source_ns, started_at)
            VALUES (?, ?, ?)
            ON CONFLICT(job_id, source_ns) DO NOTHING
            """;
        synchronized (db.writeLock()) {
            try (PreparedStatement ps = db.connection().prepareStatement(sql)) {
                ps.setString(1, jobId.value());
                ps.setString(2, sourceNs);
                setInstant(ps, 3, Instant.now());
                ps.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException("recordCollectionStart failed: " + e.getMessage(), e);
            }
        }
    }

    /** Stamps {@code ended_at} on the timing row — terminal for the collection. */
    public void recordCollectionEnd(JobId jobId, String sourceNs) {
        String sql = """
            UPDATE migration_collection_timings
               SET ended_at = ?
             WHERE job_id = ? AND source_ns = ?
            """;
        synchronized (db.writeLock()) {
            try (PreparedStatement ps = db.connection().prepareStatement(sql)) {
                setInstant(ps, 1, Instant.now());
                ps.setString(2, jobId.value());
                ps.setString(3, sourceNs);
                ps.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException("recordCollectionEnd failed: " + e.getMessage(), e);
            }
        }
    }

    /** All per-collection timings for a job, keyed by source namespace. */
    public java.util.Map<String, CollectionTiming> timingsFor(JobId jobId) {
        String sql = "SELECT source_ns, started_at, ended_at FROM migration_collection_timings WHERE job_id = ?";
        java.util.Map<String, CollectionTiming> out = new java.util.LinkedHashMap<>();
        try (PreparedStatement ps = db.connection().prepareStatement(sql)) {
            ps.setString(1, jobId.value());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.put(rs.getString("source_ns"),
                            new CollectionTiming(
                                    rs.getString("source_ns"),
                                    instantAt(rs, "started_at"),
                                    instantAt(rs, "ended_at")));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("timingsFor failed: " + e.getMessage(), e);
        }
        return out;
    }

    /** Start/end for a single collection within a job (OBS-7). */
    public record CollectionTiming(String sourceNs, Instant startedAt, Instant endedAt) {}

    /** Stamp the current OS process id on a job row so startup reconciliation can tell whether
     *  the row belongs to this JVM (and is therefore still alive) or was orphaned by a crash. */
    public void stampOwnership(JobId id, long ownerPid, Instant heartbeatAt) {
        String sql = """
            UPDATE migration_jobs
               SET owner_pid = ?, last_heartbeat_at = ?, updated_at = ?
             WHERE id = ?
            """;
        synchronized (db.writeLock()) {
            try (PreparedStatement ps = db.connection().prepareStatement(sql)) {
                ps.setLong(1, ownerPid);
                setInstant(ps, 2, heartbeatAt);
                setInstant(ps, 3, Instant.now());
                ps.setString(4, id.value());
                ps.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException("stampOwnership failed: " + e.getMessage(), e);
            }
        }
    }

    /** Refresh the heartbeat timestamp for a running job. Called by {@code JobRunner}'s flush
     *  ticker so the row keeps a fresh proof-of-life. */
    public void heartbeat(JobId id, Instant at) {
        String sql = "UPDATE migration_jobs SET last_heartbeat_at = ? WHERE id = ?";
        synchronized (db.writeLock()) {
            try (PreparedStatement ps = db.connection().prepareStatement(sql)) {
                setInstant(ps, 1, at);
                ps.setString(2, id.value());
                ps.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException("heartbeat failed: " + e.getMessage(), e);
            }
        }
    }

    /** Rows whose {@code last_heartbeat_at} is older than this threshold are treated as orphans
     *  even when their {@code owner_pid} matches the current JVM — covers PID reuse after reboot
     *  (spec §4.5, BUG-1 P1.7). Conservative at 12× the 5 s heartbeat cadence. */
    public static final long STALE_HEARTBEAT_MS = 60_000L;

    /** At application startup, mark as {@code FAILED} any row left in a non-terminal state by a
     *  prior JVM (orphaned from a crash, force-quit, or OS reboot). A row is considered orphaned
     *  when any of:
     *  <ul>
     *    <li>{@code owner_pid} is null (pre-upgrade rows)</li>
     *    <li>{@code owner_pid} does not equal the current PID (different JVM owned the row)</li>
     *    <li>{@code last_heartbeat_at} is null or older than {@link #STALE_HEARTBEAT_MS} —
     *        catches PID-reuse after a reboot where the new JVM happens to get the same PID</li>
     *  </ul>
     *  Returns the number of rows reconciled. */
    public int reconcileOrphans(long currentPid, String defaultError) {
        String sql = """
            UPDATE migration_jobs
               SET status = 'FAILED',
                   error_message = COALESCE(error_message, ?),
                   ended_at = COALESCE(ended_at, updated_at, ?),
                   updated_at = ?,
                   owner_pid = NULL,
                   last_heartbeat_at = NULL
             WHERE status IN ('PENDING','RUNNING','PAUSING','PAUSED','CANCELLING')
               AND (   owner_pid IS NULL
                    OR owner_pid <> ?
                    OR last_heartbeat_at IS NULL
                    OR last_heartbeat_at < ?)
            """;
        synchronized (db.writeLock()) {
            try (PreparedStatement ps = db.connection().prepareStatement(sql)) {
                Instant now = Instant.now();
                long staleCutoffMs = now.toEpochMilli() - STALE_HEARTBEAT_MS;
                ps.setString(1, defaultError);
                setInstant(ps, 2, now);
                setInstant(ps, 3, now);
                ps.setLong(4, currentPid);
                ps.setLong(5, staleCutoffMs);
                return ps.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException("reconcileOrphans failed: " + e.getMessage(), e);
            }
        }
    }

    public Optional<MigrationJobRecord> get(JobId id) {
        String sql = "SELECT * FROM migration_jobs WHERE id = ?";
        try (PreparedStatement ps = db.connection().prepareStatement(sql)) {
            ps.setString(1, id.value());
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(map(rs)) : Optional.empty();
            }
        } catch (Exception e) {
            throw new RuntimeException("get failed: " + e.getMessage(), e);
        }
    }

    public List<MigrationJobRecord> list(JobHistoryQuery q) {
        StringBuilder sql = new StringBuilder("SELECT * FROM migration_jobs WHERE 1=1");
        List<Object> args = new ArrayList<>();
        if (q.statuses() != null && !q.statuses().isEmpty()) {
            sql.append(" AND status IN (");
            sql.append("?,".repeat(q.statuses().size()));
            sql.setLength(sql.length() - 1);
            sql.append(")");
            for (JobStatus s : q.statuses()) args.add(s.name());
        }
        if (q.kinds() != null && !q.kinds().isEmpty()) {
            sql.append(" AND kind IN (");
            sql.append("?,".repeat(q.kinds().size()));
            sql.setLength(sql.length() - 1);
            sql.append(")");
            for (MigrationKind k : q.kinds()) args.add(k.name());
        }
        if (q.connectionId() != null) {
            sql.append(" AND (source_conn_id = ? OR target_conn_id = ?)");
            args.add(q.connectionId());
            args.add(q.connectionId());
        }
        if (q.startedAfter() != null) {
            sql.append(" AND started_at >= ?");
            args.add(q.startedAfter().toEpochMilli());
        }
        if (q.startedBefore() != null) {
            sql.append(" AND started_at <= ?");
            args.add(q.startedBefore().toEpochMilli());
        }
        sql.append(" ORDER BY COALESCE(started_at, created_at) DESC");
        if (q.limit() > 0) {
            sql.append(" LIMIT ").append(q.limit());
            if (q.offset() > 0) sql.append(" OFFSET ").append(q.offset());
        }

        try (PreparedStatement ps = db.connection().prepareStatement(sql.toString())) {
            for (int i = 0; i < args.size(); i++) ps.setObject(i + 1, args.get(i));
            try (ResultSet rs = ps.executeQuery()) {
                List<MigrationJobRecord> out = new ArrayList<>();
                while (rs.next()) out.add(map(rs));
                return out;
            }
        } catch (Exception e) {
            throw new RuntimeException("list failed: " + e.getMessage(), e);
        }
    }

    /** Total row count matching {@code q}'s filters. Offset and limit are ignored — this is
     *  the "how many are there?" the UI pager needs to compute page-of-N. */
    public long count(JobHistoryQuery q) {
        StringBuilder sql = new StringBuilder("SELECT COUNT(*) FROM migration_jobs WHERE 1=1");
        List<Object> args = new ArrayList<>();
        if (q.statuses() != null && !q.statuses().isEmpty()) {
            sql.append(" AND status IN (");
            sql.append("?,".repeat(q.statuses().size()));
            sql.setLength(sql.length() - 1);
            sql.append(")");
            for (JobStatus s : q.statuses()) args.add(s.name());
        }
        if (q.kinds() != null && !q.kinds().isEmpty()) {
            sql.append(" AND kind IN (");
            sql.append("?,".repeat(q.kinds().size()));
            sql.setLength(sql.length() - 1);
            sql.append(")");
            for (MigrationKind k : q.kinds()) args.add(k.name());
        }
        if (q.connectionId() != null) {
            sql.append(" AND (source_conn_id = ? OR target_conn_id = ?)");
            args.add(q.connectionId());
            args.add(q.connectionId());
        }
        if (q.startedAfter() != null) {
            sql.append(" AND started_at >= ?");
            args.add(q.startedAfter().toEpochMilli());
        }
        if (q.startedBefore() != null) {
            sql.append(" AND started_at <= ?");
            args.add(q.startedBefore().toEpochMilli());
        }
        try (PreparedStatement ps = db.connection().prepareStatement(sql.toString())) {
            for (int i = 0; i < args.size(); i++) ps.setObject(i + 1, args.get(i));
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        } catch (Exception e) {
            throw new RuntimeException("count failed: " + e.getMessage(), e);
        }
    }

    private MigrationJobRecord map(ResultSet rs) throws Exception {
        MigrationSpec spec = codec.fromJson(rs.getString("spec_json"));
        String resumePathStr = rs.getString("resume_path");
        String artifactDirStr = rs.getString("artifact_dir");
        long ownerPidRaw = rs.getLong("owner_pid");
        Long ownerPid = rs.wasNull() ? null : ownerPidRaw;
        return new MigrationJobRecord(
                JobId.of(rs.getString("id")),
                MigrationKind.valueOf(rs.getString("kind")),
                rs.getString("source_conn_id"),
                rs.getString("target_conn_id"),
                spec,
                rs.getString("spec_hash"),
                JobStatus.valueOf(rs.getString("status")),
                ExecutionMode.valueOf(rs.getString("execution_mode")),
                instantAt(rs, "started_at"),
                instantAt(rs, "ended_at"),
                rs.getLong("docs_copied"),
                rs.getLong("bytes_copied"),
                rs.getLong("errors"),
                rs.getString("error_message"),
                resumePathStr == null ? null : Path.of(resumePathStr),
                artifactDirStr == null ? null : Path.of(artifactDirStr),
                instantAt(rs, "created_at"),
                instantAt(rs, "updated_at"),
                ownerPid,
                instantAt(rs, "last_heartbeat_at"),
                rs.getString("source_connection_name"),
                rs.getString("target_connection_name"),
                rs.getLong("docs_processed"),
                rs.getLong("active_millis"));
    }

    private static Instant instantAt(ResultSet rs, String col) throws SQLException {
        long v = rs.getLong(col);
        if (rs.wasNull()) return null;
        return Instant.ofEpochMilli(v);
    }

    private static void setInstant(PreparedStatement ps, int idx, Instant i) throws SQLException {
        if (i == null) ps.setNull(idx, java.sql.Types.INTEGER);
        else           ps.setLong(idx, i.toEpochMilli());
    }
}

package com.kubrik.mex.events;

import com.kubrik.mex.backup.event.BackupEvent;
import com.kubrik.mex.backup.event.RestoreEvent;
import com.kubrik.mex.cluster.audit.OpsAuditRecord;
import com.kubrik.mex.cluster.model.TopologySnapshot;
import com.kubrik.mex.migration.events.JobEvent;
import com.kubrik.mex.model.ConnectionState;
import com.kubrik.mex.monitoring.alerting.AlertEvent;
import com.kubrik.mex.monitoring.model.MetricSample;
import com.kubrik.mex.monitoring.recording.RecordingEvent;
import com.kubrik.mex.monitoring.store.ProfileSampleRecord;
import com.kubrik.mex.security.cert.CertExpiryEvent;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Pub/sub hub shared across the app. Every {@code onX} returns a
 * {@link Subscription} whose {@link Subscription#close()} detaches the listener —
 * views that come and go (ConnectionCard, ExpandedMetricView, row-detail modals)
 * MUST close their subscriptions on dispose to avoid stale-listener retention.
 *
 * <p>Internals use a {@link ConcurrentHashMap} keyed by a unique token so
 * publish iterates a concurrent snapshot without the copy-on-write cost of the
 * previous {@code CopyOnWriteArrayList}, and unsubscribe is O(1).
 */
public class EventBus {

    private static final Object TOKEN_PREFIX_STATE    = new Object();
    private static final Object TOKEN_PREFIX_LOG      = new Object();
    private static final Object TOKEN_PREFIX_JOB      = new Object();
    private static final Object TOKEN_PREFIX_METRICS  = new Object();
    private static final Object TOKEN_PREFIX_AFIRED   = new Object();
    private static final Object TOKEN_PREFIX_ACLEARED = new Object();

    private final ConcurrentMap<Object, Consumer<ConnectionState>> stateListeners = new ConcurrentHashMap<>();
    private final ConcurrentMap<Object, BiConsumer<String, String>> logListeners  = new ConcurrentHashMap<>();
    private final ConcurrentMap<Object, Consumer<JobEvent>> jobListeners          = new ConcurrentHashMap<>();
    private final ConcurrentMap<Object, Consumer<List<MetricSample>>> metricListeners = new ConcurrentHashMap<>();
    private final ConcurrentMap<Object, Consumer<AlertEvent>> alertFiredListeners   = new ConcurrentHashMap<>();
    private final ConcurrentMap<Object, Consumer<AlertEvent>> alertClearedListeners = new ConcurrentHashMap<>();
    private final ConcurrentMap<Object, BiConsumer<String, TopologySnapshot>> topologyListeners =
            new ConcurrentHashMap<>();
    private final ConcurrentMap<Object, Consumer<RecordingEvent>> recordingListeners = new ConcurrentHashMap<>();
    private final ConcurrentMap<Object, Consumer<List<ProfileSampleRecord>>> profilerSampleListeners =
            new ConcurrentHashMap<>();
    private final ConcurrentMap<Object, Consumer<OpsAuditRecord>> opsAuditListeners =
            new ConcurrentHashMap<>();
    private final ConcurrentMap<Object, Consumer<BackupEvent>> backupListeners =
            new ConcurrentHashMap<>();
    private final ConcurrentMap<Object, Consumer<RestoreEvent>> restoreListeners =
            new ConcurrentHashMap<>();
    private final ConcurrentMap<Object, Consumer<CertExpiryEvent>> certExpiryListeners =
            new ConcurrentHashMap<>();
    // v2.8.4 — Labs lifecycle feed. One listener per wizard / pane.
    private final ConcurrentMap<Object, Consumer<com.kubrik.mex.labs.events.LabLifecycleEvent>> labListeners =
            new ConcurrentHashMap<>();

    /** Latest topology snapshot per connection — delivered to late subscribers so newly-mounted
     *  UI can render without waiting for the next sampler tick (v2.4 TOPO-17). */
    private final ConcurrentMap<String, TopologySnapshot> latestTopology = new ConcurrentHashMap<>();

    /** Token returned by every {@code onX}. Closing detaches the listener; idempotent. */
    public interface Subscription extends AutoCloseable {
        @Override void close();
    }

    public Subscription onState(Consumer<ConnectionState> l) {
        Object key = new Object();
        stateListeners.put(key, l);
        return () -> stateListeners.remove(key);
    }

    public Subscription onLog(BiConsumer<String, String> l) {
        Object key = new Object();
        logListeners.put(key, l);
        return () -> logListeners.remove(key);
    }

    /** Migration feature (v1.1.0) — see docs/mvp-technical-spec.md §15.3. */
    public Subscription onJob(Consumer<JobEvent> l) {
        Object key = new Object();
        jobListeners.put(key, l);
        return () -> jobListeners.remove(key);
    }

    /** Monitoring feature (v2.1.0) — see docs/v2/v2.1/technical-spec.md §6. */
    public Subscription onMetrics(Consumer<List<MetricSample>> l) {
        Object key = new Object();
        metricListeners.put(key, l);
        return () -> metricListeners.remove(key);
    }

    public Subscription onAlertFired(Consumer<AlertEvent> l) {
        Object key = new Object();
        alertFiredListeners.put(key, l);
        return () -> alertFiredListeners.remove(key);
    }

    public Subscription onAlertCleared(Consumer<AlertEvent> l) {
        Object key = new Object();
        alertClearedListeners.put(key, l);
        return () -> alertClearedListeners.remove(key);
    }

    /**
     * Cluster topology feed (v2.4 TOPO-15..17). Listeners receive
     * {@code (connectionId, TopologySnapshot)} tuples from
     * {@code ClusterTopologyService}. New subscribers immediately receive the
     * most-recent snapshot per connection so UI can render without waiting for
     * the next sampling tick (replay semantics per TOPO-17).
     */
    public Subscription onTopology(BiConsumer<String, TopologySnapshot> l) {
        Object key = new Object();
        topologyListeners.put(key, l);
        // Replay the latest known snapshot per connection so the new consumer can
        // render immediately instead of waiting out the sampler cadence.
        for (ConcurrentMap.Entry<String, TopologySnapshot> e : latestTopology.entrySet()) {
            try { l.accept(e.getKey(), e.getValue()); } catch (Exception ignored) {}
        }
        return () -> topologyListeners.remove(key);
    }

    /** Convenience for tests + UI code that only needs the latest snapshot. */
    public TopologySnapshot latestTopology(String connectionId) {
        return latestTopology.get(connectionId);
    }

    public void publishState(ConnectionState s) {
        for (Consumer<ConnectionState> l : stateListeners.values()) {
            try { l.accept(s); } catch (Exception ignored) {}
        }
    }

    public void publishLog(String connectionId, String line) {
        for (BiConsumer<String, String> l : logListeners.values()) {
            try { l.accept(connectionId, line); } catch (Exception ignored) {}
        }
    }

    public void publishJob(JobEvent e) {
        for (Consumer<JobEvent> l : jobListeners.values()) {
            try { l.accept(e); } catch (Exception ignored) {}
        }
    }

    public void publishMetrics(List<MetricSample> samples) {
        for (Consumer<List<MetricSample>> l : metricListeners.values()) {
            try { l.accept(samples); } catch (Exception ignored) {}
        }
    }

    public void publishAlertFired(AlertEvent e) {
        for (Consumer<AlertEvent> l : alertFiredListeners.values()) {
            try { l.accept(e); } catch (Exception ignored) {}
        }
    }

    public void publishAlertCleared(AlertEvent e) {
        for (Consumer<AlertEvent> l : alertClearedListeners.values()) {
            try { l.accept(e); } catch (Exception ignored) {}
        }
    }

    public void publishTopology(String connectionId, TopologySnapshot snap) {
        if (connectionId == null || snap == null) return;
        latestTopology.put(connectionId, snap);
        for (BiConsumer<String, TopologySnapshot> l : topologyListeners.values()) {
            try { l.accept(connectionId, snap); } catch (Exception ignored) {}
        }
    }

    /** Invoked on connection deletion so late subscribers don't receive stale replays. */
    public void clearTopologyFor(String connectionId) {
        latestTopology.remove(connectionId);
    }

    /**
     * Recording lifecycle feed (v2.3.0 EVENT-1). Delivery is synchronous on the
     * producer thread — UI subscribers must re-post to the JavaFX application
     * thread via {@code Platform.runLater} before touching scene nodes. There is
     * no ordering guarantee between {@link RecordingEvent.Started} and the first
     * captured sample, so consumers should read {@code RecordingService} state
     * for truth.
     */
    public Subscription onRecording(Consumer<RecordingEvent> l) {
        Object key = new Object();
        recordingListeners.put(key, l);
        return () -> recordingListeners.remove(key);
    }

    public void publishRecording(RecordingEvent e) {
        for (Consumer<RecordingEvent> l : recordingListeners.values()) {
            try { l.accept(e); } catch (Exception ignored) {}
        }
    }

    /**
     * Profiler slow-query feed (v2.3.0 technical-spec §4.4). Published by
     * {@code ProfilerSampler} after it persists, so the recording capture
     * subscriber can double-write samples into {@code recording_profile_samples}
     * without forking the sampler.
     */
    public Subscription onProfilerSamples(Consumer<List<ProfileSampleRecord>> l) {
        Object key = new Object();
        profilerSampleListeners.put(key, l);
        return () -> profilerSampleListeners.remove(key);
    }

    public void publishProfilerSamples(List<ProfileSampleRecord> samples) {
        if (samples == null || samples.isEmpty()) return;
        for (Consumer<List<ProfileSampleRecord>> l : profilerSampleListeners.values()) {
            try { l.accept(samples); } catch (Exception ignored) {}
        }
    }

    /**
     * Ops-audit feed (v2.4 AUD-*). Emitted after every successful insert into
     * {@code ops_audit}. Consumers: the Logs tab (live tail) and the Audit pane
     * (prepend row). Delivery is synchronous on the producer thread; UI
     * subscribers must re-post to the JavaFX application thread before touching
     * scene nodes. Unlike {@link #onTopology}, there is <b>no replay</b> —
     * audit rows are historical, and a new subscriber should page them from
     * {@code OpsAuditDao} directly.
     */
    public Subscription onOpsAudit(Consumer<OpsAuditRecord> l) {
        Object key = new Object();
        opsAuditListeners.put(key, l);
        return () -> opsAuditListeners.remove(key);
    }

    public void publishOpsAudit(OpsAuditRecord r) {
        if (r == null) return;
        for (Consumer<OpsAuditRecord> l : opsAuditListeners.values()) {
            try { l.accept(r); } catch (Exception ignored) {}
        }
    }

    /**
     * Backup lifecycle feed (v2.5 BKP-RUN-4). Publishes {@code Started},
     * {@code Progress}, and {@code Ended} events from the runner so the
     * catalog pane + status bar can track active backups without polling
     * the DAO. No replay — new subscribers page historical rows from
     * {@code BackupCatalogDao}. Delivery is synchronous on the producer
     * thread; UI subscribers must re-post to the JavaFX thread before
     * touching scene nodes.
     */
    public Subscription onBackup(Consumer<BackupEvent> l) {
        Object key = new Object();
        backupListeners.put(key, l);
        return () -> backupListeners.remove(key);
    }

    public void publishBackup(BackupEvent e) {
        if (e == null) return;
        for (Consumer<BackupEvent> l : backupListeners.values()) {
            try { l.accept(e); } catch (Exception ignored) {}
        }
    }

    // v2.8.4 — Labs lifecycle feed. See com.kubrik.mex.labs.events.LabLifecycleEvent.
    public Subscription onLab(Consumer<com.kubrik.mex.labs.events.LabLifecycleEvent> l) {
        Object key = new Object();
        labListeners.put(key, l);
        return () -> labListeners.remove(key);
    }

    /** Alias matching the publish/onX symmetry of other feeds. */
    public void publish(com.kubrik.mex.labs.events.LabLifecycleEvent e) {
        if (e == null) return;
        for (Consumer<com.kubrik.mex.labs.events.LabLifecycleEvent> l : labListeners.values()) {
            try { l.accept(e); } catch (Exception ignored) {}
        }
    }

    /**
     * Restore lifecycle feed (v2.5 Q2.5-E). Rehearse + Execute runs publish
     * the same event stream; consumers tell them apart via {@link
     * RestoreEvent.Started#mode()}. No replay — historical restore rows
     * live in {@code ops_audit} keyed by {@code command_name = "restore.*"}.
     */
    public Subscription onRestore(Consumer<RestoreEvent> l) {
        Object key = new Object();
        restoreListeners.put(key, l);
        return () -> restoreListeners.remove(key);
    }

    public void publishRestore(RestoreEvent e) {
        if (e == null) return;
        for (Consumer<RestoreEvent> l : restoreListeners.values()) {
            try { l.accept(e); } catch (Exception ignored) {}
        }
    }

    /** v2.6 Q2.6-E3 — cert-expiry sweep result, emitted once per connection
     *  per sweep. WelcomeView subscribes so its security chip refreshes
     *  after the scheduled fetch without the operator opening the
     *  Security tab. */
    public Subscription onCertExpiry(Consumer<CertExpiryEvent> l) {
        Object key = new Object();
        certExpiryListeners.put(key, l);
        return () -> certExpiryListeners.remove(key);
    }

    public void publishCertExpiry(CertExpiryEvent e) {
        if (e == null) return;
        for (Consumer<CertExpiryEvent> l : certExpiryListeners.values()) {
            try { l.accept(e); } catch (Exception ignored) {}
        }
    }
}

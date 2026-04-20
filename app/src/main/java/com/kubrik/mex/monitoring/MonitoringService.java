package com.kubrik.mex.monitoring;

import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.monitoring.alerting.AlertEvent;
import com.kubrik.mex.monitoring.alerting.AlertEventDao;
import com.kubrik.mex.monitoring.alerting.AlertRule;
import com.kubrik.mex.monitoring.alerting.AlertRuleDao;
import com.kubrik.mex.monitoring.alerting.Alerter;
import com.kubrik.mex.monitoring.alerting.DefaultRules;
import com.kubrik.mex.monitoring.alerting.Notifier;
import com.kubrik.mex.monitoring.exporter.MetricRegistry;
import com.kubrik.mex.monitoring.exporter.PrometheusExporter;
import com.kubrik.mex.monitoring.model.MetricId;
import com.kubrik.mex.monitoring.model.MetricSample;
import com.kubrik.mex.monitoring.sampler.ProfilerSampler;
import com.kubrik.mex.monitoring.sampler.Sampler;
import com.kubrik.mex.monitoring.sampler.SamplerKind;
import com.kubrik.mex.monitoring.sampler.SamplerScheduler;
import com.kubrik.mex.monitoring.store.MetricStore;
import com.kubrik.mex.monitoring.store.MonitoringProfileDao;
import com.kubrik.mex.monitoring.store.ProfileSampleDao;
import com.kubrik.mex.monitoring.store.RetentionJanitor;
import com.kubrik.mex.monitoring.store.RollupDao;
import com.kubrik.mex.monitoring.store.RollupWorker;
import com.kubrik.mex.store.Database;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Facade for the Monitoring subsystem. P-1 wires together the profile DAO,
 * the raw-tier {@link MetricStore}, and the {@link SamplerScheduler}. Concrete
 * samplers are registered externally (the code that owns a {@link com.kubrik.mex.core.MongoService}
 * for the connection calls {@link #registerSampler}).
 *
 * <p>Later phases extend this facade with rollup workers, a retention janitor,
 * the alerter, and the Prometheus exporter — all hanging off the same {@link EventBus}.
 */
public final class MonitoringService implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(MonitoringService.class);

    private final Database database;
    private final EventBus eventBus;
    private final MonitoringProfileDao profileDao;
    private final MetricStore metricStore;
    private final SamplerScheduler scheduler;
    private final ConcurrentMap<String, MonitoringProfile> profiles = new ConcurrentHashMap<>();

    private final RollupWorker rollupWorker;
    private final RetentionJanitor janitor;
    private final Alerter alerter;
    /** Per-connection auto-disable timers for slow-query profiling. Replaced on each setProfilingEnabled call. */
    private final ConcurrentMap<String, java.util.concurrent.ScheduledFuture<?>> profilerAutoDisablers =
            new ConcurrentHashMap<>();
    private final java.util.concurrent.ScheduledExecutorService autoDisableExec =
            java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "mex-profiler-auto-disable");
                t.setDaemon(true);
                return t;
            });
    private final AlertRuleDao ruleDao;
    private final AlertEventDao eventDao;
    private final Notifier notifier;
    private final MetricRegistry registry;
    private final PrometheusExporter exporter;

    public MonitoringService(Database database, EventBus eventBus) {
        this.database = database;
        this.eventBus = eventBus;
        this.profileDao = new MonitoringProfileDao(database.connection());
        this.metricStore = new MetricStore(database.connection());
        this.scheduler = new SamplerScheduler(this::onSamples, this::onSamplerError);
        RollupDao rollupDao = new RollupDao(database.connection());
        this.rollupWorker = new RollupWorker(rollupDao);
        this.janitor = new RetentionJanitor(database.connection(), rollupDao, profiles::values);
        this.ruleDao = new AlertRuleDao(database.connection());
        this.eventDao = new AlertEventDao(database.connection());
        this.notifier = new Notifier(eventBus);
        this.alerter = new Alerter(this::onAlertFired, this::onAlertCleared);
        this.registry = new MetricRegistry();
        this.exporter = new PrometheusExporter(registry);

        eventBus.onMetrics(alerter::onSamples);
        eventBus.onMetrics(registry::onSamples);
    }

    /**
     * Start background workers: rollup + retention. Call once at app startup
     * (after the event bus is available). Idempotent.
     */
    public void startBackgroundWorkers() {
        rollupWorker.start();
        janitor.start();
        try {
            ruleDao.installDefaultsIfMissing(DefaultRules.all(), DefaultRules.SOURCE_TAG);
            alerter.installRules(ruleDao.loadAll());
        } catch (SQLException e) {
            log.warn("failed to install default alert rules", e);
        }
    }

    public void startPrometheusExporter(String bind, int port) throws IOException {
        exporter.start(bind, port);
    }

    public void stopPrometheusExporter() { exporter.close(); }

    public boolean isPrometheusRunning() { return exporter.isRunning(); }

    /** Register / replace / disable alert rules. */
    public void upsertRule(AlertRule r) throws SQLException {
        ruleDao.upsert(r);
        alerter.installRules(ruleDao.loadAll());
    }

    public Alerter alerter() { return alerter; }
    public MetricRegistry registry() { return registry; }

    /**
     * Persist (or update) a profile and, if {@code enabled}, prepare for sampler
     * registration. Does NOT start any samplers on its own — concrete {@link Sampler}s
     * are provided by the caller once a {@link com.kubrik.mex.core.MongoService} is
     * available for this connection.
     */
    public MonitoringProfile enable(MonitoringProfile profile) throws SQLException {
        profileDao.upsert(profile);
        profiles.put(profile.connectionId(), profile);
        return profile;
    }

    public void disable(String connectionId) throws SQLException {
        scheduler.stop(connectionId);
        profiles.remove(connectionId);
        Optional<MonitoringProfile> existing = profileDao.find(connectionId);
        existing.ifPresent(p -> {
            MonitoringProfile off = new MonitoringProfile(
                    p.connectionId(), false,
                    p.instancePollInterval(), p.storagePollInterval(), p.indexUsagePollInterval(),
                    p.readPreference(),
                    p.profilerEnabled(), p.profilerSlowMs(), p.profilerAutoDisableAfter(),
                    p.topNCollectionsPerDb(), p.pinnedCollections(), p.retention(),
                    p.createdAt(), java.time.Instant.now());
            try { profileDao.upsert(off); } catch (SQLException e) {
                log.warn("failed to persist disabled profile for {}", connectionId, e);
            }
        });
    }

    /** Every profile currently held in memory (i.e. enabled this session). */
    public java.util.Collection<MonitoringProfile> enabledProfiles() {
        return profiles.values();
    }

    /** Slow-query samples captured by {@link com.kubrik.mex.monitoring.sampler.ProfilerSampler}
     *  between {@code fromMs} (inclusive) and {@code toMs} (exclusive). Returns an empty
     *  list when the DAO throws — a missing table or transient SQLite lock shouldn't crash
     *  the UI that calls this from a background thread. */
    public java.util.List<com.kubrik.mex.monitoring.store.ProfileSampleRecord>
    loadSlowSamples(String connectionId, long fromMs, long toMs) {
        try {
            return new com.kubrik.mex.monitoring.store.ProfileSampleDao(database.connection())
                    .loadRange(connectionId, fromMs, toMs);
        } catch (SQLException e) {
            log.debug("loadSlowSamples failed for {}: {}", connectionId, e.toString());
            return java.util.List.of();
        }
    }

    public Optional<MonitoringProfile> profile(String connectionId) {
        MonitoringProfile mem = profiles.get(connectionId);
        if (mem != null) return Optional.of(mem);
        try {
            Optional<MonitoringProfile> loaded = profileDao.find(connectionId);
            loaded.ifPresent(p -> profiles.put(connectionId, p));
            return loaded;
        } catch (SQLException e) {
            log.warn("failed to load profile for {}", connectionId, e);
            return Optional.empty();
        }
    }

    /** Builds a tailing {@link ProfilerSampler} bound to the shared
     *  {@link ProfileSampleDao}; callers pass it to {@link #registerSampler(Sampler)}. */
    public ProfilerSampler newProfilerSampler(String connectionId, com.kubrik.mex.core.MongoService mongo) {
        return new ProfilerSampler(connectionId, mongo,
                new ProfileSampleDao(database.connection()),
                eventBus,
                mongo::listDatabaseNames);
    }

    /**
     * Register a concrete {@link Sampler}. The scheduler will start its loop; the
     * profile for this connection must already exist and be enabled.
     */
    public void registerSampler(Sampler sampler) {
        MonitoringProfile p = profiles.get(sampler.connectionId());
        if (p == null || !p.enabled()) {
            throw new IllegalStateException(
                    "no enabled monitoring profile for connection " + sampler.connectionId());
        }
        scheduler.register(p, sampler);
    }

    /**
     * Enable or disable slow-query profiling for a single connection. Applies the
     * server-side {@code profile} command, persists the updated {@link MonitoringProfile},
     * and starts / stops the tailing {@link ProfilerSampler}. When enabling, an
     * {@code autoDisableAfter} duration greater than zero schedules a one-shot
     * that flips the flag back off — so nobody accidentally leaves profiling on
     * overnight on a production cluster.
     *
     * @return the {@link ProfilingController.Result} summarising which DBs accepted the change.
     */
    public ProfilingController.Result setProfilingEnabled(String connectionId,
                                                          com.kubrik.mex.core.MongoService mongo,
                                                          boolean enabled,
                                                          int slowMs,
                                                          java.time.Duration autoDisableAfter)
            throws SQLException {
        MonitoringProfile p = profiles.get(connectionId);
        if (p == null) {
            throw new IllegalStateException(
                    "no enabled monitoring profile for connection " + connectionId);
        }
        // Server-side toggle first — if the server rejects everything we still
        // persist the user's intent but the sampler start/stop reflects reality.
        ProfilingController.Result result = enabled
                ? ProfilingController.enable(mongo, slowMs)
                : ProfilingController.disable(mongo);

        MonitoringProfile updated = new MonitoringProfile(
                p.connectionId(), p.enabled(),
                p.instancePollInterval(), p.storagePollInterval(), p.indexUsagePollInterval(),
                p.readPreference(),
                enabled, slowMs, autoDisableAfter,
                p.topNCollectionsPerDb(), p.pinnedCollections(), p.retention(),
                p.createdAt(), java.time.Instant.now());
        profileDao.upsert(updated);
        profiles.put(connectionId, updated);

        // Cancel any outstanding auto-disabler; on enable we may install a new one.
        java.util.concurrent.ScheduledFuture<?> prev = profilerAutoDisablers.remove(connectionId);
        if (prev != null) prev.cancel(false);

        if (enabled) {
            ProfilerSampler sampler = newProfilerSampler(connectionId, mongo);
            try { scheduler.register(updated, sampler); }
            catch (RuntimeException ex) {
                log.warn("profiler register failed for {}", connectionId, ex);
            }
            if (autoDisableAfter != null && !autoDisableAfter.isZero() && !autoDisableAfter.isNegative()) {
                java.util.concurrent.ScheduledFuture<?> f = autoDisableExec.schedule(() -> {
                    try { setProfilingEnabled(connectionId, mongo, false, slowMs, java.time.Duration.ZERO); }
                    catch (Throwable t) { log.warn("auto-disable for {} failed", connectionId, t); }
                }, autoDisableAfter.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
                profilerAutoDisablers.put(connectionId, f);
            }
        } else {
            scheduler.stopOne(connectionId, SamplerKind.PROFILER);
        }
        return result;
    }

    /** Read-through for the raw tier. */
    public List<MetricSample> queryRaw(String connectionId, MetricId metric,
                                       long fromMsInclusive, long toMsExclusive) throws SQLException {
        return metricStore.queryRaw(connectionId, metric, fromMsInclusive, toMsExclusive);
    }

    /** For tests / UI diagnostics: number of samples dropped due to writer back-pressure. */
    public long droppedSamples() { return metricStore.droppedSamples(); }

    /** For tests: number of samples persisted since startup. */
    public long writtenSamples() { return metricStore.writtenSamples(); }

    public MetricStore metricStore() { return metricStore; }

    @Override
    public void close() {
        try { exporter.close(); }      catch (Throwable t) { log.warn("exporter close failed", t); }
        try { janitor.close(); }       catch (Throwable t) { log.warn("janitor close failed", t); }
        try { rollupWorker.close(); }  catch (Throwable t) { log.warn("rollupWorker close failed", t); }
        try { scheduler.close(); }     catch (Throwable t) { log.warn("scheduler close failed", t); }
        try { metricStore.close(); }   catch (Throwable t) { log.warn("metricStore close failed", t); }
        try { autoDisableExec.shutdownNow(); } catch (Throwable t) { log.warn("autoDisableExec close failed", t); }
    }

    private void onSamples(List<MetricSample> batch) {
        metricStore.persistAsync(batch);
        eventBus.publishMetrics(batch);
    }

    private void onSamplerError(Sampler sampler, Throwable t) {
        log.debug("sampler {} on {} raised {}",
                sampler.kind(), sampler.connectionId(), t.toString());
        eventBus.publishLog(sampler.connectionId(),
                "monitoring.sampler.error " + sampler.kind() + " " + t);
    }

    private void onAlertFired(AlertEvent e) {
        try { eventDao.insert(e); } catch (SQLException ex) { log.warn("persist alert failed", ex); }
        notifier.onFired(e);
    }

    private void onAlertCleared(AlertEvent e) {
        try { eventDao.markCleared(e.ruleId(), e.connectionId(), e.firedAtMs()); }
        catch (SQLException ex) { log.warn("persist alert-cleared failed", ex); }
        notifier.onCleared(e);
    }
}

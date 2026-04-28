package com.kubrik.mex.monitoring.sampler;

import com.kubrik.mex.core.MongoService;
import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.monitoring.model.MetricSample;
import com.kubrik.mex.monitoring.store.ProfileSampleDao;
import com.kubrik.mex.monitoring.store.ProfileSampleRecord;
import com.kubrik.mex.monitoring.util.DocUtil;
import com.kubrik.mex.monitoring.util.Redactor;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import org.bson.Document;
import org.bson.json.JsonWriterSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Tails {@code system.profile} on each monitored database, redacts each sample,
 * and persists into {@code profiler_samples}. Does not emit metric samples (the
 * metric facet is computed at query time over the persisted rows).
 *
 * <p>QPERF-1 caps the tail at 200 samples per poll per DB.
 */
public final class ProfilerSampler implements Sampler {

    private static final Logger log = LoggerFactory.getLogger(ProfilerSampler.class);

    /** JsonWriterSettings.builder().build() walks every BSON type on
     *  every call; caching the immutable result saves a non-trivial
     *  allocation per profiler doc. The default settings are what we
     *  want — extended JSON, no indenting. */
    private static final JsonWriterSettings JSON_SETTINGS =
            JsonWriterSettings.builder().build();

    public static final int CAP_PER_POLL = 200;

    private final String connectionId;
    private final MongoService svc;
    private final ProfileSampleDao dao;
    private final EventBus bus;
    private final Supplier<List<String>> dbNames;
    private final Map<String, Date> lastSeen = new HashMap<>();

    public ProfilerSampler(String connectionId, MongoService svc, ProfileSampleDao dao,
                           EventBus bus, Supplier<List<String>> dbNames) {
        this.connectionId = connectionId;
        this.svc = svc;
        this.dao = dao;
        this.bus = bus;
        this.dbNames = dbNames;
    }

    @Override public SamplerKind kind() { return SamplerKind.PROFILER; }
    @Override public String connectionId() { return connectionId; }

    @Override
    public List<MetricSample> sample(Instant now) throws Exception {
        for (String db : dbNames.get()) {
            tail(db);
        }
        return List.of();
    }

    private void tail(String db) throws Exception {
        MongoDatabase mdb = svc.database(db);
        MongoCollection<Document> prof;
        try { prof = mdb.getCollection("system.profile"); }
        catch (Throwable t) { return; }
        Date since = lastSeen.getOrDefault(db, new Date(0));
        List<ProfileSampleRecord> batch = new ArrayList<>();
        Date newest = since;
        try {
            for (Document d : prof.find(Filters.gt("ts", since))
                    .sort(Sorts.ascending("ts"))
                    .limit(CAP_PER_POLL)) {
                Date ts = (Date) d.get("ts");
                if (ts == null) continue;
                if (ts.after(newest)) newest = ts;
                Document cmd = DocUtil.sub(d, "command");
                String cmdJson = Redactor.redact(cmd).toJson(JSON_SETTINGS);
                batch.add(new ProfileSampleRecord(
                        connectionId,
                        ts.getTime(),
                        d.getString("ns"),
                        d.getString("op"),
                        DocUtil.longVal(d, "millis", 0),
                        d.getString("planSummary"),
                        numeric(d, "docsExamined"),
                        numeric(d, "nreturned"),
                        numeric(d, "keysExamined"),
                        numeric(d, "numYield"),
                        numeric(d, "responseLength"),
                        d.getString("queryHash"),
                        d.getString("planCacheKey"),
                        cmdJson));
            }
        } catch (com.mongodb.MongoCommandException mce) {
            // Profiler may not be enabled for this DB; that's fine, but log it so a
            // perpetually-failing tail is visible (B7). Same poll returns any rows we'd
            // gathered before the cursor threw; next poll re-reads from the old watermark.
            log.debug("profiler tail on db {} aborted: {}", db, mce.toString());
            return;
        }
        if (!batch.isEmpty()) {
            dao.insertBatch(batch);
            if (bus != null) bus.publishProfilerSamples(batch);
            lastSeen.put(db, newest);
        }
    }

    private static Long numeric(Document d, String field) {
        Object v = d.get(field);
        return v instanceof Number n ? n.longValue() : null;
    }
}

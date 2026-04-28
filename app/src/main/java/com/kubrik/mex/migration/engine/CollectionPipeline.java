package com.kubrik.mex.migration.engine;

import com.kubrik.mex.core.MongoService;
import com.kubrik.mex.migration.sink.MigrationSink;
import com.kubrik.mex.migration.sink.MigrationSinkFactory;
import com.kubrik.mex.migration.sink.BsonDumpSink;
import com.kubrik.mex.migration.sink.CsvSink;
import com.kubrik.mex.migration.sink.JsonArraySink;
import com.kubrik.mex.migration.sink.MongoSink;
import com.kubrik.mex.migration.sink.NdjsonSink;
import com.kubrik.mex.migration.sink.PluginSinkRegistry;
import com.kubrik.mex.migration.sink.SinkWriter;
import com.kubrik.mex.migration.spec.ConflictMode;
import com.kubrik.mex.migration.spec.MigrationSpec;
import com.kubrik.mex.migration.spec.SinkSpec;
import com.kubrik.mex.migration.spec.TransformSpec;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/** Copies one source namespace into one target namespace end-to-end.
 *  <ol>
 *    <li>Resolves partitions via {@link Partitioner}.</li>
 *    <li>Prepares the target collection (drop-and-recreate if asked).</li>
 *    <li>Optionally creates unique indexes up front (UPSERT mode).</li>
 *    <li>Spins a Reader → Transformer → Writer trio per partition on virtual threads.</li>
 *    <li>Copies non-unique indexes after data.</li>
 *  </ol>
 *  See docs/mvp-technical-spec.md §6.1. */
public final class CollectionPipeline {

    private static final Logger log = LoggerFactory.getLogger(CollectionPipeline.class);

    private final JobContext ctx;
    private final MongoService sourceSvc;
    private final MongoService targetSvc;
    private final CollectionPlan plan;
    private final BsonValue resumeAfterId;  // last written _id on a previous run, or null

    public CollectionPipeline(JobContext ctx,
                              MongoService sourceSvc,
                              MongoService targetSvc,
                              CollectionPlan plan,
                              BsonValue resumeAfterId) {
        this.ctx = ctx;
        this.sourceSvc = sourceSvc;
        this.targetSvc = targetSvc;
        this.plan = plan;
        this.resumeAfterId = resumeAfterId;
    }

    /** Run to completion. Returns the total docs written for this collection. */
    public long run() throws Exception {
        MigrationSpec spec = ctx.spec();
        Namespaces.Ns src = plan.source();
        Namespaces.Ns tgt = plan.target();

        MongoDatabase sourceDb = sourceSvc.database(src.db());
        MongoCollection<RawBsonDocument> source = sourceSvc.rawCollection(src.db(), src.coll());

        // EXT-1/EXT-2 — every destination goes through a MigrationSink. When no file sink is
        // configured we build a MongoSink that wraps the target collection; otherwise we build
        // the requested file sink. Mongo-specific bootstrap (drop-and-recreate, unique-index
        // prebuild, post-copy index creation) only runs on the Mongo-target branch.
        SinkSpec sinkSpec = firstSink(spec);
        boolean toMongo = sinkSpec == null;
        MongoDatabase targetDb = toMongo ? targetSvc.database(tgt.db()) : null;
        MongoCollection<RawBsonDocument> target = toMongo
                ? targetSvc.rawCollection(tgt.db(), tgt.coll())
                : null;
        List<Document> sourceIndexes = List.of();

        if (toMongo) {
            prepareTarget(targetDb, target);
            sourceIndexes = listUserIndexes(sourceSvc, src.db(), src.coll());
            if (plan.conflictMode() == ConflictMode.UPSERT_BY_ID) {
                createIndexes(target, sourceIndexes, true);  // unique first (T-16)
            }
        }

        TransformSpec xformSpec = spec.options().transform().getOrDefault(plan.sourceNs(), null);
        BsonTransform compiled = BsonTransform.compile(xformSpec);

        List<Partition> partitions = resolvePartitions(sourceDb, source);
        log.info("{}: copying with {} partition(s){}", plan.sourceNs(), partitions.size(),
                toMongo ? "" : " → sink " + sinkSpec.kind());

        int retryAttempts = spec.options().performance().retryAttempts();
        AtomicLong totalWritten = new AtomicLong();
        AtomicReference<Exception> firstError = new AtomicReference<>();

        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<?>> futures = new ArrayList<>(partitions.size() * 3);
            for (Partition p : partitions) {
                BlockingQueue<Batch> readToXform = new ArrayBlockingQueue<>(4);
                BlockingQueue<Batch> xformToWrite = new ArrayBlockingQueue<>(4);

                Reader reader = new Reader(ctx, source, p, xformSpec, readToXform, resumeAfterId);
                Transformer transformer = new Transformer(ctx, compiled, readToXform, xformToWrite, plan.sourceNs());

                MigrationSink sink = toMongo
                        ? new MongoSink(target, plan.conflictMode(), retryAttempts)
                        : buildFileSink(sinkSpec);
                SinkWriter sinkWriter = new SinkWriter(ctx, sink, tgt, xformToWrite,
                        (lastId, added) -> {
                            totalWritten.addAndGet(added);
                            ctx.setLastId(plan.sourceNs(), lastId);
                        });

                futures.add(pool.submit(wrap(reader::run, firstError)));
                futures.add(pool.submit(wrap(transformer::run, firstError)));
                futures.add(pool.submit(wrap(() -> {
                    try { sinkWriter.run(); } catch (Exception e) { throw new RuntimeException(e); }
                }, firstError)));
            }
            for (Future<?> f : futures) {
                try { f.get(); }
                catch (Exception e) { firstError.compareAndSet(null, e); }
            }
        }

        if (firstError.get() != null) throw firstError.get();

        if (toMongo) {
            createIndexes(target, sourceIndexes, false);   // remaining (or all) user indexes
        }

        log.info("{}: done, {} docs written", plan.sourceNs(), totalWritten.get());
        return totalWritten.get();
    }

    /** EXT-2 — v2.0.0 ships with a single sink per spec. The list-shape on the spec leaves
     *  room for fan-out later without a schema break. */
    private static SinkSpec firstSink(MigrationSpec spec) {
        List<SinkSpec> sinks = spec.options().sinks();
        return sinks == null || sinks.isEmpty() ? null : sinks.get(0);
    }

    private static MigrationSink buildFileSink(SinkSpec spec) {
        java.nio.file.Path base = java.nio.file.Path.of(spec.path());
        return switch (spec.kind()) {
            case NDJSON     -> new NdjsonSink(base);
            case JSON_ARRAY -> new JsonArraySink(base);
            case CSV        -> new CsvSink(base);
            case BSON_DUMP  -> new BsonDumpSink(base);
            case PLUGIN     -> buildPluginSink(spec);
        };
    }

    private static MigrationSink buildPluginSink(SinkSpec spec) {
        MigrationSinkFactory factory = PluginSinkRegistry.resolve(spec.pluginName());
        if (factory == null) {
            throw new IllegalStateException(
                    "Sink plugin `" + spec.pluginName() + "` is not registered. "
                  + "Drop the plugin JAR into ~/.mongo-explorer/plugins/ and restart. "
                  + "Registered plugins: " + PluginSinkRegistry.registered());
        }
        return factory.create(spec);
    }

    private void prepareTarget(MongoDatabase targetDb, MongoCollection<RawBsonDocument> target) {
        if (plan.conflictMode() == ConflictMode.DROP_AND_RECREATE) {
            log.info("{}: dropping target {} before copy", plan.sourceNs(), plan.targetNs());
            target.drop();
        }
        // SCOPE-5 — if the target collection does not yet exist, create it with the
        // source's capped/validator/collation/time-series options. Existing collections
        // keep their current options (we don't mutate schema on APPEND/UPSERT).
        Namespaces.Ns srcNs = plan.source();
        Namespaces.Ns tgtNs = plan.target();
        if (collectionExists(targetDb, tgtNs.coll())) return;
        Document opts = readSourceOptions(srcNs);
        if (opts == null || opts.isEmpty()) {
            // Plain collection — the driver will auto-create on first insert.
            return;
        }
        Document create = new Document("create", tgtNs.coll());
        copyIfPresent(opts, create, "capped");
        copyIfPresent(opts, create, "size");
        copyIfPresent(opts, create, "max");
        copyIfPresent(opts, create, "validator");
        copyIfPresent(opts, create, "validationLevel");
        copyIfPresent(opts, create, "validationAction");
        copyIfPresent(opts, create, "collation");
        copyIfPresent(opts, create, "timeseries");
        copyIfPresent(opts, create, "expireAfterSeconds"); // time-series TTL
        try {
            targetDb.runCommand(create);
            log.info("{}: created target with options {}", plan.targetNs(),
                    create.keySet().stream().filter(k -> !"create".equals(k)).toList());
        } catch (Exception e) {
            // Cross-version incompatibilities surface here (e.g. time-series on pre-5.0).
            // The copy still runs — the driver will auto-create on first insert — but the
            // user loses the SCOPE-5 options. Surface the warning via the log stream.
            log.warn("{}: SCOPE-5 createCollection with {} failed; falling back to auto-create: {}",
                    plan.targetNs(), create.keySet(), e.getMessage());
        }
    }

    private static boolean collectionExists(MongoDatabase db, String coll) {
        for (String name : db.listCollectionNames()) {
            if (name.equals(coll)) return true;
        }
        return false;
    }

    /** Reads {@code options} from {@code listCollections} on the source; returns null
     *  if the collection is not found (e.g. race with a DROP). */
    private Document readSourceOptions(Namespaces.Ns srcNs) {
        MongoDatabase sourceDb = sourceSvc.database(srcNs.db());
        Document filter = new Document("name", srcNs.coll());
        Document entry = sourceDb.listCollections().filter(filter).first();
        if (entry == null) return null;
        Object opts = entry.get("options");
        return opts instanceof Document d ? d : new Document();
    }

    private static void copyIfPresent(Document from, Document to, String key) {
        if (from.containsKey(key)) to.put(key, from.get(key));
    }

    private List<Partition> resolvePartitions(MongoDatabase sourceDb,
                                              MongoCollection<RawBsonDocument> source) {
        return Partitioner.split(sourceDb, source, ctx.spec().options().performance());
    }

    private List<Document> listUserIndexes(MongoService svc, String db, String coll) {
        if (!ctx.spec().scope().migrateIndexes()) return List.of();
        List<Document> out = new ArrayList<>();
        for (Document d : svc.listIndexes(db, coll)) {
            String name = d.getString("name");
            if (!"_id_".equals(name)) out.add(d);  // BR-2: never copy the implicit _id_
        }
        return out;
    }

    private void createIndexes(MongoCollection<RawBsonDocument> target,
                               List<Document> indexes,
                               boolean uniqueOnly) {
        for (Document idx : indexes) {
            Boolean unique = idx.getBoolean("unique", false);
            if (uniqueOnly && !Boolean.TRUE.equals(unique)) continue;
            if (!uniqueOnly && Boolean.TRUE.equals(unique)) {
                // already created in the uniqueOnly phase if we're in UPSERT mode
                if (plan.conflictMode() == ConflictMode.UPSERT_BY_ID) continue;
            }
            IndexOptions opts = buildIndexOptions(idx);
            Bson keys = (Bson) idx.get("key", Document.class);
            try {
                target.createIndex(keys, opts);
            } catch (Exception e) {
                log.warn("{}: index '{}' creation failed: {}",
                        plan.targetNs(), idx.getString("name"), e.getMessage());
            }
        }
    }

    private IndexOptions buildIndexOptions(Document idx) {
        IndexOptions o = new IndexOptions();
        String name = idx.getString("name");
        if (name != null) o.name(name);
        if (Boolean.TRUE.equals(idx.getBoolean("unique"))) o.unique(true);
        if (Boolean.TRUE.equals(idx.getBoolean("sparse"))) o.sparse(true);
        if (Boolean.TRUE.equals(idx.getBoolean("background"))) o.background(true);
        Number ttl = idx.get("expireAfterSeconds", Number.class);
        if (ttl != null) o.expireAfter(ttl.longValue(), TimeUnit.SECONDS);
        Document pfe = idx.get("partialFilterExpression", Document.class);
        if (pfe != null) o.partialFilterExpression(pfe);
        return o;
    }

    private static Runnable wrap(RunnableEx r, AtomicReference<Exception> err) {
        return () -> {
            try { r.run(); }
            catch (Exception e) { err.compareAndSet(null, e); }
        };
    }

    @FunctionalInterface
    private interface RunnableEx { void run() throws Exception; }
}

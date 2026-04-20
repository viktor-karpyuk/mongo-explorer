package com.kubrik.mex.migration.engine;

import com.kubrik.mex.core.MongoService;
import com.kubrik.mex.migration.spec.ConflictMode;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** SCOPE-6 — rebuilds a source view on the target, rewriting collection references inside
 *  the view's aggregation pipeline according to the job's rename map.
 *  <p>
 *  <b>Sequencing:</b> views must run after every {@link CollectionPipeline} has finished so
 *  their base collections already exist on the target; {@code JobRunner} enforces this by
 *  partitioning the plan list. Cross-DB references (e.g. {@code $lookup.from} pointing into
 *  another database) are left untouched — the migration has no way to know whether that
 *  other database exists on the target yet. */
public final class ViewCreator {

    private static final Logger log = LoggerFactory.getLogger(ViewCreator.class);

    /** Fields inside aggregation stages that hold a **local-DB** collection name and therefore
     *  need rewriting under {@link #applyRenameMap}. Other collection-referencing fields
     *  ({@code $merge.into}, {@code $out}) don't appear in view pipelines (views are read-only
     *  by definition) but are listed here for future use. */
    private static final List<String> PIPELINE_COLL_FIELDS = List.of(
            "from",   // $lookup, $graphLookup
            "coll"    // $unionWith
    );

    private final JobContext ctx;
    private final MongoService sourceSvc;
    private final MongoService targetSvc;
    private final CollectionPlan plan;
    /** `sourceCollName` → `targetCollName` within the same database. Populated from the job's
     *  rename map by the caller — we keep the translation at name-level since view pipelines
     *  reference by collection name only. */
    private final Map<String, String> localRenameMap;

    public ViewCreator(JobContext ctx,
                       MongoService sourceSvc,
                       MongoService targetSvc,
                       CollectionPlan plan,
                       Map<String, String> localRenameMap) {
        this.ctx = ctx;
        this.sourceSvc = sourceSvc;
        this.targetSvc = targetSvc;
        this.plan = plan;
        this.localRenameMap = localRenameMap;
    }

    public void run() {
        if (ctx.spec().options().executionMode()
                == com.kubrik.mex.migration.spec.ExecutionMode.DRY_RUN) {
            log.info("{}: dry-run — skipping view creation", plan.targetNs());
            return;
        }

        Namespaces.Ns src = plan.source();
        Namespaces.Ns tgt = plan.target();
        Document info = sourceSvc.collectionInfo(src.db(), src.coll());
        if (info.isEmpty()) {
            log.warn("{}: source view info not found; skipping", plan.sourceNs());
            return;
        }
        Object optsRaw = info.get("options");
        if (!(optsRaw instanceof Document opts)) {
            log.warn("{}: source has no view options; skipping", plan.sourceNs());
            return;
        }
        String viewOn = opts.getString("viewOn");
        if (viewOn == null) {
            log.warn("{}: source view has no `viewOn`; skipping", plan.sourceNs());
            return;
        }
        List<?> rawPipeline = opts.get("pipeline") instanceof List<?> l ? l : List.of();

        String rewrittenViewOn = localRenameMap.getOrDefault(viewOn, viewOn);
        List<Document> rewrittenPipeline = rewritePipeline(rawPipeline);

        MongoDatabase targetDb = targetSvc.database(tgt.db());
        if (plan.conflictMode() == ConflictMode.DROP_AND_RECREATE && viewExists(targetDb, tgt.coll())) {
            log.info("{}: dropping existing view before recreate", plan.targetNs());
            targetDb.getCollection(tgt.coll()).drop();
        }
        if (viewExists(targetDb, tgt.coll())) {
            log.info("{}: view already exists on target; leaving untouched", plan.targetNs());
            return;
        }

        Document create = new Document("create", tgt.coll())
                .append("viewOn", rewrittenViewOn)
                .append("pipeline", rewrittenPipeline);
        targetDb.runCommand(create);
        log.info("{}: view recreated (viewOn={}, {} pipeline stage(s))",
                plan.targetNs(), rewrittenViewOn, rewrittenPipeline.size());
    }

    private static boolean viewExists(MongoDatabase db, String name) {
        for (Document d : db.listCollections().filter(new Document("name", name))) {
            return true;
        }
        return false;
    }

    /** Walks each stage of the pipeline and rewrites any collection-referencing fields via
     *  {@link #applyRenameMap}. Nested pipelines (inside {@code $lookup} or {@code $unionWith})
     *  are rewritten recursively. */
    private List<Document> rewritePipeline(List<?> raw) {
        List<Document> out = new ArrayList<>(raw.size());
        for (Object stage : raw) {
            if (stage instanceof Document d) {
                out.add(rewriteDocument(d));
            } else {
                // Non-Document stage (shouldn't happen for view pipelines, but be defensive).
                out.add(new Document("_stage", stage));
            }
        }
        return out;
    }

    /** Depth-first rewrite of a pipeline stage. Recurses into nested arrays of stages so
     *  {@code $lookup.pipeline} and {@code $unionWith.pipeline} are translated too. */
    @SuppressWarnings("unchecked")
    private Document rewriteDocument(Document d) {
        Document copy = new Document();
        for (Map.Entry<String, Object> entry : d.entrySet()) {
            String k = entry.getKey();
            Object v = entry.getValue();
            if (PIPELINE_COLL_FIELDS.contains(k) && v instanceof String name) {
                copy.put(k, localRenameMap.getOrDefault(name, name));
            } else if (v instanceof Document sub) {
                copy.put(k, rewriteDocument(sub));
            } else if (v instanceof List<?> list) {
                List<Object> rewritten = new ArrayList<>(list.size());
                for (Object item : list) {
                    if (item instanceof Document inner) rewritten.add(rewriteDocument(inner));
                    else rewritten.add(item);
                }
                copy.put(k, rewritten);
            } else {
                copy.put(k, v);
            }
        }
        return copy;
    }

    /** Package-visible for unit testing — applies the rename map to a pipeline stage tree
     *  without spinning up a live MongoService. */
    static Document rewriteForTest(Document stage, Map<String, String> renameMap) {
        return new ViewCreator(null, null, null, null, renameMap).rewriteDocument(stage);
    }
}

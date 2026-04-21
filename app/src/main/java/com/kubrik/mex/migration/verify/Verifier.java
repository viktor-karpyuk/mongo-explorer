package com.kubrik.mex.migration.verify;

import com.kubrik.mex.core.MongoService;
import com.kubrik.mex.migration.engine.BsonTransform;
import com.kubrik.mex.migration.engine.CollectionPlan;
import com.kubrik.mex.migration.engine.Namespaces;
import com.kubrik.mex.migration.spec.MigrationSpec;
import com.kubrik.mex.migration.spec.TransformSpec;
import com.kubrik.mex.migration.spec.VerifySpec;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.model.Sorts;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Post-migration verification (SAFE-5). For each plan:
 *  <ul>
 *    <li>Compare source vs target {@code countDocuments} — a mismatch downgrades status to
 *        {@code FAIL}.</li>
 *    <li>Pick {@code sample} source {@code _id}s via {@code $sample}, fetch both source and
 *        target docs, and compare after applying the per-collection transform so transformed
 *        migrations don't get false positives (T-6).</li>
 *    <li>Compare non-{@code _id_} index definitions (name + key pattern + key flags).</li>
 *  </ul>
 *  Dry-run jobs should not reach here — {@link JobRunner} checks execution mode first. */
public final class Verifier {

    private static final Logger log = LoggerFactory.getLogger(Verifier.class);
    private static final String ENGINE_VERSION = "1.1.0";

    private final MongoService source;
    private final MongoService target;
    private final MigrationSpec spec;

    public Verifier(MongoService source, MongoService target, MigrationSpec spec) {
        this.source = source;
        this.target = target;
        this.spec = spec;
    }

    public VerificationReport verify(String jobId, List<CollectionPlan> plans) {
        VerifySpec vs = spec.options().verification();
        List<VerificationReport.CollectionReport> out = new ArrayList<>(plans.size());
        VerificationReport.Status status = VerificationReport.Status.PASS;

        for (CollectionPlan plan : plans) {
            VerificationReport.CollectionReport cr = verifyOne(plan, vs);
            out.add(cr);
            if (!cr.countMatch() || cr.sampleMismatches() > 0) {
                status = VerificationReport.Status.FAIL;
            } else if (!cr.indexDiff().isEmpty() && status == VerificationReport.Status.PASS) {
                status = VerificationReport.Status.WARN;
            }
        }
        return new VerificationReport(1, jobId, status, out, Instant.now(), ENGINE_VERSION);
    }

    private VerificationReport.CollectionReport verifyOne(CollectionPlan plan, VerifySpec vs) {
        Namespaces.Ns src = plan.source();
        Namespaces.Ns tgt = plan.target();
        MongoCollection<RawBsonDocument> srcColl = source.rawCollection(src.db(), src.coll());
        MongoCollection<RawBsonDocument> tgtColl = target.rawCollection(tgt.db(), tgt.coll());

        TransformSpec xformSpec = spec.options().transform().get(plan.sourceNs());
        BsonTransform xform = BsonTransform.compile(xformSpec);
        boolean transformed = !xform.isIdentity()
                || (xformSpec != null && !xformSpec.isEmpty());

        Bson filter = (xformSpec != null && xformSpec.filterJson() != null && !xformSpec.filterJson().isBlank())
                ? BsonDocument.parse(xformSpec.filterJson())
                : new BsonDocument();

        long sourceCount = srcColl.countDocuments(filter);
        long targetCount = tgtColl.countDocuments();
        boolean countMatch = sourceCount == targetCount;

        int sampleMismatches = 0;
        int sampleSize = 0;
        if (vs.sample() > 0 && sourceCount > 0) {
            sampleSize = (int) Math.min(vs.sample(), sourceCount);
            sampleMismatches = sampleDiff(srcColl, tgtColl, filter, sampleSize, xform);
        }

        List<String> indexDiff = diffIndexes(src, tgt);

        String fullHash = null;
        boolean fullHashMismatch = false;
        if (vs.fullHashCompare() && countMatch) {
            HashPair hashes = fullHashCompare(srcColl, tgtColl, filter, xform);
            fullHashMismatch = !hashes.sourceHash().equals(hashes.targetHash());
            fullHash = fullHashMismatch
                    ? "MISMATCH source=sha256:" + hashes.sourceHash() + " target=sha256:" + hashes.targetHash()
                    : "sha256:" + hashes.targetHash();
        }

        if (!countMatch || sampleMismatches > 0 || !indexDiff.isEmpty() || fullHashMismatch) {
            log.warn("{} → {}: countMatch={}, sampleMismatches={}/{}, indexDiff={}, fullHash={}",
                    plan.sourceNs(), plan.targetNs(), countMatch, sampleMismatches, sampleSize,
                    indexDiff, fullHash);
        }

        int effectiveMismatches = sampleMismatches + (fullHashMismatch ? 1 : 0);
        return new VerificationReport.CollectionReport(
                plan.sourceNs(), plan.targetNs(),
                sourceCount, targetCount, countMatch, transformed,
                sampleSize, effectiveMismatches,
                indexDiff, fullHash);
    }

    /** Deterministic SHA-256 over the whole collection (after transform on source). Sorts by
     *  {@code _id} so the digest is stable regardless of storage order. Safe memory footprint —
     *  documents are hashed one at a time, not accumulated. */
    private HashPair fullHashCompare(MongoCollection<RawBsonDocument> srcColl,
                                     MongoCollection<RawBsonDocument> tgtColl,
                                     Bson filter,
                                     BsonTransform xform) {
        MessageDigest sourceDigest = newSha256();
        MessageDigest targetDigest = newSha256();
        for (RawBsonDocument d : srcColl.find(filter).sort(Sorts.ascending("_id")).noCursorTimeout(true)) {
            RawBsonDocument transformed = xform.apply(d);
            updateDigest(sourceDigest, transformed);
        }
        for (RawBsonDocument d : tgtColl.find().sort(Sorts.ascending("_id")).noCursorTimeout(true)) {
            updateDigest(targetDigest, d);
        }
        return new HashPair(toHex(sourceDigest.digest()), toHex(targetDigest.digest()));
    }

    private static void updateDigest(MessageDigest digest, RawBsonDocument doc) {
        // Use canonical JSON — field order is already deterministic because we sort by _id and
        // the driver preserves insertion order within a document.
        digest.update(doc.toJson().getBytes(java.nio.charset.StandardCharsets.UTF_8));
        digest.update((byte) 0x1e);  // ASCII record separator — distinguishes adjacent docs
    }

    private static MessageDigest newSha256() {
        try { return MessageDigest.getInstance("SHA-256"); }
        catch (NoSuchAlgorithmException e) { throw new IllegalStateException(e); }
    }

    private static String toHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) sb.append(String.format("%02x", b));
        return sb.toString();
    }

    private record HashPair(String sourceHash, String targetHash) {}

    private int sampleDiff(MongoCollection<RawBsonDocument> srcColl,
                           MongoCollection<RawBsonDocument> tgtColl,
                           Bson filter,
                           int sampleSize,
                           BsonTransform xform) {
        int mismatches = 0;
        // Sample ids from source (after filter).
        var pipeline = List.of(
                Aggregates.match(filter),
                Aggregates.sample(sampleSize),
                Aggregates.project(new Document("_id", 1)));
        List<BsonValue> ids = new ArrayList<>(sampleSize);
        for (RawBsonDocument d : srcColl.aggregate(pipeline)) {
            ids.add(d.get("_id"));
        }
        for (BsonValue id : ids) {
            RawBsonDocument s = srcColl.find(Filters.eq("_id", id)).first();
            if (s == null) { mismatches++; continue; }
            RawBsonDocument expected = xform.apply(s);
            RawBsonDocument t = tgtColl.find(Filters.eq("_id", id)).first();
            if (t == null || !bsonEquals(expected, t)) mismatches++;
        }
        return mismatches;
    }

    private static boolean bsonEquals(RawBsonDocument a, RawBsonDocument b) {
        // Compare parsed BsonDocument views so field order doesn't matter. Byte-level equality
        // is too strict when the driver recomposes documents.
        if (a == null || b == null) return a == b;
        try {
            return BsonDocument.parse(a.toJson()).equals(BsonDocument.parse(b.toJson()));
        } catch (Exception e) {
            return false;
        }
    }

    private List<String> diffIndexes(Namespaces.Ns src, Namespaces.Ns tgt) {
        Map<String, Document> srcIdx = indexMap(source.listIndexes(src.db(), src.coll()));
        Map<String, Document> tgtIdx = indexMap(target.listIndexes(tgt.db(), tgt.coll()));
        Set<String> allNames = new HashSet<>();
        allNames.addAll(srcIdx.keySet());
        allNames.addAll(tgtIdx.keySet());
        List<String> diffs = new ArrayList<>();
        for (String name : allNames) {
            if ("_id_".equals(name)) continue;
            Document s = srcIdx.get(name);
            Document t = tgtIdx.get(name);
            if (s == null) diffs.add("index `" + name + "` present on target but not source");
            else if (t == null) diffs.add("index `" + name + "` present on source but not target");
            else if (!compareIndex(s, t)) {
                diffs.add("index `" + name + "` key/options differ between source and target");
            }
        }
        diffs.sort(Comparator.naturalOrder());
        return diffs;
    }

    private static Map<String, Document> indexMap(List<Document> indexes) {
        Map<String, Document> m = new HashMap<>(indexes.size());
        for (Document d : indexes) {
            String name = d.getString("name");
            if (name != null) m.put(name, d);
        }
        return m;
    }

    /** Relaxed index equality — same key pattern + same flag set. Ignores `v` and `ns`. */
    private static boolean compareIndex(Document a, Document b) {
        if (!a.get("key", Document.class).equals(b.get("key", Document.class))) return false;
        for (String flag : List.of("unique", "sparse", "expireAfterSeconds")) {
            if (!java.util.Objects.equals(a.get(flag), b.get(flag))) return false;
        }
        return true;
    }
}

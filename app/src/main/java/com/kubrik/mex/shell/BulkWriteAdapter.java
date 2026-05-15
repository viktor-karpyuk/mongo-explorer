package com.kubrik.mex.shell;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.DeleteManyModel;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import org.bson.Document;
import org.graalvm.polyglot.Value;

import java.util.ArrayList;
import java.util.List;

/**
 * Translates the mongosh {@code db.coll.bulkWrite([...])} array — each
 * element being a single-key object like {@code { insertOne: { document: ... } }}
 * or {@code { updateOne: { filter, update, upsert? } }} — into the
 * driver's typed {@link WriteModel} list, then summarises the result.
 */
final class BulkWriteAdapter {

    private BulkWriteAdapter() {}

    static Document run(MongoCollection<Document> coll, Value opsArg) {
        if (opsArg == null || opsArg.isNull() || !opsArg.hasArrayElements()) {
            throw new IllegalArgumentException("bulkWrite expects an array of operation objects");
        }
        List<WriteModel<Document>> models = new ArrayList<>();
        long n = opsArg.getArraySize();
        for (long i = 0; i < n; i++) {
            Value op = opsArg.getArrayElement(i);
            if (!op.hasMembers()) {
                throw new IllegalArgumentException("bulkWrite[" + i + "]: expected object");
            }
            for (String kind : op.getMemberKeys()) {
                Value spec = op.getMember(kind);
                Document s = JsBsonConverter.toDocument(spec);
                models.add(switch (kind) {
                    case "insertOne" -> new InsertOneModel<>(asDoc(s.get("document")));
                    case "updateOne" -> new UpdateOneModel<>(
                            asDoc(s.get("filter")), asDoc(s.get("update")),
                            new UpdateOptions().upsert(Boolean.TRUE.equals(s.get("upsert"))));
                    case "updateMany" -> new UpdateManyModel<>(
                            asDoc(s.get("filter")), asDoc(s.get("update")),
                            new UpdateOptions().upsert(Boolean.TRUE.equals(s.get("upsert"))));
                    case "replaceOne" -> new ReplaceOneModel<>(
                            asDoc(s.get("filter")), asDoc(s.get("replacement")));
                    case "deleteOne"  -> new DeleteOneModel<>(asDoc(s.get("filter")));
                    case "deleteMany" -> new DeleteManyModel<>(asDoc(s.get("filter")));
                    default -> throw new IllegalArgumentException("bulkWrite[" + i + "]: unknown op '" + kind + "'");
                });
                break; // mongosh wraps one op per array element
            }
        }
        BulkWriteResult r = coll.bulkWrite(models);
        Document out = new Document();
        out.put("acknowledged", r.wasAcknowledged());
        out.put("insertedCount", r.getInsertedCount());
        out.put("matchedCount", r.getMatchedCount());
        out.put("modifiedCount", r.getModifiedCount());
        out.put("deletedCount", r.getDeletedCount());
        out.put("upsertedCount", r.getUpserts().size());
        return out;
    }

    private static Document asDoc(Object o) {
        if (o == null) return new Document();
        if (o instanceof Document d) return d;
        throw new IllegalArgumentException("expected document, got " + o.getClass().getSimpleName());
    }
}

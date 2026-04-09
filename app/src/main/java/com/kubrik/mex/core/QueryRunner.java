package com.kubrik.mex.core;

import com.kubrik.mex.model.QueryRequest;
import com.kubrik.mex.model.QueryResult;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class QueryRunner {

    private final MongoService svc;

    public QueryRunner(MongoService svc) { this.svc = svc; }

    public QueryResult find(QueryRequest req) {
        long t0 = System.currentTimeMillis();
        try {
            MongoCollection<Document> coll = svc.collection(req.dbName(), req.collName());
            Bson filter = parseOrEmpty(req.filterJson());
            FindIterable<Document> it = coll.find(filter)
                    .skip(req.skip())
                    .limit(req.limit() + 1)
                    .maxTime(req.maxTimeMs(), TimeUnit.MILLISECONDS);
            if (notBlank(req.projectionJson())) it = it.projection(parseOrEmpty(req.projectionJson()));
            if (notBlank(req.sortJson())) it = it.sort(parseOrEmpty(req.sortJson()));
            List<Document> out = new ArrayList<>();
            int n = 0;
            boolean hasMore = false;
            for (Document d : it) {
                if (n >= req.limit()) { hasMore = true; break; }
                out.add(d);
                n++;
            }
            return new QueryResult(out, System.currentTimeMillis() - t0, hasMore, null);
        } catch (Exception e) {
            return new QueryResult(List.of(), System.currentTimeMillis() - t0, false, e.getMessage());
        }
    }

    public QueryResult aggregate(String db, String coll, String pipelineJson, long maxTimeMs) {
        long t0 = System.currentTimeMillis();
        try {
            // pipelineJson: [ {stage}, {stage} ]
            BsonDocument wrap = BsonDocument.parse("{p:" + pipelineJson + "}");
            List<Bson> stages = new ArrayList<>();
            for (org.bson.BsonValue v : wrap.getArray("p")) {
                stages.add(v.asDocument());
            }
            List<Document> out = new ArrayList<>();
            for (Document d : svc.collection(db, coll).aggregate(stages).maxTime(maxTimeMs, TimeUnit.MILLISECONDS)) {
                out.add(d);
                if (out.size() >= 500) break;
            }
            return new QueryResult(out, System.currentTimeMillis() - t0, false, null);
        } catch (Exception e) {
            return new QueryResult(List.of(), System.currentTimeMillis() - t0, false, e.getMessage());
        }
    }

    public long insert(String db, String coll, String json) {
        svc.collection(db, coll).insertOne(Document.parse(json));
        return 1;
    }

    public long deleteById(String db, String coll, BsonDocument idFilter) {
        return svc.collection(db, coll).deleteOne(idFilter).getDeletedCount();
    }

    public long replaceById(String db, String coll, BsonDocument idFilter, String json) {
        return svc.collection(db, coll).replaceOne(idFilter, Document.parse(json)).getModifiedCount();
    }

    private static Bson parseOrEmpty(String s) {
        if (!notBlank(s)) return new BsonDocument();
        return BsonDocument.parse(s);
    }

    private static boolean notBlank(String s) { return s != null && !s.isBlank(); }
}

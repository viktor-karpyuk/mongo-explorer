package com.kubrik.mex.monitoring;

import com.kubrik.mex.monitoring.util.Redactor;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RedactorTest {

    @Test
    void redactsStringLiteralsUnderFilterUpdatePipeline() {
        Document cmd = new Document("find", "orders")
                .append("filter", new Document("status", "PAID").append("total", 100))
                .append("update", new Document("$set", new Document("name", "bob")))
                .append("pipeline", List.of(new Document("$match", new Document("x", "y"))));
        Document out = Redactor.redact(cmd);
        Document filter = (Document) out.get("filter");
        assertEquals("?", filter.get("status"), "string literals must be replaced with ?");
        assertEquals(100, filter.get("total"), "numeric literals are preserved");
        Document update = (Document) out.get("update");
        assertEquals("?", ((Document) update.get("$set")).get("name"));
        List<?> pipeline = (List<?>) out.get("pipeline");
        assertEquals("?", ((Document) ((Document) pipeline.get(0)).get("$match")).get("x"));
    }

    @Test
    void preservesTopLevelNonLiteralKeys() {
        Document cmd = new Document("find", "orders").append("limit", 50);
        Document out = Redactor.redact(cmd);
        assertEquals("orders", out.get("find"));
        assertEquals(50, out.get("limit"));
    }

    @Test
    void redactsBinaryLiterals() {
        Document cmd = new Document("filter", new Document("key", new Binary(new byte[] { 1, 2 })));
        Document out = Redactor.redact(cmd);
        assertEquals("?", ((Document) out.get("filter")).get("key"));
    }

    @Test
    void preservesDiagnosticTypesLikeObjectIdAndDate() {
        ObjectId oid = new ObjectId();
        Date d = new Date();
        Document cmd = new Document("filter", new Document("_id", oid).append("ts", d));
        Document out = Redactor.redact(cmd);
        Document filter = (Document) out.get("filter");
        assertEquals(oid, filter.get("_id"), "ObjectId is diagnostic; not redacted");
        assertEquals(d, filter.get("ts"), "Date is diagnostic; not redacted");
    }

    @Test
    void redactsNestedArraysOfStrings() {
        Document cmd = new Document("filter", new Document("tags",
                new Document("$in", List.of("a", "b", "c"))));
        Document out = Redactor.redact(cmd);
        List<?> in = (List<?>) ((Document) ((Document) out.get("filter")).get("tags")).get("$in");
        assertEquals(List.of("?", "?", "?"), in);
    }
}

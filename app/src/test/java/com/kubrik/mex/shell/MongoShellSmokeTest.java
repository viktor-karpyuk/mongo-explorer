package com.kubrik.mex.shell;

import org.bson.Document;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.Value;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Hermetic smoke test for the shell's host bindings + JS↔BSON
 * conversion. Doesn't boot a {@link com.kubrik.mex.shell.MongoShell}
 * (that needs a live driver); instead drives a Graal {@link Context}
 * directly to confirm BSON helpers, JsBsonConverter round-trip, and
 * pretty-print all behave the way the shell view expects.
 */
class MongoShellSmokeTest {

    private Context ctx;

    @BeforeEach
    void setUp() {
        ctx = Context.newBuilder("js").allowHostAccess(HostAccess.ALL).build();
        Value b = ctx.getBindings("js");
        b.putMember("ObjectId",      BsonHostHelpers.objectId());
        b.putMember("ISODate",       BsonHostHelpers.isoDate());
        b.putMember("NumberLong",    BsonHostHelpers.numberLong());
        b.putMember("NumberDecimal", BsonHostHelpers.numberDecimal());
        b.putMember("UUID",          BsonHostHelpers.uuid());
    }

    @AfterEach
    void tearDown() { ctx.close(true); }

    @Test
    void objectIdHelperReturnsBsonObjectId() {
        Value v = ctx.eval("js", "ObjectId('6a041c5dddad223902a02930')");
        Object o = JsBsonConverter.toJava(v);
        assertInstanceOf(ObjectId.class, o, "ObjectId('…') should produce a BSON ObjectId");
        assertEquals("6a041c5dddad223902a02930", ((ObjectId) o).toHexString());
    }

    @Test
    void isoDateHelperReturnsDate() {
        Value v = ctx.eval("js", "ISODate('2026-01-01T00:00:00Z')");
        Object o = JsBsonConverter.toJava(v);
        assertInstanceOf(Date.class, o);
    }

    @Test
    void numberLongFromString() {
        Value v = ctx.eval("js", "NumberLong('9007199254740993')");
        Object o = JsBsonConverter.toJava(v);
        assertEquals(9007199254740993L, ((Number) o).longValue());
    }

    @Test
    void numberDecimalFromString() {
        Value v = ctx.eval("js", "NumberDecimal('123.456789')");
        Object o = JsBsonConverter.toJava(v);
        assertInstanceOf(Decimal128.class, o);
        assertEquals("123.456789", o.toString());
    }

    @Test
    void uuidHelper() {
        Value v = ctx.eval("js", "UUID('11111111-1111-1111-1111-111111111111')");
        Object o = JsBsonConverter.toJava(v);
        assertInstanceOf(UUID.class, o);
        assertEquals("11111111-1111-1111-1111-111111111111", o.toString());
    }

    @Test
    void nestedObjectConvertsToDocument() {
        Value v = ctx.eval("js",
                "({ _id: ObjectId('6a041c5dddad223902a02930'),"
              + "   $set: { 'companyLoginInfos.0.defaultCompany': true } })");
        Document d = JsBsonConverter.toDocument(v);
        assertInstanceOf(ObjectId.class, d.get("_id"));
        Document set = (Document) d.get("$set");
        assertEquals(true, set.get("companyLoginInfos.0.defaultCompany"));
    }

    @Test
    void arrayOfObjectsConvertsToDocumentList() {
        Value v = ctx.eval("js",
                "[ { stage: 1 }, { stage: 2, $match: { x: NumberLong('42') } } ]");
        List<Document> docs = JsBsonConverter.toDocumentList(v);
        assertEquals(2, docs.size());
        assertEquals(1, docs.get(0).getInteger("stage"));
        Document second = (Document) docs.get(1).get("$match");
        assertNotNull(second);
        assertEquals(42L, ((Number) second.get("x")).longValue());
    }

    @Test
    void prettyPrintRoundTripsBsonLiterals() {
        Document d = new Document()
                .append("_id", new ObjectId("6a041c5dddad223902a02930"))
                .append("count", 42L)
                .append("when", new Date(1735689600000L))
                .append("price", Decimal128.parse("9.99"));
        String s = JsBsonConverter.prettyPrint(d);
        assertTrue(s.contains("ObjectId(\"6a041c5dddad223902a02930\")"), s);
        assertTrue(s.contains("NumberLong(\"42\")"), s);
        assertTrue(s.contains("ISODate("), s);
        assertTrue(s.contains("NumberDecimal(\"9.99\")"), s);
    }
}

package com.kubrik.mex.migration;

import com.kubrik.mex.migration.engine.Batch;
import com.kubrik.mex.migration.engine.Namespaces;
import com.kubrik.mex.migration.sink.BsonDumpSink;
import com.kubrik.mex.migration.sink.CsvSink;
import com.kubrik.mex.migration.sink.JsonArraySink;
import com.kubrik.mex.migration.sink.MigrationSink;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Drives the three new EXT-2 sinks (JSON array, CSV, BSON dump) with canned
 * batches and asserts the output file shape. No MongoDB or pipeline wiring —
 * the sinks are deliberately independent of the rest of the engine so they
 * can be exercised in milliseconds.
 */
class SinkFormatsTest {

    @TempDir Path outDir;

    @Test
    void jsonArraySinkWrapsDocumentsInASingleArray() throws Exception {
        try (JsonArraySink sink = new JsonArraySink(outDir)) {
            sink.open(new Namespaces.Ns("app", "users"));
            sink.writeBatch(batchOf(
                    new Document("_id", 1).append("name", "alice"),
                    new Document("_id", 2).append("name", "bob")));
        }
        String out = Files.readString(outDir.resolve("app.users.json"));
        assertTrue(out.startsWith("[") && out.endsWith("]"),
                "output must be a JSON array, got: " + out);
        // Parsing the array round-trips.
        Document wrapped = Document.parse("{\"arr\":" + out + "}");
        List<?> arr = wrapped.getList("arr", Object.class);
        assertEquals(2, arr.size());
        assertEquals("alice", ((Document) arr.get(0)).getString("name"));
    }

    @Test
    void jsonArraySinkPutsCommasBetweenBatches() throws Exception {
        try (JsonArraySink sink = new JsonArraySink(outDir)) {
            sink.open(new Namespaces.Ns("app", "users"));
            sink.writeBatch(batchOf(new Document("_id", 1)));
            sink.writeBatch(batchOf(new Document("_id", 2)));
            sink.writeBatch(batchOf(new Document("_id", 3)));
        }
        String out = Files.readString(outDir.resolve("app.users.json"));
        // Three docs → two commas, plus the wrapping brackets
        assertEquals(2, countCommas(out),
                "expected 2 commas for 3 docs across 3 batches, got: " + out);
    }

    @Test
    void csvSinkUsesFirstBatchAsHeaderAndRfc4180Escapes() throws Exception {
        try (CsvSink sink = new CsvSink(outDir)) {
            sink.open(new Namespaces.Ns("app", "users"));
            sink.writeBatch(batchOf(
                    new Document("_id", 1).append("name", "alice").append("age", 30),
                    new Document("_id", 2).append("name", "bob,jr").append("age", 31)));
        }
        List<String> lines = Files.readAllLines(outDir.resolve("app.users.csv"));
        assertEquals(3, lines.size(), "1 header + 2 rows");
        assertEquals("_id,name,age", lines.get(0));
        assertEquals("1,alice,30", lines.get(1));
        assertEquals("2,\"bob,jr\",31", lines.get(2), "comma in value must be quoted");
    }

    @Test
    void csvSinkEscapesEmbeddedQuotes() throws Exception {
        try (CsvSink sink = new CsvSink(outDir)) {
            sink.open(new Namespaces.Ns("app", "users"));
            sink.writeBatch(batchOf(new Document("_id", 1).append("name", "she said \"hi\"")));
        }
        List<String> lines = Files.readAllLines(outDir.resolve("app.users.csv"));
        assertEquals("1,\"she said \"\"hi\"\"\"", lines.get(1));
    }

    @Test
    void csvSinkSkipsLaterExtraColumns() throws Exception {
        try (CsvSink sink = new CsvSink(outDir)) {
            sink.open(new Namespaces.Ns("app", "users"));
            // Header is discovered from batch 1. Batch 2 introduces a new key that
            // must not derail the row.
            sink.writeBatch(batchOf(new Document("_id", 1).append("name", "alice")));
            sink.writeBatch(batchOf(new Document("_id", 2).append("name", "bob").append("extra", "x")));
        }
        List<String> lines = Files.readAllLines(outDir.resolve("app.users.csv"));
        assertEquals("_id,name", lines.get(0));
        assertEquals(3, lines.size());
        assertFalse(lines.get(2).contains("x"), "extra column must be dropped, got: " + lines.get(2));
    }

    @Test
    void bsonDumpSinkRoundTripsThroughBsonParser() throws Exception {
        try (BsonDumpSink sink = new BsonDumpSink(outDir)) {
            sink.open(new Namespaces.Ns("app", "users"));
            sink.writeBatch(batchOf(
                    new Document("_id", 1).append("name", "alice"),
                    new Document("_id", 2).append("name", "bob")));
        }
        // mongodump format: concatenated length-prefixed BSON docs. Read them back
        // and assert content round-trips.
        byte[] bytes = Files.readAllBytes(outDir.resolve("app.users.bson"));
        List<Document> read = readBsonStream(bytes);
        assertEquals(2, read.size());
        assertEquals("alice", read.get(0).getString("name"));
        assertEquals("bob",   read.get(1).getString("name"));
    }

    // ------------------------------ helpers ------------------------------

    private static Batch batchOf(Document... docs) {
        List<RawBsonDocument> list = new ArrayList<>(docs.length);
        long bytes = 0;
        for (Document d : docs) {
            RawBsonDocument raw = RawBsonDocument.parse(d.toJson());
            list.add(raw);
            bytes += raw.getByteBuffer().remaining();
        }
        return new Batch(list, bytes, null);
    }

    private static int countCommas(String s) {
        int n = 0;
        for (int i = 0; i < s.length(); i++) if (s.charAt(i) == ',') n++;
        return n;
    }

    /** Parse a concatenated BSON stream into Documents — the shape mongorestore reads. */
    private static List<Document> readBsonStream(byte[] bytes) throws Exception {
        List<Document> out = new ArrayList<>();
        InputStream in = new ByteArrayInputStream(bytes);
        while (in.available() > 0) {
            byte[] lenBytes = in.readNBytes(4);
            if (lenBytes.length < 4) break;
            int len = ((lenBytes[0] & 0xff))
                    | ((lenBytes[1] & 0xff) << 8)
                    | ((lenBytes[2] & 0xff) << 16)
                    | ((lenBytes[3] & 0xff) << 24);
            byte[] body = new byte[len - 4];
            in.readNBytes(body, 0, len - 4);
            byte[] whole = new byte[len];
            System.arraycopy(lenBytes, 0, whole, 0, 4);
            System.arraycopy(body, 0, whole, 4, len - 4);
            BsonDocument d = new RawBsonDocument(whole);
            out.add(Document.parse(d.toJson()));
        }
        return out;
    }
}

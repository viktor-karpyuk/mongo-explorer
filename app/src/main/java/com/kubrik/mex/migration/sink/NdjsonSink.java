package com.kubrik.mex.migration.sink;

import com.kubrik.mex.migration.engine.Batch;
import com.kubrik.mex.migration.engine.Namespaces;
import org.bson.RawBsonDocument;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/** NDJSON ({@code application/x-ndjson}) file sink — one BSON relaxed-JSON document per line.
 *  <p>
 *  Output file layout: {@code <basePath>/<db>.<coll>.ndjson}. The file is recreated on every
 *  run; no append semantics (a resumed job will overwrite, matching the Mongo-target
 *  drop-and-recreate expectation). */
public final class NdjsonSink implements MigrationSink {

    /** Relaxed-mode JSON preserves readability (dates, numbers as literals) while still
     *  carrying type hints for non-primitive values (ObjectId, Decimal128, …) via
     *  {@code $oid}/{@code $numberDecimal}. */
    private static final JsonWriterSettings RELAXED = JsonWriterSettings.builder()
            .outputMode(JsonMode.RELAXED)
            .build();

    private final Path basePath;
    private BufferedWriter writer;
    private Path outputFile;

    public NdjsonSink(Path basePath) {
        this.basePath = basePath;
    }

    @Override
    public void open(Namespaces.Ns target) throws IOException {
        Files.createDirectories(basePath);
        this.outputFile = basePath.resolve(target.db() + "." + target.coll() + ".ndjson");
        this.writer = Files.newBufferedWriter(outputFile, StandardCharsets.UTF_8);
    }

    @Override
    public void writeBatch(Batch batch) throws IOException {
        for (RawBsonDocument doc : batch.docs()) {
            writer.write(doc.toJson(RELAXED));
            writer.newLine();
        }
    }

    @Override
    public void close() throws IOException {
        if (writer != null) {
            try { writer.flush(); } finally { writer.close(); }
        }
    }

    /** For tests / diagnostics: the file this sink wrote to (null before {@link #open}). */
    public Path outputFile() { return outputFile; }
}

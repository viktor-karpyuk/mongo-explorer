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

/**
 * Single-file JSON array sink (EXT-2). Emits {@code [doc1,doc2,...]} in a single
 * file at {@code <basePath>/<db>.<coll>.json}. Commas are placed between documents
 * based on whether anything has been written yet — so the output stays valid JSON
 * even when batches arrive asynchronously.
 *
 * <p>Trade-off vs NDJSON: tools like {@code jq} / browsers expect a JSON root,
 * which NDJSON isn't. Trade-off against: the whole file has to be parsed before
 * the first element is usable; streaming consumers should prefer NDJSON.
 */
public final class JsonArraySink implements MigrationSink {

    private static final JsonWriterSettings RELAXED = JsonWriterSettings.builder()
            .outputMode(JsonMode.RELAXED)
            .build();

    private final Path basePath;
    private BufferedWriter writer;
    private Path outputFile;
    private boolean first = true;

    public JsonArraySink(Path basePath) { this.basePath = basePath; }

    @Override
    public void open(Namespaces.Ns target) throws IOException {
        Files.createDirectories(basePath);
        this.outputFile = basePath.resolve(target.db() + "." + target.coll() + ".json");
        this.writer = Files.newBufferedWriter(outputFile, StandardCharsets.UTF_8);
        writer.write('[');
    }

    @Override
    public void writeBatch(Batch batch) throws IOException {
        for (RawBsonDocument doc : batch.docs()) {
            if (!first) writer.write(',');
            first = false;
            writer.write(doc.toJson(RELAXED));
        }
    }

    @Override
    public void close() throws IOException {
        if (writer != null) {
            try {
                writer.write(']');
                writer.flush();
            } finally {
                writer.close();
            }
        }
    }

    public Path outputFile() { return outputFile; }
}

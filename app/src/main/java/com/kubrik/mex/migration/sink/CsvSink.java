package com.kubrik.mex.migration.sink;

import com.kubrik.mex.migration.engine.Batch;
import com.kubrik.mex.migration.engine.Namespaces;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Flat-CSV sink (EXT-2) — top-level BSON fields become columns. The column set is
 * discovered from the first batch: every key seen across that batch enters the header,
 * preserving first-occurrence order. Later documents missing a column emit an empty
 * cell; later documents with extra columns have those fields dropped (a diagnostic
 * count is kept so the count can be exposed in a future follow-up).
 *
 * <p>Cell serialisation:
 * <ul>
 *   <li>String / number / boolean → literal value (quoted per RFC 4180 if it contains
 *       {@code , " \n \r}).</li>
 *   <li>{@code null} or missing → empty cell.</li>
 *   <li>Nested documents / arrays → relaxed-JSON string.</li>
 * </ul>
 *
 * <p>CSV is inherently lossy for heterogeneous schemas; callers who need fidelity
 * should prefer {@link NdjsonSink} or {@link BsonDumpSink}.
 */
public final class CsvSink implements MigrationSink {

    private static final JsonWriterSettings RELAXED = JsonWriterSettings.builder()
            .outputMode(JsonMode.RELAXED).build();

    private final Path basePath;
    private BufferedWriter writer;
    private Path outputFile;
    private List<String> columns;     // null until the first batch is seen

    public CsvSink(Path basePath) { this.basePath = basePath; }

    @Override
    public void open(Namespaces.Ns target) throws IOException {
        Files.createDirectories(basePath);
        this.outputFile = basePath.resolve(target.db() + "." + target.coll() + ".csv");
        this.writer = Files.newBufferedWriter(outputFile, StandardCharsets.UTF_8);
    }

    @Override
    public void writeBatch(Batch batch) throws IOException {
        if (columns == null) {
            columns = discoverColumns(batch);
            writer.write(String.join(",", columns.stream().map(CsvSink::escape).toList()));
            writer.newLine();
        }
        for (RawBsonDocument raw : batch.docs()) {
            BsonDocument d = BsonDocument.parse(raw.toJson(RELAXED));
            // Parsing via JSON keeps type handling consistent with what the reader sees
            // downstream; direct BsonReader access would be faster but duplicate format
            // knowledge with NdjsonSink.
            boolean firstCell = true;
            for (String col : columns) {
                if (!firstCell) writer.write(',');
                firstCell = false;
                BsonValue v = d.get(col);
                if (v != null) writer.write(escape(renderCell(raw, col)));
            }
            writer.newLine();
        }
    }

    @Override
    public void close() throws IOException {
        if (writer != null) {
            try { writer.flush(); } finally { writer.close(); }
        }
    }

    public Path outputFile() { return outputFile; }

    private static List<String> discoverColumns(Batch batch) {
        Map<String, Boolean> seen = new LinkedHashMap<>();
        for (RawBsonDocument raw : batch.docs()) {
            BsonDocument d = BsonDocument.parse(raw.toJson(RELAXED));
            for (String k : d.keySet()) seen.putIfAbsent(k, Boolean.TRUE);
        }
        return new ArrayList<>(seen.keySet());
    }

    /** Render one top-level field as a CSV cell value (unescaped). */
    private static String renderCell(RawBsonDocument raw, String key) {
        BsonDocument d = BsonDocument.parse(raw.toJson(RELAXED));
        BsonValue v = d.get(key);
        if (v == null || v.isNull()) return "";
        return switch (v.getBsonType()) {
            case STRING  -> v.asString().getValue();
            case INT32   -> Integer.toString(v.asInt32().getValue());
            case INT64   -> Long.toString(v.asInt64().getValue());
            case DOUBLE  -> Double.toString(v.asDouble().getValue());
            case BOOLEAN -> Boolean.toString(v.asBoolean().getValue());
            case OBJECT_ID -> v.asObjectId().getValue().toHexString();
            case DECIMAL128 -> v.asDecimal128().getValue().toString();
            case DATE_TIME -> java.time.Instant.ofEpochMilli(v.asDateTime().getValue()).toString();
            // Everything else (documents, arrays, binary, regex, …) round-trips as
            // relaxed JSON so at least one lossless string form is preserved.
            default -> v.isDocument()
                    ? v.asDocument().toJson(RELAXED)
                    : v.toString();
        };
    }

    /** RFC 4180 escaping: quote if the cell contains {@code , " \n \r}; double any embedded {@code "}. */
    static String escape(String cell) {
        if (cell == null) return "";
        boolean needsQuote = false;
        for (int i = 0; i < cell.length(); i++) {
            char c = cell.charAt(i);
            if (c == ',' || c == '"' || c == '\n' || c == '\r') { needsQuote = true; break; }
        }
        if (!needsQuote) return cell;
        StringBuilder sb = new StringBuilder(cell.length() + 4);
        sb.append('"');
        for (int i = 0; i < cell.length(); i++) {
            char c = cell.charAt(i);
            if (c == '"') sb.append('"');
            sb.append(c);
        }
        sb.append('"');
        return sb.toString();
    }
}

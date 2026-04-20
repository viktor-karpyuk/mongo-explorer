package com.kubrik.mex.migration.sink;

import com.kubrik.mex.migration.engine.Batch;
import com.kubrik.mex.migration.engine.Namespaces;
import org.bson.RawBsonDocument;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * {@code mongodump}-compatible raw BSON sink (EXT-2). Writes each document's BSON
 * byte representation verbatim, back-to-back, into {@code <basePath>/<db>.<coll>.bson}.
 *
 * <p>Every BSON document starts with a 4-byte little-endian length of its own payload,
 * so a bare concatenation is self-delimiting — that's the wire format {@code mongorestore}
 * reads. No envelope, no separator, no footer.
 */
public final class BsonDumpSink implements MigrationSink {

    private final Path basePath;
    private OutputStream out;
    private Path outputFile;

    public BsonDumpSink(Path basePath) { this.basePath = basePath; }

    @Override
    public void open(Namespaces.Ns target) throws IOException {
        Files.createDirectories(basePath);
        this.outputFile = basePath.resolve(target.db() + "." + target.coll() + ".bson");
        this.out = new BufferedOutputStream(Files.newOutputStream(outputFile));
    }

    @Override
    public void writeBatch(Batch batch) throws IOException {
        for (RawBsonDocument doc : batch.docs()) {
            ByteBuffer buf = doc.getByteBuffer().asNIO();
            // Copy into a byte[] because BufferedOutputStream.write(byte[]) is the
            // cheapest path; reading byte-by-byte from a NIO buffer would defeat
            // the buffer and dominate CPU at high throughput.
            int len = buf.remaining();
            byte[] tmp = new byte[len];
            buf.duplicate().get(tmp);
            out.write(tmp);
        }
    }

    @Override
    public void close() throws IOException {
        if (out != null) {
            try { out.flush(); } finally { out.close(); }
        }
    }

    public Path outputFile() { return outputFile; }
}

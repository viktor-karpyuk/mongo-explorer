package com.kubrik.mex.migration.resume;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

/** Atomic reader/writer for {@code resume.json}. Writes go via {@code resume.json.tmp} +
 *  rename so a crash never leaves a half-written file (T-8 canonical rule).
 *  <p>
 *  The manager is per-job; a single instance is held by {@code JobContext}. */
public final class ResumeManager {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private final Path file;
    private final Path tmp;
    private final ReentrantLock lock = new ReentrantLock();

    public ResumeManager(Path jobDir) {
        this.file = jobDir.resolve("resume.json");
        this.tmp = jobDir.resolve("resume.json.tmp");
    }

    public Path path() { return file; }

    public void save(ResumeFile state) throws IOException {
        lock.lock();
        try {
            byte[] bytes = MAPPER.writerWithDefaultPrettyPrinter().writeValueAsBytes(state);
            Files.createDirectories(file.getParent());
            Files.write(tmp, bytes);
            Files.move(tmp, file,
                    StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
        } finally {
            lock.unlock();
        }
    }

    public Optional<ResumeFile> load() {
        lock.lock();
        try {
            if (!Files.exists(file)) return Optional.empty();
            byte[] bytes = Files.readAllBytes(file);
            return Optional.of(MAPPER.readValue(bytes, ResumeFile.class));
        } catch (IOException e) {
            return Optional.empty();
        } finally {
            lock.unlock();
        }
    }

    /** Delete the resume file on successful completion (T-8). */
    public void deleteOnSuccess() {
        lock.lock();
        try {
            Files.deleteIfExists(file);
            Files.deleteIfExists(tmp);
        } catch (IOException ignored) {
        } finally {
            lock.unlock();
        }
    }

    public static ResumeFile initial(String jobId, String specHash) {
        return new ResumeFile(1, jobId, specHash, List.of(), null, java.time.Instant.now());
    }
}

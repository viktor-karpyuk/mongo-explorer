package com.kubrik.mex.labs.seed;

import com.kubrik.mex.labs.docker.DockerClient;
import com.kubrik.mex.labs.docker.DockerExecIO;
import com.kubrik.mex.labs.docker.ExecResult;
import com.kubrik.mex.labs.lifecycle.LabAutoConnectionWriter;
import com.kubrik.mex.labs.lifecycle.LabHealthWatcher;
import com.kubrik.mex.labs.lifecycle.LabLifecycleService;
import com.kubrik.mex.labs.model.LabDeployment;
import com.kubrik.mex.labs.model.LabEvent;
import com.kubrik.mex.labs.model.LabStatus;
import com.kubrik.mex.labs.model.LabTemplate;
import com.kubrik.mex.labs.model.SeedSpec;
import com.kubrik.mex.labs.store.LabEventDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

/**
 * v2.8.4 LAB-SEED-* — Hydrates a Lab's mongod with sample data.
 *
 * <p>Two paths:</p>
 * <ul>
 *   <li><b>Bundled</b> — classpath-resident archive (&lt; 5 MiB per
 *       NFR-LAB-5). Loaded into a temp file, then imported via a
 *       one-shot mongo-image sidecar that shares the Lab's compose
 *       network so it can reach the primary by hostname.</li>
 *   <li><b>Fetch-on-demand</b> — HTTP GET via {@link RemoteSeedFetcher}
 *       with SHA-256 verify + disk cache, then the same sidecar
 *       import.</li>
 * </ul>
 *
 * <p>Idempotency via {@link SeedMarker}: a successful seed writes a
 * sentinel to {@code <targetDb>._mex_labs}; restarts skip re-seed.
 * {@code docker compose down -v} removes the volume so the
 * sentinel goes with it — destroy-and-recreate seeds fresh.</p>
 */
public final class SeedRunner implements LabLifecycleService.SeedStep {

    private static final Logger log = LoggerFactory.getLogger(SeedRunner.class);

    /** How long mongorestore inside the sidecar is allowed to run.
     *  sample_mflix import takes ~45s on a modern laptop; 5 min
     *  covers slower disks without leaving a hung sidecar. */
    public static final Duration IMPORT_TIMEOUT = Duration.ofMinutes(5);

    private final RemoteSeedFetcher fetcher;
    private final SeedMarker marker;
    private final LabEventDao eventDao;
    private final Path dataDir;
    private final String dockerBinary;

    public SeedRunner(RemoteSeedFetcher fetcher, SeedMarker marker,
                       LabEventDao eventDao, Path dataDir,
                       String dockerBinary) {
        this.fetcher = fetcher;
        this.marker = marker;
        this.eventDao = eventDao;
        this.dataDir = dataDir;
        this.dockerBinary = dockerBinary == null ? "docker" : dockerBinary;
    }

    @Override
    public void run(LabDeployment lab, LabTemplate template) {
        if (template.seedSpec().isEmpty()) return;
        SeedSpec spec = template.seedSpec().get();

        String entry = LabHealthWatcher.chooseReadyContainer(template);
        int port = lab.portMap().portFor(entry);
        String uri = "mongodb://127.0.0.1:" + port + "/?directConnection=true";
        if (marker.isSeeded(uri, spec.targetDb())) {
            log.info("seed marker present on {} — skipping", lab.composeProject());
            return;
        }
        emit(lab, LabEvent.Kind.SEED_BEGIN,
                "kind=" + spec.kind() + " target=" + spec.targetDb());

        Path archive;
        try {
            archive = resolveArchive(spec);
        } catch (IOException ioe) {
            emit(lab, LabEvent.Kind.SEED_FAILED,
                    "archive resolve failed: " + ioe.getMessage());
            throw new RuntimeException("seed archive unavailable", ioe);
        }

        try {
            restoreViaSidecar(lab, template, archive, spec.targetDb());
        } catch (Exception e) {
            emit(lab, LabEvent.Kind.SEED_FAILED,
                    "mongorestore failed: " + e.getMessage());
            throw new RuntimeException("mongorestore failed", e);
        }

        marker.markSeeded(uri, spec.targetDb(), spec.locator());
        emit(lab, LabEvent.Kind.SEED_DONE, spec.targetDb());
    }

    private Path resolveArchive(SeedSpec spec) throws IOException {
        if (spec.kind() == SeedSpec.Kind.FETCH_ON_DEMAND) {
            return fetcher.fetch(spec.locator(),
                    spec.sha256().orElse(null));
        }
        // Bundled: copy classpath resource into the cache dir.
        Path cache = dataDir.resolve("cache");
        Files.createDirectories(cache);
        String key = spec.locator().replaceAll("[^a-zA-Z0-9_.-]", "_");
        Path out = cache.resolve(key);
        if (Files.exists(out)) return out;
        try (var in = SeedRunner.class.getResourceAsStream(spec.locator())) {
            if (in == null) throw new IOException(
                    "bundled seed resource missing: " + spec.locator());
            Files.copy(in, out);
            return out;
        }
    }

    /**
     * Run {@code mongorestore --archive=/seed} inside a mongo-image
     * sidecar container on the Lab's compose network. The sidecar
     * mounts the archive read-only and targets the primary by
     * service name (e.g. {@code mongo} for standalone).
     */
    private void restoreViaSidecar(LabDeployment lab, LabTemplate template,
                                    Path archive, String targetDb) throws IOException {
        String entry = LabHealthWatcher.chooseReadyContainer(template);
        String network = lab.composeProject() + "_default";
        String mongoTag = lab.mongoImageTag();
        Path stdoutLog = Path.of(lab.composeFilePath()).getParent()
                .resolve("seed-restore.stdout.log");
        Path stderrLog = Path.of(lab.composeFilePath()).getParent()
                .resolve("seed-restore.stderr.log");

        List<String> args = List.of(
                dockerBinary, "run", "--rm",
                "--network", network,
                "-v", archive.toAbsolutePath() + ":/seed/archive:ro",
                mongoTag,
                "sh", "-c",
                "mongorestore --host " + entry + ":27017 --drop "
                        + "--nsInclude=\"" + targetDb + ".*\" "
                        + "--archive=/seed/archive");
        ExecResult r = DockerExecIO.run(args, stdoutLog, stderrLog, IMPORT_TIMEOUT);
        if (!r.ok()) {
            throw new IOException("mongorestore exit=" + r.exitCode()
                    + ": " + r.combinedTail());
        }
    }

    private void emit(LabDeployment lab, LabEvent.Kind kind, String message) {
        eventDao.insert(lab.id(), kind, System.currentTimeMillis(), message);
    }
}

package com.kubrik.mex.cli;

import com.kubrik.mex.migration.profile.ProfileCodec;
import com.kubrik.mex.migration.spec.ConflictMode;
import com.kubrik.mex.migration.spec.ErrorPolicy;
import com.kubrik.mex.migration.spec.ExecutionMode;
import com.kubrik.mex.migration.spec.MigrationKind;
import com.kubrik.mex.migration.spec.MigrationSpec;
import com.kubrik.mex.migration.spec.Namespace;
import com.kubrik.mex.migration.spec.PerfSpec;
import com.kubrik.mex.migration.spec.ScopeFlags;
import com.kubrik.mex.migration.spec.ScopeSpec;
import com.kubrik.mex.migration.spec.SourceSpec;
import com.kubrik.mex.migration.spec.TargetSpec;
import com.kubrik.mex.migration.spec.VerifySpec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** Smoke coverage for CLI argument handling and spec-override plumbing. Live migrations are
 *  exercised by the existing Mongo-integration suites (and will gain a CLI IT once Docker is
 *  cheap enough to bundle with the fast test group). */
class MigrateCliTest {

    @TempDir Path tmp;

    @Test
    void rejects_missing_profile_and_resume_flags() {
        MigrateCli cli = new MigrateCli();
        StringWriter err = new StringWriter();
        cli.err = new PrintWriter(err);
        cli.out = new PrintWriter(new StringWriter());
        int exit = cli.execute();
        assertEquals(64, exit, "missing both flags should exit 64");
        assertTrue(err.toString().contains("--profile or --resume"),
                "err should name the missing flags, got: " + err);
    }

    @Test
    void rejects_both_profile_and_resume() {
        MigrateCli cli = new MigrateCli();
        cli.profilePath = tmp.resolve("ignored.yaml");
        cli.resumeJobId = "some-id";
        StringWriter err = new StringWriter();
        cli.err = new PrintWriter(err);
        cli.out = new PrintWriter(new StringWriter());
        int exit = cli.execute();
        assertEquals(64, exit, "both flags → argument error");
        assertTrue(err.toString().contains("mutually exclusive"));
    }

    @Test
    void picocli_help_renders_without_the_full_runtime() {
        StringWriter out = new StringWriter();
        int exit = new CommandLine(new MigrateCli())
                .setOut(new PrintWriter(out))
                .setErr(new PrintWriter(new StringWriter()))
                .execute("--help");
        assertEquals(0, exit);
        String help = out.toString();
        assertTrue(help.contains("--profile"),   "help should list --profile");
        assertTrue(help.contains("--resume"),    "help should list --resume");
        assertTrue(help.contains("--dry-run"),   "help should list --dry-run");
        assertTrue(help.contains("--verify"),    "help should list --verify");
        assertTrue(help.contains("--environment"), "help should list --environment");
    }

    @Test
    void apply_overrides_no_flags_returns_same_spec() {
        MigrationSpec spec = sampleSpec();
        MigrateCli cli = new MigrateCli();   // no flags set
        assertSame(spec, cli.applyOverrides(spec), "no flags → same instance");
    }

    @Test
    void apply_overrides_verify_flag_enables_verify() {
        MigrationSpec spec = sampleSpec(/*verifyEnabled=*/ false);
        assertFalse(spec.options().verification().enabled());

        MigrateCli cli = new MigrateCli();
        cli.verify = true;
        MigrationSpec modified = cli.applyOverrides(spec);

        assertNotSame(spec, modified, "should return a new spec");
        assertTrue(modified.options().verification().enabled());
        // Everything else preserved.
        assertEquals(spec.options().verification().sample(),
                modified.options().verification().sample());
        assertEquals(spec.options().performance(), modified.options().performance());
    }

    @Test
    void apply_overrides_environment_is_set() {
        MigrationSpec spec = sampleSpec();
        assertNull(spec.options().environment());

        MigrateCli cli = new MigrateCli();
        cli.environment = "staging";
        MigrationSpec modified = cli.applyOverrides(spec);

        assertEquals("staging", modified.options().environment());
    }

    @Test
    void profile_yaml_round_trip_loads_back_the_same_spec() throws Exception {
        MigrationSpec spec = sampleSpec();
        String yaml = new ProfileCodec().toYaml(spec);
        Path file = tmp.resolve("profile.yaml");
        Files.writeString(file, yaml);

        MigrationSpec loaded = new ProfileCodec().fromYaml(Files.readString(file));
        assertEquals(spec.kind(), loaded.kind());
        assertEquals(spec.source().connectionId(), loaded.source().connectionId());
        assertEquals(spec.target().connectionId(), loaded.target().connectionId());
    }

    // --- helpers -----------------------------------------------------------------

    private static MigrationSpec sampleSpec() { return sampleSpec(true); }

    private static MigrationSpec sampleSpec(boolean verifyEnabled) {
        return new MigrationSpec(
                1, MigrationKind.DATA_TRANSFER, "cli-test",
                new SourceSpec("src", "primary"),
                new TargetSpec("tgt", null),
                new ScopeSpec.Collections(
                        List.of(Namespace.parse("app.users")),
                        new ScopeFlags(false, false),
                        List.of("**"), List.of(), List.of()),
                null,
                new MigrationSpec.Options(
                        ExecutionMode.RUN,
                        new MigrationSpec.Conflict(ConflictMode.APPEND, Map.of()),
                        Map.of(),
                        PerfSpec.defaults(),
                        new VerifySpec(verifyEnabled, 1_000, false),
                        ErrorPolicy.defaults(), false, null, List.of()));
    }
}

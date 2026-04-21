package com.kubrik.mex.migration.preflight;

import com.kubrik.mex.migration.engine.CollectionPlan;
import com.kubrik.mex.migration.spec.ConflictMode;
import com.kubrik.mex.migration.spec.ErrorPolicy;
import com.kubrik.mex.migration.spec.ExecutionMode;
import com.kubrik.mex.migration.spec.MigrationKind;
import com.kubrik.mex.migration.spec.MigrationSpec;
import com.kubrik.mex.migration.spec.Namespace;
import com.kubrik.mex.migration.spec.PerfSpec;
import com.kubrik.mex.migration.spec.ScopeFlags;
import com.kubrik.mex.migration.spec.ScopeSpec;
import com.kubrik.mex.migration.spec.SinkSpec;
import com.kubrik.mex.migration.spec.SourceSpec;
import com.kubrik.mex.migration.spec.TargetSpec;
import com.kubrik.mex.migration.spec.VerifySpec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** Exercises the sink validation branches of {@link PreflightChecker#checkSinkDestination} without
 *  a live ConnectionManager — the validation is path-IO only. */
class SinkPreflightTest {

    @TempDir Path tempDir;

    @Test
    void no_sinks_produces_no_errors_or_warnings() {
        List<String> warnings = new ArrayList<>();
        List<String> errors = new ArrayList<>();
        PreflightChecker.checkSinkDestination(specWithSinks(List.of()), List.of(), warnings, errors);
        assertTrue(warnings.isEmpty());
        assertTrue(errors.isEmpty());
    }

    @Test
    void blank_path_errors() {
        List<String> warnings = new ArrayList<>();
        List<String> errors = new ArrayList<>();
        MigrationSpec spec = specWithSinks(List.of(new SinkSpec(SinkSpec.SinkKind.NDJSON, "  ")));
        PreflightChecker.checkSinkDestination(spec, List.of(), warnings, errors);
        assertTrue(errors.stream().anyMatch(e -> e.contains("has no output path")),
                "expected missing-path error, got: " + errors);
    }

    @Test
    void multiple_sinks_error_until_fan_out_is_supported() {
        List<String> warnings = new ArrayList<>();
        List<String> errors = new ArrayList<>();
        MigrationSpec spec = specWithSinks(List.of(
                new SinkSpec(SinkSpec.SinkKind.NDJSON, tempDir.toString()),
                new SinkSpec(SinkSpec.SinkKind.NDJSON, tempDir.toString())));
        PreflightChecker.checkSinkDestination(spec, List.of(), warnings, errors);
        assertTrue(errors.stream().anyMatch(e -> e.contains("single sink")),
                "expected single-sink error, got: " + errors);
    }

    @Test
    void path_is_file_not_directory_errors() throws Exception {
        Path file = tempDir.resolve("not-a-dir.txt");
        Files.writeString(file, "hello");
        List<String> warnings = new ArrayList<>();
        List<String> errors = new ArrayList<>();
        PreflightChecker.checkSinkDestination(
                specWithSinks(List.of(new SinkSpec(SinkSpec.SinkKind.NDJSON, file.toString()))),
                List.of(), warnings, errors);
        assertTrue(errors.stream().anyMatch(e -> e.contains("not a directory")),
                "expected not-a-directory error, got: " + errors);
    }

    @Test
    void missing_path_but_parent_writable_warns_about_creation() {
        Path missing = tempDir.resolve("new-export-dir");
        assertFalse(Files.exists(missing));
        List<String> warnings = new ArrayList<>();
        List<String> errors = new ArrayList<>();
        PreflightChecker.checkSinkDestination(
                specWithSinks(List.of(new SinkSpec(SinkSpec.SinkKind.NDJSON, missing.toString()))),
                List.of(), warnings, errors);
        assertTrue(errors.isEmpty(), "no errors expected, got: " + errors);
        assertTrue(warnings.stream().anyMatch(w -> w.contains("will be created")),
                "expected will-be-created warning, got: " + warnings);
    }

    @Test
    void missing_path_with_missing_parent_errors() {
        Path orphan = tempDir.resolve("a").resolve("b").resolve("c");
        List<String> warnings = new ArrayList<>();
        List<String> errors = new ArrayList<>();
        PreflightChecker.checkSinkDestination(
                specWithSinks(List.of(new SinkSpec(SinkSpec.SinkKind.NDJSON, orphan.toString()))),
                List.of(), warnings, errors);
        assertTrue(errors.stream().anyMatch(e -> e.contains("parent directory does not exist")),
                "expected missing-parent error, got: " + errors);
    }

    @Test
    void existing_output_file_is_flagged_as_overwrite_warning() throws Exception {
        Path out = tempDir.resolve("app.users.ndjson");
        Files.writeString(out, "{\"_id\":1}\n");
        List<CollectionPlan> plans = List.of(new CollectionPlan("app.users", "app.users", ConflictMode.APPEND));
        List<String> warnings = new ArrayList<>();
        List<String> errors = new ArrayList<>();
        PreflightChecker.checkSinkDestination(
                specWithSinks(List.of(new SinkSpec(SinkSpec.SinkKind.NDJSON, tempDir.toString()))),
                plans, warnings, errors);
        assertTrue(errors.isEmpty(), "no errors expected, got: " + errors);
        assertTrue(warnings.stream().anyMatch(w -> w.contains("overwrite existing file")),
                "expected overwrite warning, got: " + warnings);
    }

    // --- helpers -----------------------------------------------------------------

    private static MigrationSpec specWithSinks(List<SinkSpec> sinks) {
        return new MigrationSpec(
                1,
                MigrationKind.DATA_TRANSFER,
                "sink-preflight-test",
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
                        VerifySpec.defaults(),
                        ErrorPolicy.defaults(), false, null, sinks));
    }
}

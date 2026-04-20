package com.kubrik.mex.ui.migration;

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
import com.kubrik.mex.migration.spec.TransformSpec;
import com.kubrik.mex.migration.spec.VerifySpec;
import javafx.beans.property.SimpleBooleanProperty;
import javafx.beans.property.SimpleObjectProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Mutable state shared across wizard steps. JavaFX properties let individual steps bind
 *  directly to the model — changes on step N are visible on step N+1 without copy code. */
public final class WizardModel {

    // Source (Step 1)
    public final SimpleStringProperty sourceConnectionId = new SimpleStringProperty();
    public final SimpleStringProperty readPreference = new SimpleStringProperty("primary");
    public final SimpleBooleanProperty sourceConnected = new SimpleBooleanProperty(false);

    // Target + mode (Step 2)
    public final SimpleStringProperty targetConnectionId = new SimpleStringProperty();
    public final SimpleStringProperty targetDatabase = new SimpleStringProperty();
    public final SimpleObjectProperty<MigrationKind> kind = new SimpleObjectProperty<>(MigrationKind.DATA_TRANSFER);
    public final SimpleBooleanProperty targetConnected = new SimpleBooleanProperty(false);

    // Scope (Step 3)
    public final SimpleObjectProperty<Granularity> granularity = new SimpleObjectProperty<>(Granularity.DATABASES);
    /** Single-DB backing store still used by the legacy DATABASES path + by the COLLECTIONS
     *  step before multi-DB UI lands in P5. Also the first entry of {@link #selectedDatabases}. */
    public final SimpleStringProperty sourceDatabase = new SimpleStringProperty();
    /** SCOPE-10: N ≥ 1 source databases. When the wizard UI is still single-DB (pre-P5), this
     *  list mirrors {@link #sourceDatabase} on change. */
    public final ObservableList<String> selectedDatabases = FXCollections.observableArrayList();
    public final SimpleStringProperty includeGlobs = new SimpleStringProperty("**");
    public final SimpleStringProperty excludeGlobs = new SimpleStringProperty("");
    /** SCOPE-11: explicit namespace list, each entry is {@code db.coll}. */
    public final ObservableList<String> selectedCollections = FXCollections.observableArrayList();
    public final List<ScopeSpec.Rename> renames = new ArrayList<>();
    public final SimpleStringProperty scriptsFolder = new SimpleStringProperty();
    public final SimpleBooleanProperty migrateIndexes = new SimpleBooleanProperty(true);
    /** SCOPE-12: opt-in users copy. Defaults off; gated in the UI by the tooltip preconditions. */
    public final SimpleBooleanProperty migrateUsers = new SimpleBooleanProperty(false);

    // Options (Step 4)
    public final SimpleObjectProperty<ExecutionMode> executionMode = new SimpleObjectProperty<>(ExecutionMode.RUN);
    public final SimpleObjectProperty<ConflictMode> conflictMode = new SimpleObjectProperty<>(ConflictMode.ABORT);
    public final SimpleObjectProperty<PerfSpec> perf = new SimpleObjectProperty<>(PerfSpec.defaults());
    public final SimpleStringProperty environment = new SimpleStringProperty("");        // VER-8
    public final SimpleBooleanProperty ignoreDrift = new SimpleBooleanProperty(false);   // VER-4
    /** EXT-2 — when non-null, the job writes to a file sink of this kind instead of the target
     *  Mongo collection. Null means "use target MongoDB" (default). */
    public final SimpleObjectProperty<SinkSpec.SinkKind> sinkKind = new SimpleObjectProperty<>(null);
    public final SimpleStringProperty sinkPath = new SimpleStringProperty("");

    // Step 5 — confirmation text for DROP_AND_RECREATE
    public final SimpleStringProperty dropConfirmation = new SimpleStringProperty("");

    // UX-8 — per-collection transforms, keyed by "db.coll". Populated when the wizard is
    // opened from Query History with a filter, or (future) via the transforms step.
    public final java.util.Map<String, TransformSpec> transforms = new java.util.concurrent.ConcurrentHashMap<>();

    public WizardModel() {
        // P5 multi-DB UI now owns selectedDatabases directly. sourceDatabase is a legacy-compat
        // shortcut for "first picked DB" — WizardStepScope sets it alongside selectedDatabases.
    }

    public enum Granularity { SERVER, DATABASES, COLLECTIONS }

    /** Build the immutable spec from the current model state. */
    public MigrationSpec toSpec() {
        ScopeSpec scope = buildScope();
        String env = environment.get();
        List<SinkSpec> sinks = buildSinks();
        MigrationSpec.Options opts = new MigrationSpec.Options(
                executionMode.get(),
                new MigrationSpec.Conflict(conflictMode.get(), Map.of()),
                Map.copyOf(transforms),
                perf.get(),
                VerifySpec.defaults(),
                ErrorPolicy.defaults(),
                ignoreDrift.get(),
                env == null || env.isBlank() ? null : env.trim(),
                sinks);
        return new MigrationSpec(
                1,
                kind.get(),
                null,
                new SourceSpec(sourceConnectionId.get(), readPreference.get()),
                new TargetSpec(targetConnectionId.get(),
                        targetDatabase.get() == null || targetDatabase.get().isBlank() ? null : targetDatabase.get()),
                scope,
                scriptsFolder.get() == null || scriptsFolder.get().isBlank() ? null : scriptsFolder.get(),
                opts);
    }

    private ScopeSpec buildScope() {
        if (kind.get() == MigrationKind.VERSIONED) return null;
        List<String> include = splitGlobs(includeGlobs.get());
        List<String> exclude = splitGlobs(excludeGlobs.get());
        ScopeFlags flags = new ScopeFlags(migrateIndexes.get(), migrateUsers.get());
        List<ScopeSpec.Rename> renameCopy = List.copyOf(renames);
        return switch (granularity.get()) {
            case SERVER -> new ScopeSpec.Server(flags, include, exclude, renameCopy);
            case DATABASES -> {
                List<String> dbs = selectedDatabases.isEmpty() && sourceDatabase.get() != null
                        ? List.of(sourceDatabase.get())
                        : List.copyOf(selectedDatabases);
                yield new ScopeSpec.Databases(dbs, flags, include, exclude, renameCopy);
            }
            case COLLECTIONS -> {
                String defaultDb = sourceDatabase.get();
                List<Namespace> namespaces = new ArrayList<>();
                for (String c : selectedCollections) {
                    namespaces.add(c.contains(".") ? Namespace.parse(c) : new Namespace(defaultDb, c));
                }
                yield new ScopeSpec.Collections(namespaces, flags, include, exclude, renameCopy);
            }
        };
    }

    private List<SinkSpec> buildSinks() {
        SinkSpec.SinkKind k = sinkKind.get();
        if (k == null) return List.of();
        String p = sinkPath.get();
        if (p == null || p.isBlank()) return List.of();   // preflight surfaces the missing-path error
        return List.of(new SinkSpec(k, p.trim()));
    }

    private static List<String> splitGlobs(String text) {
        if (text == null || text.isBlank()) return List.of();
        List<String> out = new ArrayList<>();
        for (String line : text.split("\\R")) {
            String t = line.trim();
            if (!t.isEmpty()) out.add(t);
        }
        return out;
    }
}

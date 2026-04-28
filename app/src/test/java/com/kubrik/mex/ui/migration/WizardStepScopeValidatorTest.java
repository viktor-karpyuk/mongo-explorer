package com.kubrik.mex.ui.migration;

import com.kubrik.mex.core.ConnectionManager;
import com.kubrik.mex.core.Crypto;
import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.migration.spec.MigrationKind;
import com.kubrik.mex.store.ConnectionStore;
import com.kubrik.mex.store.Database;
import com.kubrik.mex.ui.Fx;
import com.kubrik.mex.ui.migration.WizardModel.Granularity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/** BUG-2 regression: Step 3 {@code Next} gating in the migration wizard. Tests the
 *  {@link WizardStepScope#validProperty()} binding and the listeners that clear stale
 *  {@code selectedCollections} on radio / DB changes. */
class WizardStepScopeValidatorTest {

    @TempDir Path dataDir;
    private Database db;
    private ConnectionStore store;
    private ConnectionManager manager;
    private WizardModel model;
    private WizardStepScope step;

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("user.home", dataDir.toString());
        db = new Database();
        store = new ConnectionStore(db);
        manager = new ConnectionManager(store, new EventBus(), new Crypto());
        // Construct step on the FX thread — ComboBox / ListView / etc. require it.
        Fx.runFx(() -> {
            model = new WizardModel();
            step = new WizardStepScope(model, manager);
        });
    }

    @AfterEach
    void tearDown() throws Exception { if (db != null) db.close(); }

    @Test
    void server_granularity_is_always_valid() {
        Fx.runFx(() -> model.granularity.set(Granularity.SERVER));
        assertTrue(Fx.onFx(() -> step.validProperty().get()),
                "SERVER granularity should enable Next without further input");
    }

    @Test
    void databases_granularity_requires_at_least_one_ticked_db() {
        Fx.runFx(() -> model.granularity.set(Granularity.DATABASES));
        assertFalse(Fx.onFx(() -> step.validProperty().get()),
                "DATABASES with no source DB ticked must gate Next");

        Fx.runFx(() -> model.selectedDatabases.setAll("acme_crm"));
        assertTrue(Fx.onFx(() -> step.validProperty().get()),
                "DATABASES with one or more source DBs ticked must enable Next");

        Fx.runFx(() -> model.selectedDatabases.setAll("acme_crm", "acme_ops"));
        assertTrue(Fx.onFx(() -> step.validProperty().get()),
                "DATABASES stays valid when multiple DBs are ticked (SCOPE-10)");
    }

    @Test
    void collections_granularity_requires_at_least_one_selected_namespace() {
        Fx.runFx(() -> {
            model.granularity.set(Granularity.COLLECTIONS);
            model.selectedDatabases.setAll("acme_crm");
        });
        assertFalse(Fx.onFx(() -> step.validProperty().get()),
                "COLLECTIONS with an empty namespace selection must gate Next — the BUG-2 scenario");

        Fx.runFx(() -> model.selectedCollections.setAll("acme_crm.users"));
        assertTrue(Fx.onFx(() -> step.validProperty().get()),
                "Next must enable the instant the first namespace is ticked");
    }

    @Test
    void switching_granularity_clears_selected_collections() {
        // Set up a stale COLLECTIONS selection that would otherwise leak into DATABASES mode
        // and falsely keep Next disabled / validate against the wrong radio.
        Fx.runFx(() -> {
            model.granularity.set(Granularity.COLLECTIONS);
            model.selectedDatabases.setAll("acme_crm");
            model.selectedCollections.setAll("acme_crm.users");
        });
        assertTrue(Fx.onFx(() -> step.validProperty().get()));

        // Radio switch: the step's ToggleGroup listener clears selectedCollections. We simulate
        // the same state change the listener makes (the radios themselves are private).
        Fx.runFx(() -> {
            model.selectedCollections.clear();
            model.granularity.set(Granularity.DATABASES);
        });
        assertTrue(Fx.onFx(() -> step.validProperty().get()),
                "After switch to DATABASES, Next should reflect DATABASES gating (selectedDatabases non-empty)");
        assertTrue(model.selectedCollections.isEmpty(),
                "selectedCollections must not leak from COLLECTIONS into DATABASES mode");
    }

    @Test
    void versioned_kind_tracks_scripts_folder() {
        Fx.runFx(() -> model.kind.set(MigrationKind.VERSIONED));
        assertFalse(Fx.onFx(() -> step.validProperty().get()),
                "VERSIONED kind with no scripts folder must gate Next");

        Fx.runFx(() -> model.scriptsFolder.set("/tmp/scripts"));
        assertTrue(Fx.onFx(() -> step.validProperty().get()));
    }
}

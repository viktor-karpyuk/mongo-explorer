package com.kubrik.mex.ui.migration;

import com.kubrik.mex.core.ConnectionManager;
import com.kubrik.mex.core.MongoService;
import com.kubrik.mex.migration.spec.MigrationKind;
import com.kubrik.mex.ui.migration.WizardModel.Granularity;
import javafx.beans.binding.Bindings;
import javafx.beans.binding.BooleanBinding;
import javafx.beans.property.BooleanProperty;
import javafx.beans.property.SimpleBooleanProperty;
import javafx.collections.FXCollections;
import javafx.geometry.Insets;
import javafx.scene.control.Button;
import javafx.scene.control.CheckBox;
import javafx.scene.control.Label;
import javafx.scene.control.ListView;
import javafx.scene.control.RadioButton;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.control.ToggleGroup;
import javafx.scene.control.cell.CheckBoxListCell;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import javafx.stage.DirectoryChooser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Step 3: granularity + include/exclude + collection preview + script folder (for versioned).
 *  v1.2.0 (SCOPE-10 / SCOPE-11) — the DB picker is a multi-select list; the collection list
 *  spans every ticked DB and each row shows {@code db.coll}. */
public final class WizardStepScope implements WizardStep {

    private static final Logger log = LoggerFactory.getLogger(WizardStepScope.class);

    private final WizardModel model;
    private final ConnectionManager manager;

    private final VBox root = new VBox(12);

    private final RadioButton serverBtn = new RadioButton("Entire server");
    private final RadioButton databaseBtn = new RadioButton("Entire database(s)");
    private final RadioButton collectionsBtn = new RadioButton("Selected collections");

    private final ListView<String> databaseList = new ListView<>();
    private final Button reloadDbsBtn = new Button("Reload");
    private final Label dbStatusLabel = new Label();
    private final TextArea includeArea = new TextArea("**");
    private final TextArea excludeArea = new TextArea("");
    private final ListView<String> collectionList = new ListView<>();
    private final Button selectAllBtn = new Button("Select all");
    private final Button selectNoneBtn = new Button("Clear");
    private final CheckBox migrateIndexes = new CheckBox("Migrate indexes");
    /** SCOPE-12 opt-in. Defaults off; tooltip explains the preconditions. */
    private final CheckBox migrateUsers = new CheckBox("Migrate users (experimental)");

    /** Per-DB ticked state. Rebuilt each time the source connection reloads. */
    private final Map<String, BooleanProperty> databaseChecked = new HashMap<>();
    /** Per-namespace ticked state for the collection list. Keys are fully-qualified
     *  {@code db.coll} strings built from every ticked DB. */
    private final Map<String, BooleanProperty> collectionChecked = new HashMap<>();

    // Versioned-mode fields
    private final TextField scriptsFolderField = new TextField();

    public WizardStepScope(WizardModel model, ConnectionManager manager) {
        this.model = model;
        this.manager = manager;

        ToggleGroup tg = new ToggleGroup();
        serverBtn.setToggleGroup(tg);
        databaseBtn.setToggleGroup(tg);
        collectionsBtn.setToggleGroup(tg);
        databaseBtn.setSelected(true);
        tg.selectedToggleProperty().addListener((o, a, b) -> {
            // Stale selection from a prior granularity / DB pick would otherwise flip Next
            // green before the user has confirmed anything in the new mode.
            model.selectedCollections.clear();
            if (b == serverBtn) model.granularity.set(Granularity.SERVER);
            else if (b == collectionsBtn) {
                model.granularity.set(Granularity.COLLECTIONS);
                rebuildCollectionList();
            } else {
                model.granularity.set(Granularity.DATABASES);
            }
        });

        // Database list — checkbox per row. Ticking a DB updates selectedDatabases (and the
        // legacy-compat sourceDatabase, pointing at whichever DB is ticked first). In COLLECTIONS
        // mode, the same ticks drive the cross-DB collection list below.
        databaseList.setCellFactory(CheckBoxListCell.forListView(item -> {
            BooleanProperty prop = databaseChecked.computeIfAbsent(item, k -> {
                BooleanProperty p = new SimpleBooleanProperty(false);
                p.addListener((o, a, b) -> onDatabaseTickedChanged());
                return p;
            });
            return prop;
        }));
        databaseList.setPrefHeight(140);

        // Re-populate whenever the user picks a different source connection in Step 1.
        model.sourceConnectionId.addListener((o, a, b) -> refreshDatabases());
        reloadDbsBtn.setOnAction(e -> refreshDatabases());
        dbStatusLabel.setStyle("-fx-font-size: 11px; -fx-text-fill: #b45309;");
        includeArea.textProperty().bindBidirectional(model.includeGlobs);
        excludeArea.textProperty().bindBidirectional(model.excludeGlobs);

        collectionList.setCellFactory(CheckBoxListCell.forListView(item -> {
            BooleanProperty prop = collectionChecked.computeIfAbsent(item, k -> {
                BooleanProperty p = new SimpleBooleanProperty(false);
                p.addListener((o, a, b) -> syncSelectedCollections());
                return p;
            });
            return prop;
        }));
        selectAllBtn.setOnAction(e -> setAllCollectionsChecked(true));
        selectNoneBtn.setOnAction(e -> setAllCollectionsChecked(false));

        migrateIndexes.selectedProperty().bindBidirectional(model.migrateIndexes);
        migrateUsers.selectedProperty().bindBidirectional(model.migrateUsers);
        migrateUsers.setTooltip(new javafx.scene.control.Tooltip(
                "Copies non-built-in users from each selected source DB to the matching target DB "
                        + "after documents and indexes. Requires userAdmin on the target."));

        HBox granRow = new HBox(16, serverBtn, databaseBtn, collectionsBtn);

        HBox dbToolbar = new HBox(8, reloadDbsBtn);
        VBox dbCell = new VBox(4, databaseList, dbToolbar, dbStatusLabel);
        VBox.setVgrow(databaseList, Priority.SOMETIMES);

        HBox collToolbar = new HBox(8, selectAllBtn, selectNoneBtn);
        VBox collCell = new VBox(4, collToolbar, collectionList);
        VBox.setVgrow(collectionList, Priority.ALWAYS);

        GridPane form = new GridPane();
        form.setHgap(12); form.setVgap(10);
        form.addRow(0, new Label("Granularity:"), granRow);
        form.addRow(1, new Label("Source DB(s):"), dbCell);
        form.addRow(2, new Label("Include globs:"), includeArea);
        form.addRow(3, new Label("Exclude globs:"), excludeArea);
        form.addRow(4, new Label(), new HBox(16, migrateIndexes, migrateUsers));
        form.addRow(5, new Label("Collections:"), collCell);

        includeArea.setPrefRowCount(2);
        excludeArea.setPrefRowCount(2);
        collectionList.setPrefHeight(180);

        GridPane versionedForm = new GridPane();
        versionedForm.setHgap(12); versionedForm.setVgap(10);
        scriptsFolderField.textProperty().bindBidirectional(model.scriptsFolder);
        Button pickFolder = new Button("Browse…");
        pickFolder.setOnAction(e -> {
            DirectoryChooser dc = new DirectoryChooser();
            File dir = dc.showDialog(root.getScene().getWindow());
            if (dir != null) model.scriptsFolder.set(dir.getAbsolutePath());
        });
        HBox scriptsRow = new HBox(8, scriptsFolderField, pickFolder);
        HBox.setHgrow(scriptsFolderField, Priority.ALWAYS);
        versionedForm.addRow(0, new Label("Scripts folder:"), scriptsRow);

        root.setPadding(new Insets(16));

        // Flip between DATA_TRANSFER and VERSIONED layouts whenever kind changes.
        model.kind.addListener((o, a, b) -> refreshRoot(form, versionedForm));
        refreshRoot(form, versionedForm);
    }

    private void refreshRoot(Region dataForm, Region versionedForm) {
        root.getChildren().setAll(
                model.kind.get() == MigrationKind.VERSIONED ? versionedForm : dataForm);
    }

    @Override public void onEnter() { refreshDatabases(); }

    private void refreshDatabases() {
        String connId = model.sourceConnectionId.get();
        databaseList.setItems(FXCollections.observableArrayList());
        databaseChecked.clear();
        if (connId == null || connId.isBlank()) {
            dbStatusLabel.setText("No source connection selected (go back to Step 1).");
            return;
        }
        MongoService src = manager.service(connId);
        if (src == null) {
            dbStatusLabel.setText(
                    "Source connection is not active. Open the connection in the main window and try Reload.");
            return;
        }
        try {
            List<String> names = src.listDatabaseNames();
            databaseList.setItems(FXCollections.observableArrayList(names));
            // Re-check any DB the user had picked on a prior visit (round-trip via the model).
            Set<String> previously = new LinkedHashSet<>(model.selectedDatabases);
            for (String name : names) {
                if (previously.contains(name)) {
                    BooleanProperty p = new SimpleBooleanProperty(true);
                    p.addListener((o, a, b) -> onDatabaseTickedChanged());
                    databaseChecked.put(name, p);
                }
            }
            dbStatusLabel.setText(names.isEmpty() ? "Source connection exposes no databases." : "");
            onDatabaseTickedChanged();
        } catch (Exception ex) {
            log.warn("listDatabaseNames failed for {}: {}", connId, ex.getMessage());
            dbStatusLabel.setText("Could not list databases: " + ex.getMessage());
        }
    }

    private void onDatabaseTickedChanged() {
        List<String> picked = new ArrayList<>();
        for (String item : databaseList.getItems()) {
            BooleanProperty p = databaseChecked.get(item);
            if (p != null && p.get()) picked.add(item);
        }
        model.selectedDatabases.setAll(picked);
        model.sourceDatabase.set(picked.isEmpty() ? null : picked.get(0));
        if (model.granularity.get() == Granularity.COLLECTIONS) rebuildCollectionList();
    }

    /** Rebuild the collection list from the union of ticked DBs. Called when the ticked-DB
     *  set changes and whenever the user flips into COLLECTIONS mode. Preserves per-row ticks
     *  across rebuilds so moving in and out of a DB pick doesn't wipe the user's selections. */
    private void rebuildCollectionList() {
        MongoService src = manager.service(model.sourceConnectionId.get());
        if (src == null) {
            collectionList.setItems(FXCollections.observableArrayList());
            collectionList.setPlaceholder(new Label("Source connection is not active."));
            return;
        }
        List<String> pickedDbs = new ArrayList<>(model.selectedDatabases);
        if (pickedDbs.isEmpty()) {
            collectionList.setItems(FXCollections.observableArrayList());
            collectionList.setPlaceholder(new Label("Tick at least one source DB above."));
            model.selectedCollections.clear();
            return;
        }
        List<String> items = new ArrayList<>();
        for (String db : pickedDbs) {
            try {
                for (String coll : src.listCollectionNames(db)) {
                    items.add(db + "." + coll);
                }
            } catch (Exception ex) {
                log.warn("listCollectionNames({}) failed: {}", db, ex.getMessage());
            }
        }
        collectionList.setItems(FXCollections.observableArrayList(items));
        collectionList.setPlaceholder(new Label(items.isEmpty()
                ? "Ticked DB(s) expose no collections."
                : ""));
        // Drop any previously-checked namespace whose DB is no longer ticked.
        Set<String> validNs = new LinkedHashSet<>(items);
        collectionChecked.keySet().removeIf(ns -> !validNs.contains(ns));
        syncSelectedCollections();
    }

    private void syncSelectedCollections() {
        List<String> picked = new ArrayList<>();
        for (String item : collectionList.getItems()) {
            BooleanProperty p = collectionChecked.get(item);
            if (p != null && p.get()) picked.add(item);
        }
        model.selectedCollections.setAll(picked);
    }

    private void setAllCollectionsChecked(boolean value) {
        for (String item : collectionList.getItems()) {
            BooleanProperty p = collectionChecked.computeIfAbsent(item, k -> {
                BooleanProperty np = new SimpleBooleanProperty(false);
                np.addListener((o, a, b) -> syncSelectedCollections());
                return np;
            });
            p.set(value);
        }
    }

    @Override public String title() { return "3. Scope"; }
    @Override public Region view() { return root; }

    @Override public BooleanBinding validProperty() {
        return Bindings.createBooleanBinding(() -> {
            if (model.kind.get() == MigrationKind.VERSIONED) {
                return model.scriptsFolder.get() != null && !model.scriptsFolder.get().isBlank();
            }
            return switch (model.granularity.get()) {
                case SERVER -> true;
                case DATABASES -> !model.selectedDatabases.isEmpty();
                case COLLECTIONS -> !model.selectedCollections.isEmpty();
            };
        }, model.kind, model.granularity, model.selectedDatabases,
           model.scriptsFolder, model.selectedCollections);
    }
}

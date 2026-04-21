package com.kubrik.mex.ui.security;

import com.kubrik.mex.security.access.Privilege;
import com.kubrik.mex.security.access.RoleBinding;
import com.kubrik.mex.security.access.RoleMatrixModel;
import com.kubrik.mex.security.access.RoleRecord;
import com.kubrik.mex.security.access.UserRecord;
import com.kubrik.mex.security.access.UsersRolesFetcher;
import javafx.application.Platform;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Button;
import javafx.scene.control.CheckBox;
import javafx.scene.control.Label;
import javafx.scene.control.ScrollPane;
import javafx.scene.control.SplitPane;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextField;
import javafx.scene.control.Tooltip;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.FlowPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import javafx.util.Duration;

import java.util.List;
import java.util.function.Supplier;

/**
 * v2.6 Q2.6-B2 + Q2.6-B3 — Role Matrix + user detail drawer.
 *
 * <p>Left pane: filter bar (free-text + include-builtins checkbox) driving
 * a {@link TableView} of every user × authentication-database tuple.
 * Right pane: user detail drawer that renders on selection — effective
 * roles as chips, effective privileges in a table, plus a <em>Capture
 * baseline</em> action (Q2.6-B4) that hands the current snapshot to the
 * caller-supplied persister.</p>
 *
 * <p>The pane itself is view-only. Data refresh and baseline persistence
 * are injected as {@link java.util.function.Supplier} / {@link Runnable}
 * so this class can be assembled from the Security tab wire-up without
 * a direct dependency on {@code MongoService}.</p>
 */
public final class RoleMatrixPane extends BorderPane {

    private final RoleMatrixModel model = new RoleMatrixModel();

    private final TextField searchField = new TextField();
    private final CheckBox includeBuiltin = new CheckBox("Include built-in roles");
    private final Button refreshBtn = new Button("Refresh");
    private final Button captureBtn = new Button("Capture baseline");

    private final ObservableList<UserRecord> userRows = FXCollections.observableArrayList();
    private final TableView<UserRecord> userTable = new TableView<>(userRows);
    private final VBox detailBox = new VBox(12);
    private final ScrollPane detailScroll = new ScrollPane(detailBox);

    private final Label statusLabel = new Label("—");

    private Supplier<UsersRolesFetcher.Snapshot> loader = () -> null;
    private Runnable onCaptureBaseline = () -> {};

    public RoleMatrixPane() {
        setStyle("-fx-background-color: white;");
        setPadding(new Insets(14, 16, 14, 16));

        setTop(buildFilterBar());
        setCenter(buildSplit());
        setBottom(buildFooter());

        wireListeners();
        renderEmptyDetail();
    }

    /** Injection point used by the Security tab to wire a live fetcher
     *  (typically {@code () -> fetcher.fetch(svc, FetchOptions.forMatrix())}). */
    public void setLoader(Supplier<UsersRolesFetcher.Snapshot> loader) {
        this.loader = loader == null ? () -> null : loader;
    }

    /** Injection point for the baseline persister (Q2.6-B4) — wired by
     *  the Security tab to {@code SecurityBaselineDao.insert}. */
    public void setOnCaptureBaseline(Runnable cb) {
        this.onCaptureBaseline = cb == null ? () -> {} : cb;
    }

    /** Force a reload on the next event-loop tick. Public so the Security
     *  tab can trigger a refresh on tab-open. */
    public void refresh() {
        refreshBtn.fire();
    }

    /* ============================ filter bar ============================ */

    private Region buildFilterBar() {
        searchField.setPromptText("Search user or role");
        searchField.setPrefWidth(240);
        searchField.textProperty().bindBidirectional(model.textFilterProperty());

        includeBuiltin.selectedProperty().bindBidirectional(
                model.includeBuiltinRolesProperty());
        includeBuiltin.setTooltip(helpTip(
                "Toggle MongoDB's built-in roles (read, readWrite, root, …) "
                + "into the matrix. Hidden by default so only operator-"
                + "defined roles show for quick audit work."));

        refreshBtn.setOnAction(e -> doRefresh());
        refreshBtn.setTooltip(helpTip(
                "Re-reads usersInfo + rolesInfo off the admin database."));

        captureBtn.setDisable(true);
        captureBtn.setTooltip(helpTip(
                "Snapshot the current users + roles into sec_baselines so "
                + "future runs can diff against it (Q2.6-D drift engine)."));
        captureBtn.setOnAction(e -> onCaptureBaseline.run());

        Region grow = new Region();
        HBox.setHgrow(grow, Priority.ALWAYS);
        HBox row = new HBox(10, small("Filter"), searchField, includeBuiltin,
                grow, refreshBtn, captureBtn);
        row.setAlignment(Pos.CENTER_LEFT);
        row.setPadding(new Insets(0, 0, 10, 0));
        return row;
    }

    /* ================================ split ================================ */

    private Region buildSplit() {
        Region left = buildUsersTable();
        detailScroll.setFitToWidth(true);
        detailBox.setPadding(new Insets(14));
        detailBox.setStyle("-fx-background-color: #fafafa;");

        SplitPane split = new SplitPane(left, detailScroll);
        split.setDividerPositions(0.48);
        VBox.setVgrow(split, Priority.ALWAYS);
        return split;
    }

    private Region buildUsersTable() {
        userTable.setPlaceholder(new Label(
                "Click Refresh to read usersInfo + rolesInfo for this connection."));
        userTable.getColumns().setAll(
                col("User",          180, u -> u.user()),
                col("Auth DB",       100, u -> u.db()),
                col("Roles",         220, u -> roleSummary(u)),
                col("Privileges",    110, u -> u.inheritedPrivileges().size() + ""));
        userTable.getSelectionModel().selectedItemProperty().addListener((o, old, u) -> {
            if (u == null) renderEmptyDetail();
            else renderDetail(u);
        });
        return userTable;
    }

    private Region buildFooter() {
        statusLabel.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px;");
        HBox foot = new HBox(statusLabel);
        foot.setPadding(new Insets(8, 0, 0, 0));
        return foot;
    }

    /* ============================== actions ============================== */

    private void wireListeners() {
        // Re-apply filtered view on either filter change.
        model.textFilterProperty().addListener((o, a, b) -> reapplyFilter());
        model.includeBuiltinRolesProperty().addListener((o, a, b) -> reapplyFilter());
    }

    private void doRefresh() {
        refreshBtn.setDisable(true);
        statusLabel.setText("Loading users + roles…");
        Thread.startVirtualThread(() -> {
            UsersRolesFetcher.Snapshot snap = loader.get();
            Platform.runLater(() -> {
                model.load(snap);
                reapplyFilter();
                captureBtn.setDisable(snap == null || snap.users().isEmpty());
                refreshBtn.setDisable(false);
                if (snap == null) {
                    statusLabel.setText("Load failed — check the connection and try again.");
                } else {
                    statusLabel.setText(snap.users().size() + " users · "
                            + snap.roles().size() + " roles");
                }
            });
        });
    }

    private void reapplyFilter() {
        List<UserRecord> filtered = model.filteredUsers();
        UserRecord kept = userTable.getSelectionModel().getSelectedItem();
        userRows.setAll(filtered);
        if (kept != null && filtered.contains(kept)) {
            userTable.getSelectionModel().select(kept);
        }
    }

    /* =============================== detail =============================== */

    private void renderEmptyDetail() {
        detailBox.getChildren().setAll(
                heading("No user selected"),
                small("Click a row on the left to see roles + effective privileges."));
    }

    private void renderDetail(UserRecord u) {
        detailBox.getChildren().clear();

        Label title = new Label(u.fullyQualified());
        title.setStyle("-fx-font-size: 15px; -fx-font-weight: 700;");
        Label sub = new Label(u.roleBindings().size() + " direct roles · "
                + u.inheritedPrivileges().size() + " effective privileges");
        sub.setStyle("-fx-text-fill: #4b5563; -fx-font-size: 11px;");

        detailBox.getChildren().addAll(title, sub, heading("Effective roles"),
                renderRoleChips(model.effectiveRoles(u)),
                heading("Effective privileges"),
                renderPrivilegeTable(model.effectivePrivileges(u)));

        if (!u.authenticationRestrictions().isEmpty()) {
            detailBox.getChildren().addAll(heading("Authentication restrictions"),
                    renderRestrictions(u));
        }
    }

    private FlowPane renderRoleChips(List<RoleBinding> bindings) {
        FlowPane chips = new FlowPane(6, 6);
        if (bindings.isEmpty()) {
            chips.getChildren().add(small("(no roles)"));
            return chips;
        }
        for (RoleBinding b : bindings) {
            Label chip = new Label(b.fullyQualified());
            RoleRecord r = model.roleLookup(b).orElse(null);
            boolean builtin = r != null && r.builtin();
            chip.setStyle(chipStyle(builtin));
            if (r != null) {
                chip.setTooltip(helpTip(
                        (builtin ? "built-in role\n" : "custom role\n")
                        + r.inheritedPrivileges().size() + " effective privileges"));
            }
            chips.getChildren().add(chip);
        }
        return chips;
    }

    private TableView<Privilege> renderPrivilegeTable(List<Privilege> privs) {
        ObservableList<Privilege> rows = FXCollections.observableArrayList(privs);
        TableView<Privilege> t = new TableView<>(rows);
        t.setPlaceholder(new Label("(no effective privileges)"));
        t.getColumns().setAll(
                col("Resource", 180, p -> p.resource().render()),
                col("Actions",  360, p -> String.join(", ", p.actions())));
        // Cap the table's height so long lists scroll inside the detail drawer
        // instead of pushing layout around.
        t.setPrefHeight(Math.min(260, 28 + 24 * Math.max(1, privs.size())));
        t.setMaxHeight(320);
        return t;
    }

    private Region renderRestrictions(UserRecord u) {
        VBox box = new VBox(4);
        for (var r : u.authenticationRestrictions()) {
            Label l = new Label("clientSource: " + r.clientSource()
                    + "  ·  serverAddress: " + r.serverAddress());
            l.setStyle("-fx-text-fill: #374151; -fx-font-size: 11px;");
            box.getChildren().add(l);
        }
        return box;
    }

    /* =============================== helpers ============================== */

    private static Label heading(String text) {
        Label l = new Label(text);
        l.setStyle("-fx-font-size: 12px; -fx-font-weight: 700; -fx-text-fill: #374151;");
        return l;
    }

    private static Label small(String text) {
        Label l = new Label(text);
        l.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px;");
        return l;
    }

    private static String chipStyle(boolean builtin) {
        return builtin
                ? "-fx-background-color: #eef2ff; -fx-text-fill: #3730a3; "
                        + "-fx-padding: 3 8 3 8; -fx-background-radius: 12;"
                : "-fx-background-color: #ecfdf5; -fx-text-fill: #065f46; "
                        + "-fx-padding: 3 8 3 8; -fx-background-radius: 12;";
    }

    private static Tooltip helpTip(String body) {
        Tooltip t = new Tooltip(body);
        t.setShowDelay(Duration.millis(250));
        t.setShowDuration(Duration.seconds(20));
        t.setWrapText(true);
        t.setMaxWidth(340);
        return t;
    }

    private static String roleSummary(UserRecord u) {
        if (u.roleBindings().isEmpty()) return "(none)";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < u.roleBindings().size(); i++) {
            if (i > 0) sb.append(", ");
            if (i >= 3) { sb.append("+").append(u.roleBindings().size() - 3).append(" more"); break; }
            sb.append(u.roleBindings().get(i).role());
        }
        return sb.toString();
    }

    private static <T> TableColumn<T, String> col(String title, int width,
                                                   java.util.function.Function<T, String> getter) {
        TableColumn<T, String> c = new TableColumn<>(title);
        c.setPrefWidth(width);
        c.setCellValueFactory(cd -> new SimpleStringProperty(getter.apply(cd.getValue())));
        return c;
    }
}

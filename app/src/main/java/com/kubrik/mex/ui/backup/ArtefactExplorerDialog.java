package com.kubrik.mex.ui.backup;

import com.kubrik.mex.backup.store.BackupCatalogRow;
import com.kubrik.mex.backup.store.BackupFileDao;
import com.kubrik.mex.backup.store.BackupFileRow;
import com.kubrik.mex.backup.verify.CatalogVerifier;
import com.kubrik.mex.backup.verify.VerifyOutcome;
import com.kubrik.mex.backup.verify.VerifyReport;
import javafx.application.Platform;
import javafx.beans.property.SimpleObjectProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TableCell;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;
import javafx.stage.Window;

import java.util.List;
import java.util.function.Consumer;

/**
 * v2.5 Q2.5-D — artefact-explorer modal for a single backup catalog row.
 *
 * <p>Shows the file inventory (path / bytes / sha256 / kind), a summary
 * strip (size + file count + current verify status), and a <em>Verify
 * now</em> button that routes through {@link CatalogVerifier}. On success
 * the status strip turns green; on failure the per-problem list is
 * rendered below the table in amber.</p>
 */
public final class ArtefactExplorerDialog {

    private ArtefactExplorerDialog() {}

    public static void show(Window owner, BackupCatalogRow row, BackupFileDao files,
                             CatalogVerifier verifier,
                             Consumer<VerifyReport> onVerified) {
        Stage stage = new Stage();
        if (owner != null) stage.initOwner(owner);
        stage.setTitle("Backup #" + row.id() + "  ·  " + row.sinkPath());

        ObservableList<BackupFileRow> inventory =
                FXCollections.observableArrayList(files.listForCatalog(row.id()));

        Label title = new Label(row.sinkPath());
        title.setStyle("-fx-font-size: 14px; -fx-font-weight: 700;");
        Label summary = new Label(summaryText(row, inventory));
        summary.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px;");

        Label verdict = new Label(row.verifyOutcome() == null
                ? "Not verified yet."
                : "Last verify: " + row.verifyOutcome());
        verdict.setStyle(verdictStyle(row.verifyOutcome()));

        TableView<BackupFileRow> table = buildTable(inventory);
        Label problems = new Label();
        problems.setWrapText(true);
        problems.setStyle("-fx-text-fill: #b45309; -fx-font-size: 11px;");
        problems.setVisible(false);
        problems.managedProperty().bind(problems.visibleProperty());

        Button verifyBtn = new Button("Verify now");
        Button closeBtn = new Button("Close");
        closeBtn.setOnAction(e -> stage.close());
        verifyBtn.setOnAction(e -> {
            verifyBtn.setDisable(true);
            verdict.setText("Verifying…");
            verdict.setStyle("-fx-text-fill: #2563eb; -fx-font-size: 11px; -fx-font-weight: 700;");
            Thread.startVirtualThread(() -> {
                VerifyReport report = verifier.verify(row.id());
                Platform.runLater(() -> {
                    verdict.setText("Verify: " + report.outcome()
                            + "  (" + report.filesChecked() + " files, "
                            + formatBytes(report.bytesChecked()) + ")");
                    verdict.setStyle(verdictStyle(report.outcome().name()));
                    if (!report.problems().isEmpty()) {
                        problems.setText(String.join("\n• ",
                                "Problems:\n•" + report.problems().get(0),
                                report.problems().size() > 1
                                        ? String.join("\n• ", report.problems().subList(1,
                                                Math.min(report.problems().size(), 20)))
                                        : ""));
                        problems.setVisible(true);
                    } else {
                        problems.setVisible(false);
                    }
                    verifyBtn.setDisable(false);
                    if (onVerified != null) onVerified.accept(report);
                });
            });
        });

        Region grow = new Region();
        HBox.setHgrow(grow, Priority.ALWAYS);
        HBox footer = new HBox(8, verdict, grow, verifyBtn, closeBtn);
        footer.setAlignment(Pos.CENTER_RIGHT);
        footer.setPadding(new Insets(10, 14, 12, 14));

        VBox header = new VBox(4, title, summary);
        header.setPadding(new Insets(12, 14, 8, 14));
        BorderPane root = new BorderPane(table);
        root.setTop(header);
        root.setBottom(new VBox(problems, footer));
        root.setStyle("-fx-background-color: white;");

        Scene scene = new Scene(root, 760, 520);
        stage.setScene(scene);
        stage.show();
    }

    /* ============================= internals ============================= */

    private static TableView<BackupFileRow> buildTable(ObservableList<BackupFileRow> inv) {
        TableView<BackupFileRow> t = new TableView<>(inv);
        t.setPlaceholder(new Label("No file rows recorded for this backup."));
        TableColumn<BackupFileRow, String> pathCol = textCol("path", 280, BackupFileRow::relativePath);
        TableColumn<BackupFileRow, Number> bytesCol = new TableColumn<>("bytes");
        bytesCol.setPrefWidth(100);
        bytesCol.setCellValueFactory(cd -> new SimpleObjectProperty<>(cd.getValue().bytes()));
        bytesCol.setCellFactory(col -> {
            TableCell<BackupFileRow, Number> cell = new TableCell<>() {
                @Override protected void updateItem(Number n, boolean empty) {
                    super.updateItem(n, empty);
                    setText(empty || n == null ? "" : formatBytes(n.longValue()));
                }
            };
            cell.setAlignment(Pos.CENTER_RIGHT);
            return cell;
        });
        TableColumn<BackupFileRow, String> kindCol = textCol("kind", 80, BackupFileRow::kind);
        TableColumn<BackupFileRow, String> shaCol = textCol("sha256", 250,
                r -> r.sha256().substring(0, 12) + "…" + r.sha256().substring(r.sha256().length() - 4));
        t.getColumns().setAll(pathCol, bytesCol, kindCol, shaCol);
        VBox.setVgrow(t, Priority.ALWAYS);
        return t;
    }

    private static TableColumn<BackupFileRow, String> textCol(String title, int w,
                                                               java.util.function.Function<BackupFileRow, String> getter) {
        TableColumn<BackupFileRow, String> c = new TableColumn<>(title);
        c.setPrefWidth(w);
        c.setCellValueFactory(cd -> new SimpleStringProperty(getter.apply(cd.getValue())));
        return c;
    }

    private static String summaryText(BackupCatalogRow row, List<BackupFileRow> files) {
        return files.size() + " files · "
                + formatBytes(row.totalBytes() == null ? 0 : row.totalBytes())
                + (row.manifestSha256() == null ? ""
                        : "  ·  manifest " + row.manifestSha256().substring(0, 10) + "…");
    }

    private static String formatBytes(long b) {
        double gb = b / (1024.0 * 1024 * 1024);
        if (gb >= 1) return String.format("%.1f GB", gb);
        double mb = b / (1024.0 * 1024);
        if (mb >= 1) return String.format("%.1f MB", mb);
        return b + " B";
    }

    private static String verdictStyle(String outcome) {
        if (outcome == null) return "-fx-text-fill: #6b7280; -fx-font-size: 11px;";
        String fg = switch (outcome) {
            case "VERIFIED" -> "#166534";
            case "MANIFEST_MISSING", "FILE_MISSING" -> "#b91c1c";
            case "MANIFEST_TAMPERED", "FILE_MISMATCH" -> "#b45309";
            case "IO_ERROR" -> "#991b1b";
            default -> "#6b7280";
        };
        return "-fx-text-fill: " + fg + "; -fx-font-size: 11px; -fx-font-weight: 700;";
    }

    // Bundle VerifyOutcome imports so switch above compiles even without
    // constant references (compile-time reachability check).
    @SuppressWarnings("unused")
    private static final VerifyOutcome[] ANCHOR = VerifyOutcome.values();
}

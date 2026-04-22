package com.kubrik.mex.ui.security;

import com.kubrik.mex.security.EvidenceSigner;
import com.kubrik.mex.security.cis.CisFinding;
import com.kubrik.mex.security.cis.CisReport;
import com.kubrik.mex.security.cis.CisRule;
import com.kubrik.mex.security.cis.CisSuppression;
import com.kubrik.mex.security.cis.CisSuppressionsDao;
import com.kubrik.mex.security.export.EvidenceBundle;
import javafx.application.Platform;
import javafx.beans.property.SimpleObjectProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.ButtonType;
import javafx.scene.control.Dialog;
import javafx.scene.control.DialogPane;
import javafx.scene.control.Label;
import javafx.scene.control.TableCell;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.stage.FileChooser;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * v2.6 Q2.6-H4 — CIS benchmark sub-tab. Run-scan button kicks the
 * Q2.6-H1 runner; each finding lands in a TableView with per-severity
 * colouring. Per-row context actions:
 * <ul>
 *   <li><b>Suppress…</b> — opens a small dialog that writes a
 *       {@link CisSuppression} via {@link CisSuppressionsDao} and
 *       re-renders the report so the finding switches to the
 *       {@code SUPPRESSED} band.</li>
 *   <li><b>Export…</b> — writes a signed evidence bundle
 *       ({@code report.json} + {@code report.html} + {@code evidence.sig})
 *       to a user-picked directory via Q2.6-I's {@link EvidenceBundle}.</li>
 * </ul>
 */
public final class CisPane extends BorderPane {

    private static final DateTimeFormatter TS_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());

    private final ObservableList<CisFinding> rows = FXCollections.observableArrayList();
    private final TableView<CisFinding> table = new TableView<>(rows);
    private final Label scorecard = new Label("Run a scan to see findings.");
    private final Button scanBtn = SecurityPaneHelpers.withTip(
            new Button("Run scan"),
            "Evaluates every CIS rule against the current connection's "
            + "users / roles / auth / encryption / cert probes. A scan "
            + "takes seconds; long runs usually mean the probes are slow.");
    private final Button exportBtn = SecurityPaneHelpers.withTip(
            new Button("Export…"),
            "Writes a signed evidence bundle (report.json + report.html + "
            + "evidence.sig) to a directory you pick. HMAC covers the "
            + "report.json bytes on disk.");

    private final AtomicReference<CisReport> lastReport = new AtomicReference<>();
    private Supplier<CisReport> scanner = () -> null;
    private CisSuppressionsDao suppressionsDao;
    private EvidenceSigner signer;
    private String connectionId;
    private String capturedBy = "";

    public CisPane() {
        setStyle("-fx-background-color: -color-bg-default;");
        setPadding(new Insets(14, 16, 14, 16));

        setTop(buildTopBar());
        setCenter(buildTable());
        setBottom(buildScorecard());

        exportBtn.setDisable(true);
        exportBtn.setOnAction(e -> exportReport());

        table.setRowFactory(tv -> {
            javafx.scene.control.TableRow<CisFinding> r = new javafx.scene.control.TableRow<>();
            javafx.scene.control.ContextMenu menu = new javafx.scene.control.ContextMenu();
            javafx.scene.control.MenuItem suppress = new javafx.scene.control.MenuItem("Suppress…");
            suppress.setOnAction(e -> {
                if (!r.isEmpty()) openSuppressDialog(r.getItem());
            });
            menu.getItems().add(suppress);
            r.setContextMenu(menu);
            return r;
        });
    }

    /** Wire-up from SecurityTab: the {@code scanner} supplier closes
     *  over the connection's probe context. {@code dao} persists
     *  suppressions; {@code signer} is used when the operator exports
     *  a signed report. */
    public void configure(String connectionId, String capturedBy,
                           Supplier<CisReport> scanner,
                           CisSuppressionsDao dao, EvidenceSigner signer) {
        this.connectionId = connectionId;
        this.capturedBy = capturedBy == null ? "" : capturedBy;
        this.scanner = scanner == null ? () -> null : scanner;
        this.suppressionsDao = dao;
        this.signer = signer;
    }

    public void runScan() { scanBtn.fire(); }

    /* ============================= layout ============================= */

    private Region buildTopBar() {
        scanBtn.setOnAction(e -> doScan());
        return SecurityPaneHelpers.topBar(
                SecurityPaneHelpers.paneTitle("CIS MongoDB Benchmark"),
                exportBtn, scanBtn);
    }

    private Region buildTable() {
        table.setPlaceholder(SecurityPaneHelpers.emptyState(
                "No scan results yet",
                "Click Run scan to evaluate the v1.2 rule set against this "
                + "connection. Right-click any finding to Suppress it with "
                + "a reason + optional TTL. Export bundles the results as "
                + "signed evidence for auditors."));
        table.getColumns().setAll(
                col("Rule", 120, CisFinding::ruleId),
                col("Title", 300, CisFinding::title),
                severityCol(),
                verdictCol(),
                col("Scope", 100, CisFinding::scope),
                col("Detail", 420, CisFinding::detail));
        SecurityPaneHelpers.describe(table,
                "CIS scan findings. Right-click a row to suppress a FAIL with a "
                + "reason and optional TTL.");
        return table;
    }

    private Region buildScorecard() {
        scorecard.setStyle("-fx-text-fill: -color-fg-muted; -fx-font-size: 12px; -fx-padding: 10 0 0 0;");
        return scorecard;
    }

    /* ============================= actions ============================= */

    private void doScan() {
        if (scanner == null) return;
        scanBtn.setDisable(true);
        exportBtn.setDisable(true);
        scorecard.setText("Running CIS scan…");
        Thread.startVirtualThread(() -> {
            CisReport report = scanner.get();
            Platform.runLater(() -> {
                scanBtn.setDisable(false);
                if (report == null) {
                    scorecard.setText("Scan failed — check the connection.");
                    return;
                }
                lastReport.set(report);
                rows.setAll(report.findings());
                exportBtn.setDisable(false);
                scorecard.setText(String.format(
                        "%s   ·   %s   ·   %d pass   ·   %d fail   ·   %d N/A   ·   %d suppressed",
                        TS_FMT.format(Instant.ofEpochMilli(report.ranAtMs())),
                        report.benchmarkVersion(),
                        report.pass(), report.fail(),
                        report.notApplicable(), report.suppressed()));
            });
        });
    }

    private void openSuppressDialog(CisFinding f) {
        if (suppressionsDao == null) return;
        Dialog<ButtonType> d = new Dialog<>();
        if (getScene() != null) d.initOwner(getScene().getWindow());
        d.setTitle("Suppress finding — " + f.ruleId());

        Label head = new Label(f.ruleId() + " · " + f.title());
        head.setStyle("-fx-font-weight: 700;");
        TextField scope = new TextField(f.scope());
        TextArea reason = new TextArea();
        reason.setPromptText("Why is this suppressed? (auditor-visible)");
        reason.setPrefRowCount(3);
        TextField expiryField = new TextField();
        expiryField.setPromptText("TTL days (blank = no expiry)");

        GridPane g = new GridPane();
        g.setHgap(10); g.setVgap(8); g.setPadding(new Insets(14));
        g.add(head, 0, 0, 2, 1);
        g.add(SecurityPaneHelpers.small("Scope"), 0, 1);    g.add(scope, 1, 1);
        g.add(SecurityPaneHelpers.small("Reason"), 0, 2);   g.add(reason, 1, 2);
        g.add(SecurityPaneHelpers.small("TTL (days)"), 0, 3); g.add(expiryField, 1, 3);

        DialogPane pane = d.getDialogPane();
        pane.setContent(g);
        pane.getButtonTypes().setAll(ButtonType.CANCEL, ButtonType.OK);

        Optional<ButtonType> choice = d.showAndWait();
        if (choice.isEmpty() || choice.get() != ButtonType.OK) return;
        if (reason.getText() == null || reason.getText().isBlank()) {
            alert("Reason required to persist a suppression.");
            return;
        }
        Long expiresAt = null;
        if (!expiryField.getText().isBlank()) {
            try {
                int days = Integer.parseInt(expiryField.getText().trim());
                expiresAt = System.currentTimeMillis() + days * 86_400_000L;
            } catch (NumberFormatException nfe) {
                alert("TTL must be a whole number of days, or blank for no expiry.");
                return;
            }
        }
        suppressionsDao.insert(new CisSuppression(-1, connectionId, f.ruleId(),
                scope.getText() == null ? "CLUSTER" : scope.getText(),
                reason.getText().trim(),
                System.currentTimeMillis(), capturedBy, expiresAt));
        doScan();  // re-run so the finding re-renders with the suppressed band
    }

    private void exportReport() {
        CisReport report = lastReport.get();
        if (report == null || signer == null) return;
        FileChooser fc = new FileChooser();
        fc.setTitle("Export CIS evidence bundle");
        fc.setInitialFileName("cis-" + report.connectionId() + "-"
                + report.ranAtMs() + ".zip");
        // Export writes three files to a *directory*, not a single zip.
        // Use DirectoryChooser instead.
        javafx.stage.DirectoryChooser dc = new javafx.stage.DirectoryChooser();
        dc.setTitle("Choose evidence bundle directory");
        java.io.File out = dc.showDialog(getScene() == null ? null : getScene().getWindow());
        if (out == null) return;
        try {
            String json = renderReportJson(report);
            String html = renderReportHtml(report);
            EvidenceBundle.Exported ex = EvidenceBundle.export(
                    out.toPath(), json, html, signer);
            alert("Exported to " + ex.dir() + "\n\nSignature: " + ex.signature());
        } catch (IOException ioe) {
            alert("Export failed: " + ioe.getMessage());
        }
    }

    /* =========================== rendering =========================== */

    private static String renderReportJson(CisReport r) {
        // Compact JSON — canonical ordering isn't required here because
        // the HMAC covers whatever bytes land on disk. Keeping field
        // order stable across exports is nice for diffing, though.
        StringBuilder sb = new StringBuilder(2048);
        sb.append("{\"connectionId\":\"").append(escape(r.connectionId())).append("\"");
        sb.append(",\"ranAtMs\":").append(r.ranAtMs());
        sb.append(",\"benchmark\":\"").append(escape(r.benchmarkVersion())).append("\"");
        sb.append(",\"scores\":{\"pass\":").append(r.pass())
                .append(",\"fail\":").append(r.fail())
                .append(",\"notApplicable\":").append(r.notApplicable())
                .append(",\"suppressed\":").append(r.suppressed()).append("}");
        sb.append(",\"findings\":[");
        for (int i = 0; i < r.findings().size(); i++) {
            CisFinding f = r.findings().get(i);
            if (i > 0) sb.append(',');
            sb.append("{\"ruleId\":\"").append(escape(f.ruleId())).append("\"");
            sb.append(",\"title\":\"").append(escape(f.title())).append("\"");
            sb.append(",\"severity\":\"").append(f.severity().name()).append("\"");
            sb.append(",\"verdict\":\"").append(f.verdict().name()).append("\"");
            sb.append(",\"scope\":\"").append(escape(f.scope())).append("\"");
            sb.append(",\"suppressed\":").append(f.suppressed());
            sb.append(",\"detail\":\"").append(escape(f.detail())).append("\"}");
        }
        return sb.append("]}").toString();
    }

    private static String renderReportHtml(CisReport r) {
        StringBuilder sb = new StringBuilder(4096);
        sb.append("<!DOCTYPE html><html><head><meta charset=\"utf-8\"><title>CIS scan · ")
          .append(escape(r.connectionId())).append("</title>")
          .append("<style>body{font-family:system-ui,sans-serif;margin:2rem;color:#111}")
          .append("table{border-collapse:collapse;width:100%}")
          .append("th,td{border:1px solid #e5e7eb;padding:6px 10px;text-align:left;vertical-align:top}")
          .append(".pass{color:#166534;font-weight:600}.fail{color:#991b1b;font-weight:600}")
          .append(".na{color:#4b5563}.sup{color:#92400e;font-style:italic}")
          .append(".crit{background:#fee2e2}.hi{background:#fef3c7}</style></head><body>");
        sb.append("<h1>CIS MongoDB Benchmark</h1>");
        sb.append("<p><strong>Connection:</strong> ").append(escape(r.connectionId()))
          .append("<br><strong>Ran at:</strong> ")
          .append(Instant.ofEpochMilli(r.ranAtMs()))
          .append("<br><strong>Benchmark:</strong> ").append(escape(r.benchmarkVersion()))
          .append("</p>");
        sb.append("<p><strong>Pass:</strong> ").append(r.pass())
          .append(" &nbsp; <strong>Fail:</strong> ").append(r.fail())
          .append(" &nbsp; <strong>N/A:</strong> ").append(r.notApplicable())
          .append(" &nbsp; <strong>Suppressed:</strong> ").append(r.suppressed())
          .append("</p>");
        sb.append("<table><thead><tr><th>Rule</th><th>Title</th><th>Severity</th>")
          .append("<th>Verdict</th><th>Detail</th></tr></thead><tbody>");
        for (CisFinding f : r.findings()) {
            String severityClass = switch (f.severity()) {
                case CRITICAL -> "crit";
                case HIGH -> "hi";
                default -> "";
            };
            String verdictClass = f.suppressed() ? "sup" : switch (f.verdict()) {
                case PASS -> "pass";
                case FAIL -> "fail";
                case NOT_APPLICABLE -> "na";
            };
            sb.append("<tr class=\"").append(severityClass).append("\">")
              .append("<td><code>").append(escape(f.ruleId())).append("</code></td>")
              .append("<td>").append(escape(f.title())).append("</td>")
              .append("<td>").append(f.severity().name()).append("</td>")
              .append("<td class=\"").append(verdictClass).append("\">")
              .append(f.suppressed() ? "SUPPRESSED" : f.verdict().name()).append("</td>")
              .append("<td>").append(escape(f.detail())).append("</td></tr>");
        }
        return sb.append("</tbody></table></body></html>").toString();
    }

    /* ============================ columns ============================ */

    private static <T> TableColumn<T, String> col(String title, int width,
                                                   java.util.function.Function<T, String> getter) {
        TableColumn<T, String> c = new TableColumn<>(title);
        c.setPrefWidth(width);
        c.setCellValueFactory(cd -> new SimpleStringProperty(getter.apply(cd.getValue())));
        return c;
    }

    private static TableColumn<CisFinding, CisRule.Severity> severityCol() {
        TableColumn<CisFinding, CisRule.Severity> c = new TableColumn<>("Severity");
        c.setPrefWidth(110);
        c.setCellValueFactory(cd -> new SimpleObjectProperty<>(cd.getValue().severity()));
        c.setCellFactory(col -> new TableCell<>() {
            @Override protected void updateItem(CisRule.Severity s, boolean empty) {
                super.updateItem(s, empty);
                if (empty || s == null) { setText(""); setStyle(""); return; }
                setText(s.name());
                setStyle(switch (s) {
                    case CRITICAL -> "-fx-text-fill: #991b1b; -fx-font-weight: 700;";
                    case HIGH -> "-fx-text-fill: #b45309; -fx-font-weight: 700;";
                    case MEDIUM -> "-fx-text-fill: #92400e;";
                    case LOW, INFO -> "-fx-text-fill: -color-fg-subtle;";
                });
            }
        });
        return c;
    }

    private static TableColumn<CisFinding, String> verdictCol() {
        TableColumn<CisFinding, String> c = new TableColumn<>("Verdict");
        c.setPrefWidth(110);
        c.setCellValueFactory(cd -> {
            CisFinding f = cd.getValue();
            return new SimpleStringProperty(
                    f.suppressed() ? "SUPPRESSED" : f.verdict().name());
        });
        c.setCellFactory(col -> new TableCell<>() {
            @Override protected void updateItem(String v, boolean empty) {
                super.updateItem(v, empty);
                if (empty || v == null) { setText(""); setStyle(""); return; }
                setText(v);
                setStyle(switch (v) {
                    case "PASS" -> "-fx-text-fill: #166534; -fx-font-weight: 700;";
                    case "FAIL" -> "-fx-text-fill: #991b1b; -fx-font-weight: 700;";
                    case "SUPPRESSED" -> "-fx-text-fill: #92400e; -fx-font-style: italic;";
                    default -> "-fx-text-fill: -color-fg-subtle;";
                });
            }
        });
        return c;
    }

    /* ============================= helpers ============================= */

    private void alert(String msg) {
        Alert a = new Alert(Alert.AlertType.INFORMATION);
        if (getScene() != null) a.initOwner(getScene().getWindow());
        a.setHeaderText(null);
        a.setContentText(msg);
        a.showAndWait();
    }

    private static String escape(String s) {
        if (s == null) return "";
        return s.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "\\\"")
                .replace("\n", "\\n");
    }

    // Keep imports honest in case future polish reintroduces the helper.
    @SuppressWarnings("unused")
    private static List<String> noOp() { return List.of(); }
}

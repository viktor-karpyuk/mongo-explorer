package com.kubrik.mex.ui.cluster;

import com.kubrik.mex.cluster.model.Member;
import com.kubrik.mex.core.MongoService;
import com.kubrik.mex.ui.JsonCodeArea;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.CheckBox;
import javafx.scene.control.Label;
import javafx.scene.control.Spinner;
import javafx.scene.control.Tooltip;
import javafx.scene.input.Clipboard;
import javafx.scene.input.ClipboardContent;
import javafx.scene.input.KeyCode;
import javafx.scene.input.KeyEvent;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;
import javafx.stage.Window;
import org.bson.Document;
import org.bson.json.JsonWriterSettings;
import org.fxmisc.flowless.VirtualizedScrollPane;

import java.util.ArrayList;
import java.util.List;

/**
 * v2.4 RS-8 — preview-only editor for a member's priority / votes / hidden
 * flags. Re-renders the {@code replSetReconfig} body on every input change so
 * the user can see exactly what a future v2.7 dispatch would send. Execute is
 * permanently disabled in v2.4; the footer explains the deferral.
 *
 * <p>v2.6.2 UX pass: JSON preview uses {@link VirtualizedScrollPane} (consistent
 * with {@link ReplConfigDialog}) and gets a zoom toolbar (⌘= / ⌘− / ⌘0
 * keyboard), a Copy-JSON button, inline baseline indicators per field
 * (<em>was: N</em>), a reset-to-baseline button, and inline validation for the
 * hidden+priority constraint. Scene floor is now 960×640 with min 720×520 so
 * the JSON pane has real room on narrow screens.</p>
 */
public final class ReconfigPreviewDialog {

    private static final double FONT_MIN = 9.0;
    private static final double FONT_MAX = 28.0;
    private static final double FONT_DEFAULT = 13.0;

    private ReconfigPreviewDialog() {}

    public static void show(Window owner, String connectionId, Member member, MongoService svc) {
        Stage stage = new Stage();
        if (owner != null) stage.initOwner(owner);
        stage.setTitle("Edit priority / votes · " + member.host());
        stage.setResizable(true);
        stage.setMinWidth(720);
        stage.setMinHeight(520);

        // ---------------- header ----------------
        Label title = new Label("Edit " + member.host());
        title.setStyle("-fx-font-weight: 700; -fx-font-size: 14px;");
        Label sub = new Label("Preview only — Execute is disabled in v2.4. Dispatch lands in v2.7.");
        sub.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px;");

        // ---------------- inputs ----------------
        int basePrio = member.priority() == null ? 1 : member.priority();
        int baseVotes = member.votes() == null ? 1 : member.votes();
        boolean baseHidden = Boolean.TRUE.equals(member.hidden());

        Spinner<Integer> prio = new Spinner<>(0, 1000, basePrio);
        prio.setEditable(true);
        prio.setPrefWidth(110);
        Tooltip.install(prio, new Tooltip(
                "0 = cannot be elected primary. 1 = default. Higher values are more likely to be elected."));

        Spinner<Integer> votes = new Spinner<>(0, 1, baseVotes);
        votes.setEditable(true);
        votes.setPrefWidth(90);
        Tooltip.install(votes, new Tooltip(
                "0 = non-voting member. 1 = voting (default). Per-member votes are always 0 or 1."));

        CheckBox hidden = new CheckBox("hidden");
        hidden.setSelected(baseHidden);
        Tooltip.install(hidden, new Tooltip(
                "Hidden members are invisible to application reads. Must have priority=0."));

        Label prioWas = wasLabel(basePrio);
        Label votesWas = wasLabel(baseVotes);
        Label hiddenWas = wasLabel(baseHidden ? "true" : "false");

        VBox prioCol = field("priority", prio, prioWas);
        VBox votesCol = field("votes", votes, votesWas);
        VBox hiddenCol = field("visibility", hidden, hiddenWas);

        Button resetBtn = new Button("Reset to current");
        resetBtn.setTooltip(new Tooltip("Revert all fields to the live config values."));
        resetBtn.setOnAction(e -> {
            prio.getValueFactory().setValue(basePrio);
            votes.getValueFactory().setValue(baseVotes);
            hidden.setSelected(baseHidden);
        });

        Region inputGrow = new Region();
        HBox.setHgrow(inputGrow, Priority.ALWAYS);
        HBox inputs = new HBox(18, prioCol, votesCol, hiddenCol, inputGrow, resetBtn);
        inputs.setAlignment(Pos.BOTTOM_LEFT);
        inputs.setPadding(new Insets(14, 0, 4, 0));

        // Inline validation (hidden=true requires priority=0)
        Label validation = new Label("");
        validation.setStyle("-fx-text-fill: #b45309; -fx-font-size: 11px;");
        validation.setManaged(false);
        validation.setVisible(false);

        // ---------------- JSON preview + zoom toolbar ----------------
        JsonCodeArea preview = new JsonCodeArea("loading config…");
        preview.setEditable(false);
        double[] fontSize = {FONT_DEFAULT};
        Runnable applyFont = () -> preview.setStyle(
                "-fx-font-size: " + fontSize[0] + "px;"
                        + "-fx-font-family: 'JetBrains Mono', 'Menlo', 'Consolas', monospace;");
        applyFont.run();

        Button fontDec = zoomBtn("A−", "Decrease JSON font size  (⌘−)");
        Button fontInc = zoomBtn("A+", "Increase JSON font size  (⌘=)");
        Button fontReset = zoomBtn("A", "Reset JSON font size  (⌘0)");
        fontDec.setOnAction(e -> {
            fontSize[0] = Math.max(FONT_MIN, fontSize[0] - 1.0);
            applyFont.run();
        });
        fontInc.setOnAction(e -> {
            fontSize[0] = Math.min(FONT_MAX, fontSize[0] + 1.0);
            applyFont.run();
        });
        fontReset.setOnAction(e -> {
            fontSize[0] = FONT_DEFAULT;
            applyFont.run();
        });

        Label previewCaption = new Label("replSetReconfig preview");
        previewCaption.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px; -fx-font-weight: 600;");
        Region capGrow = new Region();
        HBox.setHgrow(capGrow, Priority.ALWAYS);
        HBox previewBar = new HBox(6, previewCaption, capGrow, fontDec, fontReset, fontInc);
        previewBar.setAlignment(Pos.CENTER_LEFT);
        previewBar.setPadding(new Insets(10, 0, 6, 0));

        VirtualizedScrollPane<JsonCodeArea> scroll = new VirtualizedScrollPane<>(preview);
        VBox.setVgrow(scroll, Priority.ALWAYS);

        // ---------------- footer ----------------
        Button copyBtn = new Button("Copy JSON");
        copyBtn.setDisable(true);
        Button executeBtn = new Button("Execute");
        executeBtn.setDisable(true);
        Tooltip.install(executeBtn, new Tooltip(
                "Destructive dispatch for rs.reconfig lands with v2.7 guided reconfig."));
        Button closeBtn = new Button("Close");
        closeBtn.setOnAction(e -> stage.close());

        Label deferred = new Label("Deferred to v2.7 guided reconfig");
        deferred.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px;");

        Region footerGrow = new Region();
        HBox.setHgrow(footerGrow, Priority.ALWAYS);
        HBox footer = new HBox(8, deferred, footerGrow, copyBtn, executeBtn, closeBtn);
        footer.setAlignment(Pos.CENTER_RIGHT);
        footer.setPadding(new Insets(12, 16, 14, 16));

        // ---------------- layout ----------------
        VBox header = new VBox(2, title, sub, inputs, validation);
        header.setPadding(new Insets(14, 16, 0, 16));

        VBox body = new VBox(previewBar, scroll);
        body.setPadding(new Insets(0, 16, 0, 16));
        VBox.setVgrow(scroll, Priority.ALWAYS);
        VBox.setVgrow(body, Priority.ALWAYS);

        BorderPane root = new BorderPane(body);
        root.setTop(header);
        root.setBottom(footer);
        root.setStyle("-fx-background-color: white;");

        Scene scene = new Scene(root, 960, 640);

        // Cmd/Ctrl = / − / 0 for JSON zoom (matches common editor conventions).
        scene.addEventFilter(KeyEvent.KEY_PRESSED, ev -> {
            if (!ev.isShortcutDown()) return;
            KeyCode k = ev.getCode();
            if (k == KeyCode.EQUALS || k == KeyCode.PLUS || k == KeyCode.ADD) {
                fontInc.fire(); ev.consume();
            } else if (k == KeyCode.MINUS || k == KeyCode.SUBTRACT) {
                fontDec.fire(); ev.consume();
            } else if (k == KeyCode.DIGIT0 || k == KeyCode.NUMPAD0) {
                fontReset.fire(); ev.consume();
            }
        });

        stage.setScene(scene);
        stage.show();

        Thread.startVirtualThread(() -> {
            Document current;
            try {
                Document reply = svc.database("admin").runCommand(new Document("replSetGetConfig", 1));
                Object cfg = reply.get("config");
                current = cfg instanceof Document c ? c : new Document();
            } catch (Exception e) {
                current = new Document();
            }
            Document cfg = current;
            javafx.application.Platform.runLater(() -> {
                Runnable rebuild = () -> {
                    String json = buildReconfigPreview(cfg, member.host(),
                            prio.getValue(), votes.getValue(), hidden.isSelected());
                    preview.replaceText(0, preview.getLength(), json);
                    preview.refreshHighlight();

                    // Baseline diff indicators — mute when unchanged, highlight when modified.
                    paintBaseline(prioWas, !prio.getValue().equals(basePrio));
                    paintBaseline(votesWas, !votes.getValue().equals(baseVotes));
                    paintBaseline(hiddenWas, hidden.isSelected() != baseHidden);

                    // Inline validation: hidden=true requires priority=0.
                    if (hidden.isSelected() && prio.getValue() != 0) {
                        validation.setText("⚠ Hidden members must have priority = 0. MongoDB will reject this reconfig.");
                        validation.setManaged(true);
                        validation.setVisible(true);
                    } else {
                        validation.setManaged(false);
                        validation.setVisible(false);
                    }
                };
                rebuild.run();
                prio.valueProperty().addListener((o, a, b) -> rebuild.run());
                votes.valueProperty().addListener((o, a, b) -> rebuild.run());
                hidden.selectedProperty().addListener((o, a, b) -> rebuild.run());

                copyBtn.setDisable(false);
                copyBtn.setOnAction(e -> {
                    ClipboardContent c = new ClipboardContent();
                    c.putString(preview.getText());
                    Clipboard.getSystemClipboard().setContent(c);
                });
            });
        });
    }

    // ---- layout helpers ----

    private static VBox field(String label, javafx.scene.Node control, Label was) {
        Label lbl = new Label(label);
        lbl.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px; -fx-font-weight: 600;");
        VBox v = new VBox(4, lbl, control, was);
        v.setAlignment(Pos.TOP_LEFT);
        return v;
    }

    private static Label wasLabel(Object value) {
        Label l = new Label("was: " + value);
        l.setStyle("-fx-text-fill: #9ca3af; -fx-font-size: 10px;");
        return l;
    }

    private static void paintBaseline(Label was, boolean modified) {
        was.setStyle(modified
                ? "-fx-text-fill: #2563eb; -fx-font-size: 10px; -fx-font-weight: 600;"
                : "-fx-text-fill: #9ca3af; -fx-font-size: 10px;");
    }

    private static Button zoomBtn(String text, String tooltip) {
        Button b = new Button(text);
        b.setTooltip(new Tooltip(tooltip));
        b.setStyle("-fx-font-size: 11px; -fx-padding: 2 8 2 8;");
        b.setFocusTraversable(false);
        return b;
    }

    // ---- JSON preview assembly (unchanged from v2.4 RS-8) ----

    @SuppressWarnings("unchecked")
    private static String buildReconfigPreview(Document current, String host,
                                               int priority, int votes, boolean hiddenFlag) {
        if (current == null || current.isEmpty()) {
            Document stub = new Document("_id", "<replicaSet>");
            List<Document> members = new ArrayList<>();
            members.add(new Document("host", host)
                    .append("priority", priority)
                    .append("votes", votes)
                    .append("hidden", hiddenFlag));
            stub.append("members", members);
            return new Document("replSetReconfig", stub).toJson(
                    JsonWriterSettings.builder().indent(true).build());
        }
        Document next = deepCopy(current);
        List<Document> members = (List<Document>) next.get("members");
        if (members != null) {
            for (Document m : members) {
                if (host.equals(m.getString("host"))) {
                    m.put("priority", priority);
                    m.put("votes", votes);
                    m.put("hidden", hiddenFlag);
                }
            }
        }
        Object version = next.get("version");
        if (version instanceof Number n) next.put("version", n.intValue() + 1);
        return new Document("replSetReconfig", next).toJson(
                JsonWriterSettings.builder().indent(true).build());
    }

    @SuppressWarnings("unchecked")
    private static Document deepCopy(Document src) {
        Document out = new Document();
        for (java.util.Map.Entry<String, Object> e : src.entrySet()) {
            Object v = e.getValue();
            if (v instanceof Document d) out.put(e.getKey(), deepCopy(d));
            else if (v instanceof List<?> l) {
                List<Object> copy = new ArrayList<>();
                for (Object el : l) copy.add(el instanceof Document dd ? deepCopy(dd) : el);
                out.put(e.getKey(), copy);
            } else out.put(e.getKey(), v);
        }
        return out;
    }
}

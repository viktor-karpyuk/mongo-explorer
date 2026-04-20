package com.kubrik.mex.ui.cluster;

import com.kubrik.mex.cluster.model.Member;
import com.kubrik.mex.core.MongoService;
import com.kubrik.mex.ui.JsonCodeArea;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.CheckBox;
import javafx.scene.control.Hyperlink;
import javafx.scene.control.Label;
import javafx.scene.control.ScrollPane;
import javafx.scene.control.Spinner;
import javafx.scene.control.Tooltip;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;
import javafx.stage.Window;
import org.bson.Document;
import org.bson.json.JsonWriterSettings;

import java.util.ArrayList;
import java.util.List;

/**
 * v2.4 RS-8 — preview-only editor for a member's priority / votes / hidden
 * flags. Re-renders the {@code replSetReconfig} body on every input change so
 * the user can see exactly what a future v2.7 dispatch would send. Execute is
 * permanently disabled in v2.4; the footer links to the v2.7 guided-reconfig
 * milestone so the user knows where this lands.
 *
 * <p>The dialog fetches {@code replSetGetConfig} on open to capture the
 * baseline. Editing is purely client-side — nothing persists and nothing
 * touches the server beyond the initial read.</p>
 */
public final class ReconfigPreviewDialog {

    private ReconfigPreviewDialog() {}

    public static void show(Window owner, String connectionId, Member member, MongoService svc) {
        Stage stage = new Stage();
        if (owner != null) stage.initOwner(owner);
        stage.setTitle("Edit priority / votes · " + member.host());

        Label title = new Label("Edit " + member.host());
        title.setStyle("-fx-font-weight: 700; -fx-font-size: 14px;");
        Label sub = new Label("Preview only — the Execute button is disabled in v2.4.");
        sub.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px;");

        Label prioLbl = small("priority");
        Spinner<Integer> prio = new Spinner<>(0, 1000, member.priority() == null ? 1 : member.priority());
        prio.setEditable(true);
        prio.setPrefWidth(100);

        Label votesLbl = small("votes");
        Spinner<Integer> votes = new Spinner<>(0, 1, member.votes() == null ? 1 : member.votes());
        votes.setEditable(true);
        votes.setPrefWidth(80);

        CheckBox hidden = new CheckBox("hidden");
        hidden.setSelected(Boolean.TRUE.equals(member.hidden()));

        HBox inputs = new HBox(12,
                new VBox(2, prioLbl, prio),
                new VBox(2, votesLbl, votes),
                new VBox(2, small(""), hidden));
        inputs.setAlignment(Pos.CENTER_LEFT);
        inputs.setPadding(new Insets(12, 0, 4, 0));

        JsonCodeArea preview = new JsonCodeArea("loading config…");
        preview.setEditable(false);
        ScrollPane scroll = new ScrollPane(preview);
        scroll.setFitToWidth(true);
        scroll.setStyle("-fx-background-color: transparent; -fx-background: transparent;");
        VBox.setVgrow(scroll, Priority.ALWAYS);

        Button executeBtn = new Button("Execute");
        executeBtn.setDisable(true);
        Tooltip.install(executeBtn, new Tooltip(
                "Destructive dispatch for rs.reconfig lands with v2.7 guided reconfig."));
        Button closeBtn = new Button("Close");
        closeBtn.setOnAction(e -> stage.close());
        Hyperlink deferredLink = new Hyperlink("Deferred to v2.7 guided reconfig →");
        deferredLink.setStyle("-fx-text-fill: #2563eb; -fx-font-size: 11px;");
        deferredLink.setOnAction(e -> {
            // No-op link (docs/v2/v2.7/*); surfaces the intent for now.
        });
        Region grow = new Region();
        HBox.setHgrow(grow, Priority.ALWAYS);
        HBox footer = new HBox(8, deferredLink, grow, executeBtn, closeBtn);
        footer.setAlignment(Pos.CENTER_RIGHT);
        footer.setPadding(new Insets(10, 14, 12, 14));

        VBox header = new VBox(2, title, sub, inputs);
        header.setPadding(new Insets(14, 16, 0, 16));

        BorderPane root = new BorderPane(scroll);
        root.setTop(header);
        root.setBottom(footer);
        root.setStyle("-fx-background-color: white;");

        Scene scene = new Scene(root, 720, 560);
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
                };
                rebuild.run();
                prio.valueProperty().addListener((o, a, b) -> rebuild.run());
                votes.valueProperty().addListener((o, a, b) -> rebuild.run());
                hidden.selectedProperty().addListener((o, a, b) -> rebuild.run());
            });
        });
    }

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

    private static Label small(String s) {
        Label l = new Label(s);
        l.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 11px;");
        return l;
    }
}

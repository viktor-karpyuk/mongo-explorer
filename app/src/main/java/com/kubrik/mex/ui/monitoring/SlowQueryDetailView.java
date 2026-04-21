package com.kubrik.mex.ui.monitoring;

import com.kubrik.mex.monitoring.store.ProfileSampleRecord;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Label;
import javafx.scene.control.TextArea;
import javafx.scene.layout.FlowPane;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.VBox;

/**
 * ROW-EXPAND-2 — slow-query detail modal. Fields mirror the wireframes'
 * {@code W-MODAL-DRILL-SLOW}. Redacted command JSON is shown as-is (never the
 * raw form per BR-9).
 */
public final class SlowQueryDetailView extends VBox {

    public SlowQueryDetailView(ProfileSampleRecord r, String connectionDisplayName) {
        setPadding(new Insets(16));
        setSpacing(12);

        Label header = new Label("Slow query · " + r.ns());
        header.setStyle("-fx-font-size: 18px; -fx-font-weight: bold;");
        Label sub = new Label(connectionDisplayName + "  ·  " + r.op() + "  ·  " + r.millis() + " ms");
        sub.setStyle("-fx-text-fill: #6b7280; -fx-font-size: 12px;");

        FlowPane badges = new FlowPane(6, 6);
        if (r.planSummary() != null && r.planSummary().contains("COLLSCAN"))
            badges.getChildren().add(badge("COLLSCAN", "#fee2e2", "#991b1b"));
        if (r.docsExamined() != null && r.docsReturned() != null
                && r.docsReturned() > 0 && r.docsExamined() >= 100L
                && r.docsExamined().doubleValue() / Math.max(1, r.docsReturned()) >= 100.0)
            badges.getChildren().add(badge("selectivity-poor", "#fef3c7", "#92400e"));
        if (r.planSummary() != null && r.planSummary().contains("IXSCAN"))
            badges.getChildren().add(badge("IXSCAN", "#dcfce7", "#166534"));

        GridPane grid = new GridPane();
        grid.setHgap(16); grid.setVgap(6);
        int row = 0;
        row = row(grid, row, "Plan",           nv(r.planSummary()));
        row = row(grid, row, "docsExamined",   nvL(r.docsExamined()));
        row = row(grid, row, "docsReturned",   nvL(r.docsReturned()));
        row = row(grid, row, "keysExamined",   nvL(r.keysExamined()));
        row = row(grid, row, "numYield",       nvL(r.numYield()));
        row = row(grid, row, "responseLength", nvL(r.responseLength()));
        row = row(grid, row, "queryHash",      nv(r.queryHash()));
        row = row(grid, row, "planCacheKey",   nv(r.planCacheKey()));

        Label cmdTitle = new Label("Command (redacted)");
        cmdTitle.setStyle("-fx-font-weight: bold; -fx-font-size: 12px;");
        TextArea cmd = new TextArea(r.commandJson() == null ? "(none)" : r.commandJson());
        cmd.setEditable(false);
        cmd.setWrapText(true);
        cmd.setPrefRowCount(10);
        cmd.setStyle("-fx-font-family: 'Menlo', 'Monaco', monospace; -fx-font-size: 12px;");

        getChildren().addAll(header, sub, badges, grid, cmdTitle, cmd);
    }

    private static int row(GridPane g, int row, String k, String v) {
        Label key = new Label(k);
        key.setStyle("-fx-text-fill: #6b7280;");
        Label val = new Label(v);
        g.add(key, 0, row);
        g.add(val, 1, row);
        return row + 1;
    }

    private static Label badge(String text, String bg, String fg) {
        Label l = new Label(text);
        l.setStyle("-fx-background-color: " + bg + "; -fx-text-fill: " + fg
                + "; -fx-font-weight: bold; -fx-font-size: 10px;"
                + " -fx-padding: 3 10 3 10; -fx-background-radius: 10;");
        l.setAlignment(Pos.CENTER);
        return l;
    }

    private static String nv(String s) { return s == null || s.isBlank() ? "—" : s; }
    private static String nvL(Long v) { return v == null ? "—" : Long.toString(v); }
}

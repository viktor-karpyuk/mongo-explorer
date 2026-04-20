package com.kubrik.mex.ui.cluster;

import com.kubrik.mex.cluster.model.ClusterKind;
import com.kubrik.mex.cluster.model.TopologySnapshot;
import com.kubrik.mex.events.EventBus;
import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Label;
import javafx.scene.control.Tab;
import javafx.scene.control.TabPane;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.StackPane;
import javafx.scene.layout.VBox;

/**
 * v2.4 UI-OPS-1..6 — per-connection Cluster tab shell. Hosts the Topology /
 * Ops / Balancer / Oplog / Audit / Pools sub-tabs. Balancer only appears on
 * sharded clusters; the tab visibility flips in response to the first
 * {@link TopologySnapshot} arriving on the bus. Sub-tabs other than Topology
 * carry a "coming in v2.4-D…H" placeholder in this phase and will light up as
 * their workstreams land.
 */
public final class ClusterTab extends BorderPane implements AutoCloseable {

    private final String connectionId;
    private final EventBus bus;

    private final TabPane tabPane = new TabPane();
    private final Tab topologyTab;
    private final Tab opsTab;
    private final Tab balancerTab;
    private final Tab oplogTab;
    private final Tab auditTab;
    private final Tab poolsTab;

    private final TopologyPane topologyPane;
    private final EventBus.Subscription topoSub;

    public ClusterTab(String connectionId, EventBus bus) {
        this.connectionId = connectionId;
        this.bus = bus;

        this.topologyPane = new TopologyPane(connectionId, bus);
        this.topologyTab = tab("Topology", topologyPane);
        this.opsTab      = tab("Ops",       placeholder("Live ops viewer lands with Q2.4-D."));
        this.balancerTab = tab("Balancer",  placeholder("Balancer controls land with Q2.4-G."));
        this.oplogTab    = tab("Oplog",     placeholder("Oplog gauge + tail lands with Q2.4-E."));
        this.auditTab    = tab("Audit",     placeholder("Audit pane lands with Q2.4-H."));
        this.poolsTab    = tab("Pools",     placeholder("Connection-pool viewer lands with Q2.4-D."));

        tabPane.setTabClosingPolicy(TabPane.TabClosingPolicy.UNAVAILABLE);
        tabPane.getTabs().addAll(topologyTab, opsTab, oplogTab, auditTab, poolsTab);

        setCenter(tabPane);
        setStyle("-fx-background-color: #f9fafb;");

        // Balancer tab is sharded-only; flip visibility based on live topology kind.
        // Replay semantics on onTopology deliver the current snapshot immediately.
        this.topoSub = bus.onTopology((id, snap) -> {
            if (!connectionId.equals(id) || snap == null) return;
            Platform.runLater(() -> applyKind(snap.clusterKind()));
        });
        TopologySnapshot pre = bus.latestTopology(connectionId);
        if (pre != null) applyKind(pre.clusterKind());
    }

    @Override
    public void close() {
        try { topoSub.close(); } catch (Exception ignored) {}
        try { topologyPane.close(); } catch (Exception ignored) {}
    }

    /* =========================== internals ============================== */

    private void applyKind(ClusterKind kind) {
        boolean sharded = kind == ClusterKind.SHARDED;
        boolean present = tabPane.getTabs().contains(balancerTab);
        if (sharded && !present) {
            int idx = tabPane.getTabs().indexOf(opsTab) + 1;
            tabPane.getTabs().add(Math.max(0, idx), balancerTab);
        } else if (!sharded && present) {
            tabPane.getTabs().remove(balancerTab);
        }
    }

    private static Tab tab(String name, javafx.scene.Node content) {
        Tab t = new Tab(name, content);
        t.setClosable(false);
        return t;
    }

    private static VBox placeholder(String message) {
        Label title = new Label(message);
        title.setStyle("-fx-font-size: 14px; -fx-text-fill: #374151; -fx-font-weight: 600;");
        Label sub = new Label("Check the milestone doc for the target date.");
        sub.setStyle("-fx-font-size: 12px; -fx-text-fill: #9ca3af;");
        VBox v = new VBox(8, title, sub);
        v.setAlignment(Pos.CENTER);
        v.setPadding(new Insets(80, 40, 80, 40));
        StackPane.setAlignment(v, Pos.CENTER);
        return v;
    }
}

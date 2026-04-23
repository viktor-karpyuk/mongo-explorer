package com.kubrik.mex.k8s.ui;

import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.k8s.discovery.DiscoveryService;
import com.kubrik.mex.k8s.events.DiscoveryEvent;
import com.kubrik.mex.k8s.model.DiscoveredMongo;
import com.kubrik.mex.k8s.model.K8sClusterRef;
import com.kubrik.mex.k8s.model.MongoCredentials;
import com.kubrik.mex.k8s.secret.SecretPickupService;
import com.kubrik.mex.store.ConnectionStore;
import javafx.application.Platform;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.ButtonType;
import javafx.scene.control.Label;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextArea;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;

import java.util.List;
import java.util.function.Function;

/**
 * v2.8.1 Q2.8.1-B3 — Discovery sub-pane rendered under the Clusters
 * table when a cluster is selected.
 *
 * <p>Three actions:</p>
 * <ul>
 *   <li>Refresh — runs {@link DiscoveryService#discover} on a
 *       virtual thread and re-populates the discovered Mongo table
 *       via the {@link DiscoveryEvent} feed.</li>
 *   <li>Resolve credentials — picks up Secrets for the selected
 *       row via {@link SecretPickupService} and previews what was
 *       found (never the raw password bytes).</li>
 *   <li>Connect — writes a {@code origin='K8S'} connection row via
 *       {@link ConnectionStore#insertK8sOrigin}. The resulting row is
 *       visible but not live-connectable until Q2.8.1-C ships the
 *       port-forward service; the status line tells the user this.</li>
 * </ul>
 */
public final class DiscoveryPanel extends VBox {

    private final DiscoveryService discoveryService;
    private final SecretPickupService secretService;
    private final ConnectionStore connectionStore;
    private final EventBus events;

    private final ObservableList<DiscoveredMongo> rows = FXCollections.observableArrayList();
    private final TableView<DiscoveredMongo> table = new TableView<>(rows);
    private final Label statusLabel = new Label("Pick a cluster above and click Refresh.");
    private final TextArea credentialsPreview = new TextArea();
    private K8sClusterRef currentRef;
    private EventBus.Subscription busSubscription;

    public DiscoveryPanel(DiscoveryService discoveryService,
                           SecretPickupService secretService,
                           ConnectionStore connectionStore,
                           EventBus events) {
        super(6);
        this.discoveryService = discoveryService;
        this.secretService = secretService;
        this.connectionStore = connectionStore;
        this.events = events;

        setPadding(new Insets(10, 0, 0, 0));

        Label head = new Label("Discovered Mongo workloads");
        head.setStyle("-fx-text-fill: -color-fg-default; -fx-font-size: 11px; -fx-font-weight: 600;");

        table.setPlaceholder(new Label("Pick a cluster above to discover."));
        table.getColumns().setAll(
                col("Origin", 90, r -> r.origin().name()),
                col("Namespace", 140, DiscoveredMongo::namespace),
                col("Name", 180, DiscoveredMongo::name),
                col("Topology", 110, r -> r.topology().name()),
                col("Auth", 90, r -> r.authKind().name()),
                col("Version", 120, r -> r.mongoVersion().orElse("—")),
                col("Ready", 70, r -> r.ready().map(b -> b ? "yes" : "no").orElse("—")),
                col("Service", 200, r -> r.serviceName().orElse("—")));

        credentialsPreview.setEditable(false);
        credentialsPreview.setPrefRowCount(5);
        credentialsPreview.setWrapText(true);
        credentialsPreview.setStyle(
                "-fx-font-family: 'Menlo', 'Courier New', monospace; -fx-font-size: 11px;");
        credentialsPreview.setText("(nothing resolved yet)");

        Button refreshBtn = new Button("Refresh");
        refreshBtn.setOnAction(e -> onRefresh());
        Button resolveBtn = new Button("Resolve credentials");
        resolveBtn.setOnAction(e -> onResolve());
        Button connectBtn = new Button("Create connection");
        connectBtn.setOnAction(e -> onConnect());

        HBox actions = new HBox(8, refreshBtn, resolveBtn, connectBtn);

        statusLabel.setStyle("-fx-text-fill: -color-fg-muted; -fx-font-size: 11px;");
        statusLabel.setWrapText(true);

        VBox.setVgrow(table, Priority.ALWAYS);
        getChildren().addAll(head, table, new Label("Credentials preview"),
                credentialsPreview, actions, statusLabel);

        if (events != null) {
            busSubscription = events.onDiscovery(evt -> Platform.runLater(() -> onEvent(evt)));
        }
    }

    public void close() {
        if (busSubscription != null) busSubscription.close();
    }

    /** Wire a new cluster into the panel — called from ClustersPane's selection listener. */
    public void bindCluster(K8sClusterRef ref) {
        this.currentRef = ref;
        rows.clear();
        credentialsPreview.setText("(nothing resolved yet)");
        if (ref == null) {
            statusLabel.setText("Pick a cluster above and click Refresh.");
        } else {
            statusLabel.setText("Ready to discover " + ref.displayName() + ".");
        }
    }

    /* ============================ actions ============================ */

    private void onRefresh() {
        if (currentRef == null) {
            statusLabel.setText("Pick a cluster above first.");
            return;
        }
        statusLabel.setText("Discovering " + currentRef.displayName() + "…");
        K8sClusterRef ref = currentRef;
        Thread.startVirtualThread(() -> {
            try {
                discoveryService.discover(ref);
            } catch (Throwable t) {
                Platform.runLater(() ->
                        statusLabel.setText("Discovery failed: " + t.getMessage()));
            }
        });
    }

    private void onResolve() {
        DiscoveredMongo sel = table.getSelectionModel().getSelectedItem();
        if (sel == null || currentRef == null) {
            statusLabel.setText("Pick a discovered Mongo first.");
            return;
        }
        K8sClusterRef ref = currentRef;
        Thread.startVirtualThread(() -> {
            MongoCredentials creds = secretService.resolve(ref, sel);
            Platform.runLater(() -> renderCredentials(sel, creds));
        });
    }

    private void onConnect() {
        DiscoveredMongo sel = table.getSelectionModel().getSelectedItem();
        if (sel == null) {
            statusLabel.setText("Pick a discovered Mongo first.");
            return;
        }
        // Build a placeholder URI that points at the in-cluster
        // Service — not reachable yet until Q2.8.1-C wires up
        // port-forwarding, but the row is recognisable and editable.
        String svc = sel.serviceName().orElse(sel.name());
        int port = sel.port().orElse(27017);
        String uri = "mongodb://" + svc + "." + sel.namespace()
                + ".svc.cluster.local:" + port + "/";

        Alert confirm = new Alert(Alert.AlertType.CONFIRMATION,
                "Create a connection row for " + sel.origin() + " " + sel.coordinates() + "?\n\n"
                        + "The URI will point at the in-cluster Service name. Port-forwarding "
                        + "(Q2.8.1-C) will rewrite it to 127.0.0.1:<ephemeral> once available, "
                        + "making the row live-connectable.",
                ButtonType.CANCEL, ButtonType.OK);
        confirm.setHeaderText(null);
        confirm.showAndWait().ifPresent(bt -> {
            if (bt != ButtonType.OK) return;
            try {
                String name = sel.origin().name().toLowerCase() + ":" + sel.name();
                String connectionId = connectionStore.insertK8sOrigin(name, uri);
                statusLabel.setText("Connection " + connectionId + " recorded (awaiting Q2.8.1-C).");
            } catch (RuntimeException re) {
                statusLabel.setText("Connection insert failed: " + re.getMessage());
            }
        });
    }

    private void renderCredentials(DiscoveredMongo m, MongoCredentials creds) {
        StringBuilder sb = new StringBuilder();
        sb.append("row: ").append(m.coordinates()).append('\n');
        sb.append("username: ").append(creds.username().orElse("(not resolved)")).append('\n');
        sb.append("password: ").append(creds.password().isPresent() ? "(set)" : "(not resolved)").append('\n');
        sb.append("authDB:   ").append(creds.authDatabase().orElse("admin")).append('\n');
        if (creds.hasTlsMaterial()) {
            sb.append("TLS CA fingerprint: ")
                    .append(creds.tlsCaFingerprint().orElse("(unknown)")).append('\n');
            creds.tlsCaPem().ifPresent(bytes ->
                    sb.append("CA PEM bytes: ").append(bytes.length).append('\n'));
            creds.tlsClientCertPem().ifPresent(bytes ->
                    sb.append("Client cert PEM bytes: ").append(bytes.length).append('\n'));
        } else {
            sb.append("TLS: (no material found)\n");
        }
        credentialsPreview.setText(sb.toString());
        statusLabel.setText("Resolved credentials for " + m.coordinates()
                + (creds.hasCredentials() ? "." : " — some fields missing, fill in manually."));
    }

    private void onEvent(DiscoveryEvent e) {
        if (currentRef == null || currentRef.id() != e.clusterId()) return;
        switch (e) {
            case DiscoveryEvent.Refreshed r -> {
                rows.setAll(r.rows());
                statusLabel.setText("Found " + r.rows().size() + " Mongo workload"
                        + (r.rows().size() == 1 ? "" : "s") + ".");
            }
            case DiscoveryEvent.Failed f ->
                statusLabel.setText("Discovery failed: " + f.reason());
        }
    }

    /* ============================ helpers ============================ */

    private static <T> TableColumn<T, String> col(String title, int width,
                                                    Function<T, String> getter) {
        TableColumn<T, String> c = new TableColumn<>(title);
        c.setPrefWidth(width);
        c.setCellValueFactory(cd -> new SimpleStringProperty(getter.apply(cd.getValue())));
        return c;
    }

    /** Exposed for ClustersPane: the underlying TableView for layout composition. */
    public Region asRegion() { return this; }

    /** Exposed for tests — lets assertions inspect the current row list without a scene. */
    public List<DiscoveredMongo> currentRows() {
        return List.copyOf(rows);
    }
}

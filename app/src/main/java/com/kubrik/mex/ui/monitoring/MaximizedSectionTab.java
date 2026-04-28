package com.kubrik.mex.ui.monitoring;

import com.kubrik.mex.core.ConnectionManager;
import com.kubrik.mex.events.EventBus;
import com.kubrik.mex.monitoring.MonitoringService;
import javafx.geometry.Insets;
import javafx.scene.Node;
import javafx.scene.control.Label;
import javafx.scene.control.ScrollPane;
import javafx.scene.layout.VBox;

/**
 * Maximized view of a single monitoring section — Instance, Replication, Storage,
 * or Workload — bound to a specific connection id. Rendered in LARGE mode so the
 * charts / tables have room to breathe. Each instance self-subscribes to
 * {@link EventBus}; the main tab and this tab render the same section in parallel.
 */
public final class MaximizedSectionTab extends VBox implements AutoCloseable {

    private final AutoCloseable section;

    public MaximizedSectionTab(String sectionId, String connectionId,
                               EventBus bus, MonitoringService svc,
                               ConnectionManager manager,
                               GraphExpandOpener graphExpandOpener,
                               RowExpandOpener rowExpandOpener) {
        setPadding(new Insets(16));
        setSpacing(12);

        String title = sectionTitle(sectionId);
        Label header = new Label(title
                + (connectionId != null ? "  ·  " + connectionId : ""));
        header.setStyle("-fx-font-size: 20px; -fx-font-weight: bold;");
        getChildren().add(header);

        MetricExpander expander = graphExpandOpener == null ? null
                : (m, mode) -> graphExpandOpener.open(m, connectionId, mode);

        Node body;
        AutoCloseable sec;
        switch (sectionId) {
            case InstanceSection.ID -> {
                InstanceSection s = new InstanceSection(bus, MetricCell.Size.LARGE, connectionId, expander);
                body = s.view(); sec = s;
            }
            case ReplicationSection.ID -> {
                ReplicationSection s = new ReplicationSection(bus, MetricCell.Size.LARGE, connectionId, expander, rowExpandOpener);
                body = s.view(); sec = s;
            }
            case ShardingSection.ID -> {
                ShardingSection s = new ShardingSection(bus, manager, MetricCell.Size.LARGE, connectionId, expander);
                body = s.view(); sec = s;
            }
            case StorageSection.ID -> {
                StorageSection s = new StorageSection(bus, MetricCell.Size.LARGE, connectionId, rowExpandOpener);
                body = s.view(); sec = s;
            }
            case WorkloadSection.ID -> {
                WorkloadSection s = new WorkloadSection(bus, svc, MetricCell.Size.LARGE, connectionId, expander, rowExpandOpener);
                body = s.view(); sec = s;
            }
            default -> { body = new Label("Unknown section: " + sectionId); sec = null; }
        }
        this.section = sec;

        ScrollPane scroll = new ScrollPane(body);
        scroll.setFitToWidth(true);
        getChildren().add(scroll);
        javafx.scene.layout.VBox.setVgrow(scroll, javafx.scene.layout.Priority.ALWAYS);
    }

    @Override public void close() {
        if (section != null) { try { section.close(); } catch (Throwable ignored) {} }
    }

    /** Tab title for the outer app-level TabPane. */
    public static String tabTitle(String sectionId, String connectionId) {
        return "Monitoring · " + sectionTitle(sectionId)
                + (connectionId != null ? " · " + connectionId : "");
    }

    private static String sectionTitle(String sectionId) {
        return switch (sectionId) {
            case InstanceSection.ID    -> InstanceSection.TITLE;
            case ReplicationSection.ID -> ReplicationSection.TITLE;
            case ShardingSection.ID    -> ShardingSection.TITLE;
            case StorageSection.ID     -> StorageSection.TITLE;
            case WorkloadSection.ID    -> WorkloadSection.TITLE;
            default                    -> sectionId;
        };
    }
}

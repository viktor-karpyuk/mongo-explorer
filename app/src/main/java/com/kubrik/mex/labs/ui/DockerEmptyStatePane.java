package com.kubrik.mex.labs.ui;

import com.kubrik.mex.labs.docker.DockerClient;
import com.kubrik.mex.labs.model.EngineStatus;
import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Button;
import javafx.scene.control.Hyperlink;
import javafx.scene.control.Label;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;

/**
 * v2.8.4 LAB-DOCKER-3 — Empty-state pane for the Labs tab when
 * Docker isn't available. Renders a guided message + install links
 * keyed on {@link EngineStatus}, with a Retry button that re-probes
 * so a user who starts Docker mid-session sees the pane flip.
 */
public final class DockerEmptyStatePane extends VBox {

    public interface OnDockerReady { void run(); }

    private final DockerClient docker;
    private final OnDockerReady onReady;

    public DockerEmptyStatePane(DockerClient docker, OnDockerReady onReady) {
        this.docker = docker;
        this.onReady = onReady;
        setSpacing(12);
        setAlignment(Pos.CENTER);
        setPadding(new Insets(40));
        render(docker.status());
    }

    private void render(EngineStatus status) {
        getChildren().clear();

        Label title = new Label(titleFor(status));
        title.setStyle("-fx-font-size: 18px; -fx-font-weight: 700; "
                + "-fx-text-fill: -color-fg-default;");
        Label body = new Label(bodyFor(status));
        body.setStyle("-fx-text-fill: -color-fg-muted; -fx-font-size: 12px;");
        body.setWrapText(true);
        body.setMaxWidth(520);

        HBox links = new HBox(12);
        links.setAlignment(Pos.CENTER);
        if (status == EngineStatus.CLI_MISSING || status == EngineStatus.VERSION_LOW) {
            links.getChildren().addAll(
                    installLink("Docker Desktop", "https://www.docker.com/products/docker-desktop/"),
                    installLink("OrbStack", "https://orbstack.dev/"),
                    installLink("colima", "https://github.com/abiosoft/colima"));
        }

        Button retry = new Button("Retry");
        retry.setOnAction(e -> Thread.startVirtualThread(() -> {
            EngineStatus next = docker.status();
            Platform.runLater(() -> {
                if (next == EngineStatus.READY) onReady.run();
                else render(next);
            });
        }));

        getChildren().addAll(title, body);
        if (!links.getChildren().isEmpty()) getChildren().add(links);
        getChildren().add(retry);
    }

    private static String titleFor(EngineStatus status) {
        return switch (status) {
            case CLI_MISSING -> "Docker isn't installed";
            case DAEMON_DOWN -> "Docker isn't running";
            case VERSION_LOW -> "Docker is too old";
            case UNKNOWN -> "Docker isn't responding";
            case READY -> "Ready";
        };
    }

    private static String bodyFor(EngineStatus status) {
        return switch (status) {
            case CLI_MISSING -> "Mongo Explorer Labs uses Docker to spin up "
                    + "local sandbox clusters. Install any Docker-compatible "
                    + "runtime below, make sure `docker` is on your PATH, then "
                    + "click Retry.";
            case DAEMON_DOWN -> "The `docker` CLI is installed but the daemon "
                    + "isn't responding. Start Docker Desktop / OrbStack / "
                    + "colima and click Retry.";
            case VERSION_LOW -> "Labs needs Docker CLI 24.0 or newer "
                    + "(`docker compose ls --format json` landed in that "
                    + "release). Update your Docker runtime and retry.";
            case UNKNOWN -> "Something went wrong probing Docker. Check the "
                    + "app log (Help → Show log folder) for details, then "
                    + "click Retry.";
            case READY -> "Docker is ready — the Labs catalogue should load.";
        };
    }

    private static Hyperlink installLink(String label, String url) {
        Hyperlink h = new Hyperlink(label);
        h.setOnAction(e -> {
            try {
                java.awt.Desktop.getDesktop().browse(
                        java.net.URI.create(url));
            } catch (Exception ignored) {}
        });
        return h;
    }
}

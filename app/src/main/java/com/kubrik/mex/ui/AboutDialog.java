package com.kubrik.mex.ui;

import com.kubrik.mex.k8s.compute.ComputeStrategyRegistry;
import com.kubrik.mex.k8s.compute.StrategyId;
import com.kubrik.mex.k8s.compute.managedpool.ManagedPoolAdapterRegistry;
import com.kubrik.mex.k8s.compute.managedpool.OsKeychainSecretStore;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Button;
import javafx.scene.control.ButtonType;
import javafx.scene.control.Dialog;
import javafx.scene.control.Label;
import javafx.scene.input.Clipboard;
import javafx.scene.input.ClipboardContent;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;
import javafx.stage.Window;

import java.io.IOException;
import java.net.InetAddress;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.jar.Manifest;

/**
 * v2.8.4 (UX) — About / Info dialog. Shows the build version,
 * runtime fingerprint, and the live state of every feature surface
 * that has a per-release lock (compute strategies, managed-pool
 * adapters, OS keychain availability).
 *
 * <p>The "Copy" button drops the same content onto the clipboard so
 * users can paste it into bug reports without retyping.</p>
 */
public final class AboutDialog {

    private AboutDialog() {}

    public static void show(Window owner) {
        Dialog<Void> dlg = new Dialog<>();
        dlg.setTitle("About Mongo Explorer");
        if (owner != null) dlg.initOwner(owner);

        Map<String, String> rows = collect();

        Label heading = new Label("Mongo Explorer");
        heading.setStyle("-fx-font-size: 16px; -fx-font-weight: 700;");
        Label tag = new Label(rows.getOrDefault("Version", "dev")
                + "  ·  " + rows.getOrDefault("Build date", ""));
        tag.setStyle("-fx-text-fill: -color-fg-muted; -fx-font-size: 11px;");

        GridPane g = new GridPane();
        g.setHgap(12); g.setVgap(4);
        g.setPadding(new Insets(8, 0, 8, 0));
        int r = 0;
        for (var e : rows.entrySet()) {
            Label k = new Label(e.getKey());
            k.setStyle("-fx-text-fill: -color-fg-muted; -fx-font-size: 11px;");
            Label v = new Label(e.getValue());
            v.setStyle("-fx-font-family: 'Menlo','Monaco',monospace; -fx-font-size: 11px;");
            v.setWrapText(true);
            g.add(k, 0, r);
            g.add(v, 1, r);
            r++;
        }

        Button copy = new Button("Copy to clipboard");
        copy.setOnAction(ev -> {
            StringBuilder sb = new StringBuilder();
            for (var e : rows.entrySet()) {
                sb.append(e.getKey()).append(": ").append(e.getValue()).append('\n');
            }
            ClipboardContent cc = new ClipboardContent();
            cc.putString(sb.toString());
            Clipboard.getSystemClipboard().setContent(cc);
        });

        HBox actions = new HBox(8, copy);
        actions.setAlignment(Pos.CENTER_RIGHT);
        actions.setPadding(new Insets(8, 0, 0, 0));

        VBox body = new VBox(4, heading, tag, g, actions);
        body.setPadding(new Insets(14, 16, 14, 16));
        dlg.getDialogPane().setContent(body);
        dlg.getDialogPane().getButtonTypes().setAll(ButtonType.CLOSE);
        dlg.getDialogPane().setPrefWidth(560);
        dlg.showAndWait();
    }

    /** Collect everything we want to surface — keep order stable so
     *  copy-pasting into a bug report reads like a fingerprint. */
    private static Map<String, String> collect() {
        Map<String, String> rows = new LinkedHashMap<>();
        rows.put("Version", versionFromManifest());
        rows.put("Build date", buildDateFromManifest());
        rows.put("Java", System.getProperty("java.version", "?")
                + " (" + System.getProperty("java.vendor", "?") + ")");
        rows.put("JavaFX", System.getProperty("javafx.runtime.version", "?"));
        rows.put("OS", System.getProperty("os.name", "?")
                + " " + System.getProperty("os.version", "?")
                + " (" + System.getProperty("os.arch", "?") + ")");
        rows.put("User home", System.getProperty("user.home", "?"));
        try { rows.put("Hostname", InetAddress.getLocalHost().getHostName()); }
        catch (Exception e) { rows.put("Hostname", "?"); }
        rows.put("Time", Instant.now().toString());

        // Feature surfaces.
        ComputeStrategyRegistry creg = ComputeStrategyRegistry.current();
        rows.put("Compute strategies", strategyList(creg));
        rows.put("Managed-pool clouds", cloudList());
        rows.put("OS keychain", keychainStatus());
        rows.put("k8s.enabled", boolFlag("k8s.enabled"));
        rows.put("Labs k8s enabled", boolFlag("labs.k8s.enabled"));
        rows.put("Port-forward probe (s)", System.getProperty(
                "labs.k8s.portforward.health_probe_seconds", "10"));
        return rows;
    }

    private static String versionFromManifest() {
        String impl = AboutDialog.class.getPackage().getImplementationVersion();
        if (impl != null && !impl.isBlank()) return impl;
        // Fallback: read MANIFEST.MF directly so a dev gradle run still
        // shows something meaningful.
        try {
            var url = AboutDialog.class.getProtectionDomain()
                    .getCodeSource().getLocation();
            if (url != null) {
                try (var in = url.openStream()) {
                    Manifest mf = new Manifest(in);
                    String v = mf.getMainAttributes().getValue("Implementation-Version");
                    if (v != null) return v;
                }
            }
        } catch (IOException ignored) { /* fall through */ }
        return "2.8.4-alpha";
    }

    private static String buildDateFromManifest() {
        String d = AboutDialog.class.getPackage().getImplementationVendor();
        return d == null ? "" : d;
    }

    private static String strategyList(ComputeStrategyRegistry reg) {
        StringBuilder sb = new StringBuilder();
        for (StrategyId id : StrategyId.values()) {
            if (sb.length() > 0) sb.append(", ");
            sb.append(id.name().toLowerCase());
            if (!reg.isShipped(id)) sb.append("*");
        }
        sb.append("   (* = locked in this release)");
        return sb.toString();
    }

    private static String cloudList() {
        ManagedPoolAdapterRegistry reg = ManagedPoolAdapterRegistry.defaultRegistry();
        StringBuilder sb = new StringBuilder();
        for (var p : com.kubrik.mex.k8s.compute.managedpool.CloudProvider.values()) {
            if (sb.length() > 0) sb.append(", ");
            Optional<?> a = reg.lookup(p);
            sb.append(p.name()).append("=").append(a.isPresent() ? "stub" : "absent");
        }
        return sb.toString();
    }

    /** Cached so the dialog doesn't re-spawn the `which` / `where`
     *  subprocesses every time it's opened (they probe PATH for
     *  security / cmdkey / secret-tool — ~5-15 ms × 3 platforms on
     *  the FX thread). Computed once on first dialog open and
     *  reused thereafter. */
    private static volatile String cachedKeychainStatus;

    private static String keychainStatus() {
        String cached = cachedKeychainStatus;
        if (cached != null) return cached;
        OsKeychainSecretStore store = new OsKeychainSecretStore();
        String result = store.isAvailable() ? "available"
                : "unavailable (using in-memory fallback)";
        cachedKeychainStatus = result;
        return result;
    }

    private static String boolFlag(String key) {
        return Boolean.parseBoolean(System.getProperty(key, "false"))
                ? "on" : "off";
    }
}

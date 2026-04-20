package com.kubrik.mex.monitoring.alerting;

import com.kubrik.mex.events.EventBus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fans fired / cleared alerts out to the user. Primary channel is the in-app
 * banner — subscribers attach via {@link EventBus#onAlertFired} /
 * {@link EventBus#onAlertCleared}. A best-effort OS notification via
 * {@link java.awt.SystemTray} fires only when a tray icon has been installed
 * by the host app; otherwise it silently no-ops. A richer native-notification
 * layer (macOS Notification Center, Windows Toast, libnotify) is deferred.
 */
public final class Notifier {

    private static final Logger log = LoggerFactory.getLogger(Notifier.class);

    private final EventBus bus;

    public Notifier(EventBus bus) { this.bus = bus; }

    public void onFired(AlertEvent e) {
        bus.publishAlertFired(e);
        bus.publishLog(e.connectionId(), "ALERT fired " + e.severity() + " " + e.message());
        trySystemTrayToast(e.severity().name() + " — " + e.message());
    }

    public void onCleared(AlertEvent e) {
        bus.publishAlertCleared(e);
        bus.publishLog(e.connectionId(), "ALERT cleared " + e.message());
    }

    private void trySystemTrayToast(String msg) {
        try {
            if (!java.awt.GraphicsEnvironment.isHeadless() && java.awt.SystemTray.isSupported()) {
                java.awt.SystemTray tray = java.awt.SystemTray.getSystemTray();
                java.awt.TrayIcon[] icons = tray.getTrayIcons();
                if (icons.length > 0) {
                    icons[0].displayMessage("Mongo Explorer", msg, java.awt.TrayIcon.MessageType.WARNING);
                }
            }
        } catch (Throwable t) {
            log.debug("SystemTray notification failed", t);
        }
    }
}

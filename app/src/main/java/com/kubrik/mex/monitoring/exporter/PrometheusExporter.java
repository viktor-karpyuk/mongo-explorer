package com.kubrik.mex.monitoring.exporter;

import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;

/**
 * JDK-built-in {@link HttpServer} serving Prometheus text-format at {@code /metrics}.
 * Binds to loopback by default. Disabled by default; {@link #start} is explicitly
 * called by {@code MonitoringService} when the user toggles the exporter on.
 */
public final class PrometheusExporter implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(PrometheusExporter.class);
    public static final int DEFAULT_PORT = 9913;

    private final MetricRegistry registry;
    private HttpServer server;

    public PrometheusExporter(MetricRegistry registry) { this.registry = registry; }

    public synchronized void start(String bind, int port) throws IOException {
        if (server != null) return;
        server = HttpServer.create(new InetSocketAddress(bind, port), 0);
        server.createContext("/metrics", ex -> {
            String body = PrometheusFormat.render(registry.snapshot());
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().add("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
            ex.sendResponseHeaders(200, bytes.length);
            ex.getResponseBody().write(bytes);
            ex.close();
        });
        server.setExecutor(Executors.newFixedThreadPool(4, r -> {
            Thread t = new Thread(r, "mex-mon-prom");
            t.setDaemon(true);
            return t;
        }));
        server.start();
        log.info("Prometheus exporter started on {}:{}", bind, port);
    }

    public synchronized boolean isRunning() { return server != null; }

    @Override
    public synchronized void close() {
        if (server != null) {
            server.stop(0);
            server = null;
        }
    }
}

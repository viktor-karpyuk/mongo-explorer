package com.kubrik.mex.migration.sink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

/** Startup-time scanner that walks a plugin directory, spins up a {@link URLClassLoader}
 *  over every {@code *.jar} found, and registers every {@link MigrationSinkFactory}
 *  declared via {@link java.util.ServiceLoader} (EXT-1).
 *  <p>
 *  The classloader uses the app classloader as its parent so plugin factories can reference
 *  {@code MigrationSink}, {@code MigrationSinkFactory}, {@code SinkSpec} without bundling
 *  them. Plugins are loaded-at-startup only — no hot reload in v2.0.0. */
public final class PluginLoader {

    private static final Logger log = LoggerFactory.getLogger(PluginLoader.class);

    private PluginLoader() {}

    /** Scan {@code pluginsDir} and register every sink factory found. Silently skips the
     *  call if the directory doesn't exist — that's the common case for a fresh install.
     *  <p>
     *  Returns the resolved classloader so callers (e.g. tests) can dispose of it; in
     *  production the loader lives for the JVM's lifetime. */
    public static URLClassLoader loadFrom(Path pluginsDir) {
        if (!Files.isDirectory(pluginsDir)) {
            log.debug("plugins directory not present: {}", pluginsDir);
            return null;
        }

        List<URL> jars = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(pluginsDir, "*.jar")) {
            for (Path jar : stream) {
                try {
                    jars.add(jar.toUri().toURL());
                } catch (Exception e) {
                    log.warn("skipping unreadable plugin JAR {}: {}", jar, e.getMessage());
                }
            }
        } catch (Exception e) {
            log.warn("cannot list plugins directory {}: {}", pluginsDir, e.getMessage());
            return null;
        }

        if (jars.isEmpty()) {
            log.debug("no plugin JARs found in {}", pluginsDir);
            return null;
        }

        URLClassLoader classLoader = new URLClassLoader(
                "mex-plugins", jars.toArray(URL[]::new),
                PluginLoader.class.getClassLoader());

        int registered = 0;
        for (MigrationSinkFactory factory : ServiceLoader.load(MigrationSinkFactory.class, classLoader)) {
            try {
                PluginSinkRegistry.register(factory);
                registered++;
            } catch (Throwable t) {
                log.warn("plugin factory {} failed to register: {}",
                        factory.getClass().getName(), t.getMessage());
            }
        }
        log.info("loaded {} plugin JAR(s) from {}, registered {} sink factor{}",
                jars.size(), pluginsDir, registered, registered == 1 ? "y" : "ies");
        return classLoader;
    }
}

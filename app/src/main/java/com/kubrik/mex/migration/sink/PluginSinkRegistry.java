package com.kubrik.mex.migration.sink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** In-memory registry of plugin-provided {@link MigrationSinkFactory} instances (EXT-1).
 *  <p>
 *  The registry is populated at startup by {@link PluginLoader#loadFrom}. It is a static
 *  singleton because plugin discovery is an app-level concern — there is only ever one
 *  plugins folder and one app classloader graph; threading the registry through every
 *  engine constructor would add plumbing without value. Tests can reset the registry via
 *  {@link #clearForTesting()}. */
public final class PluginSinkRegistry {

    private static final Logger log = LoggerFactory.getLogger(PluginSinkRegistry.class);
    private static final Map<String, MigrationSinkFactory> factories = new ConcurrentHashMap<>();

    private PluginSinkRegistry() {}

    /** Add a factory under its declared {@link MigrationSinkFactory#name()}. Re-registering
     *  the same name overwrites silently — the loader is expected to iterate a stable order
     *  so this only happens on an explicit reload. */
    public static void register(MigrationSinkFactory factory) {
        if (factory == null || factory.name() == null || factory.name().isBlank()) {
            log.warn("ignoring plugin sink factory with null/blank name: {}", factory);
            return;
        }
        factories.put(factory.name(), factory);
        log.info("registered plugin sink factory: {} ({})",
                factory.name(), factory.getClass().getName());
    }

    /** Look up a previously-registered factory by name. {@code null} means no plugin with
     *  that name is available — callers surface this as a preflight error. */
    public static MigrationSinkFactory resolve(String name) {
        return name == null ? null : factories.get(name);
    }

    /** Snapshot of all registered plugin names. Used by the Wizard / preflight to tell the
     *  user which plugins are available without touching the factory instances directly. */
    public static Set<String> registered() {
        return Set.copyOf(factories.keySet());
    }

    /** Empties the registry. Tests only — do not call from production paths. */
    public static void clearForTesting() {
        factories.clear();
    }
}

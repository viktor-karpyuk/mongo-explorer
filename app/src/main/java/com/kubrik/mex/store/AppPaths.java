package com.kubrik.mex.store;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;

public final class AppPaths {
    private AppPaths() {}

    public static Path dataDir() {
        String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        String home = System.getProperty("user.home");
        if (os.contains("mac")) {
            return Paths.get(home, "Library", "Application Support", "MongoExplorer");
        }
        if (os.contains("win")) {
            String local = System.getenv("LOCALAPPDATA");
            return Paths.get(local != null ? local : home, "MongoExplorer");
        }
        String xdg = System.getenv("XDG_DATA_HOME");
        return Paths.get(xdg != null ? xdg : home + "/.local/share", "mongo-explorer");
    }

    public static Path databaseFile() { return dataDir().resolve("data.db"); }
    public static Path keyFile() { return dataDir().resolve("secret.key"); }

    // Migration feature (v1.1.0) — see docs/mvp-technical-spec.md §14
    public static Path migrationJobsDir() { return dataDir().resolve("jobs"); }
    public static Path migrationJobDir(String jobId) { return migrationJobsDir().resolve(jobId); }
    public static Path migrationLogsDir() { return dataDir().resolve("logs"); }

    /** EXT-1 — location scanned for user-supplied plugin JARs (sink / source / transform). */
    public static Path pluginsDir() { return dataDir().resolve("plugins"); }
}

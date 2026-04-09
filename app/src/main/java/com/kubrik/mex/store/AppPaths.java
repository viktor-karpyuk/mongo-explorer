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
}

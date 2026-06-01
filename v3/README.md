# Mongo Explorer v3

Kotlin 2.0 + Compose Multiplatform 1.7 desktop app on JDK 21.

Status: **0.1.0-alpha** — Phase A scaffold.

## Layout

```
v3/
├── build.gradle.kts          # subproject of mongo-explorer
├── FEATURES.md               # locked feature cut
└── src/main/kotlin/io/mex/
    ├── Main.kt               # window entrypoint
    └── ui/
        ├── App.kt            # root composable
        └── theme/MexTheme.kt # Material 3 colors
```

## Develop

From the repo root:

```bash
./gradlew :v3:run            # launches the desktop window
./gradlew :v3:packageDmg     # builds the DMG (macOS)
./gradlew :v3:packageMsi     # Windows installer
./gradlew :v3:packageDeb     # Linux .deb
./gradlew :v3:build          # compile + tests
```

## Architecture decisions

- **JVM-only.** Pure Kotlin, no native code, no Electron. Cold start is dominated
  by JVM warmup, mitigated by AOT class data sharing in the jpackage bundle.
- **Sync MongoDB driver.** Simpler than the coroutine driver and Compose handles
  the threading via `LaunchedEffect` + `Dispatchers.IO`.
- **sqlite-jdbc in WAL mode** with foreign keys for the local store
  (connections, history, prefs, migration jobs).
- **Master key crypto.** URIs encrypt with AES-256-GCM under a key persisted
  at `~/.mex-v3/master.key` (mode 0600). No external keychain dependency.

## Phase plan

See `FEATURES.md` for the locked scope. v3 phases A → O are tracked in the
session task list.

# Mongo Explorer

A lightweight, native MongoDB GUI client built with Java 21 and JavaFX. Inspired by Studio 3T, it provides a familiar desktop experience for browsing, querying, and managing MongoDB databases without browser dependencies or Electron overhead.

## Features

### Connection Management
- **Manual or URI mode** — configure host/port/auth step by step, or paste a full `mongodb://` / `mongodb+srv://` connection string
- **SCRAM authentication** with encrypted password storage (AES-GCM, per-install key)
- **Multiple concurrent connections** with live status indicators (connected / connecting / error / disconnected)
- **Test connection** before saving
- **Duplicate, edit, delete** connections from the tree or the Manage Connections table

### Database & Collection Browser
- **Connection tree** (left sidebar) — expand connections into databases and collections, lazy-loaded
- **Right-click context menus** — create/drop databases, create/rename/drop collections, run arbitrary commands
- **Database and collection stats** (`dbStats` / `collStats`) shown in the explorer detail pane

### Query Editor
- **Find tab** — filter, projection, sort, skip, limit, maxTimeMs with resizable input areas
- **Aggregation tab** — stage-by-stage pipeline editor with operator picker (24 stages), per-stage enable/disable, reorder, run-to-here, and 10 ready-made sample pipelines
- **Run button** at the top of every collection tab, plus `Cmd/Ctrl+Enter` shortcut
- **Pagination** — prev/next with row range indicator, driven by actual `hasMore` detection
- **Results in three views**: Table (SQL-style rows with auto-sampled columns), Tree (expandable nested documents), JSON (syntax-highlighted with `JsonCodeArea`)
- **Document operations** — insert, edit (resizable dialog with JSON syntax highlighting, live validation, format button), delete with confirmation
- **Export** results to JSON file
- **Query history** per connection with one-click reload

### Index Management
- **List indexes** with all properties (unique, sparse, TTL, partial filter, background)
- **Create indexes** with full options: keys, name, unique, sparse, TTL (expireAfterSeconds), partial filter expression, background build
- **Drop indexes** with typed-name confirmation

### User Management
- **List users** per database with roles and auth mechanisms
- **Create users** with built-in role checkboxes (read, readWrite, dbAdmin, root, etc.) or custom role JSON
- **Change password, grant/revoke roles, drop users**

### Cluster Operations (v2.4.0)
- **Per-connection Cluster tab** — Topology, Ops, Balancer (sharded only), Oplog, Audit, and Pools sub-tabs. `Cmd/Ctrl+Alt+C` opens the tab for the tree-selected connection.
- **Topology** — priority-ordered member cards, state icons for primary / secondary / arbiter / down, lag & ping, sharded decomposition into Shards / Config servers / Mongos. Read-only `rs.conf` viewer with copy + export.
- **Health pill** on every Monitoring connection card — 0..100 score with tooltip listing contributing negatives.
- **Three-gate safety model** — every destructive action (step-down, freeze, killOp, balancer start / stop / window, moveChunk, addTagRange, removeTagRange) requires (1) a role probe, (2) a dry-run preview with SHA-256 hash, (3) a typed confirmation on the target's identity. Paste is allowed but flagged in the audit row.
- **Kill-switch** — top-level toggle that blocks every destructive dispatcher; cancellations still audit.
- **Live ops** — 2 s `$currentOp` with filter chips; `killOp` from the row kebab (role-gated). Lock analytics heat-bar + top holders. Connection-pool viewer with amber highlighting on `waitQueue > 0`.
- **Oplog** — size / used / window gauge with colour bands (< 6 h red, < 24 h amber, ≥ 24 h green), filterable tail with pause/resume, streamed JSON Lines export.
- **Replica-set admin** — Step down (60 s default), Freeze / Unfreeze (30 s / 60 s / 5 min / 15 min / Unfreeze choices), preview-only priority / votes editor.
- **Sharding** — balancer status + start / stop / activeWindow controls, chunk distribution histogram across shards, zones + tag ranges with Add / Remove dialogs, `moveChunk` with majority write concern.
- **Audit trail** — `ops_audit` pane per connection with live tail, filters, detail drawer showing the full command JSON + preview hash. JSON bundle export with per-row SHA-256, CSV export. Daily 03:00 retention janitor keeps `FAIL` rows and root / clusterAdmin rows exempt.

### Data Migration (v1.2.0)
- **Multi-database scope** — one job can target N ≥ 1 source databases; Step 3 picks them with a checkbox list
- **Cross-database collection selection** — each row in the collection picker is `db.coll`, spanning every ticked DB
- **Opt-in Migrate Users** — copies non-built-in users + roles to the matching target DB after documents and indexes (passwords need to be re-set post-migration — public `createUser` can't carry hashes)
- **Migrations tab** — colored status pill, resolved source/target names, Started + Duration columns with wall/active/paused tooltip, double-click to open a details view with live progress, log tail, and Resume
- **Status-bar pill** — always visible while any migration is running; clicking it reopens the job's live view
- **Real-time progress** — fixed 200 ms publish cadence with per-batch counters and a UI frame-coalescing drain so counters tick smoothly on large copies
- **Crash recovery** — jobs orphaned by a killed JVM are reconciled to FAILED on startup; heartbeat-based detection catches PID reuse after a reboot

### UX
- **Welcome screen** with connection cards, quick actions (new connection, manage, logs)
- **Native menu bar** (File, Edit, View, Connection, Help) with keyboard shortcuts
- **Connection log** — live tail of connect/disconnect/error events with timestamps
- **Green database app icon** with native macOS `.icns`

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Java 21 (records, sealed types, virtual threads) |
| UI | JavaFX 21 + AtlantaFX (PrimerLight theme) |
| Icons | Ikonli (Feather icon pack) |
| Code editor | RichTextFX (JSON syntax highlighting) |
| Database driver | MongoDB Java Sync Driver 5.x |
| Local storage | SQLite (WAL mode, foreign keys) |
| Encryption | AES-256-GCM (javax.crypto) |
| Build | Gradle (Kotlin DSL) |
| Packaging | jpackage (DMG / MSI / DEB) |

## Requirements

- **JDK 21+** (Temurin recommended)
- **MongoDB 5.0+** (tested with 6.x and 7.x)

## Build & Run

```bash
# Run from source
./gradlew :app:run

# Package as native macOS app
rm -rf app/build/jpackage && ./gradlew jpackage
# Output: app/build/jpackage/Mongo Explorer-1.0.0.dmg
```

## Project Structure

```
com.kubrik.mex/
├── core/        ConnectionManager, MongoService, QueryRunner, Crypto, ConnectionUriBuilder
├── model/       MongoConnection, ConnectionState, QueryRequest, QueryResult (records)
├── store/       Database (SQLite), ConnectionStore, HistoryStore, AppPaths
├── events/      EventBus (state + log + topology + ops-audit broadcasts)
├── cluster/     v2.4 — ClusterTopologyService, RoleProbeService, OpsExecutor,
│                BalancerService, ChunkService, OplogService, ZonesService,
│                AuditJanitor, KillSwitch, DryRunRenderer, TypedConfirmDialog
├── ui/          MainView, ConnectionTree, QueryView, ResultsPane, AggregationView,
│                ConnectionEditDialog, IndexDialog, UserManagementDialog, WelcomeView,
│                LogsView, JsonCodeArea, UiHelpers
│   └── cluster/ ClusterTab, TopologyPane, CurrentOpPane, LockInfoPane, ConnPoolPane,
│                OplogPane, AuditPane, BalancerPane, ChunkDistributionPane, ZonesPane
├── Main.java
└── Launcher.java
```

## License

Private project.

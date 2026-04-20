# Changelog

## v2.4.0 — Cluster operations & topology

v2.4 turns Mongo Explorer into a first-class surface for MongoDB DBAs. Every destructive action flows through a three-gate safety model (role probe → dry-run preview → typed confirmation) and leaves an audit trail.

### Cluster tab (`UI-OPS-*`)
- **Per-connection Cluster tab** — right-click any connected cluster ▸ *Open cluster view…* or use `Cmd/Ctrl+Alt+C`. Sub-tabs: **Topology**, **Ops**, **Balancer** (sharded only), **Oplog**, **Audit**, **Pools**.
- **Health pill on the Monitoring card** — 0..100 score from `HealthScorer` with tooltip listing the contributing negatives (no primary, lag over threshold, unreachable shard, etc.).

### Topology (`TOPO-*`, `RS-*`)
- **`ClusterTopologyService`** samples `hello` / `replSetGetStatus` / `listShards` / `getShardMap` / `config.mongos` on a 300 ms heartbeat coalesced into a 2 s visible refresh, with a 5 s per-command timeout and exponential back-off. Snapshots are emitted on `EventBus.onTopology` (late subscribers get a replay) and persisted with `sha256` de-duplication.
- **Member map** — priority-ordered cards with state icon, lag / ping / sync-from, priority / votes / uptime, configVersion. Sharded clusters decompose into Shards, Config servers, and Mongos sections.
- **`rs.conf` viewer** — JSON pane with copy + export, opened from the Topology header.
- **Replica-set admin** — right-click the primary for *Step down…* (defaults 60 s / 10 s catch-up), right-click a secondary for *Freeze / unfreeze…* (30 s / 60 s / 5 min / 15 min / Unfreeze). Preview-only priority / votes editor renders the resulting `rs.reconfig` JSON with version bump; *Execute* is disabled in v2.4 (lands with v2.7 guided reconfig).

### Live operations (`OP-*`, `LOCK-*`, `POOL-*`)
- **currentOp viewer** — 2 s polled `$currentOp({allUsers: true})` with filters for namespace regex, op-type chips, secs-running threshold, user, and planSummary contains. Pause toggle freezes the view without stopping the poll.
- **killOp** — right-click ▸ *Kill op* opens the typed-confirm flow; disabled unless the cached role set includes `killAnyCursor` / `root`.
- **Lock analytics** — heat-bar per resource (width ∝ holder count, colour ∝ waiter ratio), top 5 holders clickable; graceful "not supported" card on MongoDB < 3.6.
- **Connection pools** — `connPoolStats` every 10 s; rows with `waitQueueSize > 0` render on an amber background; footer shows `totalInUse / totalAvailable / totalCreated` + a queued-count indicator.

### Oplog (`OPLOG-*`)
- **Gauge** — size / used / window hours with colour bands at `< 6 h red`, `< 24 h amber`, `≥ 24 h green`.
- **Tail** — newest-first list with `$natural -1` sort, server-side ns regex + op-type filters, pause toggle.
- **Export** — streamed JSON Lines writer that safely handles > 10 000 rows.

### Sharding (`SHARD-*`)
- **Balancer controls** — status pill (off / idle / running), 24 h chunk-move counter, active-migration count; *Start / Stop* buttons + *Set window…* (HH:MM UTC upsert on `config.settings.balancer`).
- **Chunk distribution** — per-collection table across shards with a compact histogram + jumbo flag; coalesces pre-5.0 ns-keyed chunks and 5.0+ uuid-keyed chunks via `config.collections` resolution.
- **moveChunk** — dialog captures ns + min / max bounds + target shard (populated from `listShards`); dispatch uses write concern `majority`.
- **Zones + tag ranges** — chip grid of shard → zones; table of tag ranges with Add / Remove dialogs that dispatch `{updateZoneKeyRange: ns, min, max, zone}` (zone = `null` for removal).

### Safety, audit, retention (`SAFE-OPS-*`, `ROLE-*`, `AUD-*`)
- **Role probe** — `connectionStatus({showPrivileges: true})` cached 5 min per connection with on-demand refresh + invalidation on role-related failures.
- **Dry-run renderer** — canonical JSON + SHA-256 hash for every destructive command; the hash is shown on the confirm dialog and persisted into `ops_audit.preview_hash`.
- **Typed-confirm dialog** — *Execute* stays disabled until the input trim-matches the target string; paste is allowed but flagged in the audit row.
- **Kill-switch** — process-wide toggle; every destructive dispatcher checks it before sending. Engaged dispatches audit as `CANCELLED` with `kill_switch = true`.
- **`ops_audit` table** — every destructive call (cancel, deny, OK) appends a row: connection, command, redacted JSON, preview hash, outcome, server message, role used, latency, caller host/user, paste flag, kill-switch flag, ui source.
- **Audit pane** — live-tailed table per connection with command / outcome / search filters and a detail drawer that shows the full command JSON + preview hash. JSON bundle export with per-row SHA-256 digest + CSV export with proper escaping.
- **Retention janitor** — daily 03:00 local sweep of `ops_audit`; rows older than 180 days are purged except `outcome = FAIL` and rows whose `role_used` is `root` or `clusterAdmin`, which stay until the user clears them.
- **Cascade delete** — deleting a connection purges `ops_audit`, `topology_snapshots`, and `role_cache` rows in the same SQLite transaction.

### Hardening (`Q2.4-J`)
- `DestructiveActionFuzz` — 108 scenarios across every Command variant × six role shapes × kill-switch on/off, asserting every dispatch produces an audit row with a valid `preview_hash`.
- `CascadeDeleteIT` — verifies the connection-delete transaction purges the v2.4 tables without touching neighbours.
- `TopologyServiceIT` — live `mongo:7.0` replset validates kind detection, primary presence, sha256 stability, and `EventBus.onTopology` fan-out.

### Schema additions
- `topology_snapshots (connection_id, captured_at, cluster_kind, snapshot_json, sha256, ...)` with unique constraint on `(connection_id, captured_at, sha256)`.
- `ops_audit (connection_id, command_name, command_json_redacted, preview_hash, outcome, role_used, paste, kill_switch, …)` with indexes on `(connection_id, started_at)`, `(command_name, started_at)`, and `(outcome, started_at)`.
- `role_cache (connection_id PRIMARY KEY, roles_json, probed_at)`.

All three tables are purely additive — v2.4 rolls back cleanly to v2.3.x (the new tables simply remain unread).

## v1.2.0 — Migration UX & history upgrade

### Migration scope
- **Multi-database migrations.** Step 3 now picks N ≥ 1 source databases with a checkbox-per-row list; a job covers every selected DB in one run (SCOPE-10).
- **Cross-database namespace selection.** On the "Selected collections" radio, the list spans every ticked DB and each row shows `db.coll` (SCOPE-11).
- **Opt-in Migrate Users.** A tail stage copies non-built-in users + roles to the matching target DB after documents and indexes (SCOPE-12). Passwords cannot be preserved via the public `createUser` command — copied users get a placeholder password and must re-set.

### Migrations tab
- Colored **status pill** per job (UX-10) and **resolved source/target names** that follow connection renames and gracefully handle deleted connections (UX-11).
- **Job Details view** — double-click a row for header, spec summary, live-or-replay progress, log tail, and Resume button (UX-12).
- **Status-bar pill** that stays visible while any job runs, clickable to reopen its live view (UX-13).
- **Started** and **Duration** columns with a wall / active / paused tooltip breakdown (OBS-7).

### Wizard
- Step 1 / Step 2 render each connection with a **colored status dot** that live-updates as the connection's state changes (UX-9).
- Closing the wizard while a job is running no longer kills it — a toast points to the status bar and Migrations tab.

### Engine + observability
- **200 ms publish cadence** for progress snapshots with per-batch counter updates and a UI frame-coalescing pulse drain — counters tick smoothly rather than in chunks (OBS-6). DAO writes stay on a ~2 s throttle to avoid SQLite thrash.
- **Dry-run docs counter fix.** `docsProcessed` is now tracked separately from `docsCopied`; the "Docs" column binds to whichever matches the execution mode (OBS-5).
- `Metrics` counters migrated to `LongAdder` for lower contention under high-partition concurrency.

### Fixes
- **BUG-1: stuck RUNNING status.** Non-terminal rows are reconciled to FAILED on application startup when the owning JVM is gone (foreign PID) or the heartbeat is older than 60 s — closes the PID-reuse-after-reboot hole. `JobRunner` writes a heartbeat every flush tick; terminal writes are guaranteed in a `finally` block.
- **BUG-2: wizard "Next" gating.** Step 3's validator now correctly depends on the multi-DB / namespace selection, with radio/DB changes clearing stale selections.

### Under the hood
- `ScopeSpec.Database(String)` → `ScopeSpec.Databases(List<String>)`. Legacy profiles round-trip cleanly via a `ProfileCodec` v1 → v2 shim: flat `migrateIndexes` → nested `ScopeFlags`, dotted namespaces → `{db, coll}` records.
- New `migration_collection_timings` table for per-collection start/end (OBS-7).
- New `owner_pid`, `last_heartbeat_at`, `source_connection_name`, `target_connection_name`, `docs_processed`, `active_millis` columns on `migration_jobs` (forward-only schema migration).

## v1.0.0 — Initial Release

### Connection Management
- Manual and URI-based MongoDB connection configuration (mongodb:// and mongodb+srv://)
- SCRAM authentication with AES-256-GCM encrypted password storage
- Multiple concurrent connections with live status indicators
- Connection test and validation before saving
- Duplicate, edit, and delete operations on saved connections

### Database & Collection Browser
- Expandable connection tree with lazy-loaded database and collection hierarchy
- Right-click context menus for creating and dropping databases and collections
- Database and collection statistics display (dbStats / collStats)

### Query Editor
- Find tab with filter, projection, sort, skip, limit, and maxTimeMs inputs
- Aggregation tab with 24-stage pipeline builder, operator picker, per-stage enable/disable, reordering, run-to-here, and 10 sample pipelines
- Keyboard shortcut (Cmd/Ctrl+Enter) for query execution

### Results Management
- Three view modes: Table (SQL-style), Tree (expandable documents), JSON (syntax-highlighted)
- Smart pagination with hasMore detection
- Document operations: insert, edit (with JSON validation and formatting), delete
- JSON export functionality
- Per-connection query history with one-click reload

### Index Management
- List, create, and drop indexes
- Full index options: unique, sparse, TTL, partial filters, background builds

### User Management
- List users per database with roles and authentication mechanisms
- Create users with role checkboxes or custom JSON
- Change passwords, grant/revoke roles, drop users

### UX
- Welcome screen with connection cards and quick actions
- Native menu bar with keyboard shortcuts
- Live connection log with timestamps
- Native macOS app icon

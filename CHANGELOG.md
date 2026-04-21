# Changelog

## v2.5.0 — Backup, Restore & Disaster Recovery

v2.5 gives every DBA a credible, rehearsable DR story inside Mongo Explorer. Define a backup policy per cluster, run ad-hoc or scheduled `mongodump` backups to a local sink, catalog + verify the artefacts, and restore to a point in time — all with the three-gate safety model + `ops_audit` trail from v2.4.

### Policies + sinks (`BKP-POLICY-*`, `STG-*`)
- **New `Backups` top-level tab** with Policies + History sub-tabs. `Cmd/Ctrl+Alt+B` opens it.
- **Policy editor** — name (1–64 chars, `[\w .-]+`), cron schedule (blank = manual-only), scope (WholeCluster / Databases / Namespaces sealed choice), archive (gzip + level 1–9 + output-dir template), retention (maxCount 1–1000 × maxAgeDays 1–3650 — tighter bound wins), sink dropdown, enabled + oplog flags. Live validation via `PolicyValidator`; Save disabled until the record is valid.
- **Storage sinks** — `LocalFsTarget` ships end-to-end (1 KB `testWrite()` probe, put/get/list/stat/delete, path-escape guard). Cloud sinks (S3 / GCS / Azure / SFTP) scaffolded in the sealed `StorageTarget` permit list; real impls defer to v2.5.1 once SDK deps are pinned.
- **`SinkDao`** — credentials encrypted via `Crypto` AES-GCM; plain-text tokens never touch SQLite.

### Backup runner (`BKP-RUN-*`)
- **`MongodumpRunner`** — subprocess wrapper (argv builder with URI redaction, `RunLog` 1000-line bounded ring buffer, SIGTERM→SIGKILL watchdog after 10 s, tqdm-style progress parsing).
- **`BackupRunner`** orchestrator — inserts RUNNING catalog row + writes `backup.start` audit row + publishes `BackupEvent.Started` → spawns mongodump → walks output dir, streams every file through `FileHasher` (64 KB buffer, GB-safe) → builds a canonical `BackupManifest` with footer SHA-256 → batch-inserts `backup_files` rows transactionally → finalises the catalog row + publishes `BackupEvent.Ended`.
- **`BackupScheduler`** — 60 s tick walks every enabled policy; cron matches invoke an injected dispatcher once per minute (deduped), with a concurrency gate so in-flight policies skip. On startup, flips orphan RUNNING rows to FAILED and back-fills MISSED rows for cron firings within the last 24 h. `BackupStatus.MISSED` enum value added.

### Catalog + verifier (`BKP-VER-*`)
- **`backup_catalog`** + **`backup_files`** tables with foreign-key cascade from policies → catalog (`ON DELETE SET NULL`) and catalog → files (`ON DELETE CASCADE`). Indexes on `(connection_id, started_at)`, `(policy_id, started_at)`, and `(catalog_id)`.
- **`CatalogVerifier`** — three gates: manifest file present, manifest bytes hash back to the stored `manifest_sha256`, every file row round-trips byte-equal. Outcomes: `VERIFIED`, `MANIFEST_MISSING`, `MANIFEST_TAMPERED`, `FILE_MISSING`, `FILE_MISMATCH`, `IO_ERROR`.
- **`ArtefactExplorerDialog`** — opens on history-row double-click, lists every artefact file (path / bytes / kind / sha256 prefix), and runs `Verify now` on-demand. Verdict badge + problem list render inline.

### Restore (`BKP-RESTORE-*`)
- **`MongorestoreRunner`** — mirror of the dump runner (argv builder with `--uri / --dir / --gzip / --oplogReplay / --dryRun / --drop / --nsFrom + --nsTo`, progress parser for three stderr shapes including failure counts, SIGTERM/SIGKILL watchdog).
- **`RestoreService`** — two explicit modes. `REHEARSE` rewrites every namespace via `*.* → <prefix>*.*` and forces `--dryRun`; never checks the kill-switch. `EXECUTE` refuses when the kill-switch is engaged; caller gathers a typed-confirm first. Every outcome publishes `RestoreEvent` (Started / Progress / Ended) + writes an `ops_audit` row with `command_name = restore.rehearse` / `restore.execute`.
- **`RestoreWizardDialog`** — target URI + mode radio + sandbox prefix + drop + oplog-replay; Execute mode pops a typed-confirm on the backup's sinkPath; live progress panel with colour-coded verdict.

### PITR (`BKP-PITR-*`)
- **`BackupRunner` captures the oplog slice** — `oplog_first_ts` / `oplog_last_ts` populated from run bounds (second precision, sufficient for PITR planning).
- **`PitrPlanner`** — given a target epoch-seconds + connectionId, returns a `RestorePlan` with the most-recent OK backup whose oplog window covers the target; refusal messages distinguish "older than earliest oplog", "newer than latest oplog", "gap between windows".
- **`PitrPickerDialog`** — UTC date/time picker + planner verdict + `Restore to this point…` handoff to the restore wizard.

### DR rehearsal report (`BKP-DR-*`)
- **`DrRehearsalReport`** — compiles every `restore.rehearse` audit row in a window into a bundle (OK / FAIL / CANCELLED counts + per-row list). `writeJson` streams a self-describing payload; `writeHtml` emits an email-safe single-file HTML report with status pills + a sortable table.
- **`BackupHistoryPane` toolbar button** `Rehearsal report…` — FileChooser (HTML / JSON), 30-day window, writes on a virtual thread.

### Audit + events
- **`EventBus.onBackup`** — BackupEvent sealed trio (Started / Progress / Ended); no replay, historical runs read from `BackupCatalogDao`.
- **`EventBus.onRestore`** — RestoreEvent sealed trio with the same shape.
- **`ops_audit` reuse** — every backup / restore phase writes a row (`backup.start` / `backup.end` / `restore.rehearse` / `restore.execute`); `ui_source = "backup.runner"` / `"backup.restore"` so the v2.4 audit pane filters cleanly.

### Schema additions
All additive via `Database.migrate()`:

- `storage_sinks (id, kind, name, root_path, credentials_enc, extras_json, ...)`.
- `backup_policies (id, connection_id, name, enabled, schedule_cron, scope_json, archive_json, retention_json, sink_id, include_oplog, ...)` with unique `(connection_id, name)` + index on `(connection_id, enabled)`.
- `backup_catalog (id, policy_id → backup_policies, connection_id, started_at, finished_at, status, sink_id, sink_path, manifest_sha256, total_bytes, doc_count, oplog_first_ts, oplog_last_ts, verified_at, verify_outcome, notes)`.
- `backup_files (id, catalog_id → backup_catalog CASCADE, relative_path, bytes, sha256, db, coll, kind)`.

v2.5 rolls back cleanly to v2.4.x — the new tables simply remain unread.

### Known gaps (deferred to v2.5.1)
- **Cloud sinks** — S3 / GCS / Azure / SFTP permit-list entries exist and can be persisted via `SinkDao`, but every I/O call throws `CloudSinkUnavailableException`. Real SDK integrations (AWS SDK, google-cloud-storage, azure-storage-blob, JSch) land with v2.5.1.
- **`--oplogLimit` in the restore wizard** — `PitrPlanner` returns an oplog limit timestamp; the wizard hands off the source backup but does not yet thread the limit into mongorestore's argv.
- **Multi-DB / multi-namespace backup fan-out** — `MongodumpCommandBuilder` emits the first entry only for `Databases(N>1)` and `Namespaces(N>1)` scopes; looping per-entry lands with v2.5.1.

## v2.4.1 — Cluster polish + shard depth

Patch release closing four concrete gaps left open in v2.4.0.

### Closed gaps
- **Per-shard `replSetGetStatus` probe** (`Q2.4-A` follow-up). Sharded topology no longer shows shard replset members as grey `UNKNOWN` cards. `ClusterTopologyService` now caches a short-lived `MongoClient` per shard (keyed by `connectionId || rsHostSpec`) and runs `replSetGetStatus` + `replSetGetConfig` against it each sharded tick. A failed probe falls back to the seed-host UNKNOWN members plus a warning banner entry; the stale client is rotated so the next tick retries. Lifecycle is tied to the topology service + per-connection stop, so shutdown closes every peer client cleanly. New `MongoService.openPeerClient(rsHostSpec, timeoutMs)` reuses the parent service's credentials + TLS settings; input-validation coverage in `OpenPeerClientTest`.
- **Kill-switch top-bar UI toggle** (`SAFE-OPS-8`). `KillSwitchPill` in the status bar shows `kill-switch off` / `kill-switch ENGAGED` with a grey / red treatment, subscribes to `KillSwitch.onChange`, and routes engagement through a confirmation alert (per the spec's no-accidents rule); disengagement is one click.
- **Keyboard matrix — `Cmd/Ctrl+Alt+O` + `Cmd/Ctrl+Alt+L`** (`UI-OPS-8`). `+O` opens the Cluster tab for the tree-selected connection, focuses the Ops sub-tab, and presets `secs_running ≥ 10` so a DBA jumps straight to long-running ops. `+L` opens the Logs tab with a new "Audit only" toggle engaged; `LogsView` now subscribes to `EventBus.onOpsAudit` and keeps an in-memory buffer so flipping the toggle off restores the full history.
- **Repo hygiene**. `app/bin/` is now gitignored (was tracked on every IDE / Gradle rebuild); 484 stale `.class` files untracked via `git rm --cached -r app/bin`.

### Still deferred
- Bespoke dark-mode palette + focus-ring audit + colour-blind-safe screenshot matrix (`Q2.4-I` tail).
- 72 h production-shape soak (`Q2.4-J` tail).
- `rs.reconfig` execution is still preview-only (by design — lands with v2.7 guided reconfig).

## v2.4.0 — Cluster operations & topology

v2.4 turns Mongo Explorer into a first-class surface for MongoDB DBAs. Every destructive action flows through a three-gate safety model (role probe → dry-run preview → typed confirmation) and leaves an audit trail.

### Cluster tab (`UI-OPS-*`)
- **Per-connection Cluster tab** — right-click any connected cluster ▸ *Open cluster view…* or use `Cmd/Ctrl+Alt+C`. Sub-tabs: **Topology**, **Ops**, **Balancer** (visible only when the live topology reports `SHARDED`), **Oplog**, **Audit**, **Pools**. The Balancer sub-tab itself hosts three nested tabs (Controls / Chunks / Zones).
- **Health pill on the Monitoring card** — 0..100 score from `HealthScorer` with tooltip listing the contributing negatives (no primary, lag over threshold, unreachable shard, etc.).

### Topology (`TOPO-*`, `RS-*`)
- **`ClusterTopologyService`** samples `hello` / `replSetGetStatus` / `replSetGetConfig` / `listShards` / `getShardMap` / `config.mongos` on a 300 ms heartbeat coalesced into a 2 s visible refresh, with a 5 s per-command timeout and exponential back-off (2 → 4 → 8 → 16 s cap). Snapshots are emitted on `EventBus.onTopology` (late subscribers receive a replay) and persisted via `TopologySnapshotDao` with `sha256` de-duplication every 60 s.
- **Member map** — priority-ordered cards with state icon (crown / rotate / shield / x-circle / help), lag / ping / sync-from, priority / votes / uptime, configVersion. Standalone shows a single card + "standalone, replset actions unavailable" caption.
- **Sharded decomposition** — Shards, Config servers, and Mongos sections. *Shard replset members are rendered from the `listShards.host` seed list as `UNKNOWN`-state cards*; per-shard `replSetGetStatus` probing (which needs a dedicated `MongoClient` per shard) is deferred — see "Known gaps" below.
- **`rs.conf` viewer** — read-only JSON pane with copy + export, opened from the Topology header on replset + sharded clusters.
- **Replica-set admin** — right-click the primary for *Step down…* (defaults: 60 s step-down, 10 s secondary catch-up), right-click a secondary for *Freeze / unfreeze…* (30 s / 60 s / 5 min / 15 min / Unfreeze). Preview-only priority / votes editor renders the resulting `rs.reconfig` JSON with a `version` bump; *Execute* is permanently disabled in v2.4 per the milestone non-goal (lands with v2.7 guided reconfig).

### Live operations (`OP-*`, `LOCK-*`, `POOL-*`)
- **currentOp viewer** — 2 s polled `$currentOp({allUsers: true, idleSessions: false})` with filters for namespace regex, op-type chips (`query` / `update` / `command` / `getmore` / `insert` / `remove` / `none`), `secs_running ≥ N`, user, and planSummary contains. Pause toggle freezes the view without stopping the poll. Row double-click opens the raw BSON document in a modal JSON viewer.
- **killOp** — right-click ▸ *Kill op* opens the typed-confirm flow (target = opid); disabled unless the cached role set includes `killAnyCursor` / `root`. Copy opid / Copy as killOp command also on the row kebab.
- **Lock analytics** — `lockInfo` at 5 s while the pane is attached; heat-bar per resource (width ∝ holder count, colour ∝ waiter / holder ratio), top 5 holders listed with opid. Graceful "not supported" card on MongoDB 3.4 / permission-denied responses.
- **Connection pools** — `connPoolStats` every 10 s; rows with `waitQueueSize > 0` render on an amber background with a ▲ indicator; footer shows `totalInUse / totalAvailable / totalCreated` + a queued-count if any pool is backed up.

### Oplog (`OPLOG-*`)
- **Gauge** — `collStats` on `local.oplog.rs` + `$natural`-sorted first / last timestamp queries; surfaces size / used / window hours with colour bands at `< 6 h red`, `< 24 h amber`, `≥ 24 h green`.
- **Tail** — newest-first (`$natural: -1`) list with server-side ns-regex + op-type filters, pause toggle, modal JSON detail.
- **Export** — streamed JSON Lines writer using `Files.newBufferedWriter` so > 10 000 rows don't balloon the heap.

### Sharding (`SHARD-*`)
- **Balancer controls** — `balancerStatus` at 5 s (pre-4.4 fallback to the `{enabled}` boolean); status pill (off / idle / running), 24 h chunk-move counter from `config.changelog`, active-migration count from `config.migrations`. *Start / Stop* dispatch `{balancerStart: 1}` / `{balancerStop: 1}` through `OpsExecutor`; *Set window…* upserts `config.settings.{_id: "balancer", activeWindow: {start, stop}}` inside the same audit path.
- **Chunk distribution** — `config.chunks` aggregated per `(ns, shard)` with jumbo tally; `$ifNull` coalesces pre-5.0 `ns`-keyed and 5.0+ `uuid`-keyed chunks, resolving uuids back to namespaces via `config.collections`. Rendered as a per-collection row with a compact `▇▇▇▇░░` shard histogram + jumbo flag.
- **moveChunk** — dialog captures ns + min / max bounds (parsed through `BsonDocument` so `MinKey` / `MaxKey` / `ObjectId` literals survive) + target shard (populated from `listShards`); dispatch uses write concern `majority`.
- **Zones + tag ranges** — `config.tags` + `config.shards.tags` feed a chip grid of shard → zones and a table of existing tag ranges; Add / Remove dialogs dispatch `{updateZoneKeyRange: ns, min, max, zone}` (zone = `null` for removal).

### Safety, audit, retention (`SAFE-OPS-*`, `ROLE-*`, `AUD-*`)
- **Role probe** — `connectionStatus({showPrivileges: true})` cached 5 min per connection with on-demand refresh + invalidation on role-related failures. Parser accepts the 4.0..7.x `authInfo.authenticatedUserRoles` shape.
- **Dry-run renderer** — canonical JSON + SHA-256 hash for every destructive command (9 variants). The hash shows on the confirm dialog and is persisted into `ops_audit.preview_hash`. `PreviewHashChecker.requireMatch` is invoked immediately before dispatch to guard against mid-dialog tampering.
- **Typed-confirm dialog** — *Execute* stays disabled until the input trim-matches the target string (host:port for step-down / freeze, opid for killOp, zone / shard name for sharding, UTC clock for the balancer window). Paste is allowed but flagged in the audit row.
- **Kill-switch** — process-wide flag (`com.kubrik.mex.cluster.safety.KillSwitch`) that `OpsExecutor` checks on every dispatch; engaged dispatches short-circuit to `CANCELLED` with `kill_switch = true` in the audit row. In-memory only; defaults OFF on every startup. Test-driven today — *the top-bar UI toggle for engaging / disengaging the switch interactively is deferred to a follow-up*.
- **`OpsExecutor`** — dispatches `StepDown`, `Freeze`, `KillOp`, `BalancerStart`, `BalancerStop`, `BalancerWindow` (via `config.settings` upsert), `MoveChunk`, `AddTagRange`, `RemoveTagRange`. Every outcome — `OK`, `FAIL`, `CANCELLED` — writes an `ops_audit` row and publishes `EventBus.onOpsAudit`.
- **`ops_audit` table** — every destructive call appends a row: connection, command, redacted JSON, preview hash, outcome, server message, role used, started / finished timestamps, latency, caller host / user, paste flag, kill-switch flag, ui source.
- **Audit pane** — live-tailed table per connection (loads 500 most-recent on mount, prepends rows arriving on `onOpsAudit`) with command / outcome / free-text filters and a detail drawer that shows the full command JSON + preview hash + flags. JSON bundle export with per-row SHA-256 digest + CSV export with quote-wrapping for any field containing commas, quotes, or newlines.
- **Retention janitor** — daily 03:00 local sweep of `ops_audit`; rows older than 180 days are purged *except* `outcome = FAIL` and rows whose `role_used` is `root` or `clusterAdmin`, which stay until the user clears them explicitly.
- **Cascade delete** — `ConnectionStore.delete` now purges `ops_audit`, `topology_snapshots`, and `role_cache` rows for the target connection in the same SQLite transaction as the `connections` row delete, rolling back on any failure.

### Hardening (`Q2.4-J`)
- **`DestructiveActionFuzz`** — 108 scenarios across every `Command` variant × six role shapes (empty, read, killAnyCursor, clusterManager, root, mixed) × kill-switch on/off. Asserts: every dispatch produces an audit row, every row carries a non-null 64-char preview hash, kill-switch engagement dominates the outcome, missing roles stop before dispatch, role-present cases reach the dispatch step.
- **`CascadeDeleteIT`** — seeds all three v2.4 tables for a target + neighbour connection, deletes the target, asserts the target's rows are purged and the neighbour's remain.
- **`TopologyServiceIT`** — live `mongo:7.0` replica-set fixture via Testcontainers: kind detection, primary presence, structural sha256 stability, DAO de-duplication on identical snapshots, and `EventBus.onTopology` fan-out to live subscribers.

### Schema additions
All three tables ship via the additive `Database.migrate()` pattern and roll back cleanly to v2.3.x (the new tables simply remain unread).

- `topology_snapshots (connection_id, captured_at, cluster_kind, snapshot_json, sha256, version_major, version_minor, member_count, shard_count)` with unique `(connection_id, captured_at, sha256)` so identical-tick duplicates can't bloat the table.
- `ops_audit (connection_id, db, coll, command_name, command_json_redacted, preview_hash, outcome, server_message, role_used, started_at, finished_at, latency_ms, caller_host, caller_user, ui_source, paste, kill_switch)` indexed on `(connection_id, started_at)`, `(command_name, started_at)`, and `(outcome, started_at)`.
- `role_cache (connection_id PRIMARY KEY, roles_json, probed_at)`.

### Known gaps (deferred)
Called out for honesty — these did not ship in 2.4.0 despite being on the milestone doc:

- **Per-shard `replSetGetStatus` probe**. Sharded topology currently renders shard replset members from `listShards.host` seeds as `UNKNOWN`-state cards; per-shard live state needs a dedicated `MongoClient` lifecycle per shard. Flagged as TODO in `ClusterTopologyService.sampleSharded`.
- **Kill-switch top-bar UI toggle**. The flag + dispatcher enforcement ship; the one-click header toggle for engaging / disengaging the switch from the UI is not wired.
- **Keyboard matrix beyond `Cmd/Ctrl+Alt+C`**. The spec also lists `Cmd/Ctrl+Alt+O` (Ops sub-tab with `secs_running > 10` preset) and `Cmd/Ctrl+Alt+L` (Logs filtered to ops-audit events); these land with a follow-up polish pass.
- **Polish tail (`Q2.4-I`)**. Bespoke dark-mode palette, focus-ring audit, colour-blind-safe screenshot matrix, and the a11y audit are unfinished.
- **Hardening tail (`Q2.4-J`)**. The 72 h production-shape soak runs as operations work post-release.

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

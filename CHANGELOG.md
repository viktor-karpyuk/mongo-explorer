# Changelog

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

# v3 Feature Cut (locked)

Selected via interactive picker on 2026-06-01. Roughly v2-Electron scope
minus heavy auth methods. ~80 features.

## 1. Connection management
- [x] Manual + URI mode (`mongodb://` / `mongodb+srv://`)
- [x] SCRAM auth + AES-256-GCM encrypted URI storage
- [x] Connection URI history dropdown
- [x] Multiple concurrent connections
- [x] Test before save
- [x] Duplicate / edit / delete
- [x] State pills + ping latency
- [ ] ~~SSH tunnel / TLS pinning / X.509 / Kerberos / IAM / OIDC~~ — dropped

## 2. Database & collection browser
- [x] Tree sidebar (lazy)
- [x] Right-click context menus
- [x] Namespace search filter
- [x] `dbStats` charts + `collStats` snapshot
- [x] Create database / collection dialogs

## 3. Query & data exploration
- [x] Find editor (filter / projection / sort / skip / limit / maxTimeMs)
- [x] Mongo shell syntax (ObjectId / ISODate / NumberLong / etc.)
- [x] 24-hex `_id` shorthand
- [x] Cmd/Ctrl+Enter
- [x] Cursor pagination
- [x] Results — Table (Compose LazyColumn-virtualized)
- [x] Results — Tree
- [x] Results — JSON
- [x] Results — Error tab

## 4. Aggregation
- [x] 24-stage pipeline editor
- [x] Per-stage enable/disable + reorder
- [x] Run-to-here
- [x] 10 sample templates

## 5. Schema & indexes
- [x] Type-distribution schema analyzer + presence %
- [x] List / create / drop indexes
- [x] Explain plan viewer
- [x] $indexStats usage
- [x] JSON schema validator editor

## 6. CRUD & bulk
- [x] Insert one / many
- [x] Update one / many + upsert
- [x] Replace one + upsert
- [x] Delete one / many
- [x] Bulk write (mixed ops)
- [x] Per-row Edit (replaceOne by _id)
- [x] Per-row Delete

## 7. Cluster ops (read-only)
- [x] Topology member cards (state / lag / ping)
- [x] 0–100 health pill
- [x] rs.conf() viewer

## 8. Live monitoring
- [x] 2s serverStatus polling
- [x] Sparkline metric cards (ops, network, connections, WT cache, latency, memory)
- [x] Profiler level + slowMs + system.profile tail

## 9. Migration
- [x] Multi-DB cross-cluster copy wizard
- [x] Preflight gates (reachability + namespace existence)
- [x] _id-cursor checkpoint + pause / resume / cancel
- [x] Orphan reconciliation on boot

### 9b. Migration hardening (v3.1 — `docs/v3/v3.1/`)
- [x] Secondary-index migration (verbatim createIndexes replay; failures are warnings)
- [x] Conflict policies: abort / append (target wins) / upsert (source wins) / drop-first
- [x] Unordered bulk writes + per-document error ledger + `docErrorLimit` job gate
- [x] Post-copy verification (count rules per policy + index diff) with persisted report + report dialog
- [x] Canonical-EJSON `_id` checkpoints (fixes string-sliced resume)
- [x] Preflight shows per-namespace source/target counts + non-empty-target warning

### 9c. Migration correctness (v3.2 — `docs/v3/migration-analysis-2026-07.md`)
- [x] Two-phase checkpoint + idempotent replay window (crash-safe resume under abort/drop)
- [x] Type-agnostic `$expr` resume filter (mixed-type `_id` no longer skips documents)
- [x] `RawBsonDocument` passthrough + 12 MB byte-capped batches
- [x] Collection-options replay (validator / collation / capped / timeseries); views rejected in preflight
- [x] Per-document error samples surfaced in the report
- [x] `cancelled` as a first-class status; Resume / Restart for interrupted jobs
- [x] Progress bar with rate + ETA, job timestamps, report export
- [x] Quiescent-source preflight warning; secondary-read and collection-option knobs

### 9d. DBA operations (v3.3 — `docs/v3/dba-gap-analysis-2026-07.md`)
- [x] Operations tab: live `$currentOp` (2 s) with filter, idle/system toggles, COLLSCAN + lock-wait badges
- [x] killOp behind typed confirm (opid passed back verbatim; mongos string opids supported)
- [x] Per-connection read-only mode (schema v4) enforced across every mutating affordance
- [x] Read-only connections excluded as migration targets; runner double-checks pre-existing jobs
- [x] Security tab: estate-wide user list (`usersInfo.forAllDBs`) + user CRUD (create / edit roles / change password / drop)
- [x] Role browser (`rolesInfo` per db) with privilege expansion + custom-role create/drop
- [x] Access check: "who can read/write/drop/index db.coll" from server-expanded `inheritedPrivileges`
- [x] Dangerous-account insights: superusers, roleless users, unused custom roles
- [x] Replication section on Cluster tab: oplog window (severity-coloured) + per-secondary lag sparklines (5 s poll)
- [x] Lag measured against the primary's optime everywhere (was wall-clock-relative)
- [x] Storage tab: sortable per-collection weight (reclaimable % via freeStorageSize), estate totals
- [x] Unused-index candidates (`$indexStats` × `listIndexes`, TTL-excluded, unique-badged) with gated drop
- [x] TTL index overview

## 10. I/O
- [x] Export JSON / NDJSON / CSV (streaming)
- [x] Import JSON / NDJSON / CSV (dry-run + commit)

## 11. Shell
- [x] Embedded mongosh subprocess + stdin/stdout/stderr

## 12. UI / settings
- [x] Welcome view + sidebar tree
- [x] Collapsible cluster nodes + per-connection actions (refresh / create db / disconnect)
- [x] Filter reaches collections in unexpanded databases
- [x] Schema-aware autocomplete for filter / projection / sort (fields, operators, values)
- [x] Keyboard row navigation, scrollbars and selectable JSON in the results pane
- [x] Pagination bar (page x of y, first/prev/next, page size) on find and aggregate
- [x] Material 3 dark / light theme toggle
- [x] Customizable editor font size
- [x] Tab width preference
- [x] Status pills, toasts, dialogs

## 13. Platform
- [x] Kotlin 2.0 + Compose Multiplatform 1.7
- [x] JDK 21 toolchain
- [x] sqlite-jdbc local store (WAL + FK)
- [x] AES-256-GCM URI encryption via `~/.mex-v3/master.key` (mode 0600)
- [x] jpackage packaging (DMG / MSI / DEB) via Compose Desktop

## Explicitly dropped vs. v2 (Electron)
- SSH tunnel + TLS pinning + X.509 / Kerberos / AWS IAM / OIDC
- Destructive cluster ops + safety framework
- Historical monitoring agent
- Maintenance / DBA / Backups / Security compliance / k8s / Local Labs
- Migration sink plugin SDK
- Customizable keyboard shortcuts (documented only)

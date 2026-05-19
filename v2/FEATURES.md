# v2 Feature Cut (locked)

Selected via interactive picker on 2026-05-18. ~85 features across 12 categories.
Performance is the primary driver — everything below justified its weight.

---

## 1. Connection management
- [x] Manual + URI mode (`mongodb://` / `mongodb+srv://`)
- [x] SCRAM auth + encrypted password storage
- [x] SSH tunnel via private key
- [x] Multiple concurrent connections
- [x] Test before save
- [x] Duplicate / edit / delete
- [x] State pills + ping latency
- [ ] ~~X.509 / Kerberos / AWS IAM / OIDC / TLS pinning~~ — dropped

## 2. Database & collection browser
- [x] Tree sidebar (lazy-loaded)
- [x] Right-click context menus (create/drop/rename)
- [x] Namespace search (real-time tree filter)
- [x] `dbStats` charts (size / doc count / index size)
- [x] `collStats` snapshot (per-collection storage)
- [x] Create database dialog
- [x] Create collection (validation / capped / time-series)

## 3. Query & data exploration
- [x] Find editor (filter / projection / sort / skip / limit / maxTimeMs)
- [x] Aggregation pipeline editor (24 stages, drag reorder, run-to-here)
- [x] Stage picker with operator templates
- [x] 10 sample pipelines (templates)
- [x] Cmd/Ctrl+Enter run shortcut
- [x] Cursor-driven pagination
- [x] Results — Table view
- [x] Results — Tree view
- [x] Results — JSON view
- [x] Results — Error tab
- [x] In-place document edit
- [x] BSON constructors in editor (`ObjectId()`, `ISODate()`, etc.)
- [x] 24-hex `_id` shorthand
- [x] Query history per connection (100-entry SQLite)
- [x] Export JSON / JSON Lines
- [x] Export CSV
- [x] Import JSON / NDJSON / CSV (dry-run + commit)
- [x] `mongodump` / `mongorestore` launcher

## 4. Schema & indexes
- [x] Type-distribution schema analyzer
- [x] Field presence %
- [x] JSON schema validator editor
- [x] List indexes (all metadata)
- [x] Create index (compound / geo / text / wildcard / TTL / partial)
- [x] Drop index (typed-confirm)
- [x] Explain plan viewer
- [x] `$indexStats` usage

## 5. CRUD & bulk
- [x] Insert one
- [x] Insert many (paste JSON array / NDJSON)
- [x] Update one
- [x] Update many
- [x] Replace + upsert
- [x] Delete one / many
- [x] Bulk writes (batched mixed ops)

## 6. Cluster ops (READ-ONLY)
- [x] Topology viz (member cards, lag, ping)
- [x] 0–100 health pill on connection card
- [x] `rs.conf()` viewer (read-only)
- [ ] ~~All destructive RS/sharded ops, balancer, oplog tail, audit pane~~ — dropped

## 7. Monitoring — live (no historical)
- [x] Live cluster stats on cards
- [x] `serverStatus` charts (2s refresh)
- [x] Per-member breakdown
- [x] Workload P50/P95/P99
- [x] Replication lag + heartbeat
- [x] Sharding imbalance metrics
- [x] Storage per namespace
- [x] Heap + WiredTiger cache
- [x] Profiler / slow-ops surface
- [x] Slow query detail with explain
- [ ] ~~Historical recording / in-cluster agent~~ — dropped

## 8. Migration
- [x] Migration wizard (step-by-step)
- [x] Multi-DB scope (N source databases per job)
- [x] Opt-in user migration
- [x] Live progress + pause/resume from checkpoint
- [x] Crash recovery (orphan reconciliation)
- [x] Preflight gates (version / namespace / auth / storage)
- [ ] ~~Sink plugin interface~~ — dropped

## 9. UI surfaces
- [x] Welcome view + connection cards
- [x] Main tab bar
- [x] Explorer view (tree + stats pane)
- [x] Query view (Find + Aggregation, side-by-side)
- [x] Results pane (Table / Tree / JSON / Error)
- [x] Monitoring tab
- [x] Cluster tab (read-only topology + rs.conf)
- [x] Migrations tab
- [x] Embedded shell (mongosh integration)
- [x] Logs view
- [x] History view
- [x] Connections view
- [x] Native menu bar + quick-action dialogs
- [x] Collapsible form rows + font-size A−/A+
- [x] Status bar (running-jobs pill, version)
- [x] Enter-to-activate tree rows
- [x] Connect feedback (spinner + toast + error dialog)
- [x] Bottom-right toast notifications

## 10. Theming & shortcuts
- [x] Light / Dark mode (semantic dark tokens that adapt globally)
- [x] Customizable keyboard shortcuts
- [x] Query editor preferences (font size / highlighting / tab width)
- [x] Profiler settings (threshold + level)

## 11. Platform
- [x] Java 21 virtual threading
- [x] Event bus pub/sub
- [x] Offline cached-metadata browse
- [x] Native packaging (jpackage DMG / MSI / DEB)
- [x] WAL-mode SQLite (local store, FK constraints)

## 12. Explicitly dropped
- Heavy auth (X.509, Kerberos, AWS IAM, OIDC, TLS pinning)
- Destructive cluster ops (step down, freeze, reconfig, killOp, balancer, moveChunk, zones)
- Oplog viewer / tail
- Audit pane / export / janitor
- Safety guardrails framework (dry-run / typed-confirm / approvals / kill-switch / role probe)
  — Migration retains its own preflight gates
- Historical monitoring (recording windows, in-cluster agent, time-series charts)
- All Maintenance / DBA (params, drift, upgrade planner, compaction)
- All Backups / PITR / DR
- All Security / compliance (RBAC UI, CIS, audit log viewer, cert inventory, evidence signing)
- Kubernetes integration (kubeconfig, port-forward, provisioning, operators, node pools)
- Local Labs (minikube / k3d)
- Migration sink plugin SDK

**Total in scope: ~85 features. Roughly 60 % of v1 surface area, focused on the high-value daily-driver workflows.**

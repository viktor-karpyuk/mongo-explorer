# Changelog

## v2.8.0-alpha — Kubernetes port-forward (Q2.8.1-C)

Closes the "awaiting Q2.8.1-C" loop from the discovery chunk: spin a port-forward against a discovered Mongo workload, bind a loopback listener, and pump bytes through the Kubernetes API's SPDY upgrade to the backing pod.

### Highlights

- **`PortForwardService`.** `open(clusterRef, connectionId, target) → PortForwardSession`: resolves the backing pod (Service → Endpoints lookup for Service targets; direct pass-through for Pod targets), binds a kernel-assigned ephemeral port on 127.0.0.1, and starts an accept loop on a virtual thread. Each accepted client connection spins off a fresh `io.kubernetes.client.PortForward` WebSocket and pumps bytes bidirectionally on its own virtual-thread pair. No single long-lived upstream, so a pod restart doesn't kill the session — the next client connection just opens a fresh SPDY stream.
- **`PortForwardTarget`.** Sealed-like xor: exactly one of `pod` or `service` is set. Canonical constructor validates the invariant so the service layer can't accidentally dereference an empty target.
- **`PortForwardAuditDao`.** Writes open + close rows into `portforward_audit`; first-write-wins close stays idempotent under app-exit + explicit-close races.
- **`PodResolver`.** Service target resolution via the Endpoints object (ready addresses only; `notReadyAddresses` filtered out). Pod target pass-through is direct with no API call.
- **Shutdown hook.** `PortForwardService.closeAll()` wired into a JVM shutdown hook by `MainView.ensureK8sWiring`; every live forward gets a `reason_closed = 'APP_EXIT'` audit update before the process exits.
- **`DiscoveryPanel` integration.** New "Open forward" button next to Refresh / Resolve credentials / Create connection. Records the ephemeral local port in the status line so the user can paste it straight into a connection form.
- **Event bus.** `onPortForward` / `publishPortForward` with sealed `PortForwardEvent` (`Opened` / `Closed` / `Error`).

### Scope notes

Deliberately kept narrow. Richer patterns from our in-house reference implementation at `~/dev/port-forward-app` are left for follow-up chunks:

- **TCP health probes.** Port-forward-app polls `Socket.connect(..)` every 5s against the local listener and marks the session `ERROR` on failure. Unnecessary here because each client connection opens its own WebSocket — a dead WebSocket only takes down one client connection, and the accept loop survives.
- **Exponential-backoff auto-restart.** Needs a per-target autoRestart flag + a restart counter on the session. Deferred until the wizard path (Q2.8.1-D+) actually needs "keep this forward alive through a rolling restart."
- **kubectl subprocess fallback.** Port-forward-app falls back to a `kubectl port-forward` process for MFA-heavy auth flows (AWS SSO w/ browser redirect) that stress the Java client's exec-plugin path. Deferred to Q2.8.1-L hardening — foundation code ships pure-Java.
- **Service selector → labelled pod list.** An alternative to Endpoints-based resolution; slightly lower latency when Endpoints lag a pod becoming ready. Not shipped because Endpoints filtered for ready addresses is already the canonical signal.

### Tests

16 new unit tests across four classes: `EphemeralPortAllocatorTest` (loopback + distinctness), `PortForwardTargetTest` (xor invariant + validation), `PortForwardAuditDaoTest` (open / close / idempotence / count-open), `PortForwardServiceTest` (open → audit → bus events; close idempotence; closeAll; end-to-end byte pipe with a stubbed opener). All run on the JVM without a live cluster via a `PortForwardOpener` seam.

Live streaming against kind is tracked for Q2.8.1-L (matrix hardening).

## v2.8.0-alpha — Kubernetes discovery + secret pickup (Q2.8.1-B)

Builds on Q2.8.1-A to surface `DISC-*` and `SEC-*`: enumerate Mongo workloads on an attached cluster, resolve credentials from operator-convention Secrets, and record a placeholder connection row that port-forward (Q2.8.1-C) will make live.

### Highlights

- **Three-way discovery.** `McoDiscoverer` queries `MongoDBCommunity` CRs, `PsmdbDiscoverer` queries `PerconaServerMongoDB` CRs, and `PlainStsDiscoverer` walks every visible namespace for StatefulSets that reference a Mongo image (with owner-reference + `app.kubernetes.io/managed-by` filters to drop operator-owned STS). 404 on a CRD is treated as "operator not installed" and yields an empty list — per-discoverer failures never fail the whole refresh.
- **Unified projection.** `DiscoveredMongo` normalises origin / namespace / topology / service / auth / ready across the three sources so the UI renders one table instead of three.
- **Operator-aware secret resolvers.** `McoSecretResolver` keys on `<cr-name>-admin-user` + `<cr-name>-ca`. `PsmdbSecretResolver` keys on `<cr-name>-secrets` (userAdmin creds) + `<cr-name>-ssl` (external TLS bundle including optional BYO client cert).
- **TLS-material hygiene.** `MongoCredentials` keeps PEM bytes on the heap only. Persistence is limited to `SecretReader.fingerprint(..)` — a SHA-256 hex digest — matching milestone §2.4.
- **Discovery UI.** `DiscoveryPanel` slots into `ClustersPane` as a split-below panel bound to the selected cluster. Actions: Refresh, Resolve credentials (shows username + password-present + TLS fingerprint, never the password itself), Create connection (writes `origin='K8S'` row via `ConnectionStore.insertK8sOrigin`).
- **Event bus.** `onDiscovery` / `publishDiscovery` with sealed `DiscoveryEvent` (`Refreshed` / `Failed`). Matches milestone §3.2.

### Tests

22 new unit tests: 6 `McoDiscovererTest` (topology, auth-modes, malformed-CR tolerance), 4 `PsmdbDiscovererTest` (unsharded, sharded, fallthroughs), 6 `PlainStsDiscovererTest` (image match, owner-filter, ready aggregation, image-tag parsing), 6 `SecretReaderTest` (`data` vs `stringData`, SHA-256 fingerprint). All run on the JVM without a live cluster.

### Deferred

- **Informer-driven live deltas** — the current discover call is one-shot; informers land with Q2.8.1-C's port-forward lifecycle, which is the first surface that benefits from live updates.
- **Plain-STS secret resolver** — surfaced as "(no material found)" in the preview; B3's next iteration lets users pick a Secret + key mapping manually.

## v2.8.0-alpha — Kubernetes foundation (Q2.8.1-A)

First slice of the v2.8.1 Kubernetes Integration milestone — the `K8S-*` foundation: attach a cluster by kubeconfig context, probe it for reachability, and classify which auth strategy each context uses so pre-flight can catch `plugin X not on PATH` before an Apply. Discovery, port-forward, secret pickup, and provisioning land in subsequent chunks (Q2.8.1-B onwards).

Spec set: `docs/v2/v2.8/v2.8.1/`. Package root: `com.kubrik.mex.k8s`. Gated behind the `k8s.enabled` system property so the Alpha surface is discoverable in the Tools menu without wiring into day-to-day use.

### Highlights

- **Kubeconfig aware picker.** `KubeConfigLoader` walks `$KUBECONFIG` / `~/.kube/config`, parses every context, and classifies each user stanza (`EXEC_PLUGIN`, `OIDC`, `TOKEN`, `CLIENT_CERT`, `BASIC_AUTH`, `IN_CLUSTER`, `UNKNOWN`). `exec:` rows surface the binary name (`aws-iam-authenticator`, `gke-gcloud-auth-plugin`) so pre-flight can show a friendly hint.
- **Client factory with mtime cache.** One `io.kubernetes.client.openapi.ApiClient` per `(kubeconfig_path, context_name)` pair; rebuilt only when the kubeconfig file's mtime changes. Exec-plugin / OIDC / static-token / client-cert auth delegated to the upstream library.
- **Reach probe.** `ClusterProbeService` runs `/version` on a bounded 8 s budget and opportunistically counts nodes, mapping failures into `REACHABLE` / `AUTH_FAILED` / `PLUGIN_MISSING` / `TIMED_OUT` / `UNREACHABLE` for the UI chip.
- **RBAC probe.** `RBACProbeService` issues a batch of `SelfSubjectAccessReview` calls covering the facts the provisioning path will need: list pods, read / create Secrets, create PVCs, read Events, create `MongoDBCommunity` CRs, create `PerconaServerMongoDB` CRs, list / create namespaces.
- **Clusters tab.** `ClustersPane` hosts Add / Probe / Probe-all / Forget actions over a TableView bound to `k8s_clusters`. Detail drawer shows kubeconfig path, context, default namespace, server URL, last-probe result. Bus-driven status chip — no polling.
- **Forget guard.** `KubeClusterService.remove` refuses while any `provisioning_records` row in APPLYING or READY points at the cluster; RESTRICT FK on the schema is the second line.

### Schema

Four new tables (milestone §3.1 verbatim): `k8s_clusters`, `provisioning_records`, `portforward_audit`, `rollout_events`. Indexes on the hot lookup paths (`idx_prov_status_time`, `idx_pfwd_conn_time`, `idx_rollout_prov_time`). FK cascades: RESTRICT from `provisioning_records → k8s_clusters`, SET NULL from `portforward_audit → k8s_clusters`, CASCADE from `rollout_events → provisioning_records`.

### Dependency

`io.kubernetes:client-java:20.0.1` (decision §7.7: first-party tracks upstream faster; exec-plugin / OIDC support built in; typed models + informer framework ready for Q2.8.1-B onward). `slf4j-simple` excluded so logging stays on logback.

### Event bus

Added `onKubeCluster` / `publishKubeCluster` with sealed `ClusterEvent` (`Added` / `Removed` / `Probed` / `AuthRefreshFailed`). Matches the milestone §3.2 contract.

### Tests

17 new unit tests: `KubeConfigLoaderTest` (token / exec-plugin / OIDC / client-cert / unknown / multi-context / missing file), `KubeClusterDaoTest` (CRUD + unique constraint + touch + live-provision count), `KubeClusterServiceTest` (events on add / remove / probe, last_used_at bump semantics).

### Deferred

- **Discovery + secret pickup** (Q2.8.1-B) — operator-aware discovery of `MongoDBCommunity`, `PerconaServerMongoDB`, plain-Mongo StatefulSets; TLS-material-in-memory hygiene.
- **Port-forward service** (Q2.8.1-C) — ephemeral-port session bound to a Mongo Explorer connection; health probe + reconnect; `portforward_audit` writes.
- **Provisioning wizard** (Q2.8.1-D onwards) — profile model, MCO + PSMDB adapters, pre-flight engine, rollout viewer, tear-down, clone / export.
- **Kind-cluster IT coverage** (Q2.8.1-L RC gate) — exec-plugin / OIDC auth matrix, `DiscoveryIT`, `McoProvisionIT`, `PsmdbProvisionIT`, etc. Foundation code verified by unit tests only so far.

## v2.8.0-alpha — Local Sandbox Labs

Pivots the v2.8 series — Labs takes the v2.8.0 slot; the K8s workstreams that were originally scheduled at v2.8.0–v2.8.3 have shifted to v2.8.1–v2.8.4. Market-study driven: no desktop MongoDB GUI offers a credible one-click sandbox today.

Spec set: `docs/v2/v2.8/v2.8.0/`. Package root: `com.kubrik.mex.labs`. Nothing shared with the K8s surfaces that the rest of v2.8.1–v2.8.4 will ship — the two provisioning tracks stay cleanly separated.

### Highlights

- **6 curated templates** (standalone, rs-3, rs-5, sharded-1, triple-rs, sample-mflix) covering ~95% of training / evaluation needs. Adding a 7th is a YAML file + golden test, no Java code change.
- **Auto-connection on healthy.** The moment mongod answers ping, a `connections.origin = 'LAB'` row lands and the user is dropped into the query surface. No connection form to fill in.
- **Sample-data seeding.** Bundled tiny datasets (`_mex_labs` sentinel for idempotency) + fetch-on-demand for `sample_mflix` (SHA-256 verified; HTTP fetch with disk cache; mongorestore via a one-shot sidecar container).
- **Full lifecycle.** Apply / Stop / Start / Destroy with typed-confirm for destroy. Guarded transitions — wrong-state returns `Rejected`, compose errors return `Failed`.
- **App-start reconciler.** `docker compose ls --format json` cross-checked against `lab_deployments` rows; orphan containers logged, missing projects flipped to FAILED.
- **JVM shutdown hook.** `labs.on_exit=stop` (default) / `leave_running` / `destroy`. Parallel per-Lab shutdown on virtual threads; 15s / 30s wall budgets so exit doesn't drag.
- **Docker detection empty state.** Guided install links (Docker Desktop / OrbStack / colima) + Retry button probing `docker version`; minimum CLI 24.0 (`compose ls --format json` requires it).

### Schema

Three additive changes:
- `lab_deployments` table (compose project + port map + status + timing + mongo tag + connection FK back-pointer).
- `lab_events` table (append-only; separate from `ops_audit` so Lab demo noise stays out of the compliance trail).
- `connections.origin` (default `'LOCAL'`, new value `'LAB'`) + `connections.lab_deployment_id` (nullable FK back-pointer).

### Tests

Unit: 30 new tests across templates / renderer / ports / models / DAOs / seed / lifecycle parsers. Live: `LabLifecycleLiveIT` tagged `labDocker` (Apply → Stop → Start → Destroy against a real Docker runtime; run with `./gradlew :app:labDockerTest`).

### Deferred

- **Cross-platform smoke matrix** (Q2.8.4-G RC gate) — macOS arm64/amd64 + Linux amd64 + Windows 11 Docker Desktop, per-template resource footprint per NFR-LAB-5. Lands before GA.
- **Orphan-project adoption UI** — reconciler logs orphans but the banner + adopt button ship with the v2.8.x polish series.
- **Reset-to-seed action** — `sample-mflix` "restart with fresh seed" one-click (open question §9.7).
- **Licensing review for bundled sample-data** (open question §9.1) — currently all bundled sets are small classpath resources we author; MongoDB's official samples fetch on demand.

## v2.7.0-alpha — Maintenance & Change Management

Last leg of the v2.4–v2.7 production-DBA roadmap — wizards for the day-two operations a DBA does after the cluster is up: schema-validator edits, rolling index builds, `rs.reconfig`, compact / resync, `setParameter` tuning, mongod upgrade planning, and config-drift tracking. Every destructive action is gated by a two-person approval checkpoint (solo-mode opt-in, in-tool approval dialog, or pre-signed JWS token) and emits a rollback plan attached to its `ops_audit` row.

**Scope of this alpha:** Every workstream's *headless kernel* (models, runners, preflights, DAOs, renderers) is landed and unit-tested. Full JavaFX wizard UIs are stubbed (one placeholder sub-tab per workstream) and will fill in post-alpha as each UI story lands. Live-cluster IT tests for dispatch paths (`replSetReconfig`, `createIndexes`, `compact`, `collMod`) are tracked separately under `TEST-*` and require the testcontainers rig.

### Package layout (new)

All v2.7 code lives under `com.kubrik.mex.maint.*`:
- `model/` — every input / output record (Approval, RollbackPlan, ReconfigSpec, ValidatorSpec, IndexBuildSpec, CompactSpec, ClusterShape, ParamProposal, UpgradePlan, ConfigSnapshot).
- `approval/`, `rollback/`, `reconfig/`, `schema/`, `index/`, `compact/`, `param/`, `upgrade/`, `drift/` — one package per workstream.
- `events/` — `MaintenanceEvent`, `ApprovalEvent`, `ConfigDriftEvent` event-bus payloads.
- `ui/` — `MaintenanceTab` (sub-tab host; wizard UIs pending).

### Q2.7-A — Approval service + rollback plan scaffolding

- **Schema migrations** — five new tables: `config_snapshots`, `approvals` (with expiry), `rollback_plans` (linked to v2.4 `ops_audit`), `maintenance_runbooks`, `param_tuning_proposals`. All additive; the tab itself is gated by the `maintenance.enabled` flag so downgrades leave the data quiescent.
- **ApprovalService** — three modes wired end-to-end: `SOLO` (one-shot insert as APPROVED, per-connection opt-in), `TWO_PERSON` (PENDING → APPROVED via a reviewer name; self-approval refused; same-row re-approval refused), and `TOKEN` (JWS payload carries action-uuid + action-name + payload-hash + expiry; a payload swap or action-name swap fails verify). Expiry sweep flips overdue PENDING rows to EXPIRED; consumption is one-way from APPROVED to CONSUMED.
- **JwsSigner** — minimal HS256 compact-serialization signer piggy-backing on the v2.6 `EvidenceSigner` key. Fixed `{"alg":"HS256","typ":"JWT"}` header forecloses the "strip signature, set alg=none" attack; the tiny flat-JSON codec keeps the signing path dependency-free so there's no jose4j in the jpackage image.
- **RollbackPlanWriter** — persists plans keyed to an existing `ops_audit` row (pre-check throws `IllegalStateException` rather than orphan a plan). Plans are write-once; `markApplied` records replay outcome without rewriting the plan JSON so historical rollback intent stays recoverable even after a partial replay.

### Q2.7-D — Reconfig wizard

- **ReconfigSpec** — sealed `Change` hierarchy covers every RCFG-1 kind (AddMember, RemoveMember, ChangePriority, ChangeVotes, ToggleHidden, ToggleArbiter, RenameMember, WholeConfig). `Member` record carries the fields preflight reasons about (priority, votes, hidden, arbiterOnly, host).
- **ReconfigPreflight** — pure-function quorum math. Blocking findings: `MAX_MEMBERS` / `MAX_VOTES` / `NO_VOTERS` / `NO_ELECTABLE` / `NO_MAJORITY` / `DUP_ID` / `DUP_HOST`. Warn findings: `ALL_PRIO_ZERO`, `ARBITER_PRESENT`. Majority check is conservative: `ceil((n+1)/2)` electable members required in the proposed config.
- **ReconfigSerializer** — typed `Request` → BSON `replSetReconfig` body with default-valued member flags omitted for audit clarity; inverse parse of `replSetGetConfig` reply for rollback capture.
- **ReconfigRunner** — dispatch with `writeConcern: majority`, 60 s watchdog (RCFG-5), sealed `Outcome` hierarchy (`Ok` / `Failed` / `TimedOut`).
- **PostChangeVerifier** — polls `replSetGetStatus` until a majority of reachable members bumps `configVersion` (RCFG-6), with a `StatusFetcher` seam so the 120 s wait loop is unit-testable.

### Q2.7-B — Schema validator editor + preview + rollout

- **ValidatorSpec** — `Current` (loaded state) + `Rollout` (proposed change) + `PreviewResult` with `FailedDoc` offenders. `Level` enum (OFF / MODERATE / STRICT), `Action` enum (WARN / ERROR).
- **StarterTemplates** — the four SCHV-8 seeds: empty, required-id, enum-status, typed-timestamps. Users pick and edit; no from-scratch authoring (NG-2.7-5).
- **ValidatorFetcher** — reads current validator + level + action via `listCollections`.
- **ValidatorPreviewService** — server-side `$sample` + `$nor` + `$jsonSchema` pipeline; up to 10 offenders + failed count within the sample. Default 500 docs per SCHV-3.
- **ValidatorRolloutRunner** — `collMod` dispatcher + rollback-command builder (so `RollbackPlanWriter` has the prior-state command ready).

### Q2.7-C — Rolling index builder

- **IndexBuildSpec** — every IDX-BLD-2 option captured (TTL, partial, collation, weights, storage engine); optional fields are true `Optional` so BSON serialization omits them.
- **RollingIndexPlanner** — pure-function member ordering. Secondaries by priority ascending, arbiters skipped, primary last with `isPrimary=true` so the runner knows to step down first.
- **RollingIndexRunner** — per-member `createIndexes` with `commitQuorum: 0` (node-local, no 2-phase commit). `DispatchContext` seam decouples from `ConnectionManager`. Abort path drops on completed members + `killOp`s the active build (IDX-BLD-6).
- **BuildProgressTailer** — parses `$currentOp` `IXBUILD` entries — total / done / elapsed μs → fraction for a UI progress bar.

### Q2.7-E — Compact + resync

- **CompactSpec** — `Compact` (target host + collections + takeOutOfRotation + force) and `Resync` (target host + optional wait) records.
- **CompactRunner** — per-collection outcome list. Primary refusal both client-side (`wouldTargetPrimary`) and server-side (`hello` probe before dispatch).
- **ResyncRunner** — sealed `Outcome` (`Ok` / `PrimaryRefused` / `Failed`); doesn't block on catch-up.

### Q2.7-F — Parameter tuning

- **ClusterShape** — storage engine + RAM + CPU + doc count + workload mix + server version.
- **ParamProposal** — with `Severity` (INFO / CONSIDER / ACT) + `isActionable()` for UI chip state.
- **ParamCatalogue** — 5 curated entries from PARAM-2: `wiredTigerConcurrentReadTransactions`, `wiredTigerConcurrentWriteTransactions`, `ttlMonitorSleepSecs`, `notablescan`, `internalQueryPlanEvaluationMaxResults`. Each carries a rationale string, `appliesTo` predicate, `recommend` function, optional numeric range.
- **Recommender** — classifier: ACT when delta ≥ 25% of allowed range, CONSIDER otherwise, INFO when already matching.
- **ParamRunner** — `getParameter` / `setParameter` driver dispatch; `Optional<Object>` on get so a version-skewed name surfaces as empty not an exception.
- **ParamProposalDao** — persistence for the proposals pane — insert, listOpenForConnection, transition to ACCEPTED / REJECTED / SUPERSEDED.

### Q2.7-G — Upgrade planner + runbook renderer

- **UpgradePlan** — `Finding` (kind + severity + remediation), `Step` (ordered kind list), `Version` (parse handles patch + -rc suffixes).
- **UpgradeRules** — versioned pack covering the 4.4→5.0→6.0→7.0 hops. Rules: version-gap guard (blocks skipping majors + downgrades), deprecated-operators against profile data, removed parameters against `getParameter` snapshot, FCV lower/raise info cards.
- **UpgradeScanner** — orchestrates the rules + emits the ordered step list a runbook needs (pre-check, backup, FCV-lower, rolling secondary swaps, step-down + primary swap, FCV-raise, post-check).
- **RunbookRenderer** — hand-rolled Markdown + HTML; HTML escapes user content so a malicious rule author can't inject script tags. Dependency-free — Mustache-java would add 300 KB for a curated template that never exceeds a few kB.

### Q2.7-H — Config-drift subsystem

- **ConfigSnapshot** — `Kind` enum (PARAMETERS / CMDLINE / FCV / SHARDING), SHA-256 keyed off canonical JSON so equivalent snapshots collapse in the v2.6 drift engine.
- **ConfigSnapshotService.captureAll** — walks `getParameter(*)`, `getCmdLineOpts`, FCV, `balancerStatus` (sharded-only). Redacts any key containing `password` / `secret` / `token` / `key` substrings so `keyFile` paths + SAS tokens never land in the plaintext snapshot.
- **canonicalize()** — `TreeMap`-deep key sort so two capture runs on unchanged config produce byte-identical JSON and byte-identical SHA-256 — the property the drift engine relies on.

### Events + UI stub

- **`MaintenanceEvent`** — per-wizard lifecycle (STARTED / APPROVED / RUNNING / SUCCEEDED / FAILED / ROLLED_BACK).
- **`ApprovalEvent`** — approval state transitions; drives the toolbar queue chip.
- **`ConfigDriftEvent`** — emitted by the snapshot scheduler when a new snapshot's hash differs from the previous for the same (connection, host, kind) triple.
- **`MaintenanceTab`** — sub-tab host. Each workstream has a placeholder pane describing what it does; the full wizard UIs land incrementally post-alpha.

### Tests

89 new unit tests across the workstreams (sinks still green): ApprovalService (13) · JwsSigner (4) · RollbackPlanWriter (5) · ReconfigPreflight (18) · ReconfigSerializer (4) · PostChangeVerifier (3) · ValidatorRolloutRunner (2) · ValidatorFetcher (2) · StarterTemplates (3) · RollingIndexPlanner (4) · IndexBuildSpec (3) · CompactRunner (3) · Recommender (7) · ParamProposalDao (3) · UpgradeScanner (8) · RunbookRenderer (3) · ConfigSnapshotService (7).

### Post-alpha additions (GA-track)

- **UI wizards** — every workstream has a real pane: `ApprovalsPane` (queue with approve/reject/token-export), `SchemaValidatorPane` (starter templates + preview table + typed-confirm rollout), `ReconfigWizardPane` (all 7 `Change` kinds, colour-coded preflight findings, post-change verifier), `RollingIndexPane` (per-member progress strip driven one step at a time), `CompactResyncPane` (primary-refusal client-side + typed-confirm), `ParameterTuningPane` (cluster-shape inputs → Recommender → severity-coloured table + rationale drawer), `UpgradePlannerPane` (scan + MD/HTML export via `FileChooser`), `ConfigDriftPane` (capture-now + line-by-line diff). `MaintenanceTab` takes DAOs + `Supplier<MongoClient>` + `Supplier<String>` (connection id) by constructor; the placeholder ctor is retained for visual previews.
- **`RollbackReplayService`** — `lookup(planId)` + `lookupByAuditId(auditId)` hand the UI a `ReplayRequest` with kind + plan JSON + already-applied flag so the matching wizard opens pre-filled with the inverse spec.
- **`RollingRestartOrchestrator`** — walks `UpgradePlan` steps: `BINARY_SWAP` sends `shutdown` + blocks on the operator-gate callback (UI's "binary swap complete?" dialog); `ROLLING_RESTART` sends `replSetStepDown`; informational kinds emit info events. `MongoSocketException` on shutdown is the success signal, not an error.
- **Fuzz suite (Q2.7-J)** — `JwsTamperFuzz` (single-byte flip at every token position, 2 000 random payloads, truncation at every prefix, segment permutations; verified count = 0), `RunbookMarkdownFuzz` (200 random-ASCII titles + 100 step bodies; HTML shell stays well-formed, `<` always escaped, step order preserved), `ChaosReconfigFuzz` (2 000 random `ReconfigSpec.Request`s; preflight must be deterministic, never throw, classify 100 curated scenarios cleanly).

### Live-cluster ITs (new)

Four {@code @Testcontainers(disabledWithoutDocker=true)} classes verify the dispatch paths against a live {@code mongo:7.0} replset:

- `ValidatorPreviewIT` — seeds 20 conforming + 12 non-conforming docs; preview must report failed-count within sample.
- `ParamRunnerIT` — get/set round-trip on `ttlMonitorSleepSecs`; unknown-param returns empty without throwing.
- `ConfigSnapshotIT` — unchanged config → matching SHA-256 across captures; mutating a parameter diverges the hash.
- `ReconfigPreflightIT` — `fromConfigReply` round-trips live `rs.conf`; priority bump on a single-node rs is non-blocking.

Two real bugs the live ITs caught and fixed this pass:
- **`ReconfigSerializer.fromConfigReply` ClassCast** — live `replSetGetConfig` returns numeric fields as a mix of `Int32` / `Int64` / `Double`. `getInteger()` ClassCasted on `Long`/`Double` values; now coerces via `Number.intValue()` / `Number.doubleValue()` so the parser survives every supported server version.
- **`ValidatorPreviewService` ConversionFailure 241** — the server-side `$substr($toString($$ROOT))` projection failed because `$toString` refuses object inputs. Now renders the offender summary client-side (`Document.toJson()` truncated to 200 chars).

### A11y pass

Every maintenance pane sets `accessibleText` (tab-name-equivalent label) + `accessibleHelp` (descriptive paragraph mirroring the header hint). Tables set `accessibleText` so VoiceOver / screen readers announce their semantic role. The existing tooltip dwell / wrap pattern from the v2.6 SecurityPane helpers carries forward; wiring those helpers here is a later polish pass.

### Still deferred

- **3-node replica-set ITs** — the `@Testcontainers` rig uses the default single-node replset. Full add / remove / vote round-trips + election behaviour live with the 72 h soak rig.
- **72 h soak** with daily parameter proposals + scheduled drift monitor + weekly rolling index build.
- **Dark-mode** — panes consume hex colour values for status text; swapping to atlantafx semantic tokens (matching the v2.6 SecurityPane convention) is a later polish.
- **`ConfigDriftPane` plugs into `DriftDiffEngine`** (milestone §9.4) — current pane ships a line-by-line diff; the structural path-based diff lives on a follow-up.
- **`MaintenanceTab` wiring into `MainView`** — the pane + 4-arg ctor are ready; flipping `maintenance.enabled = true` + instantiating it from `MainView` is the last FX-integration step, blocked on the in-progress `MainView` work for `openClusterEmptyTab`.

## v2.6.1-alpha — Cloud sinks complete

Follow-up to v2.6.0-alpha2 filling in the three backup sinks that were deferred from v2.5 and shipped as stubs in every earlier v2.x. Also lands the Backups → Sinks sub-tab so every sink kind is creatable from the UI; v2.5 only supported sink creation via the `SinkDao` CLI path.

### New sink implementations

- **S3 (v2.6.0-alpha already)** — AWS SDK v2 with URL-connection transport. Credentials: `{accessKeyId, secretAccessKey, sessionToken?}` or blank for the default provider chain (IAM / SSO / env vars).
- **GCS (Q2.6.1-A)** — google-cloud-storage 2.40.1 with HTTP/JSON transport (gRPC-Netty excluded to keep the runtime image lean). Credentials: full service-account JSON key content, or blank for Application Default Credentials.
- **Azure Blob (Q2.6.1-B)** — azure-storage-blob 12.27.1. URI accepts `azblob://<account>/<container>/<prefix>` or the full `https://<account>.blob.core.windows.net/<container>/<prefix>` form. Credentials: `{sasToken}` **or** `{accountName, accountKey}`; SAS wins when both are present. AAD/MSAL is out of scope for 2.6.1.
- **SFTP (Q2.6.1-C)** — maintained JSch fork (`com.github.mwiede:jsch` 0.2.18). URI `sftp://user@host[:port]/path`. Credentials: `{password}` **or** `{privateKey, passphrase?}`; private-key wins when both are present. `ensureParent()` does mkdir-p equivalents so fresh date-templated subdirectories don't break the upload.

### New UI

- **Backups → Sinks sub-tab** (Q2.6.1-D) — list of existing sinks + kind-aware editor form. Test-connection button runs `StorageTarget.testWrite()` with the in-flight form values on a virtual thread and reports latency or a classified error — **without** persisting the sink, so a bad credential paste doesn't leave junk rows behind. Save encrypts credentials via the existing per-install `Crypto` AES key.
- Focus-switch listener reloads the Policies sink picker when you switch tabs, so a freshly saved sink shows up in the policy editor without a restart.

### Tests

- 60 new unit tests covering URI parsing (including Gov Cloud host-suffix preservation, `?query` / `#fragment` stripping, SFTP password-in-userinfo rejection) and credential classifiers for every cloud sink. Sink test totals: S3 12 · GCS 9 · Azure 16 · SFTP 15 · Sinks misc 8.

### Pre-release review fixes

Three rounds of deep review ahead of the alpha tag drove out every bug found:

**Round 1 (`f991e12`)**
- **Azure Gov Cloud endpoint** — `AzureBlobTarget.parseUri` preserves the full host suffix from the `https://` form so Gov Cloud (`blob.core.usgovcloudapi.net`) and China Cloud endpoints route correctly. `azblob://` short-form keeps assuming commercial since it doesn't carry the suffix.
- **GCS silent ADC fallback** — when credentials JSON is set but fails to parse, `GcsTarget` now throws `IllegalArgumentException` instead of falling back to Application Default Credentials. An operator who pasted broken JSON gets a clear error instead of the surprise of picking up the host's unrelated credentials.
- **SFTP `list()` semantics** — missing-directory failures propagate as `IOException`, matching S3 / GCS / Azure. Previously the SftpException was swallowed and an empty list returned, masking sink misconfiguration.
- **`StorageTarget.close()`** — default no-op added to the interface; S3 + GCS sinks override to close their SDK client pools so long-running sessions with many connected sinks don't bloat.
- **SinksPane** — save-time URI validation via each kind's parser so `s3://` with no bucket fails at save, not on first backup; test-button re-enable moved into a `finally` block so an SDK-init error can't wedge the button; sibling form state cleared on kind-switch; JSON escape strengthened to cover the 0x00–0x1F control range and backspace / formfeed.

**Round 2 (`d36ecfa`)**
- **SDK client leak on every Test-connection click** — `onTestConnection` now wraps `testWrite()` in try/finally with `target.close()`. Previously each click leaked a fresh S3Client / GCS Storage / Azure BlobContainerClient.
- **Stale form on delete** — `onDelete` clears the form via a shared `clearForm()` helper (reused by `onSave`). Save right after delete no longer recreates the just-removed sink.
- **SFTP URI with embedded password** — `parseUri` rejects `sftp://user:password@host/path` with a specific hint pointing at the credentials field; closes a silent-auth-fail + password-leak path.
- **Same-kind row click stale fields** — `populateForm` always calls `forms.get(k).clear()` before populate; JavaFX listeners don't fire on `setValue(x)` when `x` equals the current value, so relying on the listener to clear the target form was wrong.
- **`?query` / `#fragment` in pasted URIs** — every URI parser strips both via a shared `S3Target.stripQueryAndFragment()` helper so a user pasting an AWS-console URL with `?version=123` no longer silently lands the querystring in the key prefix.

**Round 3 (this commit)**
- **`SinkDao.insert` error handling** — `SinksPane.onSave` catches `RuntimeException`, unwraps the deepest cause's message (`UNIQUE constraint failed`, etc.), and surfaces it in the status label. Previously a unique-name conflict or locked DB propagated and wedged the FX thread, freezing the whole pane.
- **`java.prefs` module in jpackage runtime** — Google's auth library caches ADC state via `java.util.prefs`; jlink was stripping it so first-run ADC fallback failed at runtime with `PreferencesFactory not found`. Added to the `runtime { modules }` list.

**Round 4 (this commit)**
- **S3 region hardcoded to `us-east-1`** — `SinksPane.buildTarget` passed a hardcoded region to every `S3Target`, so a bucket in `eu-west-2` reached the wrong endpoint and every PUT returned `PermanentRedirect`. The S3 form now has a Region field, the value lands in `storage_sinks.extras_json` (not encrypted — region isn't sensitive), and `buildTarget` reads it via a shared `extractS3Region()` helper with a `us-east-1` fallback for legacy rows saved before the field existed.
- **SFTP `privateKey` byte[] mutation race** — the decoded private-key buffer was stored as a `final byte[]` and handed straight to `JSch.addIdentity` on every operation. JSch retains the array reference, so a second `withSession()` call arriving mid-parse could see the first caller's buffer and vice-versa. `privateKey.clone()` at the call site gives each session its own byte array; negligible GC cost, eliminates the race outright.
- **`backup_policies.sink_id` foreign-key constraint missing** — deleting a storage sink left orphan policy rows that broke silently at the next scheduled run. The new-install schema now carries `REFERENCES storage_sinks(id) ON DELETE RESTRICT`; `SinksPane.onDelete` adds an application-level pre-check via `BackupPolicyDao.countBySinkId()` so upgraded installs (whose existing table predates the FK and can't grow one in place) get the same protection. The confirmation dialog now names the referencing policies and blocks the delete outright until they're unbound or removed.
- **Azure credentials silent anonymous fallback** — `AzureBlobTarget.buildClient` used to log-warn and fall back to anonymous access when the credentials JSON wouldn't parse. A bad paste would then surface as an opaque 403 on the next write. Now throws `IllegalArgumentException` at construction with a shape hint (`{"sasToken":"…"}` vs `{"accountName":"…","accountKey":"…"}`), matching `GcsTarget`'s behaviour from round 1. Blank creds still fall through to anonymous — the throw fires only when the operator *supplied* creds that don't classify.
- **Unsupported sink `kind` silent coercion** — `SinksPane.populateForm` coerced unknown `Kind` enum values to `LOCAL_FS`. A row saved by a newer build of the app (e.g. a future cloud-provider kind) would then load into the Local-FS form, inviting an Edit-then-Save that clobbers the original config. The populate now refuses with a clear status-label message and leaves the current form untouched.

### Deferred

- **Backup runner wiring from the scheduler** — `BackupScheduler`'s dispatcher still logs instead of invoking `BackupRunner.execute` (pre-existing v2.5 scaffold). Needs its own focused session; tracked separately from v2.6.1's cloud-sink scope.
- **`runtime.modules` audit for Windows MSI + Linux DEB** — macOS DMG builds cleanly; other jpackage targets to verify.
- **Azure AAD / managed-identity auth** — MSAL is large and uncommon for backup sinks; re-open in v2.6.2 if ops teams ask.
- **SFTP known-hosts verification** — tracked as a v2.6.2 polish item.

## v2.6.0-alpha — Security, Audit & Compliance (preview)

Preview of the v2.6 Security milestone. **Not production-ready** — 72 h soak (Q2.6-K2) and the release screenshot matrix (Q2.6-K3) still outstanding; three of four cloud sinks (GCS / Azure / SFTP) moved to v2.6.1. Wire-up lives behind the Tools → Security menu (`Cmd/Ctrl+Alt+S`); no existing surface changes behaviour.

### What's new

- **Security tab** (per-connection) hosting seven sub-tabs: Roles, Audit, Drift, Certificates, Auth, Encryption, CIS.
- **Role matrix + user detail drawer** — users × roles × databases with effective-privilege resolution; captures a `sec_baselines` snapshot for later diffing.
- **Native audit log viewer** — `AuditTailerService` hooks into connection-state events; on CONNECTED it probes `getCmdLineOpts` for a readable `auditLog.path` and pipes parsed events into a SQLite FTS5 index. Search takes FTS5 match grammar (`authenticate who:dba`, `atype:createUser`). Adversarial-fuzz hardened (deep-nesting + BSON-typed-exception corpus).
- **Drift diff + ack / mute workflow** — path-scoped diff between two baselines; ACK hides a finding for that baseline only, MUTE hides a path across every future diff.
- **TLS cert inventory** — per-member handshake capture with green/amber/red/expired bands at 30-day / 7-day / 0-day thresholds. `CertExpiryScheduler` runs a full sweep every 24 h (first sweep 30 s after app start) and emits `onCertExpiry` events so the welcome-card chip stays fresh without opening the Security tab.
- **Auth-backend probe** — SCRAM-SHA-256/1, MONGODB-X509, LDAP (PLAIN), Kerberos (GSSAPI); secret-bearing config keys (passwords, keyfile passphrases) redacted at the probe boundary.
- **Encryption-at-rest probe** — per-node status with KMIP / Vault / local-keyfile detection. Both the encryption and cert probes now expand across every replset / shard / mongos / config member via `TopologySnapshot.allHosts()` so sweeps reflect the live topology on each Refresh.
- **CIS MongoDB v1.2 scanner** — five starter rules (SCRAM-256, SCRAM-1, encryption-at-rest, cert expiry, root-without-restrictions) with suppression + signed evidence-bundle export (JSON + HTML + HMAC-SHA-256 `.sig`).
- **Welcome-card security chip** — small coloured pill per connection flagging expired / expiring certs + unacked drift, with a pointer to `Cmd/Ctrl+Alt+S`.
- **S3 backup sink** — real AWS SDK v2 implementation replacing the v2.5 stub. URL-connection transport keeps the app image lean; credentials flow through `SinkDao`'s AES-wrapped JSON with fallback to the default provider chain (IAM instance profile, SSO, env vars).

### Polish + a11y

- Every Security sub-pane consumes shared `SecurityPaneHelpers` factories — consistent title / subtitle / small / footer typography, standard tooltip dwell (250 ms) + duration (20 s), and a two-line empty-state component (headline + call-to-action) so first-time operators see what Refresh does without reading an empty table.
- Tooltip bodies mirror into JavaFX `accessibleHelp` so VoiceOver / screen readers announce the same explanation sighted users see on hover. Tables and the audit-search field carry `accessibleText` descriptions.
- Neutral colours resolve through atlantafx theme variables (`-color-fg-default/muted/subtle`, `-color-bg-default/subtle`) so dark-mode support is a one-Main.java-line change when a theme-switcher menu lands. Semantic colours (pass-green / fail-red / warn-amber) stay hex by design.

### Schema additions

All additive via `Database.migrate()`:
- `sec_baselines`, `sec_drift_acks`, `sec_cert_cache`, `cis_suppressions`, `cis_reports`
- `evidence_key` (singleton AES-wrapped HMAC-SHA-256 key, distinct from the connection-password key so signed reports are shareable)
- `audit_native_fts` (FTS5 virtual table, porter + ascii tokenizer)

### v2.5 backup-tail completions rolled into v2.6

- **`--oplogLimit`** threading from `PitrPlanner` → `MongorestoreRunner` argv. PITR handoff now stops oplog replay at the planner-picked cut-off instead of replaying the full captured slice.
- **Multi-DB / multi-namespace backup fan-out** — `BackupRunner` now loops mongodump once per entry in `Databases(N>1)` / `Namespaces(N>1)` scopes and aggregates the manifest.
- **S3 cloud sink** — real AWS SDK v2 integration (see above). GCS / Azure Blob / SFTP still stubbed and planned for v2.6.1; see `docs/v2/v2.6/milestone-v2.6.1.md`.

### Still to land before v2.6.0 GA

- **72 h soak** tailing a rotating audit log (plan in `docs/v2/v2.6/soak-test-plan.md`).
- **Release screenshot matrix** captured from the GA build (checklist in `docs/v2/v2.6/screenshot-matrix.md`).
- **Theme-switcher menu** so the dark-mode-ready Security panes actually swap palettes.

### Deferred to v2.6.1

- GCS / Azure Blob / SFTP cloud sinks (see `docs/v2/v2.6/milestone-v2.6.1.md`).
- Live-S3 round-trip IT harness (requires testcontainers-localstack).

150+ new tests pin the headless contracts for every subsystem.

## v2.5.1 — Backup polish + UX overhaul

Patch release closing the issues raised in the post-v2.5.0 deep review plus the "Backups UI is totally broken" + rs.conf modal feedback. No schema changes; drops cleanly onto a v2.5.0 install.

### Correctness fixes
- **`BackupRunner` manifest byte-count** — the catalog's `total_bytes` and the `backup_files` row for `manifest.json` now reflect the UTF-8 byte size of the canonical JSON, not the Java `String` character count. Non-ASCII policy names (e.g. `nachtlauf-äöü`) previously triggered a verify mismatch.
- **`BackupRunner` crash leaks** — the orchestrator body is wrapped in `try { … } finally { finaliseFail(…) if not already finalised }` so a JVM crash mid-run no longer leaves a `RUNNING` row stranded forever. Regression test pins the guard.
- **`BackupRunner` sink path** — the runner uses `LocalFsTarget.rootPath()` directly instead of re-parsing `canonicalRoot()`, removing a latent bug when the sink root contained characters that round-trip-normalised differently.
- **`PitrPlanner` empty-windows message** — when no catalog row has an oplog window (i.e. every backup ran with `includeOplog = false`), the planner now returns a plain-English refusal ("no backup in the catalog captured an oplog window — enable includeOplog on the policy and run a fresh backup") instead of leaking the `Long.MAX_VALUE` reduce sentinel.

### Security
- **`RestoreService` gains a role-probe gate** (`BKP-SEC-1`) — Execute mode calls `RoleProbeService.currentOrProbe(connectionId)` and refuses with `outcome=FAIL, message=role_denied` when the effective user lacks one of `{restore, root}`. The MongoDB `backup` role is deliberately **not** accepted — it grants `mongodump` rights but no write privilege on target collections, so letting it through would let a backup-only operator trip a Restore they can't actually complete. Rehearse mode is unchanged (safe-by-default, no role probe). Matches the v2.4 `Command.allowedRoles()` pattern. New tests pin the gate.
- **`RestoreWizardDialog` confirm preview redacts the target URI** — the TypedConfirmDialog body used to show the raw `mongodb://user:pass@host/…` URI, which would appear on-screen to anyone looking over the operator's shoulder (and in any session recording). The preview now routes the URI through `Redactor.defaultInstance().redact(…)` so the password slot becomes `***`. Audit rows were already password-free (sinkPath only).

### Scheduler + editor polish
- **`BackupScheduler.backfillMissed` is bounded** — the missed-runs walk used to pull every catalog row for a policy regardless of age; now uses the new `BackupCatalogDao.listForPolicySince(policyId, sinceMs)` so the 24 h window is the only range touched.
- **`PolicyEditorPane` sink orphan guard** — saving a policy whose selected sink was removed in another pane is refused at the Save button with a specific message instead of silently falling through. Kept out of live validation so the form doesn't hit SQLite on every keystroke.
- **Per-line scope errors** — the old catch-all "scope is empty / malformed" is replaced with line-specific feedback ("line 3: `orders` — expected db.collection"), matching the editor's live-validation contract.
- **History pane refresh on `Started`** — `BackupEvent.Started` now prepends/upserts a `RUNNING` row into the history table instead of the table only filling on `Ended`, closing the "backup runs but history is empty" gap.

### Restore confirm surface
- **`RestoreWizardDialog` uses the shared `TypedConfirmDialog`** from v2.4 cluster ops — preview JSON, summary, predicted effect, and preview-hash footer. The old stand-alone `TextInputDialog` is gone; Execute mode now has the same confirm UX as `stepDown` / `moveChunk` / `addTagRange`.

### UI overhaul (feedback-driven)
- **Backups tab** — connection dropdown renders `Name (cluster-id)` instead of the previous `Name · id`. Every form label is title-cased with a bold foreground (Name, Schedule (cron), Scope, Archive, Retention, Destination, Options). Inline helpers read as sentences ("Keep last N runs or up to M days — whichever is tighter") instead of raw technical tokens. An info banner explains what a backup policy is, and every label + field now carries a hover `Tooltip` with plain-English explanations of cron grammar, scope choices, gzip levels, retention semantics, and when to include the oplog — enough context for operators who aren't seasoned DBAs. History pane column headers and filter labels are capitalised; PITR + Rehearsal-report buttons gain hover tooltips.
- **`ReplConfigDialog` (Cluster → View rs.conf)** — window is explicitly resizable with a 640×440 minimum and a 960×640 default (mirrors `DocumentEditorDialog`). The JSON pane now sits in a `VirtualizedScrollPane` wrapping `JsonCodeArea` so scrolling stays smooth on multi-thousand-line configs.

### Deferred to v2.6

No v2.5.2 is planned — the remaining backup tails roll into v2.6 alongside the Security & Compliance milestone:

- Cloud sinks (S3 / GCS / Azure / SFTP) — permit-list entries exist, SDK impls still throw `CloudSinkUnavailableException`.
- `--oplogLimit` threading from `PitrPlanner` → `MongorestoreRunner` argv.
- Multi-DB / multi-namespace backup fan-out — `MongodumpCommandBuilder` still emits the first entry only for `Databases(N>1)` / `Namespaces(N>1)`.

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

### Known gaps (addressed in v2.5.1 or deferred to v2.6)

Closed in v2.5.1 (polish release): BackupRunner manifest byte-count + crash-leak guard, RestoreService role-probe gate, PITR empty-windows message, Backups UI overhaul, `rs.conf` viewer resizable, shared TypedConfirm for restore.

Deferred to v2.6 (folded into the Security & Compliance milestone — no dedicated v2.5.2 patch):
- **Cloud sinks** — S3 / GCS / Azure / SFTP permit-list entries exist and can be persisted via `SinkDao`, but every I/O call throws `CloudSinkUnavailableException`. Real SDK integrations (AWS SDK, google-cloud-storage, azure-storage-blob, JSch) land with v2.6.
- **`--oplogLimit` in the restore wizard** — `PitrPlanner` returns an oplog limit timestamp; the wizard hands off the source backup but does not yet thread the limit into mongorestore's argv.
- **Multi-DB / multi-namespace backup fan-out** — `MongodumpCommandBuilder` emits the first entry only for `Databases(N>1)` and `Namespaces(N>1)` scopes; looping per-entry lands with v2.6.

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

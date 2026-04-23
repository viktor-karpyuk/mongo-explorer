# Sharded test cluster

Minimal-realistic sharded cluster for the v2.7 maintenance / reconfig /
rolling-index ITs: one `mongos`, a 3-node config replset (`cfgrs`), and
a 3-node shard replset (`shard1rs`).

## Layout

| Role            | Container   | Host port | Internal URI                |
|-----------------|-------------|-----------|------------------------------|
| router (mongos) | mex-mongos  | 27100     | `mongodb://localhost:27100`  |
| cfg member 1    | mex-cfg1    | 27101     | `mongodb://localhost:27101`  |
| cfg member 2    | mex-cfg2    | 27102     | `mongodb://localhost:27102`  |
| cfg member 3    | mex-cfg3    | 27103     | `mongodb://localhost:27103`  |
| shard1 member a | mex-shard1a | 27111     | `mongodb://localhost:27111`  |
| shard1 member b | mex-shard1b | 27112     | `mongodb://localhost:27112`  |
| shard1 member c | mex-shard1c | 27113     | `mongodb://localhost:27113`  |

The shard / config members also resolve each other by short name inside
the compose network (e.g. `cfg1:27017`), which is what the replica-set
configs reference — so the names you see in `rs.conf()` / the reconfig
wizard match what mongod reports.

## Usage

```sh
cd testing/db-sharded
docker compose up -d

# Wait for init to finish (10–30s). Watch progress:
docker compose logs -f init

# Connect Mongo Explorer to mongodb://localhost:27100 for the
# cluster-wide (mongos) view, or directly to any member for
# node-level maintenance ops.

# Tear down everything + volumes:
docker compose down -v
```

## Idempotency

The init script detects an already-initiated replset (via
`rs.status().ok`) and skips re-initiation. A cold restart with existing
volumes therefore comes back up without side-effects. `sh.addShard` is
idempotent server-side.

## Typical workflows this supports

| v2.7 pane       | Connect to      | Exercises                         |
|-----------------|-----------------|-----------------------------------|
| Reconfig wizard | 27111 (shard1a) | Add/remove members, priority bumps |
| Rolling index   | 27100 (mongos)  | Per-member builds across shard1rs  |
| Compact wizard  | 27112 / 27113   | Compact on a non-primary secondary |
| Config drift    | 27100           | Cluster-wide getCmdLineOpts diff   |
| Upgrade planner | Any             | Runbook generation for the topology|

## Known limits

- Single shard. A second shard (`shard2rs`) + its three members would
  be a symmetric block in the compose; left out here for boot speed.
- No authentication. Adding SCRAM-SHA-256 needs a keyfile mounted
  into every container and a root user created in init; see the
  v2.6 security test rig for the pattern.
- TLS disabled. Enabling it means generating a CA + cert per member
  and mounting them; out of scope for the base rig.

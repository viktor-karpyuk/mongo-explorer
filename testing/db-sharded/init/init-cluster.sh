#!/usr/bin/env bash
# v2.7 — Bootstraps a sharded cluster (config replset + shard1 replset
# + shard registration via mongos). Idempotent: a second run against
# an already-initiated cluster exits 0 without touching state.

set -euo pipefail

CFG_RS="cfgrs"
CFG_MEMBERS=("cfg1" "cfg2" "cfg3")
SHARD_RS="shard1rs"
SHARD_MEMBERS=("shard1a" "shard1b" "shard1c")
MONGOS="mongos"

log() { echo "[init] $*"; }
warn() { echo "[init] WARN: $*" >&2; }

# Per-call connect timeout is encoded in the mongodb:// URI because
# mongosh exposes these knobs as connection-string query params, not
# CLI flags. 8 s is generous for a loopback connection inside a
# compose network; a stuck server still fails fast so the polling
# loops below can bound total wall time.
mongo_uri() { echo "mongodb://$1:27017/?serverSelectionTimeoutMS=8000&connectTimeoutMS=8000"; }

# Thin wrapper around mongosh that preserves stderr into the init
# container's log when things fail — the previous script redirected
# everything to /dev/null, which made diagnosing a broken bootstrap
# nearly impossible. On failure we replay the error lines so
# `docker compose logs mex-sharded-init` tells the full story.
run_mongosh() {
    local host=$1
    local script=$2
    local out rc=0
    out=$(mongosh --quiet "$(mongo_uri "$host")" --eval "$script" 2>&1) || rc=$?
    if [[ $rc -ne 0 ]]; then
        warn "mongosh on $host failed ($rc):"
        printf '%s\n' "$out" | sed 's/^/[init]   /' >&2
        return $rc
    fi
    printf '%s' "$out"
    return 0
}

# Poll until a host answers ping — healthcheck already gates us, but
# a fresh mongod takes an extra second or two past port-open to accept
# commands.
wait_ping() {
    local host=$1
    for i in {1..60}; do
        if mongosh --quiet "$(mongo_uri "$host")" --eval \
                'db.adminCommand("ping").ok' >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    log "FATAL: $host never answered ping"
    return 1
}

# Block until SOME member of the replset is PRIMARY. The initial
# election can pick any voting member — not necessarily the seed —
# so checking the seed's own state (as the first draft did) fails
# when a different node wins the election.
wait_primary() {
    local host=$1
    local rs=$2
    local primary="none"
    for i in {1..60}; do
        primary=$(mongosh --quiet "$(mongo_uri "$host")" --eval \
                'try { const m = rs.status().members.find(x => x.stateStr === "PRIMARY"); m ? m.name : "none" } catch (_) { "err" }' \
                2>/dev/null | tr -d '\r\n ' || true)
        if [[ -n "$primary" && "$primary" != "none" && "$primary" != "err" ]]; then
            log "$rs primary elected: $primary"
            return 0
        fi
        sleep 1
    done
    log "FATAL: $rs never elected a primary (last state: $primary)"
    return 1
}

# Block until mongos reports cfgrs reachable. ping alone succeeds
# before mongos has finished talking to cfgrs, so sh.addShard can race
# a warming mongos and fail spuriously. `listShards` round-trips to
# cfgrs and only returns ok once that round-trip works.
wait_mongos_ready() {
    local host=$1
    local out
    for i in {1..90}; do
        out=$(mongosh --quiet "$(mongo_uri "$host")" --eval \
                'try { db.adminCommand({ listShards: 1 }).ok } catch (_) { 0 }' \
                2>/dev/null | tr -d '\r\n ' || true)
        if [[ "$out" == "1" ]]; then
            log "$host ready (cfgrs reachable via listShards)"
            return 0
        fi
        sleep 1
    done
    log "FATAL: $host never reached cfgrs (last: $out)"
    return 1
}

# Initiate an rs if not already initiated. Detection: rs.status()
# throws with code 94 (NotYetInitialized) on a fresh mongod.
initiate_if_fresh() {
    local seed=$1
    local rs=$2
    local configsvr=$3
    shift 3
    local members=("$@")

    local status_code
    status_code=$(mongosh --quiet "$(mongo_uri "$seed")" --eval \
            'try { rs.status().ok } catch (e) { e.code }' 2>/dev/null \
            | tr -d '\r\n ' || true)

    if [[ "$status_code" == "1" ]]; then
        log "$rs already initiated — skipping"
        return 0
    fi

    log "initiating $rs on $seed (members: ${members[*]})"

    # Build the members array JS expression.
    local members_js="["
    local i=0
    for m in "${members[@]}"; do
        [[ $i -gt 0 ]] && members_js+=","
        members_js+="{ _id: $i, host: \"$m:27017\" }"
        i=$((i + 1))
    done
    members_js+="]"

    local cfg_flag=""
    [[ "$configsvr" == "true" ]] && cfg_flag="configsvr: true,"

    run_mongosh "$seed" "
        rs.initiate({
            _id: \"$rs\",
            $cfg_flag
            members: $members_js
        })
    " >/dev/null
}

# Register $SHARD_RS with mongos if it is not already registered.
# sh.addShard returns a server error (code 47 ShardExists) when the
# shard is already part of the cluster, so we can't rely on the
# previous script's "it's idempotent, just run it" assumption. Listing
# current shards first lets a re-run exit 0 cleanly.
add_shard_if_fresh() {
    local mongos_host=$1
    local rs_name=$2
    shift 2
    local members=("$@")

    local already
    already=$(mongosh --quiet "$(mongo_uri "$mongos_host")" --eval \
            'try { db.adminCommand({ listShards: 1 }).shards.map(s => s._id).join(",") } catch (_) { "err" }' \
            2>/dev/null | tr -d '\r\n' || true)

    # listShards output example: "shard1rs,shard2rs" or "" on a fresh
    # cluster. The check is a plain substring match against the csv so
    # the order of shards registered later does not matter.
    if [[ ",$already," == *",$rs_name,"* ]]; then
        log "$rs_name already registered with $mongos_host — skipping"
        return 0
    fi

    local spec="$rs_name/"
    local i=0
    for m in "${members[@]}"; do
        [[ $i -gt 0 ]] && spec+=","
        spec+="$m:27017"
        i=$((i + 1))
    done

    log "registering $rs_name with mongos ($spec)"
    run_mongosh "$mongos_host" "sh.addShard(\"$spec\")" >/dev/null
}

# ============================ config replset ============================
for host in "${CFG_MEMBERS[@]}"; do wait_ping "$host"; done
initiate_if_fresh "${CFG_MEMBERS[0]}" "$CFG_RS" "true" "${CFG_MEMBERS[@]}"
wait_primary "${CFG_MEMBERS[0]}" "$CFG_RS"

# ============================= shard replset =============================
for host in "${SHARD_MEMBERS[@]}"; do wait_ping "$host"; done
initiate_if_fresh "${SHARD_MEMBERS[0]}" "$SHARD_RS" "false" "${SHARD_MEMBERS[@]}"
wait_primary "${SHARD_MEMBERS[0]}" "$SHARD_RS"

# =============================== add shard ===============================
# wait_ping is not enough — mongos ping answers before mongos has
# finished handshaking with cfgrs, and sh.addShard needs that handshake.
# wait_mongos_ready round-trips through cfgrs so we only proceed when
# the router is functionally ready, not just alive.
wait_ping "$MONGOS"
wait_mongos_ready "$MONGOS"
add_shard_if_fresh "$MONGOS" "$SHARD_RS" "${SHARD_MEMBERS[@]}"

# =============================== summary ===============================
log "✓ cluster ready"
log "  mongos:   localhost:27100"
log "  cfgrs:    cfg1:27101  cfg2:27102  cfg3:27103"
log "  shard1rs: shard1a:27111  shard1b:27112  shard1c:27113"

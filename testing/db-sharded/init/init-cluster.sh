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

# Poll until a host answers ping — healthcheck already gates us, but
# a fresh mongod takes an extra second or two past port-open to accept
# commands.
wait_ping() {
    local host=$1
    for i in {1..60}; do
        if mongosh --quiet --host "$host" --eval 'db.adminCommand("ping").ok' \
                >/dev/null 2>&1; then
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
    for i in {1..60}; do
        local primary
        primary=$(mongosh --quiet --host "$host" --eval \
                'try { const m = rs.status().members.find(x => x.stateStr === "PRIMARY"); m ? m.name : "none" } catch (_) { "err" }' \
                2>/dev/null | tr -d '\r\n ' || echo "err")
        if [[ "$primary" != "none" && "$primary" != "err" && -n "$primary" ]]; then
            log "$rs primary elected: $primary"
            return 0
        fi
        sleep 1
    done
    log "FATAL: $rs never elected a primary (last: $primary)"
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
    status_code=$(mongosh --quiet --host "$seed" --eval \
            'try { rs.status().ok } catch (e) { e.code }' 2>/dev/null \
            | tr -d '\r\n ' || echo "err")

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
        ((i+=1))
    done
    members_js+="]"

    local cfg_flag=""
    [[ "$configsvr" == "true" ]] && cfg_flag="configsvr: true,"

    mongosh --host "$seed" --eval "
        rs.initiate({
            _id: \"$rs\",
            $cfg_flag
            members: $members_js
        })
    " >/dev/null
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
wait_ping "$MONGOS"

# sh.addShard is idempotent — running it against an already-registered
# shard returns ok:1 with a "Shard already exists" message.
log "registering $SHARD_RS with mongos"
mongosh --host "$MONGOS" --eval "
    sh.addShard(\"$SHARD_RS/${SHARD_MEMBERS[0]}:27017,${SHARD_MEMBERS[1]}:27017,${SHARD_MEMBERS[2]}:27017\")
" >/dev/null

# =============================== summary ===============================
log "✓ cluster ready"
log "  mongos:   localhost:27100"
log "  cfgrs:    cfg1:27101  cfg2:27102  cfg3:27103"
log "  shard1rs: shard1a:27111  shard1b:27112  shard1c:27113"

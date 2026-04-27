#!/usr/bin/env bash
# Bootstraps three independent 5-node replica sets (rsA, rsB, rsC).
# Idempotent: a second run against already-initiated replsets exits 0
# without touching state. Patterned on db-sharded/init/init-cluster.sh
# but without the mongos / sh.addShard steps — these three replsets
# stay independent.

set -euo pipefail

RS_A="rsA"
RS_A_MEMBERS=("rsA-n1" "rsA-n2" "rsA-n3" "rsA-n4" "rsA-n5")

RS_B="rsB"
RS_B_MEMBERS=("rsB-n1" "rsB-n2" "rsB-n3" "rsB-n4" "rsB-n5")

RS_C="rsC"
RS_C_MEMBERS=("rsC-n1" "rsC-n2" "rsC-n3" "rsC-n4" "rsC-n5")

log() { echo "[init] $*"; }

# Poll until a host answers ping — compose healthchecks already gate
# us, but a fresh mongod takes an extra second or two past port-open
# to accept commands.
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

# Block until SOME member of the replset is PRIMARY. Initial elections
# can pick any voting member — not necessarily the seed — so checking
# the seed's own state would be fragile on a 5-member set.
wait_primary() {
    local host=$1
    local rs=$2
    local primary="none"
    for i in {1..60}; do
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
# throws with code 94 (NotYetInitialized) on a fresh mongod; once
# initiated it returns ok=1 on every member.
initiate_if_fresh() {
    local seed=$1
    local rs=$2
    shift 2
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

    local members_js="["
    local i=0
    for m in "${members[@]}"; do
        [[ $i -gt 0 ]] && members_js+=","
        members_js+="{ _id: $i, host: \"$m:27017\" }"
        i=$((i+1))
    done
    members_js+="]"

    mongosh --quiet --host "$seed" --eval \
            "rs.initiate({ _id: \"$rs\", members: $members_js })" \
            >/dev/null
}

bootstrap_rs() {
    local rs=$1
    shift
    local members=("$@")
    local seed="${members[0]}"

    for m in "${members[@]}"; do wait_ping "$m"; done
    initiate_if_fresh "$seed" "$rs" "${members[@]}"
    wait_primary "$seed" "$rs"
}

log "bootstrapping 3 independent replsets × 5 nodes"

bootstrap_rs "$RS_A" "${RS_A_MEMBERS[@]}"
bootstrap_rs "$RS_B" "${RS_B_MEMBERS[@]}"
bootstrap_rs "$RS_C" "${RS_C_MEMBERS[@]}"

log "done — rsA / rsB / rsC are up and have elected primaries"

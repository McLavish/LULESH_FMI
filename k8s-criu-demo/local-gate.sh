#!/usr/bin/env bash
# Cluster-free dress rehearsal of the LULESH CRIU node-evacuation-to-serverless demo.
#
# Runs the two "machines" as two privileged containers on a user-defined Docker bridge network,
# then plays the whole orchestrator flow against them:
#
#   0. golden reference FIRST (safe here, unlike on the cluster: the machines are not started
#      yet, so nothing is racing toward a migration window) — a one-off container runs
#      golden_run.py and prints the non-migrated Final Origin Energy;
#   1. wait for all ranks ACTIVE, then for the FMI_MIGRATE window marker in machine-a's log:
#      the cut MUST land inside the held window (survivors parked outside any operation) or a
#      survivor blocked in a recv from a dumped peer would be stranded;
#   2. evacuate machine-a's ranks in one cut (`docker exec ... fmi-rank-agent evacuate-local`,
#      the stand-in for `kubectl exec`): dumped + staged in Redis, no restore;
#   3. `docker stop lulesh-machine-a` — the machine is GONE, like scaling its Deployment to 0;
#   4. start one fresh privileged container per evacuated rank running restore_server.py
#      (the stand-in for a cold-started lulesh-restore Knative instance, distinct hostname +
#      IP) and POST /restore to each in parallel — each request criu-restores its rank and
#      stays open while the restored LULESH computes its remaining cycles;
#   5. once the CRIU registry shows every rank RUNNING on its restore container, promote the
#      epoch from a one-off container (the orchestrator's role);
#   6. PASS iff every survivor rank exits 0 (machine-b's supervisor logs), every /restore
#      response reports exit_code 0, rank 0's migrated Final Origin Energy is STRING-identical
#      to the golden one, and the epoch is 1.
#
# This exercises everything the cluster path needs except Knative itself: the image, the
# supervisor's pid band + start gate, cross-container Direct hole-punching, Redis image
# shipping, restore-into-a-fresh-container, and the real-workload correctness proof.
#
# Requires: docker, and the demo image (default lulesh-criu-evac:dev).
set -uo pipefail

IMAGE="${LULESH_IMAGE:-lulesh-criu-evac:dev}"
NET="${NET:-lulesh-evac-net}"
NUM_PEERS="${NUM_PEERS:-8}"
RANKS_PER_MACHINE="${RANKS_PER_MACHINE:-4}"
COMM_NAME="${COMM_NAME:-lulesh-evac-ctr-$(date +%s)}"
LULESH_SIZE="${LULESH_SIZE:-15}"
LULESH_ITERS="${LULESH_ITERS:-200}"
MIGRATE_AT_CYCLE="${MIGRATE_AT_CYCLE:-100}"
MIGRATE_WINDOW_MS="${MIGRATE_WINDOW_MS:-20000}"
DEMO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

log() { echo "[driver] $*"; }
fail() { echo "[driver] FAIL: $*" >&2; exit 1; }

docker image inspect "${IMAGE}" >/dev/null 2>&1 \
    || fail "image ${IMAGE} not found (build it first: docker build -f k8s-criu-demo/Dockerfile -t ${IMAGE} . — submodules initialized)"

NAMES=(lulesh-redis lulesh-tcpunch lulesh-machine-a lulesh-machine-b)
for r in $(seq 0 $((RANKS_PER_MACHINE - 1))); do NAMES+=("lulesh-restore-${r}"); done
cleanup() {
    docker rm -f "${NAMES[@]}" >/dev/null 2>&1
    docker network rm "${NET}" >/dev/null 2>&1
}
trap cleanup EXIT
cleanup  # clear any stale leftovers from a previous run

docker network create "${NET}" >/dev/null || fail "could not create network ${NET}"

# 0. Golden reference first: nothing else is running, so there is no window to race. On the
#    cluster the orchestrator runs it LAST for exactly that reason.
log "golden reference: ${NUM_PEERS} non-migrated ranks (-s ${LULESH_SIZE} -i ${LULESH_ITERS}) in a one-off container"
GOLDEN_OUT="$(docker run --rm \
    -e NUM_PEERS="${NUM_PEERS}" -e COMM_NAME="${COMM_NAME}" \
    -e LULESH_SIZE="${LULESH_SIZE}" -e LULESH_ITERS="${LULESH_ITERS}" \
    "${IMAGE}" python3 -u /opt/fmi/app/golden_run.py)" \
    || fail "golden run failed: ${GOLDEN_OUT}"
GOLDEN_ENERGY="$(echo "${GOLDEN_OUT}" | sed -n 's/^GOLDEN_ENERGY=//p')"
[ -n "${GOLDEN_ENERGY}" ] || fail "golden run printed no GOLDEN_ENERGY (output: ${GOLDEN_OUT})"
log "golden Final Origin Energy = ${GOLDEN_ENERGY}"

log "starting redis + tcpunch on ${NET}"
docker run -d --name lulesh-redis --network "${NET}" redis:7 \
    --save "" --appendonly no >/dev/null || fail "redis start failed"
docker run -d --name lulesh-tcpunch --network "${NET}" "${IMAGE}" \
    /opt/fmi/bin/tcpunchd 10000 >/dev/null || fail "tcpunch start failed"

# START_GATE=0: this driver owns the timing (the containers start ready-to-run and we watch
# them from the first second), so the k8s start-gate handshake is unnecessary here.
run_machine() {  # name host_id base_peer_id
    docker run -d --name "$1" --network "${NET}" --privileged "${IMAGE}" \
        bash -lc "HOST_ID=$2 NUM_PEERS=${NUM_PEERS} BASE_PEER_ID=$3 RANKS_HERE=${RANKS_PER_MACHINE} \
            COMM_NAME=${COMM_NAME} REDIS_HOST=lulesh-redis TCPUNCH_HOST=lulesh-tcpunch \
            LULESH_SIZE=${LULESH_SIZE} LULESH_ITERS=${LULESH_ITERS} \
            MIGRATE_AT_CYCLE=${MIGRATE_AT_CYCLE} MIGRATE_WINDOW_MS=${MIGRATE_WINDOW_MS} \
            START_GATE=0 CONFIG_PATH=/tmp/fmi.json PID_BASE=3000 \
            python3 -u /opt/fmi/app/supervisor.py" \
        >/dev/null || fail "$1 start failed"
}
log "starting machine-a (ranks 0..$((RANKS_PER_MACHINE - 1))) and machine-b (ranks ${RANKS_PER_MACHINE}..$((NUM_PEERS - 1)))"
run_machine lulesh-machine-a machine-a 0
run_machine lulesh-machine-b machine-b "${RANKS_PER_MACHINE}"

redis() { docker exec lulesh-redis redis-cli "$@"; }
PREFIX="fmi:ft:${COMM_NAME}:"

log "waiting for all ${NUM_PEERS} ranks ACTIVE at epoch 0"
active=0
for _ in $(seq 1 200); do
    active=1
    for r in $(seq 0 $((NUM_PEERS - 1))); do
        [ "$(redis hget "${PREFIX}epoch:0:states" "${r}" 2>/dev/null)" = "ACTIVE" ] || { active=0; break; }
    done
    [ "${active}" -eq 1 ] && break
    sleep 0.5
done
[ "${active}" -eq 1 ] || fail "not all ranks reached ACTIVE (check: docker logs lulesh-machine-a)"
log "all ranks ACTIVE"

# 1. Wait for the migration window: at cycle ${MIGRATE_AT_CYCLE} every rank prints the marker
#    and holds ${MIGRATE_WINDOW_MS}ms before its dt allreduce — the only moment a cut is safe.
log "waiting for the FMI_MIGRATE window marker (cycle ${MIGRATE_AT_CYCLE}) in machine-a's log"
window=0
for _ in $(seq 1 600); do
    if docker logs lulesh-machine-a 2>&1 | grep -q "FMI_MIGRATE: window open"; then window=1; break; fi
    sleep 0.5
done
[ "${window}" -eq 1 ] || fail "migration window never opened (check: docker logs lulesh-machine-a)"

# 2. Evacuate machine-a inside the window: dump + stage in Redis (each rank's log travels with
#    its image).
log "window open — staging machine-a's ranks: docker exec fmi-rank-agent evacuate-local"
docker exec -e FMI_CRIU_EXTRA_FILES='/tmp/rank-{rank}.log' lulesh-machine-a \
    /opt/fmi/bin/fmi-rank-agent evacuate-local "${COMM_NAME}" "${NUM_PEERS}" /tmp/fmi.json \
    || fail "evacuate-local failed (check: docker exec lulesh-machine-a cat /tmp/fmi-criu-images/${COMM_NAME}/epoch-1/rank-0/dump.log)"

# 3. The machine is gone. Everything its ranks need from here on lives in Redis.
log "stopping lulesh-machine-a (the evacuated node)"
docker stop -t 2 lulesh-machine-a >/dev/null

# 4. One fresh restore container per rank (a cold-started serverless instance: fresh fs, fresh
#    pid ns, its own hostname/IP), then POST /restore to each in parallel. Responses are held
#    until the restored workers finish, so collect them in the background.
declare -a POST_PIDS=()
for r in $(seq 0 $((RANKS_PER_MACHINE - 1))); do
    docker run -d --name "lulesh-restore-${r}" --hostname "lulesh-restore-${r}" --network "${NET}" \
        --privileged \
        -e COMM_NAME="${COMM_NAME}" -e NUM_PEERS="${NUM_PEERS}" \
        -e REDIS_HOST=lulesh-redis -e TCPUNCH_HOST=lulesh-tcpunch \
        "${IMAGE}" python3 -u /opt/fmi/app/restore_server.py >/dev/null \
        || fail "lulesh-restore-${r} start failed"
done
for r in $(seq 0 $((RANKS_PER_MACHINE - 1))); do
    up=0
    for _ in $(seq 1 60); do
        if docker exec "lulesh-restore-${r}" python3 -c \
            "import urllib.request; urllib.request.urlopen('http://localhost:8080/healthz', timeout=2)" \
            >/dev/null 2>&1; then up=1; break; fi
        sleep 0.5
    done
    [ "${up}" -eq 1 ] || fail "lulesh-restore-${r} never became healthy"
done
log "POSTing ${RANKS_PER_MACHINE} parallel /restore requests"
for r in $(seq 0 $((RANKS_PER_MACHINE - 1))); do
    docker exec "lulesh-restore-${r}" python3 -c "
import json, urllib.error, urllib.request
req = urllib.request.Request('http://localhost:8080/restore',
                             data=json.dumps({'rank': ${r}}).encode(),
                             headers={'Content-Type': 'application/json'})
try:
    print(urllib.request.urlopen(req, timeout=560).read().decode())
except urllib.error.HTTPError as e:
    print(e.read().decode())
" >"${DEMO_DIR}/.restore-rank${r}.json" 2>&1 &
    POST_PIDS+=($!)
done

# 5. Restores are confirmed via the registry (the held responses only complete after promotion).
log "waiting for every evacuated rank to be RUNNING on its restore container"
relocated=0
for _ in $(seq 1 120); do
    relocated=1
    for r in $(seq 0 $((RANKS_PER_MACHINE - 1))); do
        state="$(redis hget "${PREFIX}criu:rank:${r}" state 2>/dev/null)"
        host="$(redis hget "${PREFIX}criu:rank:${r}" host_id 2>/dev/null)"
        { [ "${state}" = "RUNNING" ] && [ "${host}" = "lulesh-restore-${r}" ]; } || { relocated=0; break; }
    done
    [ "${relocated}" -eq 1 ] && break
    sleep 0.5
done
[ "${relocated}" -eq 1 ] || fail "ranks did not relocate (check: docker logs lulesh-restore-0; docker exec lulesh-restore-0 cat /tmp/fmi-criu-images/${COMM_NAME}/epoch-1/rank-0/restore.log)"
for r in $(seq 0 $((RANKS_PER_MACHINE - 1))); do
    log "rank ${r} relocated: machine-a -> $(redis hget "${PREFIX}criu:rank:${r}" host_id)"
done

# Promote from a one-off container — the orchestrator Job's role in the cluster.
log "promoting the epoch"
docker run --rm --network "${NET}" "${IMAGE}" bash -lc \
    "REDIS_HOST=lulesh-redis TCPUNCH_HOST=lulesh-tcpunch HOST_ID=driver \
        envsubst '\${REDIS_HOST} \${TCPUNCH_HOST} \${HOST_ID}' \
        </opt/fmi/app/fmi-lulesh.json.tmpl >/tmp/fmi.json \
     && /opt/fmi/bin/fmi-rank-agent promote ${COMM_NAME} ${NUM_PEERS} /tmp/fmi.json" \
    || fail "promote failed"

log "waiting for the held /restore responses (restored ranks computing cycles ${MIGRATE_AT_CYCLE}..${LULESH_ITERS})"
for pid in "${POST_PIDS[@]}"; do wait "${pid}"; done

# 6. Verify. Survivors' only success signal is exit 0 (LULESH ranks other than 0 print nothing
#    at completion), reported by machine-b's supervisor as `rank=<r> exit=0`; the reporting
#    lags the exits, so poll. Evacuated ranks report through their /restore responses.
survivors_ok=0
for _ in $(seq 1 120); do
    survivors_ok="$(docker logs lulesh-machine-b 2>&1 | grep -Ec "rank=[0-9]+ exit=0")"
    [ "${survivors_ok}" -ge $((NUM_PEERS - RANKS_PER_MACHINE)) ] && break
    sleep 0.5
done
responses_ok=0
for r in $(seq 0 $((RANKS_PER_MACHINE - 1))); do
    if grep -q '"status": "ok"' "${DEMO_DIR}/.restore-rank${r}.json" \
        && grep -q '"exit_code": 0' "${DEMO_DIR}/.restore-rank${r}.json"; then
        responses_ok=$((responses_ok + 1))
    else
        log "rank ${r} restore response not ok: $(cat "${DEMO_DIR}/.restore-rank${r}.json")"
    fi
done
# Rank 0's energy arrives in its /restore response's log_tail (rank 0 is evacuated in the
# default topology). Exact string comparison against the golden run.
MIGRATED_ENERGY="$(python3 - "${DEMO_DIR}/.restore-rank0.json" <<'PY'
import json, re, sys
try:
    tail = json.load(open(sys.argv[1])).get("log_tail", "")
except (OSError, json.JSONDecodeError):
    sys.exit(0)
m = re.search(r"Final Origin Energy\s*=\s*([0-9.eE+-]+)", tail)
if m:
    print(m.group(1))
PY
)"
EPOCH="$(redis hget "${PREFIX}meta" current_epoch 2>/dev/null)"

echo "----- machine-b supervisor exits -----"
docker logs lulesh-machine-b 2>&1 | grep -E "rank=[0-9]+ (exit|killed)" || true
for r in $(seq 0 $((RANKS_PER_MACHINE - 1))); do
    echo "----- restore response rank ${r} (head) -----"
    python3 -m json.tool "${DEMO_DIR}/.restore-rank${r}.json" 2>/dev/null | head -8 \
        || cat "${DEMO_DIR}/.restore-rank${r}.json"
done
echo "golden   Final Origin Energy = ${GOLDEN_ENERGY}"
echo "migrated Final Origin Energy = ${MIGRATED_ENERGY:-<none>}"
log "survivors_ok=${survivors_ok}/$((NUM_PEERS - RANKS_PER_MACHINE)) restored_ok=${responses_ok}/${RANKS_PER_MACHINE} final_epoch=${EPOCH}"

if [ "${survivors_ok}" -ge $((NUM_PEERS - RANKS_PER_MACHINE)) ] \
    && [ "${responses_ok}" -eq "${RANKS_PER_MACHINE}" ] \
    && [ -n "${MIGRATED_ENERGY}" ] && [ "${MIGRATED_ENERGY}" = "${GOLDEN_ENERGY}" ] \
    && [ "${EPOCH}" = "1" ]; then
    log "PASS: machine-a's ${RANKS_PER_MACHINE} LULESH ranks were evacuated mid-run into fresh restore containers (machine-a stopped); all ${NUM_PEERS} ranks finished at epoch 1 with Final Origin Energy bit-identical to the golden run"
    exit 0
fi
fail "survivors_ok=${survivors_ok} restored_ok=${responses_ok} migrated=${MIGRATED_ENERGY:-<none>} golden=${GOLDEN_ENERGY} epoch=${EPOCH}"

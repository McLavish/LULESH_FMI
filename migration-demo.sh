#!/usr/bin/env bash
# CRIU rank-migration demo for LULESH on FMI.
#
# Migrates one running LULESH rank to a fresh process image *mid-run* using CRIU,
# driven by FMI's transparent-migration control plane, and proves the migrated
# rank's full in-memory physics state survived: the run finishes with a
# "Final Origin Energy" bit-identical to a non-migrated (golden) run of the same
# binary, size, and rank count. State loss would diverge the physics, so an exact
# match is the proof.
#
# Sequence:
#   1. compute the golden energy with a normal (non-migrated, no-FT) run
#   2. launch N fault-tolerant ranks; each opens a deterministic migration window
#      at cycle MIGRATE_CYCLE (FMI shim env FMI_MIGRATE_AT_CYCLE) -- a clean
#      quiesce point at the per-cycle dt allreduce
#   3. when the window opens, request migration of MIGRATE_RANK via Redis
#   4. run fmi-migration-supervisor: it criu-dumps + criu-restores the target and
#      promotes epoch 0 -> 1
#   5. the restored rank + survivors reconfigure at epoch 1 and finish the run
#   6. assert: migrated energy == golden, epoch == 1, supervisor clean, CRIU images present
#
# Requires: a running redis-server (control plane), criu on PATH, and the
# WITH_FMI_CRIU build (build-fmi-criu/{lulesh2.0, extern/fmi/tools/fmi-migration-supervisor}).
# For rootless criu, FMI_CRIU_EXTRA_ARGS defaults to "--unprivileged".
#
# Usage:   ./migration-demo.sh
# Tunables (env): N NX ITERS MIGRATE_RANK MIGRATE_CYCLE WINDOW_MS COMM_NAME
#                 BUILD_DIR LULESH_EXE SUPERVISOR FT_CONFIG NOFT_CONFIG
#                 TCPUNCHD FMI_DIRECT_PORT IMAGES_DIR FMI_CRIU_EXTRA_ARGS
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ---- configuration (env-overridable) ----
N="${N:-8}"                       # rank count (must be a perfect cube for LULESH)
NX="${NX:-15}"                    # per-rank cube edge (-s)
ITERS="${ITERS:-200}"            # iteration cap (-i)
MIGRATE_RANK="${MIGRATE_RANK:-0}"
MIGRATE_CYCLE="${MIGRATE_CYCLE:-40}"
WINDOW_MS="${WINDOW_MS:-8000}"

BUILD="${BUILD_DIR:-${ROOT}/build-fmi-criu}"
EXE="${LULESH_EXE:-${BUILD}/lulesh2.0}"
SUPERVISOR="${SUPERVISOR:-${BUILD}/extern/fmi/tools/fmi-migration-supervisor}"
FT_CONFIG="${FT_CONFIG:-${ROOT}/fmi-lulesh-ft.json}"
NOFT_CONFIG="${NOFT_CONFIG:-${ROOT}/fmi-lulesh.json}"
TCPUNCHD="${TCPUNCHD:-${ROOT}/extern/fmi/extern/TCPunch/server/build/tcpunchd}"
PORT="${FMI_DIRECT_PORT:-10000}"
IMAGES_DIR="${IMAGES_DIR:-/tmp/fmi-criu-images}"
COMM_NAME="${COMM_NAME:-lulesh-criu-$$-$(date +%s)}"
export FMI_CRIU_EXTRA_ARGS="${FMI_CRIU_EXTRA_ARGS:---unprivileged}"
export OMP_NUM_THREADS=1

PREFIX="fmi:ft:${COMM_NAME}:"
LOGDIR="$(mktemp -d)"
FINAL_RC=0

log()  { echo "[demo] $*"; }
die()  { echo "[demo] ERROR: $*" >&2; exit 1; }
fail() { echo "[demo] FAIL: $*" >&2; FINAL_RC=1; }
extract_energy() { grep -E 'Final Origin Energy' "$1" 2>/dev/null | sed -E 's/.*=[[:space:]]*//' | tr -d '[:space:]'; }

# ---- preflight ----
[ -x "$EXE" ]         || die "missing LULESH binary: $EXE (build: cmake -S . -B build-fmi-criu -DWITH_FMI_CRIU=ON -DWITH_OPENMP=OFF && cmake --build build-fmi-criu -j)"
[ -x "$SUPERVISOR" ]  || die "missing supervisor: $SUPERVISOR"
[ -f "$FT_CONFIG" ]   || die "missing FT config: $FT_CONFIG"
[ -f "$NOFT_CONFIG" ] || die "missing non-FT config: $NOFT_CONFIG"
command -v criu      >/dev/null 2>&1 || die "criu not on PATH"
command -v redis-cli >/dev/null 2>&1 || die "redis-cli not on PATH"
redis-cli ping       >/dev/null 2>&1 || die "Redis not reachable (start redis-server)"

# Build tcpunchd on first use if missing.
if [ ! -x "$TCPUNCHD" ]; then
  log "building tcpunchd"
  cmake -S "${ROOT}/extern/fmi/extern/TCPunch/server" -B "$(dirname "$TCPUNCHD")" -DCMAKE_BUILD_TYPE=Release >/dev/null
  cmake --build "$(dirname "$TCPUNCHD")" >/dev/null
fi

# ---- tcpunchd rendezvous (reuse or start; torn down on exit) ----
port_in_use() { { ss -ltn 2>/dev/null || netstat -ltn 2>/dev/null; } | grep -q ":${PORT}[[:space:]]"; }
TPID=""
RANK_PIDS=()
cleanup() {
  [ -n "$TPID" ] && kill "$TPID" 2>/dev/null
  for p in "${RANK_PIDS[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null; done
}
trap cleanup EXIT INT TERM

if port_in_use; then
  log "reusing rendezvous server on :${PORT}"
else
  "$TCPUNCHD" "$PORT" >"${LOGDIR}/tcpunchd.log" 2>&1 &
  TPID=$!
  for _ in $(seq 1 50); do port_in_use && break; sleep 0.1; done
  port_in_use || { cat "${LOGDIR}/tcpunchd.log" >&2; die "tcpunchd failed to listen on :${PORT}"; }
fi

log "comm_name=${COMM_NAME} logs=${LOGDIR} criu_extra='${FMI_CRIU_EXTRA_ARGS}'"

# ---- 1. golden (non-migrated, no fault tolerance) ----
log "computing golden Final Origin Energy: N=${N} nx=${NX} i=${ITERS} (no migration)"
GCOMM="golden-${COMM_NAME}"
gpids=()
for ((r=0; r<N; r++)); do
  FMI_RANK="$r" FMI_WORLD_SIZE="$N" FMI_CONFIG="$NOFT_CONFIG" FMI_COMM_NAME="$GCOMM" \
    "$EXE" -s "$NX" -i "$ITERS" >"${LOGDIR}/golden-rank-${r}.log" 2>&1 &
  gpids+=($!)
done
for p in "${gpids[@]}"; do wait "$p"; done
GOLDEN="$(extract_energy "${LOGDIR}/golden-rank-0.log")"
[ -n "$GOLDEN" ] || { cat "${LOGDIR}/golden-rank-0.log" >&2; die "golden run produced no Final Origin Energy"; }
log "golden Final Origin Energy = ${GOLDEN}"

# ---- 2. clear stale control-plane keys + images for this comm ----
mapfile -t STALE < <(redis-cli --scan --pattern "${PREFIX}*" 2>/dev/null)
[ "${#STALE[@]}" -gt 0 ] && redis-cli del "${STALE[@]}" >/dev/null 2>&1
rm -rf "${IMAGES_DIR:?}/${COMM_NAME}"

# ---- 3. launch N fault-tolerant ranks with a migration window ----
log "launching ${N} FT ranks (migrate rank ${MIGRATE_RANK} at cycle ${MIGRATE_CYCLE}, window ${WINDOW_MS}ms)"
for ((r=0; r<N; r++)); do
  FMI_RANK="$r" FMI_WORLD_SIZE="$N" FMI_CONFIG="$FT_CONFIG" FMI_COMM_NAME="$COMM_NAME" \
  FMI_MIGRATE_AT_CYCLE="$MIGRATE_CYCLE" FMI_MIGRATE_WINDOW_MS="$WINDOW_MS" \
    setsid "$EXE" -s "$NX" -i "$ITERS" >"${LOGDIR}/rank-${r}.log" 2>&1 &
  RANK_PIDS+=($!)
done
R0LOG="${LOGDIR}/rank-${MIGRATE_RANK}.log"

# ---- 4. wait for all ranks ACTIVE at epoch 0 ----
log "waiting for all ${N} ranks ACTIVE at epoch 0"
active_all() {
  local r
  for ((r=0; r<N; r++)); do
    [ "$(redis-cli hget "${PREFIX}epoch:0:states" "$r" 2>/dev/null)" = "ACTIVE" ] || return 1
  done
  return 0
}
ok=0; for _ in $(seq 1 200); do active_all && { ok=1; break; }; sleep 0.2; done
if [ "$ok" != 1 ]; then
  for ((r=0; r<N; r++)); do echo "  rank $r state: $(redis-cli hget "${PREFIX}epoch:0:states" "$r" 2>/dev/null)"; done
  die "ranks did not all reach ACTIVE at epoch 0"
fi
log "all ranks ACTIVE"

# ---- 5. wait for the migration window marker on the target rank ----
log "waiting for the migration window (cycle ${MIGRATE_CYCLE}) on rank ${MIGRATE_RANK}"
ok=0; for _ in $(seq 1 600); do grep -q "FMI_MIGRATE: window open" "$R0LOG" 2>/dev/null && { ok=1; break; }; sleep 0.1; done
[ "$ok" = 1 ] || { tail -n 20 "$R0LOG" >&2; die "migration window never opened on rank ${MIGRATE_RANK}"; }
log "migration window open -> requesting migration"

# ---- 6. request migration of the target rank ----
redis-cli sadd "${PREFIX}pending" "$MIGRATE_RANK" >/dev/null
redis-cli hset "${PREFIX}epoch:0:states" "$MIGRATE_RANK" MIGRATION_PENDING >/dev/null

# ---- 7. run the supervisor: real criu dump/restore + epoch promotion ----
log "running fmi-migration-supervisor (criu dump/restore + promote epoch 1)"
SUP_OUT="$("$SUPERVISOR" migrate "$COMM_NAME" "$N" "$FT_CONFIG" "$MIGRATE_RANK" 2>&1)"; SUP_RC=$?
echo "$SUP_OUT" | sed 's/^/[supervisor] /'
DUMP_PID="$(redis-cli hget "${PREFIX}criu:rank:${MIGRATE_RANK}" pid 2>/dev/null)"

# ---- 8. wait for the restored (detached) target rank to finish ----
log "waiting for rank ${MIGRATE_RANK} to complete after restore"
MIGRATED=""
for _ in $(seq 1 400); do
  MIGRATED="$(extract_energy "$R0LOG")"
  [ -n "$MIGRATED" ] && break
  grep -qiE 'fatal|Abort|terminate called|reconfigure timeout' "$R0LOG" 2>/dev/null && break
  sleep 0.2
done
EPOCH="$(redis-cli hget "${PREFIX}meta" current_epoch 2>/dev/null)"
IMG_DIR="${IMAGES_DIR}/${COMM_NAME}/epoch-1/rank-${MIGRATE_RANK}"

# ---- verification ----
echo
echo "================= RESULT ================="
echo "golden   Final Origin Energy = ${GOLDEN}"
echo "migrated Final Origin Energy = ${MIGRATED:-<none>}"
echo "supervisor                   = rc=${SUP_RC}  ${SUP_OUT}"
echo "epoch (meta current_epoch)   = ${EPOCH:-<none>}"
echo "rank ${MIGRATE_RANK} dumped pid (Redis)      = ${DUMP_PID:-<none>}  (criu restores the same PID; the process image was replaced)"
echo "criu image tree              = ${IMG_DIR}"
[ -d "$IMG_DIR" ] && ls -1 "$IMG_DIR" 2>/dev/null | sed 's/^/    /' | head -n 12
echo "------------------------------------------"

[ -n "$MIGRATED" ]                                            || fail "migrated run produced no Final Origin Energy (the rank may have hung)"
[ -n "$MIGRATED" ] && [ "$MIGRATED" = "$GOLDEN" ]             || fail "energy mismatch (migrated=${MIGRATED:-<none>} golden=${GOLDEN}): state NOT preserved"
[ "$SUP_RC" = 0 ]                                             || fail "supervisor exited non-zero (${SUP_RC})"
echo "$SUP_OUT" | grep -q "migrated_rank=${MIGRATE_RANK} promoted_epoch=1" || fail "supervisor did not report a clean promotion"
[ "$EPOCH" = 1 ]                                              || fail "epoch did not advance to 1 (got ${EPOCH:-<none>})"
[ -f "${IMG_DIR}/dump.log" ] || [ -f "${IMG_DIR}/restore.log" ] || [ -n "$(ls -A "$IMG_DIR" 2>/dev/null)" ] || fail "no CRIU image artifacts (checkpoint/restore did not run)"

echo "=========================================="
if [ "$FINAL_RC" = 0 ]; then
  echo "[demo] PASS"
  echo "[demo]   rank ${MIGRATE_RANK} was CRIU checkpoint/restored mid-run; FMI promoted epoch 0 -> 1;"
  echo "[demo]   the run finished with Final Origin Energy identical to the non-migrated golden."
  echo "[demo]   => the migrated rank's in-memory physics state survived the migration."
else
  echo "[demo] FAIL (see assertions above; per-rank logs in ${LOGDIR})"
fi
exit "$FINAL_RC"

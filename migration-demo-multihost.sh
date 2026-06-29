#!/usr/bin/env bash
# Cross-host CRIU rank-migration demo for LULESH on FMI.
#
# This relaxes the same-host limitation (#1) of migration-demo.sh: a LULESH rank
# checkpointed on host A is *restored on a different host B* (true cross-host
# migration), driven from one driver. It proves the migrated rank's full in-memory
# physics state survived the host change: the run finishes with a "Final Origin
# Energy" bit-identical to a non-migrated (golden) run. State loss would diverge the
# physics, so an exact match is the proof.
#
# It needs **zero changes to the base FMI library**. migration-demo.sh delegates the
# CRIU cut to `fmi-rank-agent migrate` (dump+restore+promote in one host-local
# process); this driver instead *unbundles* that wrapper across two hosts in shell:
#   - request the cut  : poke Redis directly (sadd pending / hset MIGRATION_PENDING)
#   - wait for quiesce : poll the CRIU registry until the rank marks itself QUIESCED
#                        and writes its pid (the rank does this on its own, reacting
#                        to the Redis request -- not the agent)
#   - dump on SRC      : ssh SRC `criu dump`   (flags identical to LocalRankAgent::dump_rank)
#   - restore on DST   : ssh DST `criu restore` (flags identical to LocalRankAgent::restore_rank)
#   - promote epoch    : redis EVAL of the verbatim promote_epoch Lua (ControlPlane.cpp:472)
# Everything the restored rank does next (poll epoch -> rejoin -> reconfigure ->
# re-pair Direct via tcpunchd from B's IP) is already built into FMI; the data plane
# (Direct/tcpunchd) and control plane (Redis) are both config-addressed, so a rank
# restored on B with a new IP re-pairs automatically.
#
# Topology (all addresses config-driven from $FT_CONFIG, shared by every node):
#   - shared Redis        : fault_tolerance.control_host / control_port
#   - shared tcpunchd RDV : backends.Direct.host / port
#   - shared NFS images   : fault_tolerance.criu.images_dir  (identical path everywhere)
# The driver runs each host's command locally when that host IS this machine and over
# ssh otherwise (so it works co-located with one node, and needs ssh only for remote
# nodes). Set HOSTS to one host (== this machine) to smoke-test the whole unbundled
# flow on a single box without ssh.
#
# Requires: passwordless ssh driver->remote hosts; a shared redis-server + tcpunchd
# reachable from every host; the NFS images_dir mounted identically on all hosts;
# criu on PATH on every host (rootless via FMI_CRIU_EXTRA_ARGS="--unprivileged" by
# default); and the WITH_FMI_CRIU build of lulesh2.0 on a path valid on every host
# (e.g. the same NFS tree). The rank agent is NOT used here.
#
# Usage:   HOSTS="A B" MIGRATE_SRC=A MIGRATE_DST=B ./migration-demo-multihost.sh
#          ./migration-demo-multihost.sh           # single-box smoke test (no ssh)
# Tunables (env): N NX ITERS MIGRATE_RANK MIGRATE_CYCLE WINDOW_MS MAX_ATTEMPTS
#                 RANK_TIMEOUT HOSTS PLACEMENT MIGRATE_SRC MIGRATE_DST SSH_CMD
#                 LOCAL_ALIASES KEEP_ARTIFACTS COMM_NAME BUILD_DIR LULESH_EXE
#                 FT_CONFIG NOFT_CONFIG TCPUNCHD IMAGES_DIR SHARED_DIR LOG_DIR
#                 FMI_CRIU_EXTRA_ARGS
# The rendezvous port / hosts come from the config (see fmi-common.sh getters).
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=fmi-common.sh
. "${ROOT}/fmi-common.sh"

# ---- configuration (env-overridable) ----
N="${N:-8}"                          # rank count (must be a perfect cube for LULESH)
NX="${NX:-15}"                       # per-rank cube edge (-s)
ITERS="${ITERS:-200}"                # iteration cap (-i)
MIGRATE_RANK="${MIGRATE_RANK:-0}"
MIGRATE_CYCLE="${MIGRATE_CYCLE:-40}"
WINDOW_MS="${WINDOW_MS:-8000}"
MAX_ATTEMPTS="${MAX_ATTEMPTS:-3}"    # retries around transient FMI Direct pairing failures
RANK_TIMEOUT="${RANK_TIMEOUT:-120}"  # hard per-rank wall-clock cap (s) for the golden run

# ---- host topology ----
HOSTS="${HOSTS:-$(hostname)}"        # space-separated compute hosts
read -r -a HOST_ARR <<< "$HOSTS"
NUM_HOSTS=${#HOST_ARR[@]}
[ "$NUM_HOSTS" -ge 1 ] || { echo "[mh-demo] ERROR: HOSTS is empty" >&2; exit 1; }
MIGRATE_SRC="${MIGRATE_SRC:-${HOST_ARR[0]}}"
# default DST = the second host if there is one, else the only host (single-box smoke test)
if [ "$NUM_HOSTS" -ge 2 ]; then MIGRATE_DST="${MIGRATE_DST:-${HOST_ARR[1]}}"; else MIGRATE_DST="${MIGRATE_DST:-${HOST_ARR[0]}}"; fi
SSH_CMD="${SSH_CMD:-ssh -o BatchMode=yes -o StrictHostKeyChecking=accept-new}"
LOCAL_ALIASES="${LOCAL_ALIASES:-}"   # extra names that also mean "this machine"

BUILD="${BUILD_DIR:-${ROOT}/build-fmi-criu}"
EXE="${LULESH_EXE:-${BUILD}/lulesh2.0}"
FT_CONFIG="${FT_CONFIG:-${ROOT}/fmi-lulesh-ft-multihost.json}"
NOFT_CONFIG="${NOFT_CONFIG:-${ROOT}/fmi-lulesh.json}"
TCPUNCHD="${TCPUNCHD:-${ROOT}/extern/fmi/extern/TCPunch/server/build/tcpunchd}"
IMAGES_DIR="${IMAGES_DIR:-$(fmi_config_images_dir "$FT_CONFIG")}"
COMM_NAME="${COMM_NAME:-lulesh-mh-$$-$(date +%s)}"
COMM_BASE="$COMM_NAME"
export FMI_CRIU_EXTRA_ARGS="${FMI_CRIU_EXTRA_ARGS:---unprivileged}"

# Shared run dir for per-rank logs/pidfiles. Must be on the same shared (NFS) tree as
# IMAGES_DIR so the driver can read every host's rank logs regardless of which host
# produced them. Defaults under IMAGES_DIR so one shared mount covers everything.
SHARED_DIR="${SHARED_DIR:-${IMAGES_DIR%/}}"
LOGDIR="${LOG_DIR:-${SHARED_DIR}/.runs/${COMM_BASE}}"

# Shared rendezvous / control-plane addresses, read from the SAME config the ranks
# load (so the driver never carries an address FMI does not see).
CTRL_HOST="$(fmi_config_control_host "$FT_CONFIG")"
CTRL_PORT="$(fmi_config_control_port "$FT_CONFIG")"
RDV_HOST="$(fmi_config_direct_host "$FT_CONFIG")"
PORT="$(fmi_config_port "$FT_CONFIG")"

PREFIX="fmi:ft:${COMM_NAME}:"
FINAL_RC=0

log()  { echo "[mh-demo] $*"; }
die()  { echo "[mh-demo] ERROR: $*" >&2; exit 1; }
fail() { echo "[mh-demo] FAIL: $*" >&2; FINAL_RC=1; }
extract_energy() { grep -E 'Final Origin Energy' "$1" 2>/dev/null | sed -E 's/.*=[[:space:]]*//' | tr -d '[:space:]'; }
# Every control-plane poke goes to the shared Redis named in the config.
rc() { redis-cli -h "$CTRL_HOST" -p "$CTRL_PORT" "$@"; }

# is_local HOST -> 0 if HOST names this machine (run directly, no ssh).
is_local() {
  local h="$1" a
  case "$h" in localhost|127.0.0.1|::1) return 0;; esac
  [ "$h" = "$(hostname)" ] && return 0
  [ "$h" = "$(hostname -s 2>/dev/null)" ] && return 0
  [ "$h" = "$(hostname -f 2>/dev/null)" ] && return 0
  for a in $LOCAL_ALIASES; do [ "$h" = "$a" ] && return 0; done
  return 1
}

# on_host HOST CMD -> run CMD on HOST (CMD is one shell string): locally when HOST is
# this machine, else over ssh. The remote shell sees CMD verbatim, so paths inside it
# must be valid on HOST (true for the shared NFS image/log paths and the shared EXE).
on_host() {
  local h="$1"; shift
  if is_local "$h"; then bash -c "$*"; else $SSH_CMD "$h" "$*"; fi
}

# host_for_rank RANK -> which host runs RANK. PLACEMENT="h0 h1 ..." (indexed by rank)
# overrides; otherwise round-robin across HOSTS, forcing the migrating rank onto SRC.
host_for_rank() {
  local r="$1" arr
  if [ -n "${PLACEMENT:-}" ]; then read -r -a arr <<< "$PLACEMENT"; echo "${arr[$r]}"; return; fi
  [ "$r" = "$MIGRATE_RANK" ] && { echo "$MIGRATE_SRC"; return; }
  echo "${HOST_ARR[$(( r % NUM_HOSTS ))]}"
}

# launch_rank RANK COMM CONFIG HOST [EXTRA_ENV] -> start one detached rank on HOST,
# writing its log + session-leader pid (pgid) to the shared LOGDIR. Ranks never run
# criu themselves, so they carry no FMI_CRIU_EXTRA_ARGS.
launch_rank() {
  local r="$1" comm="$2" cfg="$3" host="$4" extra="${5:-}"
  local logf="${LOGDIR}/rank-${r}.log" pidf="${LOGDIR}/rank-${r}.pid"
  : > "$logf"; rm -f "$pidf"
  local env="FMI_RANK=$r FMI_WORLD_SIZE=$N FMI_CONFIG='$cfg' FMI_COMM_NAME='$comm' OMP_NUM_THREADS=1 $extra"
  on_host "$host" "$env setsid nohup '$EXE' -s $NX -i $ITERS >'$logf' 2>&1 & echo \$! >'$pidf'"
}

# kill every rank we launched (best effort) on its host: TERM the whole session group.
kill_all_ranks() {
  local r host pid
  for ((r=0; r<N; r++)); do
    [ -f "${LOGDIR}/rank-${r}.pid" ] || continue
    pid="$(cat "${LOGDIR}/rank-${r}.pid" 2>/dev/null)"
    [ -n "$pid" ] || continue
    host="$(host_for_rank "$r")"
    on_host "$host" "kill -TERM -- -$pid 2>/dev/null; kill -TERM $pid 2>/dev/null" >/dev/null 2>&1 || true
    # the migrated rank is restored on DST under the same pid -- reap it there too.
    [ "$r" = "$MIGRATE_RANK" ] && on_host "$MIGRATE_DST" "kill -TERM -- -$pid 2>/dev/null; kill -TERM $pid 2>/dev/null" >/dev/null 2>&1 || true
  done
}

# ---- preflight ----
[ -x "$EXE" ]         || die "missing LULESH binary: $EXE (build: cmake -S . -B build-fmi-criu -DWITH_FMI_CRIU=ON -DWITH_OPENMP=OFF && cmake --build build-fmi-criu -j)"
[ -f "$FT_CONFIG" ]   || die "missing FT config: $FT_CONFIG"
[ -f "$NOFT_CONFIG" ] || die "missing non-FT config: $NOFT_CONFIG"
command -v criu      >/dev/null 2>&1 || die "criu not on PATH (driver host; it must also be on every compute host)"
command -v timeout   >/dev/null 2>&1 || die "timeout (coreutils) not on PATH"
command -v redis-cli >/dev/null 2>&1 || die "redis-cli not on PATH"
rc ping              >/dev/null 2>&1 || die "shared Redis not reachable at ${CTRL_HOST}:${CTRL_PORT}"
mkdir -p "$LOGDIR"   || die "cannot create shared log dir: $LOGDIR (must be on a shared mount visible to all hosts)"

log "comm_name=${COMM_NAME} hosts='${HOSTS}' src=${MIGRATE_SRC} dst=${MIGRATE_DST} rank=${MIGRATE_RANK}"
log "redis=${CTRL_HOST}:${CTRL_PORT} rdv=${RDV_HOST}:${PORT} images=${IMAGES_DIR} logs=${LOGDIR}"
log "criu_extra='${FMI_CRIU_EXTRA_ARGS}'"

# ---- tcpunchd rendezvous (shared) ----
fmi_build_tcpunchd_if_missing "$TCPUNCHD" "$ROOT"
TPID=""
if is_local "$RDV_HOST"; then
  TPID="$(fmi_start_tcpunchd "$TCPUNCHD" "$PORT" "${LOGDIR}/tcpunchd.log")" \
    || die "could not bring up tcpunchd on ${RDV_HOST}:${PORT}"
else
  # Start (or reuse) tcpunchd on the rendezvous host; we do not tear down a shared
  # remote rendezvous on exit (other jobs may share it). Binary must exist on RDV_HOST.
  on_host "$RDV_HOST" "pgrep -f 'tcpunchd ${PORT}' >/dev/null 2>&1 || (setsid nohup '$TCPUNCHD' ${PORT} >'${LOGDIR}/tcpunchd.log' 2>&1 & echo started)" \
    || log "WARNING: could not confirm tcpunchd on ${RDV_HOST}:${PORT} (assuming it is already running)"
  log "using tcpunchd on ${RDV_HOST}:${PORT}"
fi

cleanup() {
  kill_all_ranks
  [ -n "$TPID" ] && kill "$TPID" 2>/dev/null
  if [ -z "${KEEP_ARTIFACTS:-}" ] && [ -n "${COMM_BASE:-}" ]; then
    local stale_keys
    mapfile -t stale_keys < <(rc --scan --pattern "fmi:ft:${COMM_BASE}*" 2>/dev/null)
    [ "${#stale_keys[@]}" -gt 0 ] && rc del "${stale_keys[@]}" >/dev/null 2>&1
    # image trees + logs live on the shared mount, so the driver removes them directly.
    [ -n "${IMAGES_DIR:-}" ] && rm -rf "${IMAGES_DIR:?}/${COMM_BASE}"*
    [ -n "${SHARED_DIR:-}" ] && rm -rf "${SHARED_DIR:?}/.runs/${COMM_BASE}"*
  fi
}
trap cleanup EXIT INT TERM

# ---- 1. golden (non-migrated, no fault tolerance) ----
# LULESH's Final Origin Energy is a deterministic function of (binary, N, NX, ITERS)
# only -- independent of host and of FT -- so the reference is computed by running all
# N non-FT ranks on the rendezvous host (where the NOFT config's 127.0.0.1 Direct host
# resolves to the shared tcpunchd). Retried like migration-demo.sh against transient
# Direct pairing timeouts.
compute_golden() {
  local attempt gcomm r p
  for ((attempt=1; attempt<=MAX_ATTEMPTS; attempt++)); do
    gcomm="golden-${COMM_BASE}-a${attempt}"
    for ((r=0; r<N; r++)); do
      launch_rank "$r" "$gcomm" "$NOFT_CONFIG" "$RDV_HOST" "" >/dev/null 2>&1
    done
    # wait (bounded) for rank 0 to print the energy, killing the batch when done/stuck
    GOLDEN=""
    for _ in $(seq 1 $((RANK_TIMEOUT*5))); do
      GOLDEN="$(extract_energy "${LOGDIR}/rank-0.log")"
      [ -n "$GOLDEN" ] && break
      sleep 0.2
    done
    for ((r=0; r<N; r++)); do
      p="$(cat "${LOGDIR}/rank-${r}.pid" 2>/dev/null)"
      [ -n "$p" ] && on_host "$RDV_HOST" "kill -TERM -- -$p 2>/dev/null; kill -TERM $p 2>/dev/null" >/dev/null 2>&1 || true
    done
    [ -n "$GOLDEN" ] && return 0
    log "golden attempt ${attempt}/${MAX_ATTEMPTS}: no energy (transient); retrying"
  done
  return 1
}
log "computing golden Final Origin Energy: N=${N} nx=${NX} i=${ITERS} (no migration, on ${RDV_HOST})"
compute_golden || { cat "${LOGDIR}/rank-0.log" >&2; die "golden run produced no Final Origin Energy after ${MAX_ATTEMPTS} attempts"; }
log "golden Final Origin Energy = ${GOLDEN}"

# All ranks ACTIVE at epoch 0? (reads the global PREFIX, set per attempt.)
active_all() {
  local r
  for ((r=0; r<N; r++)); do
    [ "$(rc hget "${PREFIX}epoch:0:states" "$r" 2>/dev/null)" = "ACTIVE" ] || return 1
  done
  return 0
}

# wait_quiesced -> echo the target rank's pid once it has marked itself QUIESCED at
# generation 1 with a pid set (the rank does this itself on seeing the Redis request;
# this is the LocalRankAgent::wait_for_ready_ranks condition). It is also the NFS read
# barrier: once QUIESCED, the rank is frozen in its promotion-wait loop and its memory
# is stable for the dump. Returns 1 on a fatal rank or timeout.
wait_quiesced() {
  local r="$MIGRATE_RANK" v
  for _ in $(seq 1 600); do
    mapfile -t v < <(rc hmget "${PREFIX}criu:rank:${r}" state quiesced_generation pid 2>/dev/null)
    if [ "${v[0]:-}" = "QUIESCED" ] && [ "${v[1]:-}" = "1" ] && [ -n "${v[2]:-}" ] && [ "${v[2]:-0}" -gt 0 ] 2>/dev/null; then
      echo "${v[2]}"; return 0
    fi
    grep -qiE 'fatal|terminate called' "${LOGDIR}"/rank-*.log 2>/dev/null && return 1
    sleep 0.1
  done
  return 1
}

# Verbatim promote_epoch Lua from FMI's ControlPlane.cpp:472 (promote_epoch). Keep this
# byte-identical to the library: it flips current_epoch atomically AND GCs the leaving
# epoch's per-epoch hashes (members/states/placement). KEYS: 1=meta 2=pending; ARGV:
# 1=next_epoch 2="<prefix>epoch:" 3=":members" 4=":states" 5=":placement".
PROMOTE_LUA="local current = redis.call('HGET', KEYS[1], 'current_epoch') if not current then current = '0' end if tonumber(ARGV[1]) > tonumber(current) then redis.call('HSET', KEYS[1], 'current_epoch', ARGV[1]) redis.call('DEL', KEYS[2]) redis.call('DEL', ARGV[2]..current..ARGV[3], ARGV[2]..current..ARGV[4], ARGV[2]..current..ARGV[5]) return 1 end return 0"

# One full cross-host migrate-a-rank attempt under its own comm_name.
#   0 : restored run produced a Final Origin Energy
#   1 : transient failure (ranks never ACTIVE / window never opened / no energy) -> retry
#   2 : hard failure (a rank crashed/aborted after migration, or a criu step failed) -> no retry
attempt_migration() {
  local attempt="$1" ok=0 fataled=0 r p
  COMM_NAME="${COMM_BASE}-a${attempt}"
  PREFIX="fmi:ft:${COMM_NAME}:"
  MIGLOG="${LOGDIR}/rank-${MIGRATE_RANK}.log"
  E0LOG="${LOGDIR}/rank-0.log"
  IMG_DIR="${IMAGES_DIR}/${COMM_NAME}/epoch-1/rank-${MIGRATE_RANK}"
  MIGRATED=""; EPOCH=""; DUMP_PID=""; DUMP_RC=1; RESTORE_RC=1; PROMOTE_RC=1; SRC_PS="?"; DST_PS="?"

  # fresh control-plane keys + images for this attempt's comm
  mapfile -t STALE < <(rc --scan --pattern "${PREFIX}*" 2>/dev/null)
  [ "${#STALE[@]}" -gt 0 ] && rc del "${STALE[@]}" >/dev/null 2>&1
  rm -rf "${IMAGES_DIR:?}/${COMM_NAME}"

  log "attempt ${attempt}/${MAX_ATTEMPTS}: launching ${N} FT ranks across '${HOSTS}' (migrate rank ${MIGRATE_RANK} ${MIGRATE_SRC}->${MIGRATE_DST} at cycle ${MIGRATE_CYCLE})"
  for ((r=0; r<N; r++)); do
    launch_rank "$r" "$COMM_NAME" "$FT_CONFIG" "$(host_for_rank "$r")" \
      "FMI_MIGRATE_AT_CYCLE=$MIGRATE_CYCLE FMI_MIGRATE_WINDOW_MS=$WINDOW_MS"
  done

  # wait for all ranks ACTIVE at epoch 0
  for _ in $(seq 1 150); do active_all && { ok=1; break; }; sleep 0.2; done
  [ "$ok" = 1 ] || { log "attempt ${attempt}: ranks did not all reach ACTIVE"; return 1; }
  log "all ranks ACTIVE"

  # wait for the migration window on the target rank; bail early on any fatal
  ok=0
  for _ in $(seq 1 450); do
    grep -q "FMI_MIGRATE: window open" "$MIGLOG" 2>/dev/null && { ok=1; break; }
    grep -qiE 'fatal|terminate called' "${LOGDIR}"/rank-*.log 2>/dev/null && break
    sleep 0.1
  done
  [ "$ok" = 1 ] || { log "attempt ${attempt}: window never opened (transient pairing failure)"; return 1; }
  log "migration window open -> requesting cut"

  # 2. request the cut (exactly as migration-demo.sh:196-197, but on the shared Redis)
  rc sadd "${PREFIX}pending" "$MIGRATE_RANK" >/dev/null
  rc hset "${PREFIX}epoch:0:states" "$MIGRATE_RANK" MIGRATION_PENDING >/dev/null

  # 3. wait for the rank to quiesce on SRC and publish its pid
  DUMP_PID="$(wait_quiesced)" || { log "attempt ${attempt}: rank ${MIGRATE_RANK} never quiesced"; return 1; }
  log "rank ${MIGRATE_RANK} QUIESCED@gen=1 pid=${DUMP_PID} on ${MIGRATE_SRC}"

  # 4. dump on SRC  (flags == LocalRankAgent::dump_rank: -t pid -D dir -o dump.log --shell-job --tcp-close [extra])
  on_host "$MIGRATE_SRC" "mkdir -p '$IMG_DIR'"
  on_host "$MIGRATE_SRC" "criu dump -t $DUMP_PID -D '$IMG_DIR' -o dump.log --shell-job --tcp-close ${FMI_CRIU_EXTRA_ARGS}"
  DUMP_RC=$?
  [ "$DUMP_RC" = 0 ] || { log "attempt ${attempt}: criu dump failed on ${MIGRATE_SRC} (rc=${DUMP_RC})"; [ -f "${IMG_DIR}/dump.log" ] && tail -n 5 "${IMG_DIR}/dump.log" | sed 's/^/[dump] /'; return 2; }
  # dump reaps the rank on SRC: its pid is now free here (snapshot before the restore reclaims it).
  SRC_PS="$(on_host "$MIGRATE_SRC" "ps -p $DUMP_PID -o pid= 2>/dev/null" 2>/dev/null | tr -d '[:space:]')"
  log "criu dump OK on ${MIGRATE_SRC} (image on shared NFS: ${IMG_DIR})"

  # 5. restore on DST  (flags == LocalRankAgent::restore_rank: -D dir -o restore.log --shell-job --tcp-close --restore-detached [extra])
  on_host "$MIGRATE_DST" "criu restore -D '$IMG_DIR' -o restore.log --shell-job --tcp-close --restore-detached ${FMI_CRIU_EXTRA_ARGS}"
  RESTORE_RC=$?
  [ "$RESTORE_RC" = 0 ] || { log "attempt ${attempt}: criu restore failed on ${MIGRATE_DST} (rc=${RESTORE_RC})"; [ -f "${IMG_DIR}/restore.log" ] && tail -n 8 "${IMG_DIR}/restore.log" | sed 's/^/[restore] /'; return 2; }
  # restore recreated the rank on DST at its original pid; it is alive now, parked in its
  # promotion-wait loop (snapshot before the promote below releases it to run to completion).
  DST_PS="$(on_host "$MIGRATE_DST" "ps -p $DUMP_PID -o pid= 2>/dev/null" 2>/dev/null | tr -d '[:space:]')"
  log "criu restore OK on ${MIGRATE_DST} (rank ${MIGRATE_RANK} now lives on ${MIGRATE_DST})"

  # 6. promote the epoch (releases the restored rank + every survivor into epoch 1)
  rc EVAL "$PROMOTE_LUA" 2 "${PREFIX}meta" "${PREFIX}pending" 1 "${PREFIX}epoch:" ":members" ":states" ":placement" >/dev/null
  PROMOTE_RC=$?
  [ "$PROMOTE_RC" = 0 ] || { log "attempt ${attempt}: promote_epoch EVAL failed (rc=${PROMOTE_RC})"; return 2; }
  log "promoted epoch 0 -> 1"

  # 7. wait for the run to finish (energy printed by rank 0); a crash now is a hard fail
  log "waiting for the run to complete after cross-host restore"
  for _ in $(seq 1 400); do
    MIGRATED="$(extract_energy "$E0LOG")"
    [ -n "$MIGRATED" ] && break
    grep -qiE 'fatal|Abort|terminate called|reconfigure timeout' "${LOGDIR}"/rank-*.log 2>/dev/null && { fataled=1; break; }
    sleep 0.2
  done
  EPOCH="$(rc hget "${PREFIX}meta" current_epoch 2>/dev/null)"
  if [ -z "$MIGRATED" ]; then
    [ "$fataled" = 1 ] && { log "attempt ${attempt}: a rank crashed/aborted after migration"; return 2; }
    log "attempt ${attempt}: no energy after restore (transient)"
    return 1
  fi
  return 0
}

# ---- 2. migrate one rank cross-host, retrying only transient pairing failures ----
got_result=0
for ((a=1; a<=MAX_ATTEMPTS; a++)); do
  attempt_migration "$a"; mrc=$?
  [ "$mrc" = 0 ] && { got_result=1; break; }
  kill_all_ranks
  [ "$mrc" = 2 ] \
    && die "hard failure on attempt ${a} (criu step failed or a rank crashed after migration): NOT a transient pairing flake -- investigate possible state loss/regression (logs in ${LOGDIR})"
  sleep 1
done
[ "$got_result" = 1 ] \
  || die "no attempt produced a completed cross-host migrated run after ${MAX_ATTEMPTS} tries (transient FMI Direct pairing failures under load)"

# ---- verification ----
# SRC_PS / DST_PS were snapshotted at the only race-free moments: SRC right after the
# dump reaped the rank (pid must be gone there), DST right after the restore recreated
# it (pid must be alive there, parked pre-promotion). By the time the run completes the
# restored rank has exited normally, so a snapshot *now* would read <gone> on both.
echo
echo "================= RESULT ================="
echo "golden   Final Origin Energy = ${GOLDEN}"
echo "migrated Final Origin Energy = ${MIGRATED:-<none>}"
echo "migrate                      = rank ${MIGRATE_RANK}: ${MIGRATE_SRC} (dump) -> ${MIGRATE_DST} (restore)"
echo "criu dump/restore/promote rc = ${DUMP_RC}/${RESTORE_RC}/${PROMOTE_RC}"
echo "epoch (meta current_epoch)   = ${EPOCH:-<none>}"
echo "rank ${MIGRATE_RANK} pid (Redis)           = ${DUMP_PID:-<none>}  (criu restores the same pid on ${MIGRATE_DST})"
echo "  pid on SRC ${MIGRATE_SRC} after dump    = ${SRC_PS:-<gone>}  (expect gone: dump reaped it)"
echo "  pid on DST ${MIGRATE_DST} after restore = ${DST_PS:-<gone>}  (expect present: restored alive)"
echo "criu image tree (shared NFS) = ${IMG_DIR}"
[ -d "$IMG_DIR" ] && ls -1 "$IMG_DIR" 2>/dev/null | sed 's/^/    /' | head -n 12
echo "------------------------------------------"

[ -n "$MIGRATED" ]                                || fail "migrated run produced no Final Origin Energy (the rank may have hung)"
[ -n "$MIGRATED" ] && [ "$MIGRATED" = "$GOLDEN" ] || fail "energy mismatch (migrated=${MIGRATED:-<none>} golden=${GOLDEN}): state NOT preserved across hosts"
[ "$DUMP_RC" = 0 ] && [ "$RESTORE_RC" = 0 ] && [ "$PROMOTE_RC" = 0 ] || fail "a criu/promote step failed (dump=${DUMP_RC} restore=${RESTORE_RC} promote=${PROMOTE_RC})"
[ "$EPOCH" = 1 ]                                  || fail "epoch did not advance to 1 (got ${EPOCH:-<none>})"
[ -f "${IMG_DIR}/dump.log" ]                      || fail "no dump.log on SRC (dump did not run)"
[ -f "${IMG_DIR}/restore.log" ]                   || fail "no restore.log (restore did not run on DST)"
# the restored rank must be alive on DST under its original pid
[ -n "$DST_PS" ]                                  || fail "restored rank not found on DST ${MIGRATE_DST} (pid ${DUMP_PID})"
# and gone from SRC (only meaningful when SRC != DST; on a single-box smoke test SRC==DST)
if [ "$MIGRATE_SRC" != "$MIGRATE_DST" ]; then
  [ -z "$SRC_PS" ]                                || fail "old rank still present on SRC ${MIGRATE_SRC} (pid ${DUMP_PID})"
fi

echo "=========================================="
if [ "$FINAL_RC" = 0 ]; then
  echo "[mh-demo] PASS"
  if [ "$MIGRATE_SRC" != "$MIGRATE_DST" ]; then
    echo "[mh-demo]   rank ${MIGRATE_RANK} was CRIU-dumped on ${MIGRATE_SRC} and restored on ${MIGRATE_DST} mid-run;"
    echo "[mh-demo]   FMI promoted epoch 0 -> 1 over the shared Redis; the rank re-paired Direct from ${MIGRATE_DST};"
    echo "[mh-demo]   the run finished with Final Origin Energy identical to the non-migrated golden."
    echo "[mh-demo]   => the migrated rank's in-memory physics state survived a CROSS-HOST migration."
  else
    echo "[mh-demo]   (single-box smoke test: SRC==DST) the unbundled dump/restore/promote flow ran"
    echo "[mh-demo]   end-to-end and the run matched the golden energy. Set HOSTS=\"A B\" for a true"
    echo "[mh-demo]   cross-host migration."
  fi
else
  echo "[mh-demo] FAIL (see assertions above; per-rank logs in ${LOGDIR})"
fi
exit "$FINAL_RC"

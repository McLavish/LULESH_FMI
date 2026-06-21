#!/usr/bin/env bash
# Launch LULESH on the FMI backend (Direct/TCP transport).
#
# Unlike MPI there is no mpirun: each rank is a separate process that learns its
# identity from environment variables, and a tcpunchd rendezvous server provides
# TCP NAT hole-punching for the Direct channel. This script starts tcpunchd and
# spawns the rank processes.
#
# Usage:   ./run-fmi.sh <num_ranks> [lulesh args...]
# Example: ./run-fmi.sh 8 -s 10 -i 20
#
# <num_ranks> must be a perfect cube (1, 8, 27, 64, ...), as LULESH decomposes
# the domain into a cubic processor grid.
#
# Env overrides: LULESH_EXE, FMI_CONFIG, TCPUNCHD, FMI_COMM_NAME, OMP_NUM_THREADS
# (defaults to 1 to avoid thread oversubscription across ranks). The rendezvous
# port is taken from the config's backends.Direct.port (the port FMI pairs on);
# to change it, edit the JSON or point FMI_CONFIG at a different file.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=fmi-common.sh
. "${ROOT}/fmi-common.sh"

if [ "$#" -lt 1 ]; then
  echo "usage: $0 <num_ranks> [lulesh args...]" >&2
  exit 2
fi
N="$1"; shift
LULESH_ARGS=("$@")

EXE="${LULESH_EXE:-${ROOT}/build-fmi/lulesh2.0}"
CONFIG="${FMI_CONFIG:-${ROOT}/fmi-lulesh.json}"
TCPUNCHD="${TCPUNCHD:-${ROOT}/extern/fmi/extern/TCPunch/server/build/tcpunchd}"
COMM_NAME="${FMI_COMM_NAME:-lulesh-$$-$(date +%s)}"
export OMP_NUM_THREADS="${OMP_NUM_THREADS:-1}"

[ -x "$EXE" ]      || { echo "executable not found: $EXE (build with: cmake -S . -B build-fmi -DWITH_FMI=ON && cmake --build build-fmi)" >&2; exit 1; }
[ -f "$CONFIG" ]   || { echo "config not found: $CONFIG" >&2; exit 1; }
PORT="$(fmi_config_port "$CONFIG")"

# tcpunchd: build on first use, then reuse an existing rendezvous server on this
# port or start our own (torn down on exit, including when killed by `timeout`). A
# stale tcpunchd holding the port with leftover state is the classic pairing hang.
fmi_build_tcpunchd_if_missing "$TCPUNCHD" "$ROOT"

TPLOG="$(mktemp)"
TPID=""
cleanup() { [ -n "$TPID" ] && kill "$TPID" 2>/dev/null; rm -f "$TPLOG"; }
trap cleanup EXIT INT TERM

TPID="$(fmi_start_tcpunchd "$TCPUNCHD" "$PORT" "$TPLOG")" \
  || { echo "[run-fmi] could not bring up tcpunchd on port $PORT" >&2; exit 1; }

LOGDIR="$(mktemp -d)"
echo "[run-fmi] N=$N comm_name=$COMM_NAME port=$PORT logs=$LOGDIR"

pids=()
for ((r=0; r<N; r++)); do
  FMI_RANK="$r" FMI_WORLD_SIZE="$N" FMI_CONFIG="$CONFIG" FMI_COMM_NAME="$COMM_NAME" \
    "$EXE" "${LULESH_ARGS[@]}" >"${LOGDIR}/rank-${r}.log" 2>&1 &
  pids+=($!)
done

rc=0
for p in "${pids[@]}"; do wait "$p" || rc=1; done

# tcpunchd is torn down by the EXIT trap.
echo "===== rank 0 output ====="
cat "${LOGDIR}/rank-0.log"
[ "$rc" -ne 0 ] && echo "[run-fmi] WARNING: at least one rank exited non-zero (per-rank logs in ${LOGDIR})" >&2
exit "$rc"

#!/usr/bin/env bash
# Shared helpers for the FMI launchers (run-fmi.sh, migration-demo.sh).
#
# This is the single source of truth for the Direct/TCP rendezvous (tcpunchd)
# bring-up. Source it; do not execute it.
#
# The rendezvous PORT is read from the FMI config's backends.Direct.port, which is
# the port FMI itself pairs on -- the scripts must agree with the config rather than
# carry an independent port that FMI never sees. To run on a different port, change
# the port in the JSON config (or point FMI_CONFIG at a different file); there is no
# separate env knob, because FMI has no env override for the Direct port.

# Echo backends.Direct.port from an FMI JSON config (falls back to 10000).
fmi_config_port() {
  local cfg="$1" port=""
  if command -v python3 >/dev/null 2>&1; then
    port="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["backends"]["Direct"]["port"])' "$cfg" 2>/dev/null)"
  fi
  # Fallback: first "port" line within the "Direct" block.
  [ -n "$port" ] || port="$(awk '/"Direct"/{d=1} d&&/"port"/{gsub(/[^0-9]/,"");print;exit}' "$cfg" 2>/dev/null)"
  echo "${port:-10000}"
}

# True if something is already listening on the given TCP port.
fmi_port_in_use() { { ss -ltn 2>/dev/null || netstat -ltn 2>/dev/null; } | grep -q ":$1[[:space:]]"; }

# Build tcpunchd on first use if its binary is missing.
fmi_build_tcpunchd_if_missing() {
  local tcpunchd="$1" root="$2"
  [ -x "$tcpunchd" ] && return 0
  echo "[fmi] building tcpunchd..." >&2
  cmake -S "${root}/extern/fmi/extern/TCPunch/server" -B "$(dirname "$tcpunchd")" -DCMAKE_BUILD_TYPE=Release >/dev/null
  cmake --build "$(dirname "$tcpunchd")" >/dev/null
}

# Reuse an already-running rendezvous server on $port, or start one and wait for it
# to listen. Echoes the PID of a server we started on stdout (empty when reusing an
# existing one, so the caller only tears down what it launched). Returns non-zero if
# a freshly started server never came up.
fmi_start_tcpunchd() {
  local tcpunchd="$1" port="$2" logfile="$3"
  if fmi_port_in_use "$port"; then
    echo "[fmi] reusing existing rendezvous server on port $port" >&2
    return 0
  fi
  "$tcpunchd" "$port" >"$logfile" 2>&1 &
  local pid=$!
  local _
  for _ in $(seq 1 50); do fmi_port_in_use "$port" && break; sleep 0.1; done
  if ! fmi_port_in_use "$port"; then
    cat "$logfile" >&2
    echo "[fmi] tcpunchd failed to listen on port $port" >&2
    return 1
  fi
  echo "$pid"
}

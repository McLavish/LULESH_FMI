#!/usr/bin/env python3
"""Golden (non-migrated) LULESH reference run.

LULESH's `Final Origin Energy` is a bit-deterministic function of (binary, -s, -i, number of
ranks): the demo's PASS criterion is that the migrated run's energy is STRING-identical to a
non-migrated one — a single flipped byte of restored state diverges it. This module produces
that reference: NUM_PEERS local ranks over a PRIVATE tcpunchd on 127.0.0.1 with fault
tolerance disabled, returning rank 0's energy.

Importable (the orchestrator Job runs it after the migrated run finishes — never before,
because LULESH starts computing the moment its ranks connect and a golden run up front would
eat the migration-window budget) and a CLI (the local gate runs it in a one-off container
before any demo infrastructure exists):

    python3 golden_run.py     ->      GOLDEN_ENERGY=<value>
"""
import os
import re
import subprocess
import sys
import time
from string import Template

ENERGY_RE = re.compile(r"Final Origin Energy\s*=\s*([0-9.eE+-]+)")


def extract_energy(text):
    """The Final Origin Energy string from a rank-0 log, or None (unit-tested)."""
    match = ENERGY_RE.search(text)
    return match.group(1) if match else None


def run_golden(num_peers, comm_name, size, iters, timeout_s=300,
               tcpunch_port=10001, log_dir="/tmp/golden",
               template_path="/opt/fmi/app/fmi-lulesh-golden.json.tmpl",
               lulesh_bin=None, tcpunchd_bin="/opt/fmi/bin/tcpunchd"):
    """Run a non-migrated num_peers-rank LULESH and return rank 0's Final Origin Energy
    string. Raises RuntimeError when any rank fails or no energy was printed."""
    lulesh_bin = lulesh_bin or os.environ.get("FMI_LULESH_BIN", "/opt/fmi/bin/lulesh2.0")
    os.makedirs(log_dir, exist_ok=True)
    config_path = os.path.join(log_dir, "fmi-golden.json")
    with open(template_path) as f:
        rendered = Template(f.read()).safe_substitute(
            TCPUNCH_HOST="127.0.0.1", TCPUNCH_PORT=str(tcpunch_port))
    with open(config_path, "w") as f:
        f.write(rendered)

    # Private rendezvous: the golden ranks must not pair through the shared demo tcpunchd
    # (whose pairing-name namespace belongs to the FT run) and must also work where none
    # exists at all (a one-off container, the orchestrator pod).
    tcpunchd_log = open(os.path.join(log_dir, "tcpunchd.log"), "w")
    tcpunchd = subprocess.Popen([tcpunchd_bin, str(tcpunch_port)],
                                stdout=tcpunchd_log, stderr=subprocess.STDOUT)
    procs = []
    try:
        time.sleep(0.5)  # let tcpunchd bind before the first pairing request
        for rank in range(num_peers):
            rank_env = dict(os.environ,
                            FMI_RANK=str(rank), FMI_WORLD_SIZE=str(num_peers),
                            FMI_CONFIG=config_path, FMI_COMM_NAME=f"{comm_name}-golden",
                            OMP_NUM_THREADS="1")
            rank_env.pop("FMI_MIGRATE_AT_CYCLE", None)  # golden never opens a window
            fh = open(os.path.join(log_dir, f"rank-{rank}.log"), "w")
            procs.append(subprocess.Popen([lulesh_bin, "-s", str(size), "-i", str(iters)],
                                          env=rank_env, stdout=fh, stderr=subprocess.STDOUT))
            fh.close()
        deadline = time.time() + timeout_s
        for rank, proc in enumerate(procs):
            try:
                rc = proc.wait(timeout=max(1.0, deadline - time.time()))
            except subprocess.TimeoutExpired:
                raise RuntimeError(f"golden rank {rank} still running after {timeout_s}s")
            if rc != 0:
                with open(os.path.join(log_dir, f"rank-{rank}.log")) as f:
                    tail = f.read()[-2000:]
                raise RuntimeError(f"golden rank {rank} exited {rc}:\n{tail}")
    finally:
        for proc in procs:
            if proc.poll() is None:
                proc.kill()
        tcpunchd.kill()
        tcpunchd.wait()
        tcpunchd_log.close()

    with open(os.path.join(log_dir, "rank-0.log")) as f:
        energy = extract_energy(f.read())
    if not energy:
        raise RuntimeError("golden rank 0 printed no Final Origin Energy")
    return energy


def main():
    energy = run_golden(
        num_peers=int(os.environ.get("NUM_PEERS", "8")),
        comm_name=os.environ.get("COMM_NAME", "lulesh-evac"),
        size=os.environ.get("LULESH_SIZE", "15"),
        iters=os.environ.get("LULESH_ITERS", "200"),
        timeout_s=int(os.environ.get("GOLDEN_TIMEOUT_S", "300")),
    )
    print(f"GOLDEN_ENERGY={energy}", flush=True)


if __name__ == "__main__":
    try:
        main()
    except RuntimeError as exc:
        sys.exit(f"[golden] {exc}")

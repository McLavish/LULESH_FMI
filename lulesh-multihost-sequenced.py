#!/usr/bin/env python3
"""LULESH across four machines, with cross-host SEQUENCED-protocol migrations: the phase-D
equivalent of the drain campaign's capstone, on the retention/replay transport instead.

The sibling of `lulesh-multihost-drain.py`, and built the same way: everything about the
cluster and criu is **imported** from `fmi/runbooks/criu-transparent-checkpoint/
multihost_sweep.py` -- `Cluster` (ssh, no ControlMaster), `checkpoint_restore`,
`evacuate_node`, `destinations`, the pid-band assertions and the cross-host move check -- so a
phase-D verdict is produced by the same code as a phase-B or phase-C one, and the criu flags
are the harness's, not a copy that could drift.

What is different from the drain capstone, and it is the protocols' core difference: there is
no control plane. Nothing is announced. The driver freezes a rank -- or every rank of a machine
-- with `sudo criu dump --tcp-close --shell-job` at an instant the job knows nothing about, and
the correctness story is entirely the sequenced link's: the frames the peers had not
acknowledged are retained, and the reconnect after the restore replays them. Sequenced images
therefore CARRY sockets by design (`--tcp-close` drops them on restore), which is exactly the
flag difference from the drain runbook.

What is LULESH-specific, and therefore all that lives in this file:

  * **the launch**: a LULESH rank takes (peer_id, num_peers, config, comm_name) through the
    environment (`FMI_RANK`, `FMI_WORLD_SIZE`, `FMI_CONFIG`, `FMI_COMM_NAME`), not argv. The
    hygiene is the harness's, clause for clause: `cd X; setsid ... & echo LAUNCHED:$!` (`;`,
    never `&&`), stdin from /dev/null, stdout to a regular file on the shared tree,
    `OMP_NUM_THREADS=1`, `stdbuf -oL` so the cycle counter is readable while the job runs, and
    the pid taken from `$!` at the instant of the launch (never pgrep: every LULESH rank has an
    identical command line).
  * **the progress oracle**: rank 0's `cycle = N` line. LULESH is globally synchronised once per
    cycle, so rank 0 at cycle N is a statement about every rank. It replaces the subject's
    per-rank round counter by monkey-patching `multihost_sweep.last_round`, which is the one
    subject-specific reading the imported orchestration makes.
  * **the correctness oracle**: rank 0's `Final Origin Energy`, bit-identical to the golden
    `7.130703e+05`. A migration that lost or duplicated a single halo byte moves it.

Per-trial acceptance (the sequenced criteria, not the drain ones):
energy == golden; rank 0's cycle counter past the cycle every cut was taken at; both criu legs
rc 0 (privileged, WITH --tcp-close); every migrated rank actually on a different machine
afterwards; the restored pid still in its home band and outside the destination's. There is no
socketless assertion and no counter cross-check -- the replay working IS the mechanism, and the
energy is its oracle.

    python3 lulesh-multihost-sequenced.py --nodes 10.164.0.3 10.164.0.4 10.164.0.5 10.164.0.6
    python3 lulesh-multihost-sequenced.py --nodes ... --only D1a,D1b --keep

The imported runbook is located **relative to this file**, through the FMI submodule:
`<this dir>/extern/fmi/runbooks/criu-transparent-checkpoint`. Two environment variables
override that, innermost first -- `FMI_CKPT_RUNBOOK` names this one runbook directory
outright, and `FMI_RUNBOOKS` relocates the whole `runbooks/` tree (for a cluster that shares a
single FMI checkout at a path of its own). Neither is needed for an ordinary checkout with the
submodule initialised.
"""
import argparse
import json
import os
import random
import re
import shlex
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
# Resolved through the very FMI checkout this tree builds against -- the `extern/fmi`
# submodule, relative to this file -- so the harness cannot come from a different FMI than the
# library under test. `FMI_RUNBOOKS` / `FMI_CKPT_RUNBOOK` override it (see the docstring).
RUNBOOKS = os.environ.get("FMI_RUNBOOKS", os.path.join(HERE, "extern", "fmi", "runbooks"))
RUNBOOK = os.environ.get("FMI_CKPT_RUNBOOK",
                         os.path.join(RUNBOOKS, "criu-transparent-checkpoint"))
if RUNBOOK not in sys.path:
    sys.path.insert(0, RUNBOOK)

import multihost_sweep as mhs                                                   # noqa: E402

CYCLE_RE = re.compile(r"^cycle = (\d+),", re.M)
ENERGY_RE = re.compile(r"Final Origin Energy\s*=\s*(\S+)")
SINGLE_HOST_GOLDEN = "7.130703e+05"

# Block placement, as the drain capstone used: r0,r1@T r2,r3@N2 r4,r5@N1 r6,r7@N3. LULESH's
# 2x2x2 decomposition makes every rank every other's neighbour, so this leaves 24 of the 28
# links crossing a machine boundary.
def block_placement(nodes, npeers):
    per = npeers // len(nodes)
    return {r: nodes[r // per] for r in range(npeers)}


def rank0_cycle(outdir):
    try:
        text = open(os.path.join(outdir, "r0.log")).read()
    except OSError:
        return -1
    hits = CYCLE_RE.findall(text)
    return int(hits[-1]) if hits else -1


def energy_of(outdir):
    try:
        m = ENERGY_RE.search(open(os.path.join(outdir, "r0.log")).read())
    except OSError:
        return None
    return m.group(1) if m else None


def cycles_of(outdir):
    try:
        m = re.search(r"Iteration count\s*=\s*(\d+)",
                      open(os.path.join(outdir, "r0.log")).read())
    except OSError:
        return None
    return int(m.group(1)) if m else None


# The one subject-specific reading the imported orchestration makes. LULESH has no per-rank
# round counter; rank 0's cycle is the global one.
mhs.last_round = lambda outdir, rank: rank0_cycle(outdir)


def launch(cluster, comm, place_map, args, outdir):
    placement = {}
    npeers = len(place_map)
    for rank in sorted(place_map):
        node = place_map[rank]
        log = os.path.join(outdir, "r%d.log" % rank)
        inner = ("cd %s; setsid env FMI_RANK=%d FMI_WORLD_SIZE=%d FMI_CONFIG=%s "
                 "FMI_COMM_NAME=%s OMP_NUM_THREADS=1 stdbuf -oL %s -s %d -i %d -p "
                 "< /dev/null > %s 2>&1 & echo LAUNCHED:$!"
                 % (shlex.quote(HERE), rank, npeers, shlex.quote(args.config),
                    shlex.quote(comm), shlex.quote(args.binary), args.size, args.iters,
                    shlex.quote(log)))
        out = cluster.run(node, inner, timeout=60)
        m = re.search(r"LAUNCHED:(\d+)", out.stdout)
        if out.returncode != 0 or not m:
            raise mhs.SetupError("failed to launch rank %d on %s: %s %s"
                                 % (rank, node, out.stdout, out.stderr))
        pid = int(m.group(1))
        mhs.assert_pid_in_band(node, pid, "launch of rank %d" % rank)
        placement[rank] = (node, pid)
    return placement


def kill_lulesh(cluster):
    # `pkill -f` reads the command line of the shell running it too; the bracket keeps it from
    # matching its own argv. There is no communicator in a LULESH command line to narrow this
    # (the name is in the environment), so it is all-or-nothing per node -- correct for a
    # campaign that runs one job at a time.
    cluster.all_nodes("pkill -9 -f '[l]ulesh2.0' || true")


def wait_for_finish(cluster, placement, timeout_s):
    """Every rank's process gone from the machine it is on. Waiting for the PROCESS is the only
    per-rank finish evidence LULESH offers -- ranks other than 0 print nothing whatsoever in a
    healthy run -- and it is also what makes the log readings that follow trustworthy: the
    rank's stdout is flushed and its NFS client has written back before the pid disappears."""
    deadline = time.time() + timeout_s
    alive = []
    for rank in sorted(placement):
        node, pid = placement[rank]
        remaining = max(5.0, deadline - time.time())
        if not mhs.wait_pid_gone(cluster, node, pid, timeout_s=remaining):
            alive.append(rank)
    return not alive


def registry_del(comm, host, port):
    subprocess.run(["redis-cli", "-h", host, "-p", str(port), "DEL", "fmi:direct:%s" % comm],
                   capture_output=True)


SCENARIOS = [
    # name, kind, spec
    ("D0",  "clean", {}),
    ("D1a", "seq",   {"ranks": [3], "to": ["3"]}),      # r3 @N2 -> N3
    ("D1b", "seq",   {"ranks": [6], "to": ["0"]}),      # r6 @N3 -> T
    ("D1c", "seq",   {"ranks": [0], "to": ["2"]}),      # r0 @T  -> N1  (rank 0 itself)
    ("D2a", "seq",   {"ranks": [2, 5, 7], "to": ["0", "1", "2"]}),
    ("D2b", "seq",   {"ranks": [0, 3, 6], "to": ["3", "2", "0"]}),
    ("D3a", "cut",   {"evac": ["1"], "to": "spread"}),  # N2's r2,r3
    ("D3b", "cut",   {"evac": ["2"], "to": "spread"}),  # N1's r4,r5
    ("D3c", "cut",   {"evac": ["0"], "to": "spread"}),  # T's  r0,r1
    ("D4a", "cut",   {"evac": ["2,3"], "to": "spread"}),   # N1+N3, k=4, one cut
    ("D4b", "cut",   {"evac": ["0,1"], "to": "spread"}),   # T+N2,  k=4, one cut
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", nargs="+", required=True)
    ap.add_argument("--ssh-user", default=os.environ.get("USER", "luca"))
    ap.add_argument("--ssh-key", default="")
    ap.add_argument("--config", default=os.path.join(HERE, "fmi-lulesh-seq-multihost.json"))
    ap.add_argument("--binary", default=os.path.join(HERE, "build-fmi-seq", "lulesh2.0"))
    ap.add_argument("--peers", type=int, default=8)
    ap.add_argument("--size", type=int, default=30)
    ap.add_argument("--iters", type=int, default=9999999)
    ap.add_argument("--golden", default=SINGLE_HOST_GOLDEN)
    ap.add_argument("--only", default="")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--delay-range", type=float, nargs=2, default=[3.0, 12.0])
    ap.add_argument("--keep", action="store_true")
    args = ap.parse_args()

    with open(args.config) as f:
        backend = json.load(f)["backends"]["DirectTCP"]
    if not backend.get("enabled") or not backend.get("recover_links"):
        print("config must enable DirectTCP with recover_links", file=sys.stderr)
        return 2
    rhost, rport = backend["registry_host"], int(backend.get("registry_port", 6379))
    if rhost in ("127.0.0.1", "localhost"):
        print("registry_host is loopback", file=sys.stderr)
        return 2

    cluster = mhs.Cluster(args.nodes, args.ssh_key, args.ssh_user)
    for n, res in cluster.all_nodes("echo ok"):
        if res.returncode != 0 or "ok" not in res.stdout:
            print("cannot ssh to %s" % n, file=sys.stderr)
            return 2

    root = os.path.join(HERE, "seq-runs", "run%d" % os.getpid())
    os.makedirs(root, exist_ok=True)
    place = block_placement(args.nodes, args.peers)
    only = [s.strip() for s in args.only.split(",") if s.strip()]
    rng = random.Random(args.seed)

    passed = failed = 0
    results = []
    for name, kind, spec in SCENARIOS:
        if only and name not in only:
            continue
        comm = "seqlul%s-%d" % (name, os.getpid())
        outdir = os.path.join(root, comm)
        os.makedirs(outdir, exist_ok=True)
        registry_del(comm, rhost, rport)
        kill_lulesh(cluster)
        log = []
        started = time.time()
        placement = launch(cluster, comm, place, args, outdir)
        log.append("placement " + " ".join("%d@%s" % (r, n)
                                           for r, (n, _) in sorted(placement.items())))
        move_failures, cuts, ok = [], [], True
        events = spec.get("ranks") or spec.get("evac") or []
        for k, ev in enumerate(events):
            time.sleep(rng.uniform(args.delay_range[0], args.delay_range[1]))
            to = spec["to"][k] if isinstance(spec.get("to"), list) else None
            policy = spec["to"] if isinstance(spec.get("to"), str) else "next"
            if kind == "cut":
                nodes = [mhs.resolve_node(cluster, d) for d in str(ev).split(",")]
                at = rank0_cycle(outdir)
                log.append("cut %d: evacuating %s at cycle %d" % (k, ",".join(nodes), at))
                res = mhs.evacuate_node(cluster, comm, nodes, placement,
                                        os.path.join(outdir, "img%d" % k), policy, rng, log,
                                        outdir, to, move_failures)
                if res is None or res is False:
                    log.append("cut %d: %s" % (k, "no live ranks" if res is None else "criu"))
                    ok = False
                    break
                cuts.append(at)
            else:
                node, pid = placement[ev]
                if not mhs.pid_alive(cluster, node, pid):
                    log.append("ckpt %d: rank %d already finished" % (k, ev))
                    ok = False
                    break
                at = rank0_cycle(outdir)
                log.append("ckpt %d: rank %d pid %d on %s at cycle %d" % (k, ev, pid, node, at))
                if not mhs.checkpoint_restore(cluster, comm, ev, placement,
                                              os.path.join(outdir, "img%d" % k), "next", rng,
                                              log, to, move_failures):
                    ok = False
                    break
                cuts.append(at)

        finished = wait_for_finish(cluster, placement, timeout_s=600)
        energy, iters = energy_of(outdir), cycles_of(outdir)
        after = rank0_cycle(outdir)
        kill_lulesh(cluster)
        failures = list(move_failures)
        if not ok:
            failures.append("a migration did not complete")
        if not finished:
            failures.append("timed out before every rank finished")
        if energy != args.golden:
            failures.append("Final Origin Energy %s != golden %s" % (energy, args.golden))
        for at in cuts:
            if after <= at:
                failures.append("no cycle past %d after a cut" % at)
        for r in range(args.peers):
            try:
                text = open(os.path.join(outdir, "r%d.log" % r)).read()
            except OSError:
                failures.append("rank %d: no log" % r)
                continue
            for bad in ("terminate called", "Segmentation", "Timeout", "MISMATCH"):
                if bad in text:
                    failures.append("rank %d: %s" % (r, bad))
        wall = time.time() - started
        verdict = "FAIL" if failures else "pass"
        if failures:
            failed += 1
        else:
            passed += 1
        results.append((name, verdict, len(cuts), energy, iters, wall))
        print("%s: %s energy=%s cycles=%s migrations=%d wall=%.0fs | %s%s"
              % (name, verdict, energy, iters,
                 sum(len(str(e).split(",")) * 2 for e in events) if kind == "cut" else len(cuts),
                 wall, " | ".join(log), (" :: " + "; ".join(failures)) if failures else ""))
        sys.stdout.flush()
        if failures:
            print("STOPPING: trial dir kept at %s" % outdir, file=sys.stderr)
            break
        if not args.keep:
            pass  # keep the logs; images are the bulk and stay under img*/

    print("\n== %d passed, %d failed ==" % (passed, failed))
    with open(os.path.join(root, "results.json"), "w") as f:
        json.dump(results, f, indent=2)
    return 1 if failed else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except mhs.SetupError as e:
        print("SETUP ERROR: %s" % e, file=sys.stderr)
        sys.exit(3)

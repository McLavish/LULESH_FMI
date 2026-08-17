#!/usr/bin/env python3
"""LULESH across four machines, with cross-host drain migrations: phase D of the campaign.

The capstone of `fmi/runbooks/drain-migration`. Everything about the cluster, criu and the
control plane is **imported** from that runbook -- `cluster.py` for ssh/criu/lease/events and
`multihost_drain.py` for the migration orchestration itself (`migrate_one`, `evacuate` and the
stages they are made of). Nothing of that is restated here, so a verdict from this driver and a
verdict from the campaign driver mean the same thing, and a fix to either lands in both.

What is LULESH-specific, and therefore all that lives in this file:

  * **the launch**: a LULESH rank takes (peer_id, num_peers, config, comm_name) through the
    environment (`FMI_RANK`, `FMI_WORLD_SIZE`, `FMI_CONFIG`, `FMI_COMM_NAME` --
    `lulesh-fmi.cc:223`), not through argv, and its argv is the simulation's
    (`-s <size> -i <iters> -p`). The launch *hygiene* is `cluster.launch_ranks`' verbatim: `cd
    X; setsid … & echo LAUNCHED:$!` (`;`, never `&&`), stdin from /dev/null, stdout to a regular
    file on the shared tree, the pid taken from `$!` at the instant of the launch and asserted
    in its node's band -- never re-derived with pgrep, which here could not tell two ranks apart
    at all: every LULESH rank has an identical command line and the rank is in the environment.
  * **the progress oracle**: rank 0's `cycle = N` line. LULESH is globally synchronised once per
    cycle (a dt allreduce), so rank 0 being at cycle N is a statement about every rank -- which
    is why `multihost_drain.last_round`, a per-rank round counter for that subject, is replaced
    below by this one reading for all ranks.
  * **the correctness oracle**: rank 0's `Final Origin Energy`, bit-identical to a clean run's.
    LULESH's energy is bit-deterministic for a given (binary, size, rank count), so a migration
    that lost or duplicated a single halo byte moves it.

Verified per trial, exactly as the campaign driver does it (and by its code):
zero socket fds at state T, no socket line in `dump.log`, no socket image file, both criu legs
rc 0 with no `--tcp-*`/`--shell-job` flag, the event trail `leaving(e) -> sealed(e) ->
restored(e+1)` with the expected batch id, pairwise counter agreement inside every cut
(`sealed[a].sent.b == sealed[b].received.a`), the restored pid still in its home band and
outside the destination's, and the rank actually on a different machine afterwards. On top of
those, LULESH's own: every rank's process gone at the end, rank 0's energy equal to the golden,
and rank 0's cycle counter past the cycle each cut was taken at.

    python3 lulesh-multihost-drain.py --nodes 10.164.0.3 10.164.0.4 10.164.0.5 10.164.0.6
    python3 lulesh-multihost-drain.py --nodes … --only D1a,D1b --keep
    python3 lulesh-multihost-drain.py --nodes … --dry-run

The imported runbook is located **relative to this file**, through the FMI submodule:
`<this dir>/extern/fmi/runbooks/drain-migration`. Two environment variables override that,
innermost first -- `FMI_DRAIN_RUNBOOK` names this one runbook directory outright, and
`FMI_RUNBOOKS` relocates the whole `runbooks/` tree (for a cluster that shares a single FMI
checkout at a path of its own). Neither is needed for an ordinary checkout with the submodule
initialised.
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
# The runbook is reached through the very FMI checkout this tree builds against -- the
# `extern/fmi` submodule, resolved relative to this file, or whatever `FMI_RUNBOOKS` /
# `FMI_DRAIN_RUNBOOK` point at (a shared cluster checkout, say). Either way the driver cannot
# import a different FMI's helpers than the library it is testing.
RUNBOOKS = os.environ.get("FMI_RUNBOOKS", os.path.join(HERE, "extern", "fmi", "runbooks"))
RUNBOOK = os.environ.get("FMI_DRAIN_RUNBOOK",
                         os.path.join(RUNBOOKS, "drain-migration"))
if RUNBOOK not in sys.path:
    sys.path.insert(0, RUNBOOK)

import cluster as cl                                                            # noqa: E402
import multihost_drain as mhd                                                   # noqa: E402
from drain_driver import (clean_comm, data_plane, drain_signal_number,          # noqa: E402
                          read_events, save_events)

LULESH = os.environ.get("LULESH_BIN", os.path.join(HERE, "build-fmi-drain", "lulesh2.0"))
CONFIG = os.environ.get("FMI_CONFIG", os.path.join(HERE, "fmi-lulesh-drain-multihost.json"))
FMI_TREE = os.path.realpath(os.path.join(HERE, "extern", "fmi"))

CYCLE_RE = re.compile(r"^cycle = (\d+),", re.M)
ENERGY_RE = re.compile(r"Final Origin Energy\s*=\s*(\S+)")
ITER_RE = re.compile(r"Iteration count\s*=\s*(\d+)")
# A rank that died of anything the shim or the library threw. LULESH prints nothing at all on
# ranks other than 0 in a healthy run, so these are the only per-rank readings there are.
BAD_MARKERS = ("terminate called", "Segmentation", "stack smashing", "what():",
               "Assertion", "MISMATCH")

# The single-host port's golden, from the earlier validation (8 ranks, -s 30, run to
# completion at 2031 cycles). The four-machine clean run must reproduce it exactly: a
# cross-host job that agrees with it separates "the cluster is wired right" from "the
# migrations are correct" before a single rank is frozen.
SINGLE_HOST_GOLDEN = "7.130703e+05"

# Timeouts and sizes. Anything `multihost_drain`'s stages ask for by name (`seal_timeout`,
# `dump_timeout`, …) is read from here through `Trial.f`, so the two drivers are bounded the
# same way; the rest is LULESH's own pacing.
DEFAULTS = {
    "seal_timeout": 60.0,
    "restore_timeout": 120.0,
    "dump_timeout": 300.0,
    "criu_restore_timeout": 300.0,
    "wait_gone_timeout": 60.0,
    "ssh_timeout": 60.0,
    "lease_free_timeout": 30.0,
    "batch_lease_ms": 120000,
}

# Phase D's scenarios. One entry is one job: `kind` and the destination language are
# `multihost_drain.plan_steps`', so the itinerary of every trial is computed by the campaign
# driver's own planner and can be reviewed with --dry-run.
#
# Node indices are into --nodes, in campaign order T(.3) N2(.4) N1(.5) N3(.6), and the
# placement is `block`: r0,r1@T  r2,r3@N2  r4,r5@N1  r6,r7@N3. LULESH's 2x2x2 decomposition
# makes every rank a neighbour of every other, so placement decides only which of the 28 links
# are co-located and which cross a machine boundary -- block keeps rank pairs together and
# leaves 24 of the 28 crossing.
SCENARIOS = [
    {"name": "D0", "kind": "clean",
     "why": "the four-machine clean run: the golden every migrated trial is scored against, "
            "and it must equal the single-host golden"},

    {"name": "D1a", "kind": "seq", "ranks": [3], "to": [3],
     "why": "one cross-host migration mid-simulation: rank 3, N2 -> N3"},
    {"name": "D1b", "kind": "seq", "ranks": [6], "to": [0],
     "why": "one cross-host migration mid-simulation: rank 6, N3 -> T"},
    {"name": "D1c", "kind": "seq", "ranks": [0], "to": [2],
     "why": "one cross-host migration of RANK 0 -- the rank that owns the energy oracle and "
            "whose log is the progress oracle: T -> N1"},

    {"name": "D2a", "kind": "seq", "ranks": [2, 5, 7], "to": [0, 1, 2],
     "why": "three sequential migrations of three different ranks in one job: "
            "r2 N2->T, r5 N1->N2, r7 N3->N1"},
    {"name": "D2b", "kind": "seq", "ranks": [0, 3, 6], "to": [3, 2, 0],
     "why": "three sequential migrations, rank 0 first: r0 T->N3, r3 N2->N1, r6 N3->T"},

    {"name": "D3a", "kind": "cut", "evacuate": 1, "to": "spread",
     "why": "single-cut evacuation of N2 (r2, r3) onto two different survivors"},
    {"name": "D3b", "kind": "cut", "evacuate": 2, "to": "spread",
     "why": "single-cut evacuation of N1 (r4, r5) onto two different survivors"},
    {"name": "D3c", "kind": "cut", "evacuate": 0, "to": "spread",
     "why": "single-cut evacuation of T (r0, r1) -- the machine the driver itself runs on, and "
            "the one holding rank 0"},

    {"name": "D4a", "kind": "cut", "evacuate": [2, 3], "to": "spread",
     "why": "TWO machines in one k=4 cut: N1 and N3 (r4..r7) onto T and N2"},
    {"name": "D4b", "kind": "cut", "evacuate": [0, 1], "to": "spread",
     "why": "TWO machines in one k=4 cut: T and N2 (r0..r3) onto N1 and N3"},
]


# ------------------------------------------------------------------ LULESH's readings on a run

def rank0_cycle(outdir, rank=None):
    """The last cycle rank 0 logged, or -1. **LULESH's progress counter, for every rank.**

    Installed over `multihost_drain.last_round` below. That driver's subject prints a round
    counter per rank; LULESH prints progress on rank 0 only (`lulesh.cc:2750`, guarded by
    `myRank == 0`) -- and it does not need more, because every cycle ends in a global dt
    allreduce, so rank 0 at cycle N means all eight ranks are within one cycle of N. The `rank`
    argument is therefore accepted and ignored.

    Same NFS caveat as the campaign driver's: when rank 0 sits on another machine its log is
    written over NFS and the reading can lag. It is used for two things and both tolerate it --
    the cut is *recorded* at a possibly undercounted cycle (which makes the progressed-past-the-
    cut check conservative in the trial's favour), and warm-up waits for a cycle to be reached,
    which lag can only delay.
    """
    try:
        with open(os.path.join(outdir, "r0.log")) as f:
            text = f.read()
    except OSError:
        return -1
    hits = CYCLE_RE.findall(text)
    return int(hits[-1]) if hits else -1


# The substitution, done once and loudly: from here on `multihost_drain`'s stages record
# progress with LULESH's counter. Nothing else in that module reads it.
mhd.last_round = rank0_cycle


def energy_of(outdir):
    try:
        with open(os.path.join(outdir, "r0.log")) as f:
            m = ENERGY_RE.search(f.read())
    except OSError:
        return None
    return m.group(1) if m else None


def iterations_of(outdir):
    try:
        with open(os.path.join(outdir, "r0.log")) as f:
            m = ITER_RE.search(f.read())
    except OSError:
        return None
    return int(m.group(1)) if m else None


def scan_logs(outdir, npeers):
    """Anything in a rank log that says the rank did not have a healthy run."""
    failures = []
    for r in range(npeers):
        path = os.path.join(outdir, "r%d.log" % r)
        try:
            with open(path, errors="replace") as f:
                text = f.read()
        except OSError:
            failures.append("rank %d: no log at %s" % (r, path))
            continue
        for bad in BAD_MARKERS:
            if bad in text:
                failures.append("rank %d: %s" % (r, bad))
    return failures


# ----------------------------------------------------------------------------- the job

def launch_lulesh(cluster, comm, place_map, args, outdir, bands, span):
    """Launch every LULESH rank on the node its placement names. Returns {rank: (node, pid)}.

    `cluster.launch_ranks`' hygiene, clause for clause, with the subject's argv replaced by
    LULESH's and the four FMI parameters moved into the environment where `lulesh-fmi.cc` reads
    them:

      * `;` before setsid, never `&&`: `cd X && cmd &` backgrounds the whole list, so `$!` would
        name a subshell (criu would dump the wrong process) and that subshell would hold the ssh
        channel open until the job ended.
      * stdin from /dev/null: a rank holding an end of the launching ssh session's socketpair
        makes criu refuse the dump with "External socket is used", which under this runbook's
        acceptance rule would read as a socket surviving the drain.
      * stdout to a regular file on the shared tree, setsid for its own session: with those,
        neither criu leg needs `--shell-job`. The path matters twice -- criu reopens that fd by
        path on the *destination* host, so the log has to be on /scratch.
      * `OMP_NUM_THREADS=1` and a build with `-DWITH_OPENMP=OFF`: criu images of a multi-threaded
        LULESH are not what any of this was measured on.
      * `stdbuf -oL`: LULESH ends its progress line with "\\n", not `std::endl`, so with stdout
        redirected to a file the counter would only appear in 4 KiB blocks and the driver could
        not read it while the job runs. stdbuf execs the target, so `$!` is still LULESH itself.
    """
    npeers = len(place_map)
    placement = {}
    for rank in sorted(place_map):
        node = place_map[rank]
        log = os.path.join(outdir, "r%d.log" % rank)
        inner = ("cd %s; setsid env FMI_RANK=%d FMI_WORLD_SIZE=%d FMI_CONFIG=%s "
                 "FMI_COMM_NAME=%s OMP_NUM_THREADS=1 stdbuf -oL %s -s %d -i %d -p "
                 "< /dev/null > %s 2>&1 & echo LAUNCHED:$!"
                 % (shlex.quote(HERE), rank, npeers, shlex.quote(args.config),
                    shlex.quote(comm), shlex.quote(args.binary), args.size, args.iters,
                    shlex.quote(log)))
        out = cluster.run(node, inner, timeout=args.ssh_timeout)
        m = re.search(r"LAUNCHED:(\d+)", out.stdout)
        if out.returncode != 0 or not m:
            raise cl.SetupError("failed to launch rank %d on %s: %s %s"
                                % (rank, node, out.stdout, out.stderr))
        pid = int(m.group(1))
        cl.assert_launched_pid(bands, node, rank, pid, span)
        placement[rank] = (node, pid)
    return placement


def kill_lulesh(cluster, timeout=30):
    """pkill every LULESH rank on every node.

    The bracket is `cluster.kill_all`'s and is load-bearing: `pkill -f` reads the command line of
    the shell running it too, and an unbracketed pattern would match the shell's own argv and
    kill the killer. There is no communicator in a LULESH command line to narrow this with (the
    name is in the environment), so this is all-or-nothing per node -- which is correct for a
    campaign that runs one job at a time, and is the same pattern the cleanup check greps for.
    """
    cluster.all_nodes("pkill -9 -f %s || true" % shlex.quote("[l]ulesh2.0"), timeout=timeout)


def gone_ranks(cluster, placement, ranks):
    """Which of `ranks` are no longer running, by one `test -d /proc/<pid>` each."""
    gone = []
    for rank in ranks:
        node, pid = placement[rank]
        if not cl.pid_alive(cluster, node, pid):
            gone.append(rank)
    return gone


def wait_until_cycle(cluster, placement, outdir, cycle, timeout_s):
    """Wait for rank 0 to log `cycle`. False if it never does (or the job died first).

    Polls the log, not the cluster: rank 0's log is on the shared tree and, in every scenario
    here, rank 0 *starts* on the driver's own host, where the tree is a local filesystem. The
    liveness check that guards against waiting for a dead job costs one ssh call and is made
    rarely, not per poll.
    """
    deadline = time.time() + timeout_s
    checked = 0.0
    while time.time() < deadline:
        if rank0_cycle(outdir) >= cycle:
            return True
        # Rank 0 alone, and rarely: it owns the counter being waited for, so if it is gone the
        # wait can only end at the deadline. Asking all eight would cost eight ssh handshakes a
        # time for an answer the one rank already gives.
        if time.time() - checked > 5.0:
            checked = time.time()
            if gone_ranks(cluster, placement, [0]):
                return False
        time.sleep(0.1)
    return False


def wait_for_finish(cluster, placement, timeout_s):
    """Every rank's process gone from the machine it is on. Returns (ok, [ranks still alive]).

    One `node_helper.py wait-gone` per rank, whose loop runs inside the node -- no polling over
    ssh. Waiting for the *process* is the only per-rank finish evidence LULESH offers: ranks
    other than 0 print nothing whatsoever in a healthy run. It is also what makes the log
    readings that follow trustworthy: the rank's stdout is flushed and its NFS client has written
    back before the pid disappears.
    """
    deadline = time.time() + timeout_s
    alive = []
    for rank in sorted(placement):
        node, pid = placement[rank]
        remaining = max(5.0, deadline - time.time())
        if not cl.wait_pid_gone(cluster, node, pid, timeout_s=remaining):
            alive.append(rank)
    return (not alive), alive


# ----------------------------------------------------------------------------- one trial

def run_trial(args, cluster, params, scen, root, bands, drain_signal, signal_offset, golden, rng):
    """One LULESH job, with the scenario's itinerary executed on it. Returns a result dict."""
    comm = "lul-%s-%d" % (scen["name"], os.getpid())
    outdir = os.path.join(root, comm)
    os.makedirs(outdir, exist_ok=True)
    clean_comm(params, comm)

    t = mhd.Trial(args, scen, DEFAULTS, None, args.peers, comm, outdir, cluster.nodes, cluster,
                  params, bands, args.pid_band_span, drain_signal, signal_offset)
    place_map = mhd.placement_for(args.place, cluster.nodes, args.peers)
    steps = mhd.plan_steps(scen, DEFAULTS, cluster.nodes, args.peers, place_map)

    started = time.monotonic()
    result = {"name": scen["name"], "comm": comm, "outdir": outdir, "cuts": [],
              "migrations": [], "verdict": "ok", "note": "", "failures": [], "windows": []}
    try:
        t.placement = launch_lulesh(cluster, comm, place_map, args, outdir, bands,
                                    args.pid_band_span)
    except cl.SetupError:
        kill_lulesh(cluster)
        clean_comm(params, comm)
        raise
    t.home = {rank: node for rank, (node, _) in t.placement.items()}
    t.log.append("placement " + " ".join("r%d@%s:%d" % (r, n, p)
                                         for r, (n, p) in sorted(t.placement.items())))

    verdict, note = "ok", ""
    for step in steps:
        if not wait_until_cycle(cluster, t.placement, outdir, args.warmup_cycle,
                                args.warmup_timeout):
            verdict, note = "void", ("rank 0 never reached cycle %d before the migration was due"
                                     % args.warmup_cycle)
            break
        time.sleep(rng.uniform(*args.delay_range))
        ranks = [m["rank"] for m in step["moves"]]
        gone = gone_ranks(cluster, t.placement, ranks)
        if gone:
            # The job outran the plan. Not a protocol verdict and not a pass: the trial proves
            # nothing, and saying so is the whole point of scoring it as void.
            verdict, note = "void", ("rank(s) %s had already finished when the migration was "
                                     "due" % gone)
            break
        at_cycle = rank0_cycle(outdir)
        imgroot = os.path.join(outdir, "img%d" % step["index"])
        t.log.append("cut %d at rank-0 cycle %d" % (step["index"], at_cycle))
        window = time.monotonic()
        try:
            if step["kind"] == "cut":
                verdict, note = mhd.evacuate(t, step, imgroot)
            else:
                verdict, note = mhd.migrate_one(t, step["moves"][0], imgroot)
        except cl.SetupError:
            kill_lulesh(cluster)
            save_events(params, comm, outdir)
            clean_comm(params, comm)
            raise
        result["cuts"].append({"index": step["index"], "kind": step["kind"], "at_cycle": at_cycle,
                               "ranks": ranks, "window_s": round(time.monotonic() - window, 2),
                               "verdict": verdict})
        result["windows"].append(round(time.monotonic() - window, 2))
        t.log.append("cut %d %s in %.2fs (rank-0 cycle now %d)"
                     % (step["index"], verdict, time.monotonic() - window, rank0_cycle(outdir)))
        if verdict != "ok":
            break
        if len(steps) > 1 and not cl.batch_lease_wait_free(params, comm,
                                                           t.f("lease_free_timeout")):
            verdict, note = "drain", ("the batch lease was still held %ss after step %d"
                                      % (t.f("lease_free_timeout"), step["index"]))
            break

    result["migrations"] = list(t.migrations)
    if verdict in mhd.HARD_VERDICTS:
        kill_lulesh(cluster)
        save_events(params, comm, outdir)
        result.update(verdict=verdict, note=note, log=t.log,
                      elapsed=time.monotonic() - started, energy=energy_of(outdir),
                      cycles=rank0_cycle(outdir))
        clean_comm(params, comm)
        return result

    finished, still_alive = wait_for_finish(cluster, t.placement, args.finish_timeout)
    failures = []
    if not finished:
        failures.append("rank(s) %s never exited within %ss" % (still_alive, args.finish_timeout))
    failures += scan_logs(outdir, args.peers)

    energy = energy_of(outdir)
    iterations = iterations_of(outdir)
    cycles = rank0_cycle(outdir)
    if energy is None:
        failures.append("rank 0 printed no Final Origin Energy")
    elif golden is not None and energy != golden:
        failures.append("Final Origin Energy %s != golden %s" % (energy, golden))
    for cut in result["cuts"]:
        if cycles <= cut["at_cycle"]:
            failures.append("rank 0 logged no cycle past %d, the cycle cut %d was taken at"
                            % (cut["at_cycle"], cut["index"]))
    for m in t.migrations:
        if m["dest"] == m["src"]:
            failures.append("rank %d did not change machine" % m["rank"])
    events = read_events(params, comm)
    failures += mhd.check_event_trail(events, t.migrations, t.cuts)

    if verdict == "void" and not t.migrations:
        clean = finished and not failures
        result.update(verdict="skip" if clean else "fail", note=note, log=t.log,
                      energy=energy, iterations=iterations, cycles=cycles,
                      elapsed=time.monotonic() - started,
                      failures=failures if not clean else [])
        if not clean:
            result["failures"].append("no migration landed (%s) and the job was not clean" % note)
        save_events(params, comm, outdir)
        kill_lulesh(cluster)
        clean_comm(params, comm)
        return result
    if verdict == "void":
        t.log.append("note: %s" % note)

    save_events(params, comm, outdir)
    kill_lulesh(cluster)
    clean_comm(params, comm)
    result.update(verdict="pass" if not failures else "fail", note=note, log=t.log,
                  failures=failures, energy=energy, iterations=iterations, cycles=cycles,
                  elapsed=time.monotonic() - started)
    return result


def print_result(res, golden):
    head = res["verdict"].upper()
    if res["verdict"] in mhd.HARD_VERDICTS:
        head = mhd.VERDICT_LABEL[res["verdict"]]
    print("  %s: %s" % (res["name"], head))
    for line in res.get("log", []):
        print("      %s" % line)
    print("      energy %s (golden %s) | %s cycles | %.1fs wall"
          % (res.get("energy"), golden, res.get("cycles"), res.get("elapsed", 0.0)))
    moved = ", ".join("r%d %s->%s@e%d" % (m["rank"], m["src"], m["dest"], m["epoch"])
                      for m in res["migrations"])
    if moved:
        print("      moved: %s" % moved)
    if res["note"]:
        print("      note: %s" % res["note"])
    for failure in res["failures"]:
        print("      FAILURE: %s" % failure)
    if res["verdict"] not in ("pass", "skip"):
        print("      evidence kept in %s" % res["outdir"])


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0],
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--nodes", nargs="+", required=True,
                    help="node addresses in campaign order T N2 N1 N3; a scenario's node "
                         "indices index THIS list")
    ap.add_argument("--only", action="append", default=[],
                    help="run only these scenarios (comma-separated or repeated)")
    ap.add_argument("--config", default=CONFIG)
    ap.add_argument("--binary", default=LULESH)
    ap.add_argument("--tree", default=FMI_TREE,
                    help="the shared FMI checkout whose HEAD every node must agree on")
    ap.add_argument("--peers", type=int, default=8,
                    help="must be a perfect cube: LULESH decomposes onto a cubic grid")
    ap.add_argument("--size", type=int, default=30, help="-s: elements per domain edge")
    ap.add_argument("--iters", type=int, default=9999999,
                    help="-i: LULESH's own default, so the run stops on simulated time (2031 "
                         "cycles at -s 30) exactly as the single-host golden did")
    ap.add_argument("--place", default="block", help="rr | block")
    ap.add_argument("--golden-energy", default=None,
                    help="skip the clean run and score against this value")
    ap.add_argument("--warmup-cycle", type=int, default=60)
    ap.add_argument("--warmup-timeout", type=float, default=300.0)
    ap.add_argument("--delay-range", type=float, nargs=2, default=[2.0, 6.0],
                    help="seconds after warm-up (and between steps) before a migration")
    ap.add_argument("--finish-timeout", type=float, default=900.0)
    ap.add_argument("--ssh-timeout", type=float, default=60.0)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--keep", action="store_true", help="keep the logs of passes too")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--continue-on-fail", action="store_true",
                    help="the failure policy says STOP and write the finding up first, so this "
                         "is not the default")
    ap.add_argument("--skip-preflight", action="store_true")
    ap.add_argument("--ssh-key", default=os.environ.get("FMI_SSH_KEY", ""))
    ap.add_argument("--ssh-user",
                    default=os.environ.get("FMI_SSH_USER", os.environ.get("USER", "luca")))
    ap.add_argument("--pid-bands", default="")
    ap.add_argument("--pid-band-span", type=int, default=cl.PID_BAND_SPAN)
    ap.add_argument("--run-root", default=os.path.join(HERE, "drain-runs"))
    ap.add_argument("--signal-offset", type=int, default=None)
    args = ap.parse_args()

    args.config = os.path.abspath(args.config)
    args.binary = os.path.abspath(args.binary)
    args.delay_range = tuple(args.delay_range)
    if round(args.peers ** (1.0 / 3.0)) ** 3 != args.peers:
        sys.exit("--peers %d is not a perfect cube; LULESH decomposes onto a cubic grid"
                 % args.peers)

    wanted = [n.strip() for item in args.only for n in item.split(",") if n.strip()]
    selected = [s for s in SCENARIOS if not wanted or s["name"] in wanted]
    missing = [n for n in wanted if n not in {s["name"] for s in SCENARIOS}]
    if missing:
        sys.exit("no such scenario: %s" % ", ".join(missing))

    plane, params = data_plane(args.config)
    signal_offset = args.signal_offset if args.signal_offset is not None \
        else int(params.get("drain_signal_offset", 3))
    drain_signal = drain_signal_number(signal_offset)
    registry = str(params.get("registry_host", ""))
    if registry in ("127.0.0.1", "localhost", "::1", ""):
        sys.exit("registry_host is %r: a restored rank re-resolves its advertise address against "
                 "the registry, so a loopback registry publishes the loopback of whatever "
                 "machine each rank woke up on." % registry)
    print("LULESH   %s" % args.binary)
    print("config   %s (%s, registry %s:%s)"
          % (args.config, plane, registry, params.get("registry_port")))
    print("drain signal SIGRTMIN+%d = %d" % (signal_offset, drain_signal))

    nodes = list(args.nodes)
    bands = mhd.parse_bands(args.pid_bands)
    if args.dry_run:
        print("\nDRY RUN — nothing is launched, no ssh, no Redis\n")
        place_map = mhd.placement_for(args.place, nodes, args.peers)
        print("placement (%s): %s" % (args.place, " ".join("r%d@%s" % (r, place_map[r])
                                                           for r in sorted(place_map))))
        for scen in selected:
            print("\nscenario %s (%s)" % (scen["name"], scen["kind"]))
            print("  why: %s" % scen["why"])
            for step in mhd.plan_steps(scen, DEFAULTS, nodes, args.peers, dict(place_map)):
                for move in step["moves"]:
                    print("  step %d [%s] rank %d: %s -> %s"
                          % (step["index"], step["kind"], move["rank"], move["src"],
                             move["dest"]))
        print("\ncriu (both legs): %s dump %s -t <pid> -D <img> -o dump.log" % (cl.CRIU,
                                                                                cl.CRIU_FLAGS))
        print("                  cd <dir> && %s restore %s -D <img> -o restore.log -d"
              % (cl.CRIU, cl.CRIU_FLAGS))
        return 0

    if not os.path.exists(args.binary):
        sys.exit("LULESH is not built at %s" % args.binary)
    cluster = cl.Cluster(nodes, args.ssh_key, args.ssh_user)
    root = os.path.join(args.run_root, "mh%d" % os.getpid())
    os.makedirs(root, exist_ok=True)
    print("logs and images under %s" % root)

    if not args.skip_preflight:
        problems, facts = cl.preflight(cluster, params, args.binary, args.tree, args.config,
                                       bands=bands, span=args.pid_band_span)
        with open(os.path.join(root, "preflight.json"), "w") as f:
            json.dump({"problems": problems, "facts": facts}, f, indent=2, sort_keys=True)
        if problems:
            print("PREFLIGHT FAILED — a setup error, never a protocol verdict:", file=sys.stderr)
            for problem in problems:
                print("  * %s" % problem, file=sys.stderr)
            return 2
        print("preflight ok: FMI HEAD %s, lulesh2.0 %s"
              % (sorted(set(facts.get("head", {}).values())),
                 sorted(set(str(s)[:12] for s in facts.get("subject_sha256", {}).values()))))

    rng = random.Random(args.seed)
    golden = args.golden_energy
    results = []
    stopped = None
    for scen in selected:
        print("\n=== %s (%s) %s" % (scen["name"], scen["kind"], scen["why"]))
        try:
            res = run_trial(args, cluster, params, scen, root, bands, drain_signal,
                            signal_offset, golden, rng)
        except cl.SetupError as exc:
            print("SETUP ERROR (never a protocol verdict): %s" % exc, file=sys.stderr)
            return 2
        if scen["kind"] == "clean" and golden is None and res["energy"]:
            golden = res["energy"]
            if res["verdict"] == "pass":
                print("  GOLDEN Final Origin Energy = %s (%s cycles)"
                      % (golden, res.get("iterations")))
                if golden != SINGLE_HOST_GOLDEN:
                    print("  WARNING: the four-machine clean run does not reproduce the "
                          "single-host golden %s — the cluster, not a migration, is what "
                          "differs" % SINGLE_HOST_GOLDEN, file=sys.stderr)
        print_result(res, golden)
        results.append(res)
        with open(os.path.join(root, "results.json"), "w") as f:
            json.dump({"golden": golden, "single_host_golden": SINGLE_HOST_GOLDEN,
                       "binary": args.binary, "config": args.config,
                       "results": results}, f, indent=2, sort_keys=True, default=str)
        if res["verdict"] not in ("pass", "skip"):
            stopped = "%s: %s %s" % (scen["name"], res["verdict"],
                                     res["note"] or "; ".join(res["failures"]))
            if not args.continue_on_fail:
                break

    passed = sum(1 for r in results if r["verdict"] == "pass")
    print("\n== %d passed, %d failed, %d skipped =="
          % (passed, sum(1 for r in results if r["verdict"] not in ("pass", "skip")),
             sum(1 for r in results if r["verdict"] == "skip")))
    print("results: %s" % os.path.join(root, "results.json"))
    if stopped:
        print("STOPPED after %s" % stopped)
        print("The failure policy: keep the trial directory, write the finding up (file:line, "
              "seed, repro) BEFORE running anything else. Widening a timeout, retrying, pinning "
              "advertise_host or adding a TCP flag are not responses to it.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

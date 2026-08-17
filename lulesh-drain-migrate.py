#!/usr/bin/env python3
"""Migrate one LULESH rank mid-simulation with criu, over FMI's DrainTCP backend.

The LULESH counterpart of fmi/runbooks/drain-migration/drain_driver.py. Same protocol,
same acceptance criterion (no `--tcp-close` on either criu leg), different subject: a real
application whose every cycle is a global `MPI_Allreduce` plus three 26-neighbour halo
exchanges, and whose correctness oracle is its own bit-deterministic
`Final Origin Energy` on rank 0.

Rank pids come from Popen directly (every LULESH rank has an identical command line —
the rank is in FMI_RANK — so pgrep cannot tell them apart).

Because LULESH is globally synchronised once per cycle, rank 0's cycle counter advancing
past the cut is proof that *every* rank, the migrated one included, resumed.
"""
import argparse
import ctypes
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
LULESH = os.environ.get("LULESH_BIN", os.path.join(HERE, "build-fmi-drain", "lulesh2.0"))
CONFIG = os.environ.get("FMI_CONFIG", os.path.join(HERE, "fmi-lulesh-drain.json"))

CYCLE_RE = re.compile(r"^cycle = (\d+),", re.M)
ENERGY_RE = re.compile(r"Final Origin Energy\s*=\s*(\S+)")
ITER_RE = re.compile(r"Iteration count\s*=\s*(\d+)")

FS, RS = "\x1e", "\x1d"
STREAM_LUA = """local res = redis.call('XRANGE', KEYS[1], ARGV[1], '+')
local out = {}
for i = 1, #res do
  local parts = {res[i][1]}
  local fields = res[i][2]
  for j = 1, #fields do parts[#parts + 1] = fields[j] end
  out[#out + 1] = table.concat(parts, '\\30')
end
return table.concat(out, '\\29')"""

SOCKET_EVIDENCE = re.compile(r"inetsk|Connected TCP socket|External socket is used")
SOCKET_IMAGES = ("inetsk.img", "unixsk.img", "tcp-stream.img", "sk-queues.img",
                 "packetsk.img", "netlinksk.img")


class _Sigval(ctypes.Union):
    _fields_ = [("sival_int", ctypes.c_int), ("sival_ptr", ctypes.c_void_p)]


_libc = ctypes.CDLL("libc.so.6", use_errno=True)
_libc.sigqueue.argtypes = [ctypes.c_int, ctypes.c_int, _Sigval]


def sigqueue(pid, sig, value):
    if _libc.sigqueue(pid, sig, _Sigval(sival_int=value)) != 0:
        err = ctypes.get_errno()
        raise OSError(err, os.strerror(err), f"sigqueue({pid}, {sig}, {value})")


def redis_cli(params, *args):
    host = str(params.get("registry_host", "127.0.0.1"))
    port = str(params.get("registry_port", 6379))
    return subprocess.run(["redis-cli", "-h", host, "-p", port] + [str(a) for a in args],
                          capture_output=True, text=True)


def read_events(params, comm, after="-"):
    out = redis_cli(params, "EVAL", STREAM_LUA, 1, f"fmi:drain:{comm}:events", after)
    if out.returncode != 0:
        return []
    events = []
    for record in out.stdout.strip("\n").split(RS):
        if not record:
            continue
        parts = record.split(FS)
        fields = {parts[i]: parts[i + 1] for i in range(1, len(parts) - 1, 2)}
        events.append((parts[0], fields))
    return events


def wait_for_event(params, comm, kind, rank, epoch, timeout_s):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        for _, fields in read_events(params, comm):
            if (fields.get("type") == kind and fields.get("rank") == str(rank)
                    and fields.get("epoch") == str(epoch)):
                return fields
        time.sleep(0.02)
    return None


def save_events(params, comm, outdir):
    try:
        with open(os.path.join(outdir, "events.log"), "w") as f:
            for eid, fields in read_events(params, comm):
                f.write(f"{eid} {fields}\n")
    except OSError:
        pass


def clean_comm(params, comm):
    redis_cli(params, "DEL", f"fmi:drain:{comm}", f"fmi:drain:{comm}:members",
              f"fmi:drain:{comm}:events", f"fmi:drain:{comm}:batch")


def data_plane(config_path):
    with open(config_path) as f:
        backends = json.load(f)["backends"]
    enabled = {n: b for n, b in backends.items()
               if str(b.get("enabled", True)).lower() == "true"}
    if list(enabled) != ["DrainTCP"]:
        sys.exit(f"{config_path} must enable DrainTCP and nothing else; enables {sorted(enabled)}")
    block = enabled["DrainTCP"]
    if str(block.get("drain", "false")).lower() != "true":
        sys.exit(f"{config_path}: DrainTCP needs drain:true")
    if str(block.get("trigger", "both")) not in ("both", "signal"):
        sys.exit(f"{config_path}: trigger={block.get('trigger')!r} does not listen for a signal")
    return block


def start_job(comm, npeers, args, outdir):
    """Launch every rank. Hygiene copied from drain_driver.start_job."""
    procs = []
    for r in range(npeers):
        log = open(os.path.join(outdir, f"r{r}.log"), "w")
        env = os.environ.copy()          # inherited, not rebuilt
        env.update(FMI_RANK=str(r), FMI_WORLD_SIZE=str(npeers),
                   FMI_CONFIG=CONFIG, FMI_COMM_NAME=comm, OMP_NUM_THREADS="1")
        # stdbuf -oL: LULESH's per-cycle progress goes to a fully buffered std::cout when
        # stdout is a file, and the driver has to read the cycle counter while the job runs.
        # stdbuf execs the target, so p.pid is the LULESH process itself.
        cmd = ["stdbuf", "-oL", LULESH, "-s", str(args.size), "-i", str(args.iters), "-p"]
        p = subprocess.Popen(cmd, env=env, stdin=subprocess.DEVNULL,
                             stdout=log, stderr=subprocess.STDOUT,
                             preexec_fn=os.setsid, cwd=HERE)
        procs.append((p, log))
    return procs


def last_cycle(outdir):
    try:
        text = open(os.path.join(outdir, "r0.log")).read()
    except OSError:
        return -1
    hits = CYCLE_RE.findall(text)
    return int(hits[-1]) if hits else -1


def energy_of(outdir):
    try:
        text = open(os.path.join(outdir, "r0.log")).read()
    except OSError:
        return None
    m = ENERGY_RE.search(text)
    return m.group(1) if m else None


def proc_state(pid):
    try:
        with open(f"/proc/{pid}/stat") as f:
            text = f.read()
    except OSError:
        return None
    tail = text[text.rfind(")") + 1:].split()
    return tail[0] if tail else None


def wait_for_state(pid, want, timeout_s):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if proc_state(pid) == want:
            return True
        time.sleep(0.02)
    return False


def alive(pid):
    # 'Z' counts as gone: a rank that exited stays in /proc as a zombie until its parent
    # (this driver) reaps it, and waiting on /proc alone would never see the job finish.
    return proc_state(pid) not in (None, "Z")


def wait_until_cycle(outdir, cycle, timeout_s, pids):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if last_cycle(outdir) >= cycle:
            return True
        if not any(alive(p) for p in pids):
            return False
        time.sleep(0.05)
    return False


def wait_for_finish(outdir, pids, timeout_s):
    """Every rank process gone, and rank 0 printed its final block."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if not any(alive(p) for p in pids):
            return True
        time.sleep(0.2)
    return False


def kill_all(procs, pids):
    for p, _ in procs:
        try:
            os.killpg(os.getpgid(p.pid), signal.SIGKILL)
        except OSError:
            pass
    for pid in pids:
        try:
            os.kill(pid, signal.SIGKILL)
        except OSError:
            pass


def socket_lines(path, limit=8):
    try:
        text = open(path, errors="replace").read()
    except OSError:
        return [], 0
    hits = [line for line in text.splitlines() if SOCKET_EVIDENCE.search(line)]
    return hits[:limit], len(hits)


def migrate(params, comm, target, epoch, pid, imgdir, reap, log, args):
    os.makedirs(imgdir, exist_ok=True)
    t0 = time.monotonic()
    sigqueue(pid, args.signal, epoch)
    log.append(f"sigqueue {args.signal} epoch={epoch} -> pid {pid}")

    sealed = wait_for_event(params, comm, "sealed", target, epoch, args.seal_timeout)
    if sealed is None:
        return "drain", f"no sealed event for rank {target} within {args.seal_timeout}s", {}
    counters = " ".join(f"{k}={v}" for k, v in sorted(sealed.items())
                        if k.startswith(("sent.", "received.")))
    log.append(f"sealed after {time.monotonic()-t0:.2f}s: {counters or '(no links)'}")
    if not wait_for_state(pid, "T", args.seal_timeout):
        return "drain", f"rank {target} sealed but never reached T (now {proc_state(pid)})", {}
    t_sealed = time.monotonic()

    fds, sockets = [], []
    try:
        for fd in sorted(os.listdir(f"/proc/{pid}/fd")):
            fds.append(fd)
            try:
                if os.readlink(f"/proc/{pid}/fd/{fd}").startswith("socket:"):
                    sockets.append(fd)
            except OSError:
                pass
    except OSError:
        pass
    log.append(f"at T: fds={len(fds)} sockets={len(sockets)}")
    if sockets:
        return "socket", f"rank {target} still holds {len(sockets)} socket fd(s)", {}

    dumped = subprocess.run(["criu", "dump", "--unprivileged", "-t", str(pid), "-D", imgdir,
                             "-v4", "-o", "dump.log"], capture_output=True, text=True)
    t_dumped = time.monotonic()
    hits, n_hits = socket_lines(os.path.join(imgdir, "dump.log"))
    log.append(f"dump rc={dumped.returncode} ({t_dumped-t_sealed:.2f}s) socket-lines={n_hits}")
    timings = {"seal_s": t_sealed - t0, "dump_s": t_dumped - t_sealed,
               "dump_rc": dumped.returncode, "socket_lines": n_hits, "fds": len(fds)}
    if n_hits:
        return "socket", "dump.log mentions a socket: " + " / ".join(hits[:3]), timings
    if dumped.returncode != 0:
        return "criu", f"dump rc={dumped.returncode}: {dumped.stderr.strip()[:300]}", timings
    in_image = [n for n in SOCKET_IMAGES if os.path.exists(os.path.join(imgdir, n))]
    if in_image:
        return "socket", f"image contains {', '.join(in_image)}", timings

    if reap is not None:
        try:
            reap.wait(timeout=30)
        except Exception:
            pass

    restored = subprocess.run(["criu", "restore", "--unprivileged", "-D", imgdir, "-d",
                               "-v4", "-o", "restore.log"], capture_output=True, text=True)
    t_restored = time.monotonic()
    timings["restore_s"] = t_restored - t_dumped
    timings["restore_rc"] = restored.returncode
    log.append(f"restore rc={restored.returncode} ({timings['restore_s']:.2f}s)")
    if restored.returncode != 0:
        return "criu", f"restore rc={restored.returncode}: {restored.stderr.strip()[:300]}", timings

    if wait_for_state(pid, "T", 2.0):
        os.kill(pid, signal.SIGCONT)
        log.append("SIGCONT (restored in group-stop)")
    else:
        log.append(f"restored running (state {proc_state(pid)}), no SIGCONT")

    back = wait_for_event(params, comm, "restored", target, epoch + 1, args.restore_timeout)
    if back is None:
        return "drain", f"rank {target} never emitted restored at epoch {epoch+1}", timings
    timings["window_s"] = time.monotonic() - t0
    log.append(f"restored epoch={back.get('epoch')} incarnation={back.get('incarnation')} "
               f"| whole window {timings['window_s']:.2f}s")
    return "ok", "", timings


def run_once(params, comm, args, outdir, target=None):
    """One run. target=None means the golden (non-migrated) run."""
    os.makedirs(outdir, exist_ok=True)
    clean_comm(params, comm)
    log = [f"comm={comm} peers={args.peers} -s {args.size} -i {args.iters}"]
    started = time.monotonic()
    procs = start_job(comm, args.peers, args, outdir)
    pids = [p.pid for p, _ in procs]
    log.append("pids " + " ".join(f"{r}:{p}" for r, p in enumerate(pids)))

    verdict, note, timings, cut = "ok", "", {}, None
    if target is not None:
        if not wait_until_cycle(outdir, args.warmup_cycle, args.warmup_timeout, pids):
            verdict, note = "void", f"rank 0 never reached cycle {args.warmup_cycle}"
        else:
            time.sleep(args.delay)
            cut = last_cycle(outdir)
            if not alive(pids[target]):
                verdict, note = "void", f"rank {target} had already finished"
            else:
                log.append(f"cut at rank-0 cycle {cut}")
                verdict, note, timings = migrate(params, comm, target, 0, pids[target],
                                                 os.path.join(outdir, "img"),
                                                 procs[target][0], log, args)
                log.append(f"rank-0 cycle just after the restore: {last_cycle(outdir)}")

    failures = []
    if verdict not in ("ok", "void"):
        failures.append(f"{verdict}: {note}")
        kill_all(procs, pids)
    else:
        if verdict == "void":
            failures.append(f"no migration landed: {note}")
        if not wait_for_finish(outdir, pids, args.finish_timeout):
            failures.append("timed out before every rank exited")
            kill_all(procs, pids)

    rcs = {}
    for r, (p, handle) in enumerate(procs):
        if target is not None and r == target and verdict == "ok":
            # criu killed the pre-dump child (p.returncode is that kill, not the run's
            # outcome) and the restored process is no longer ours to wait on. What
            # matters is that its pid left /proc without being killed by us.
            rcs[r] = f"criu-restored, pid {p.pid} {'gone' if not alive(p.pid) else 'ALIVE'}"
        else:
            try:
                rcs[r] = p.wait(timeout=5)
            except Exception:
                rcs[r] = "still running"
        handle.close()
    log.append("exit codes " + " ".join(f"{r}:{c}" for r, c in sorted(rcs.items())))

    energy = energy_of(outdir)
    final_cycle = last_cycle(outdir)
    elapsed = time.monotonic() - started
    save_events(params, comm, outdir)
    clean_comm(params, comm)
    return {"verdict": verdict, "note": note, "failures": failures, "energy": energy,
            "cut_cycle": cut, "final_cycle": final_cycle, "elapsed": elapsed,
            "log": log, "timings": timings, "outdir": outdir, "rcs": rcs}


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--peers", type=int, default=8)
    ap.add_argument("--size", type=int, default=12)
    ap.add_argument("--iters", type=int, default=400)
    ap.add_argument("--targets", type=int, nargs="*", default=[3, 0],
                    help="one migration trial per rank listed")
    ap.add_argument("--golden-energy", default=None,
                    help="skip the golden run and compare against this value")
    ap.add_argument("--warmup-cycle", type=int, default=20)
    ap.add_argument("--warmup-timeout", type=float, default=180.0)
    ap.add_argument("--delay", type=float, default=5.0,
                    help="seconds after warm-up before the migration signal")
    ap.add_argument("--seal-timeout", type=float, default=60.0)
    ap.add_argument("--restore-timeout", type=float, default=120.0)
    ap.add_argument("--finish-timeout", type=float, default=900.0)
    ap.add_argument("--signal-offset", type=int, default=None)
    ap.add_argument("--root", default=os.path.join(HERE, "drain-runs"))
    args = ap.parse_args()

    params = data_plane(CONFIG)
    if args.signal_offset is None:
        args.signal_offset = int(params.get("drain_signal_offset", 3))
    args.signal = int(signal.SIGRTMIN) + args.signal_offset
    print(f"LULESH {LULESH}\nconfig {CONFIG}\n"
          f"drain signal SIGRTMIN+{args.signal_offset} = {args.signal}")

    root = os.path.join(args.root, f"run{os.getpid()}")
    shutil.rmtree(root, ignore_errors=True)
    os.makedirs(root)
    print(f"logs and images under {root}")

    tag = f"lul{os.getpid()}"
    golden = args.golden_energy
    if golden is None:
        res = run_once(params, f"{tag}g", args, os.path.join(root, "golden"))
        print(f"golden: {' | '.join(res['log'])}")
        print(f"golden: energy={res['energy']} cycles={res['final_cycle']} "
              f"{res['elapsed']:.1f}s failures={res['failures']}")
        if res["failures"] or not res["energy"]:
            sys.exit("golden run failed")
        golden = res["energy"]
    print(f"GOLDEN Final Origin Energy = {golden}")

    bad = 0
    for i, target in enumerate(args.targets):
        res = run_once(params, f"{tag}t{i}", args, os.path.join(root, f"trial{i}rank{target}"),
                       target=target)
        ok = (not res["failures"]) and res["energy"] == golden
        print(f"\ntrial {i} target rank {target}: {'PASS' if ok else 'FAIL'}")
        for line in res["log"]:
            print(f"    {line}")
        print(f"    cut cycle {res['cut_cycle']} -> final cycle {res['final_cycle']}")
        print(f"    energy {res['energy']} vs golden {golden}")
        print(f"    wall {res['elapsed']:.1f}s timings {res['timings']}")
        if res["failures"]:
            print(f"    failures: {res['failures']}")
        if not ok:
            bad += 1
    print(f"\n== {len(args.targets)-bad} passed, {bad} failed ==")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())

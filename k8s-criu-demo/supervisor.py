#!/usr/bin/env python3
"""Per-machine supervisor for the LULESH CRIU node-evacuation demo.

Runs as the container's main process in each "machine" pod. It:
  1. renders the FMI config from the template, pinning this machine's criu.host_id (so the rank
     agent can later discover exactly this pod's ranks) plus the shared Redis + tcpunchd
     endpoints;
  2. optionally (START_GATE=1) idles until the orchestrator releases the start gate — a Redis
     key it SETs once it is up and watching. LULESH computes for real from the moment its ranks
     connect, so without the gate a slow cluster preflight could burn through the migration
     window before the orchestrator even looks;
  3. launches this machine's slice of LULESH ranks (BASE_PEER_ID .. BASE_PEER_ID+RANKS_HERE-1),
     each in its own session, writing to a regular log file (criu can re-open a regular file on
     restore; a container stdout pipe could not);
  4. stays alive, tails the per-rank logs to its own stdout (so `kubectl logs` shows progress
     and the FMI_MIGRATE window markers), and reports every rank's exit from a reaper thread:
     LULESH ranks other than 0 print nothing on success, so `rank=<r> exit=0` IS a survivor's
     success signal (the orchestrator polls for these lines).

The supervisor never restarts a worker: an evacuated rank is killed by criu dump and brought
back by criu restore as a detached process in another pod, so its death here is expected
(reported as `killed=signal-N`, not an error).
"""
import os
import subprocess
import sys
import threading
import time
from string import Template


def env(name, default=None, required=False):
    val = os.environ.get(name, default)
    if required and (val is None or val == ""):
        sys.exit(f"[supervisor] missing required env {name}")
    return val


def wait_for_start_gate(redis_host, comm_name):
    """Block until the orchestrator SETs fmi:ft:{comm}:demo:start. Keeps this machine's ranks
    unlaunched while the cluster is still being preflighted, killing the "the run finished (or
    passed its migration window) before the orchestrator Job was applied" race class."""
    import redis
    key = f"fmi:ft:{comm_name}:demo:start"
    r = redis.Redis(host=redis_host, port=6379)
    polls = 0
    while True:
        try:
            if r.get(key) is not None:
                print(f"[supervisor] start gate released ({key})", flush=True)
                return
        except redis.exceptions.RedisError as exc:
            print(f"[supervisor] start gate: redis not reachable yet ({exc})", flush=True)
        if polls % 20 == 0:
            print(f"[supervisor] waiting for start gate {key} ({polls // 2}s)", flush=True)
        polls += 1
        time.sleep(0.5)


def main():
    host_id = env("HOST_ID", required=True)            # e.g. machine-a
    num_peers = int(env("NUM_PEERS", required=True))   # total ranks across all machines
    base_peer_id = int(env("BASE_PEER_ID", required=True))
    ranks_here = int(env("RANKS_HERE", required=True)) # ranks this machine runs
    comm_name = env("COMM_NAME", required=True)
    redis_host = env("REDIS_HOST", "lulesh-redis")
    tcpunch_host = env("TCPUNCH_HOST", "lulesh-tcpunch")
    template_path = env("CONFIG_TEMPLATE", "/opt/fmi/app/fmi-lulesh.json.tmpl")
    config_path = env("CONFIG_PATH", "/tmp/fmi.json")
    lulesh_bin = env("FMI_LULESH_BIN", "/opt/fmi/bin/lulesh2.0")
    log_dir = env("LOG_DIR", "/tmp")
    lulesh_size = env("LULESH_SIZE", "15")
    lulesh_iters = env("LULESH_ITERS", "200")
    migrate_at_cycle = env("MIGRATE_AT_CYCLE", "")     # empty = never open a window
    migrate_window_ms = env("MIGRATE_WINDOW_MS", "20000")
    start_gate = env("START_GATE", "0")

    # Render the per-machine config (only host_id / endpoints vary between machines).
    with open(template_path) as f:
        rendered = Template(f.read()).safe_substitute(
            REDIS_HOST=redis_host, TCPUNCH_HOST=tcpunch_host, HOST_ID=host_id)
    with open(config_path, "w") as f:
        f.write(rendered)

    print(f"[supervisor] host_id={host_id} comm={comm_name} num_peers={num_peers} "
          f"ranks={base_peer_id}..{base_peer_id + ranks_here - 1} redis={redis_host} "
          f"tcpunch={tcpunch_host} lulesh=-s {lulesh_size} -i {lulesh_iters} "
          f"migrate_at_cycle={migrate_at_cycle or '-'} window_ms={migrate_window_ms} "
          f"start_gate={start_gate} config={config_path}", flush=True)

    if start_gate == "1":
        wait_for_start_gate(redis_host, comm_name)

    # Cross-host restore recreates each rank at its DUMPED pid, so those pids must be free in
    # the fresh restore pod (whose own pids stay tiny). Start this machine's ranks in a high pid
    # band. Needs CAP_SYS_ADMIN (the machine pods run privileged); best-effort elsewhere.
    pid_base = env("PID_BASE", "3000")
    try:
        with open("/proc/sys/kernel/ns_last_pid", "w") as f:
            f.write(pid_base)
        print(f"[supervisor] set ns_last_pid={pid_base} (rank pids start above it)", flush=True)
    except OSError as exc:
        print(f"[supervisor] warning: could not set ns_last_pid ({exc}); "
              f"cross-host restore may hit a pid collision on the restore host", flush=True)

    rank_of_pid = {}
    procs = []       # keep the Popen objects alive so only the reaper thread reaps
    log_files = []
    for peer_id in range(base_peer_id, base_peer_id + ranks_here):
        log_path = os.path.join(log_dir, f"rank-{peer_id}.log")
        log_files.append(log_path)
        rank_env = dict(os.environ,
                        FMI_RANK=str(peer_id), FMI_WORLD_SIZE=str(num_peers),
                        FMI_CONFIG=config_path, FMI_COMM_NAME=comm_name,
                        OMP_NUM_THREADS="1")
        if migrate_at_cycle:
            rank_env["FMI_MIGRATE_AT_CYCLE"] = migrate_at_cycle
            rank_env["FMI_MIGRATE_WINDOW_MS"] = migrate_window_ms
        # Truncate/create up front so `tail -F` below has something to follow.
        fh = open(log_path, "w")
        # This pod is privileged, so a bare child would carry the full capability set —
        # credentials a capability-scoped restore pod can never reproduce (bounding sets
        # only shrink). LULESH needs no capabilities: exec the rank through setpriv with
        # an empty bounding set (root's permitted/effective then collapse to empty at
        # execve), so the criu image records zero caps and restores anywhere. setpriv
        # execs in place, so proc.pid is still the rank's pid.
        proc = subprocess.Popen(["setpriv", "--bounding-set", "-all",
                                 lulesh_bin, "-s", lulesh_size, "-i", lulesh_iters],
                                env=rank_env, stdout=fh, stderr=subprocess.STDOUT,
                                start_new_session=True)
        fh.close()
        procs.append(proc)
        rank_of_pid[proc.pid] = peer_id
        print(f"[supervisor] launched rank {peer_id} (pid {proc.pid}) -> {log_path}", flush=True)

    # Mirror every rank's log to our stdout. tail -F survives the file being re-opened by a
    # criu-restored worker.
    tail = subprocess.Popen(["tail", "-n", "+1", "-F", *log_files], stdout=sys.stdout)
    procs.append(tail)

    # Reaper thread: report every rank's exit. An exit code is the only success signal a
    # non-zero LULESH rank produces, and a rank killed by criu dump (SIGKILL) is expected
    # during evacuation, not a failure. Non-rank children (the tail) are reaped silently.
    def reap_forever():
        while True:
            try:
                wpid, status = os.waitpid(-1, 0)
            except ChildProcessError:
                time.sleep(1.0)
                continue
            rank = rank_of_pid.get(wpid)
            if rank is None:
                continue
            if os.WIFSIGNALED(status):
                print(f"[supervisor] rank={rank} killed=signal-{os.WTERMSIG(status)} "
                      f"(expected if criu-dumped)", flush=True)
            else:
                print(f"[supervisor] rank={rank} exit={os.WEXITSTATUS(status)}", flush=True)

    threading.Thread(target=reap_forever, daemon=True).start()

    # Stay alive forever: the pod's lifetime is managed by its Deployment (scaled to zero when
    # the machine is evacuated), never by the workers finishing. The reaper owns waitpid, so
    # don't tail.wait() here — it would steal the tail's status.
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()

# CRIU rank-migration demo (LULESH on FMI)

> **HISTORICAL — this file describes the epoch-era control plane, which FMI no longer has.**
> It was deleted upstream (no `fmi-rank-agent`, no `promote_epoch`, no `FMI_ENABLE_CRIU`, and
> `WITH_FMI_CRIU` is now only an alias for `WITH_FMI`), so **nothing below runs against the
> pinned submodule**; it is kept for reference only. Two protocols replaced it, both armed by
> the JSON config alone with nothing in the application: the **neighborhood drain**
> (`DrainTCP`) and the **sequenced links** (`DirectTCP`, framed + `recover_links`).
>
> For the current system read **`CLAUDE.md`** (build, launch, migrate, and what else here is
> historical); for the evidence, **`MULTIHOST.md`** (four machines, 23 cross-host migrations,
> every run bit-identical) and `extern/fmi/runbooks/{drain-migration,criu-transparent-checkpoint}/`.

This demo migrates a **running LULESH rank** to a fresh process image **mid-run**
using CRIU, driven by FMI's transparent-migration control plane, and proves the
migrated rank's full **in-memory physics state survived**: the run finishes with a
`Final Origin Energy` **bit-identical** to a non-migrated run. Losing state would
diverge the physics, so an exact match is the proof.

It builds on the MPI→FMI port (see the FMI section of the top-level `README`); the
LULESH source is unchanged apart from one demo-only, env-gated migration window in
the FMI shim (`lulesh-fmi.cc`), which is a no-op on ordinary runs.

## What it proves

`N=8` LULESH ranks run the Sedov problem on FMI. Partway through (cycle 40 by
default) **rank 0** is CRIU checkpoint/restored to a new process image: FMI advances
the epoch `0 → 1`, the original rank-0 process is dumped and killed, and a restored
image resumes the run. The run completes and rank 0 reports the same
`Final Origin Energy` as a golden (non-migrated) run of the same binary and size.

The demo asserts all of:

- migrated `Final Origin Energy` **==** golden (state preserved across CRIU);
- `fmi:ft:<comm>:meta current_epoch` **== 1** (the epoch advanced);
- the rank agent printed `migrated_rank=0 promoted_epoch=1` and exited 0;
- a CRIU image tree (`core-*.img`, `files.img`, `dump.log`, …) was produced.

> CRIU restores a process under its **original PID**, so the PID does not change —
> the evidence that migration physically happened is the epoch bump, the rank-agent
> result, and the on-disk CRIU images, not a new PID.

## How it quiesces cleanly

The FMI shim emulates LULESH's non-blocking 26-neighbour halo exchange with many
blocking send/recv per cycle. A migration landing mid-exchange could strand a
TCP-buffered message when the old epoch's sockets are torn down. To avoid that, the
demo quiesces at the **per-cycle `dt` `MPI_Allreduce`** in `TimeIncrement`: at that
point the previous cycle's halo flushes have all completed (no in-flight
point-to-point) and every rank meets collectively. FMI's transparent-migration
runtime then checkpoints the target at that operation boundary.

The quiesce point is made deterministic by an env-gated window in the shim: with
`FMI_MIGRATE_AT_CYCLE=C` set, the `C`-th allreduce prints a marker and holds for
`FMI_MIGRATE_WINDOW_MS` before entering the collective, giving the driver a window
to request the migration. With the env unset the shim is unchanged.

## Prerequisites

- A running **redis-server** control plane on `127.0.0.1:6379` and `libhiredis-dev`
  at build time: `sudo apt install -y redis-server libhiredis-dev`.
- **criu** on `PATH`. For rootless operation `criu check --unprivileged` should
  report "Looks good"; the driver passes `FMI_CRIU_EXTRA_ARGS="--unprivileged"` by
  default. (Verified on Ubuntu 24.04, kernel 6.17, criu 4.2.)
- The FMI submodule: `git submodule update --init --recursive`.

## Build

```bash
cmake -S . -B build-fmi-criu -DWITH_FMI_CRIU=ON -DWITH_OPENMP=OFF
cmake --build build-fmi-criu -j
```

`WITH_FMI_CRIU` implies the FMI backend and turns on FMI's Redis control plane, CRIU
state transfer, and tools, producing `build-fmi-criu/lulesh2.0` and
`build-fmi-criu/extern/fmi/tools/fmi-rank-agent`. Build single-threaded
(`-DWITH_OPENMP=OFF`, the driver also sets `OMP_NUM_THREADS=1`) so the CRIU image is
clean.

## Run

```bash
./migration-demo.sh
```

Expected tail:

```
================= RESULT =================
golden   Final Origin Energy = 8.105927e+05
migrated Final Origin Energy = 8.105927e+05
rank-agent                   = rc=0  migrated_rank=0 promoted_epoch=1
epoch (meta current_epoch)   = 1
...
[demo] PASS
[demo]   => the migrated rank's in-memory physics state survived the migration.
```

Tunables (env): `N`, `NX`, `ITERS`, `MIGRATE_RANK`, `MIGRATE_CYCLE`, `WINDOW_MS`,
`MAX_ATTEMPTS`, `COMM_NAME`, `IMAGES_DIR`, `FMI_CRIU_EXTRA_ARGS`, and the path
overrides `BUILD_DIR`/`LULESH_EXE`/`RANK_AGENT`/`FT_CONFIG`/`NOFT_CONFIG`. The
rendezvous port comes from the config's `backends.Direct.port`. Per-rank logs are
written under a fresh temp dir printed at startup. The CRIU fault-tolerance config
is `fmi-lulesh-ft.json` (Direct data plane + Redis control plane,
`state_transfer="criu"`).

Under machine load the Direct (TCP NAT hole-punch) pairing can occasionally time
out on the first halo exchange, which would otherwise fail the run before the
migration window. The driver retries such *transient* failures up to
`MAX_ATTEMPTS` (default 3) times, each under a fresh `comm_name`; a genuine
state-loss bug instead surfaces as a wrong-but-present energy and is never retried
(the `energy == golden` check fails hard).

## Cross-host migration (relaxes limitation #1)

`migration-demo-multihost.sh` migrates a rank **across hosts**: dumped on host **A**,
restored on host **B** (true cross-host migration), driven from one driver. It needs
**zero changes to the base FMI library** — it only *unbundles* what `fmi-rank-agent
migrate` does into scriptable steps, because the two communication planes are already
host-agnostic and the rank itself drives the quiesce:

- the **data plane** (`Direct` + `tcpunchd`) is config-addressed and re-pairs lazily
  under epoch-qualified names, so a rank restored on B with a new IP re-pairs itself;
- the **control plane** (Redis) is config-addressed;
- the **rank** marks itself `QUIESCED` and publishes its pid on its own when it sees
  the migration request in Redis — the agent never touches that.

So the driver does, all over the *shared* Redis/rendezvous/NFS named in the config:

1. **request the cut** — `sadd <prefix>pending R` + `hset <prefix>epoch:0:states R
   MIGRATION_PENDING` (identical to the single-host demo);
2. **wait for quiesce** — poll `<prefix>criu:rank:R` until `state=QUIESCED`,
   `quiesced_generation=1`, `pid>0` (the rank wrote these; it is now frozen, so the
   image is stable — this is also the NFS read barrier);
3. **dump on A** — `ssh A criu dump -t <pid> -D <nfs>/<comm>/epoch-1/rank-R -o
   dump.log --shell-job --tcp-close $FMI_CRIU_EXTRA_ARGS` (flags identical to
   `LocalRankAgent::dump_rank`);
4. **restore on B** — `ssh B criu restore -D <same dir> -o restore.log --shell-job
   --tcp-close --restore-detached $FMI_CRIU_EXTRA_ARGS` (flags identical to
   `LocalRankAgent::restore_rank`);
5. **promote the epoch** — a Redis `EVAL` of the **verbatim** `promote_epoch` Lua from
   FMI's `ControlPlane.cpp` (flips `current_epoch` 0→1 and GCs the old per-epoch
   hashes), which releases the restored rank and every survivor into epoch 1.

The driver runs each host's command **locally when that host is the driver's own
machine** and over `ssh` only for genuinely remote hosts, so it works co-located with
one node and — with `HOSTS` set to a single host that is this machine — runs the whole
unbundled flow on one box with no ssh (a smoke test of the cut logic).

### Prerequisites (in addition to the single-host ones)

- Passwordless `ssh` from the driver to every remote compute host.
- One **shared** `redis-server` and one **shared** `tcpunchd`, both reachable from
  every host. Put their addresses in the config (see below) — not `127.0.0.1`.
- The CRIU `images_dir` on an **NFS mount with the identical path on every host**
  (the dump writes it on A, the restore reads it on B). Per-rank logs default under
  the same shared tree so the driver can read every host's logs.
- The `lulesh2.0` binary reachable at the same path on every host (e.g. on the NFS
  tree). `criu` working (rootless `--unprivileged` by default) on **every** host.

### Config

`fmi-lulesh-ft-multihost.json` is `fmi-lulesh-ft.json` with the CRIU `host_id` override
dropped and three addresses that you **must** point at the shared services for a true
multi-host run (they default to `127.0.0.1`, which only works when everything is
co-located / for the single-box smoke test):

- `backends.Direct.host` → the shared `tcpunchd` rendezvous address;
- `fault_tolerance.control_host` (+ `control_port`) → the shared Redis;
- `fault_tolerance.criu.images_dir` → the shared **NFS** path (identical everywhere).

### Run

```bash
# single-box smoke test of the cut logic (no ssh, no second host):
./migration-demo-multihost.sh

# true cross-host migration (rank 0 dumped on A, restored on B):
HOSTS="A B" MIGRATE_SRC=A MIGRATE_DST=B ./migration-demo-multihost.sh
```

It asserts: migrated `Final Origin Energy` **==** golden; `meta current_epoch` **==**
1; `dump.log` on A and `restore.log` on B both produced; and (race-free snapshots) the
rank's pid is **gone on A right after the dump** and **alive on B right after the
restore**. Extra tunables over the single-host demo: `HOSTS`, `PLACEMENT` (per-rank
host list), `MIGRATE_SRC`, `MIGRATE_DST`, `SSH_CMD`, `LOCAL_ALIASES`, `SHARED_DIR`/
`LOG_DIR`.

### Caveats

- **PID reclaim on B**: criu restores the rank at its *original* pid; on a different
  host that pid is usually free but not guaranteed. If it is taken, restore on fresh
  nodes or restore into a fresh PID namespace via `FMI_CRIU_EXTRA_ARGS` (no code
  change). This is the main CRIU risk to watch.
- **Promote-Lua / criu-flag drift**: the driver duplicates FMI's `promote_epoch` Lua
  and the criu flag sets. They are copied verbatim with pointers to `ControlPlane.cpp`
  and `LocalRankAgent::dump_rank`/`restore_rank`; if FMI's epoch protocol or criu flags
  change, re-sync the script.
- **Stale placement string**: the restored rank re-joins with the `placement` it
  computed at startup (says host A). Cosmetic — re-pairing keys on epoch-qualified
  names, not on placement.
- **No agent validation / consistent cut**: bypassing `fmi-rank-agent` drops its
  `ensure_migration_mode` checks and batched consistent-cut. Fine for a single-rank
  cut; a first-class cross-host feature would split the library's `dump`/`restore`
  instead (out of scope here).

## v1 limitations (inherited from FMI's CRIU path)

- **Same host** for `fmi-rank-agent migrate` / `migration-demo.sh`. **Relaxed** by
  `migration-demo-multihost.sh` above (dump on A, restore on B) — script + config
  only, no base-FMI change.
- `Direct` (TCP) is the only supported data backend; Redis is the control plane.
- One targeted rank per migration; in-flight collectives are not preserved — hence
  the allreduce quiesce point above.
- Single-threaded for a clean checkpoint.

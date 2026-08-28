# CLAUDE.md

This file provides guidance to agents when working with code in this repository.

## What this is

LULESH 2.0 (LLNL's Sedov shock-hydrodynamics proxy app) ported to run on **FMI**
(FaaS Message Interface) in place of MPI, as the substrate for **live rank-migration
demos**: a running rank — or every rank of a whole machine — is checkpoint/restored
mid-simulation, across machines, and the run finishes with its in-memory physics state
intact. The LULESH sources are upstream-unchanged; all FMI integration lives in the
MPI-compatible shim `lulesh-fmi.{h,cc}`. FMI is a git submodule at `extern/fmi`, pinned
to **`McLavish/fmi` branch `dev`**, with its own CLAUDE.md — read it when working inside
the library.

**Nothing in this repository participates in a migration.** LULESH calls MPI; the shim
calls FMI; FMI's transport does the rest. There is no migration API, no hook and no
compile-time switch in the application. That is the whole point of the demo.

The submodule offers **two independent migration protocols**, chosen by the JSON config
alone:

| | **neighborhood drain** (`DrainTCP`) | **sequenced links** (`DirectTCP`) |
| --- | --- | --- |
| config | `"drain": true` on `DrainTCP` | `"framed": true` + `"recover_links": true` on `DirectTCP` |
| how | coordinated: the rank is *asked* to leave, drains its links, ends up holding **zero sockets** | uncoordinated: frames are numbered and retained until acked, then replayed after the break |
| per-message cost | zero (headerless raw stream) | one `LinkFrame` header per message |
| criu flags | **no** `--tcp-close`, no `--tcp-established`, no `--shell-job` (there are no sockets to close) | **with** `--tcp-close --shell-job` (the image carries sockets by design) |
| needs | Redis registry **and** coordinator stream; `trigger` must include `control` | Redis registry only |
| this repo's configs | `fmi-lulesh-drain.json`, `fmi-lulesh-drain-multihost.json` | `fmi-lulesh-seq.json`, `fmi-lulesh-seq-multihost.json` |
| drivers | `lulesh-drain-migrate.py`, `lulesh-multihost-drain.py` | `lulesh-multihost-sequenced.py` |

The **epoch-era control plane is gone** from FMI: there is no `FMI_ENABLE_CRIU`, no
`FMI_BUILD_TOOLS`, no `fmi-rank-agent`, no `promote_epoch`, and consequently no
`WITH_FMI_CRIU` feature here (the option survives only as an alias for `WITH_FMI`, so old
build-dir names still work). See "Historical, non-functional" below.

**Correctness oracle:** rank 0's `Final Origin Energy` is bit-deterministic for a given
(binary, size, rank count). Serial, MPI, FMI and migrated runs all print the identical
value; a migration that loses or duplicates a single halo byte moves it. The established
golden is

    8 ranks, -s 30, run to completion:  Iteration count = 2031
                                        Final Origin Energy = 7.130703e+05

Change the rank count or `-s` and that number no longer applies — establish a fresh golden
first. Every driver asserts `migrated == golden` as its pass condition.

## Build

Fetch the submodule first: `git submodule update --init` (`--recursive` is unnecessary and
slow: FMI's own `extern/TCPunch` submodule is only needed by the `Direct` backend, which is
force-disabled below).

```bash
# The build used for every migration demo and for the golden energy.
cmake -S . -B build-fmi-drain -DWITH_FMI=ON -DWITH_OPENMP=OFF -DCMAKE_BUILD_TYPE=Release
cmake --build build-fmi-drain -j8

# Classic MPI build, for comparison
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DMPI_CXX_COMPILER=$(which mpicxx)
cmake --build build -j8
```

- `WITH_FMI` and `WITH_MPI` are mutually exclusive; FMI wins if both are on.
- Under `WITH_FMI` the top-level `CMakeLists.txt` **forces FMI's four options** before
  `add_subdirectory(extern/fmi)`, because that is now the entire option surface:
  `FMI_ENABLE_S3=OFF` (never needed, pulls the AWS SDK), `FMI_ENABLE_TCPUNCH=OFF` (the ranks
  reach each other directly, so the TCPunch submodule need not even be checked out),
  `FMI_ENABLE_REDIS=ON` (**required** — Redis is the peer registry both TCP-direct transports
  discover through, and `DrainTCP`'s coordinator event stream), `FMI_BUILD_TESTS=OFF`.
- Build deps: a C++17 compiler, Boost (log), ZLIB, `libhiredis-dev`. Plus `redis-server`
  and `criu` on PATH at run time.
- **`-DWITH_OPENMP=OFF` is not optional for migration work**: criu images must be
  single-threaded. The drivers also set `OMP_NUM_THREADS=1`. Since the energy is
  per-binary-deterministic, keep `-DCMAKE_BUILD_TYPE=Release -DWITH_OPENMP=OFF` for anything
  compared against the golden above.
- Build-dir convention: `build-fmi-drain/` (drain demos; the drivers' default `LULESH_BIN`)
  and `build-fmi-seq/` (the sequenced multihost driver's default `--binary`). Both are
  ordinary identical builds — the protocol is a config choice, not a build choice — so
  `cp -a build-fmi-drain build-fmi-seq` is a legitimate way to make the second.
- The `Makefile` is upstream LULESH's (MPI/serial only, Livermore paths); use CMake.
- **`single/lulesh.{cpp,hpp}`** is the whole program as one translation unit (the shim
  inline, backend fixed to FMI, silo body dropped, LLNL license hoisted to the top). It is
  **generated** — `single/generate.sh` concatenates the sources above; never edit it by
  hand, regenerate after touching any `lulesh*.cc/.h`. The `WITH_FMI` build also produces
  `lulesh2.0-single` from it; same flags, same env contract, and it reproduces the golden
  (the drivers accept it via `LULESH_BIN` / `--binary`). The `.hpp` is the merged
  `lulesh-fmi.h` + `lulesh.h`; standalone build is `c++ -std=c++17 -O3 -I<fmi>/include
  lulesh.cpp` plus `libFMI` and its deps. A launcher-driven variant of the same squash lives
  in the FMI repo as `example_programs/lulesh.cpp` (`--ranks 8 --size 30`, CLI only, no env
  contract) — a workload example, not the migration subject the drivers use.

## Run

There is no `mpirun` under FMI. **Each rank is a separate process** that reads its four FMI
parameters from the environment — `FMI_RANK`, `FMI_WORLD_SIZE`, `FMI_CONFIG`,
`FMI_COMM_NAME` (`lulesh-fmi.cc`, `MPI_Init`) — and takes the simulation's own arguments on
argv (`-s <size> -i <iters> -p`). Nothing else launches them; the drivers do exactly this.

```bash
redis-server --daemonize yes --bind 127.0.0.1 --port 6379 --save '' --appendonly no
COMM=lul$$
for r in 0 1 2 3 4 5 6 7; do
  OMP_NUM_THREADS=1 FMI_RANK=$r FMI_WORLD_SIZE=8 \
  FMI_CONFIG=$PWD/fmi-lulesh-drain.json FMI_COMM_NAME=$COMM \
  stdbuf -oL ./build-fmi-drain/lulesh2.0 -s 30 -i 9999999 -p \
    </dev/null >/tmp/r$r.log 2>&1 &
done; wait
grep -E "Iteration count|Final Origin Energy" /tmp/r0.log
```

- Rank count must be a **perfect cube** (1, 8, 27, …): LULESH decomposes onto a cubic
  processor grid. 8 is what every demo uses.
- `-i` is a cycle *limit*, not a count: pass something huge (the drivers use `9999999`) to
  run to natural completion at 2031 cycles, which is where the golden energy is printed.
- **Ranks 1–7 print nothing** in a healthy run. "Finished" is read from the process exiting,
  never from a log.
- `FMI_COMM_NAME` must be **unique per run**: it keys the Redis registry hash
  (`fmi:direct:<comm>` / `fmi:drain:<comm>`). Reset a stale one with
  `redis-cli DEL fmi:direct:<comm>`; the drivers clean up after themselves.
- Redis must be reachable at the config's `registry_host`/`registry_port` before rank 0
  starts. There is **no `tcpunchd`** anywhere in this flow.
- `pkill` cannot be scoped to a communicator here — every rank has an identical command line
  and the name is in the environment — so cleanup kills on `[l]ulesh2.0`. **Run one job at a
  time.**
- The shim keeps one demo-only, env-gated hook: with `FMI_MIGRATE_AT_CYCLE=C`, the C-th
  per-cycle `dt` allreduce prints a marker and holds `FMI_MIGRATE_WINDOW_MS` before entering
  the collective. Unset, it is a no-op, and **none of the current drivers use it** — they cut
  at instants the job knows nothing about, which is the stronger claim.

## Migrate

### Single host, drain protocol

```bash
python3 lulesh-drain-migrate.py --size 30 --iters 9999999 --targets 3 \
        --golden-energy 7.130703e+05
```

Launches 8 ranks on `fmi-lulesh-drain.json`, warms up, signals one rank to leave
(`SIGRTMIN+3`), waits for `sealed`, asserts the process holds **zero sockets**, criu
dump+restores it **in place** (unprivileged criu is fine same-host), `SIGCONT`s it, and lets
the run finish. Pass condition: `dump rc=0` with zero socket lines, `restore rc=0`, rank 0's
cycle counter past the cut, and the energy equal to the golden. Omit `--golden-energy` to
have it run its own clean baseline first. Evidence lands in `drain-runs/` (gitignored).

### Single host, sequenced protocol

There is no dedicated single-host driver: launch 8 ranks as in "Run" above but with
`FMI_CONFIG=$PWD/fmi-lulesh-seq.json`, then, mid-run,

```bash
criu dump    --unprivileged -t <pid> -D img --tcp-close --shell-job -v4 -o dump.log
criu restore --unprivileged        -D img -d --tcp-close --shell-job -v4 -o restore.log
```

(reap the dumped process between the two legs, or the restore fails with "File exists").
The energy must still be `7.130703e+05`. Note the `--tcp-close --shell-job` that the drain
protocol must **not** have: sequenced images carry sockets on purpose, and the replay after
the reconnect is the mechanism.

### Across machines

`lulesh-multihost-drain.py` and `lulesh-multihost-sequenced.py` run the same eight ranks
over four machines and move ranks between them. Both **import** their orchestration from the
FMI submodule's runbooks (`runbooks/drain-migration/{cluster,multihost_drain}.py` and
`runbooks/criu-transparent-checkpoint/multihost_sweep.py`) so a verdict here and a verdict in
the FMI campaign are produced by the same code. The runbook is resolved relative to the
driver file through `extern/fmi`; `FMI_RUNBOOKS` (whole `runbooks/` tree) or
`FMI_DRAIN_RUNBOOK` / `FMI_CKPT_RUNBOOK` (one directory) override it.

```bash
python3 lulesh-multihost-drain.py --nodes 10.164.0.3 10.164.0.4 10.164.0.5 10.164.0.6 --dry-run
python3 lulesh-multihost-drain.py --nodes … --only D1a --golden-energy 7.130703e+05
python3 lulesh-multihost-sequenced.py --nodes … --only D1a
```

Cluster prerequisites, all four of them load-bearing:

1. **The tree at the same absolute path on every node**, on shared storage (the campaign used
   `/scratch/LULESH_FMI` over NFS, with `extern/fmi` a symlink to a shared `/scratch/fmi`).
   The binary must be byte-identical everywhere; the drivers' preflight checks its sha and
   the FMI HEAD on each node and refuses to run otherwise.
2. **Privileged criu (`sudo`, or `CAP_SYS_ADMIN`) for anything cross-host.** Not a
   convenience: only a privileged restore lands in a time namespace that preserves
   `CLOCK_MONOTONIC`. `--unprivileged` silently skips the namespace, the restored rank
   inherits the destination machine's clock, and a forward jump expires every absolute
   deadline at once — a `Timeout` immediately after a migration that *succeeded*. Same-host
   restores are unaffected, which is why the single-host driver runs unprivileged.
3. **Disjoint pid bands per node**, seeded into `/proc/sys/kernel/ns_last_pid` (bases in
   `runbooks/drain-migration/cluster.py`: `.3`→1000000, `.4`→1500000, `.5`→2000000,
   `.6`→2500000, span 500000). criu restores a pid verbatim, so overlapping bands make a
   restore fail with "File exists". **A reboot loses the seeding** and the preflight then
   fails loudly; re-seed per node with
   `sudo sh -c 'echo 1500000 > /proc/sys/kernel/ns_last_pid'`.
4. **A cluster-reachable Redis** at the multihost configs' `registry_host:registry_port`
   (`10.164.0.3:6380`), i.e. bound to something other than loopback:
   `redis-server --daemonize yes --bind 0.0.0.0 --port 6380 --save '' --appendonly no
   --protected-mode no`. Passwordless `ssh` and `sudo` to every node are also assumed.

`MULTIHOST.md` is the four-machine evidence (11 trials, 23 cross-host migrations, all
`7.130703e+05`) and the place to look for what each scenario `D0…D4b` moves. The FMI
runbooks under `extern/fmi/runbooks/` document the protocols themselves.

## Architecture

- `lulesh-fmi.h` declares only the MPI subset LULESH uses (pulled in by `lulesh.h` under
  `-DUSE_FMI=1`); `lulesh-fmi.cc` implements it on one global `FMI::Communicator`.
  `MPI_Datatype` values encode the element size.
- Non-blocking p2p (`MPI_Isend/Irecv/Wait*`) is emulated by deferring sends/recvs and
  flushing them in a deadlock-free order on top of FMI's blocking API — the 26-neighbour halo
  exchange in `lulesh-comm.cc` runs unchanged.
- LULESH is globally synchronised once per cycle (a `dt` allreduce), which is why rank 0's
  `cycle = N` line is a statement about *every* rank, and why the drivers use it as the
  progress oracle. With a 2×2×2 decomposition every rank is every other's neighbour, so all
  28 links carry traffic and a 4-machine block placement puts 24 of them across a boundary.

## Historical, non-functional

The following predate the protocol change and are **kept for reference only**. They drive
FMI's deleted epoch control plane — `fmi-rank-agent`, `promote_epoch`, `FMI_ENABLE_CRIU`,
`WITH_FMI_CRIU`'s old meaning — none of which exists in the pinned submodule, so they cannot
work against it and must not be read as documentation of the current system:

- `run-fmi.sh`, `fmi-common.sh` — launchers built around a `tcpunchd` rendezvous server and
  the `Direct` backend.
- `migration-demo.sh`, `migration-demo-multihost.sh` — the epoch-era migration demos; the
  multihost one duplicates the deleted `promote_epoch` Lua verbatim.
- `fmi-lulesh.json`, `fmi-lulesh-ft.json`, `fmi-lulesh-ft-multihost.json` — configs with
  `Direct`/control-plane/`criu` blocks the current library does not read.
- `k8s-criu-demo/` — the Kubernetes/Knative node-evacuation demo (own README, `local-gate.sh`
  rehearsal, pytest suite), built on the same epoch protocol.
- `MIGRATION.md` — the epoch-era demo write-up. Its header now says so and points here.

Replaced by, respectively: the "Run" launch loop above, the three drivers, the four
`fmi-lulesh-{drain,seq}[-multihost].json` configs, and `MULTIHOST.md`.

## Gotchas

- **Bumping FMI** means committing in the FMI repo on `dev`, pushing it, then updating the
  gitlink here in its own commit. The gitlink must point at a commit that exists on
  `origin/dev` at GitHub — that history has been force-rewritten before, and a gitlink into
  an orphaned old history makes the submodule unfetchable for everyone else. Check with
  `git -C extern/fmi branch -r --contains <sha>`.
- A drain-armed config needs `"trigger"` to include `control` on **every** rank: the
  migrator's drain only finishes once its peers have half-closed, and they learn to from the
  coordinator, not from the application.
- Each config enables **exactly one** transport on purpose. Enabling two lets FMI's cost
  model route operations to the cheaper one, and the protocol under test is then never
  exercised — the run goes green having proved nothing.
- criu restores a process under its **original pid**. Proof of migration is the event trail
  (`leaving → sealed → restored`), the two criu logs naming two different hosts, and the
  incarnation bump — never a new pid.
- A `Timeout` from FMI is **terminal for the communicator**; neither protocol makes it
  recoverable. Cross-host, suspect prerequisite 2 (the time namespace) first.

## Git commits

- Commit after every meaningful, self-contained change; each commit should leave the tree
  working. Stage only the files relevant to the change.
- Write short, clear messages describing what changed (see `git log` for the style).

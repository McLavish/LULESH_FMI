# CLAUDE.md

This file provides guidance to agents when working with code in this repository.

## What this is

LULESH 2.0 (LLNL's Sedov shock-hydrodynamics proxy app) ported to run on **FMI**
(FaaS Message Interface) in place of MPI, as the substrate for **CRIU rank-migration
demos**: a running rank (or a whole node's ranks) is checkpoint/restored mid-simulation
and the run finishes with its in-memory physics state intact. The LULESH sources are
upstream-unchanged; all FMI integration lives in the MPI-compatible shim
`lulesh-fmi.{h,cc}`. FMI is a git submodule at `extern/fmi` (pinned to
`McLavish/fmi` branch `feat/criu-state-transfer`) with its own CLAUDE.md — read it
when working inside the FMI library.

**Correctness oracle:** rank 0's `Final Origin Energy` is bit-deterministic for a
given (binary, size, iterations, rank count). Serial, MPI, FMI, and migrated runs
must all print the identical value; a migration that loses state diverges the energy.
Every demo asserts `migrated == golden` as its pass condition.

## Build

Fetch the submodule first: `git submodule update --init --recursive`.

```bash
# FMI backend (Direct/TCP only; deps: Boost, ZLIB, bundled TCPunch)
cmake -S . -B build-fmi -DWITH_FMI=ON && cmake --build build-fmi -j

# FMI + CRIU migration (adds Redis control plane + fmi-rank-agent;
# deps: redis-server, libhiredis-dev, criu on PATH; single-threaded for clean images)
cmake -S . -B build-fmi-criu -DWITH_FMI_CRIU=ON -DWITH_OPENMP=OFF && cmake --build build-fmi-criu -j

# Classic MPI build
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DMPI_CXX_COMPILER=$(which mpicxx) && cmake --build build -j
```

- `WITH_FMI` and `WITH_MPI` are mutually exclusive; FMI wins if both are on.
- `WITH_FMI_CRIU` implies `WITH_FMI` and force-enables FMI's Redis/CRIU/tools
  (producing `build-fmi-criu/extern/fmi/tools/fmi-rank-agent`). S3 is always forced off.
- `build-fmi/` and `build-fmi-criu/` are the established build-dir names; the demo
  scripts default to them.
- The `Makefile` is upstream LULESH's (MPI/serial only, Livermore paths); use CMake.

## Run

There is no `mpirun` under FMI: each rank is a separate process reading
`FMI_RANK`, `FMI_WORLD_SIZE`, `FMI_CONFIG`, `FMI_COMM_NAME`, and a `tcpunchd`
rendezvous server provides Direct/TCP pairing. The launchers handle all of that:

```bash
./run-fmi.sh 8 -s 10 -i 20        # plain FMI run: 8 ranks, no fault tolerance
./migration-demo.sh                # single-host CRIU migration of rank 0 (needs build-fmi-criu)
./migration-demo-multihost.sh      # cross-host cut (single-box smoke test without HOSTS set)
HOSTS="A B" MIGRATE_SRC=A MIGRATE_DST=B ./migration-demo-multihost.sh   # true cross-host
```

- Rank count must be a **perfect cube** (1, 8, 27, ...): LULESH decomposes onto a
  cubic processor grid.
- The rendezvous port comes from the config's `backends.Direct.port`, never an env
  var — edit the JSON or point `FMI_CONFIG` elsewhere.
- Configs: `fmi-lulesh.json` (Direct only, FT off), `fmi-lulesh-ft.json` (CRIU FT,
  single host), `fmi-lulesh-ft-multihost.json` (cross-host: its `Direct.host`,
  `control_host`, and `criu.images_dir` must point at shared tcpunchd/Redis/NFS).
- Shared launcher helpers (config parsing, tcpunchd build/start/reuse) live in
  `fmi-common.sh`.
- `k8s-criu-demo/` is the Kubernetes/Knative node-evacuation demo (all ranks on one
  machine dumped in one cut, restored onto scale-from-zero Knative pods); it has its
  own README, a cluster-free rehearsal in `local-gate.sh`, and unit tests under
  `k8s-criu-demo/tests/` (pytest).

Demo docs: `MIGRATION.md` covers the single-host and cross-host demos, prerequisites
(rootless `criu check --unprivileged`, Redis on 6379), tunables, and caveats.

## Architecture

- `lulesh-fmi.h` declares only the MPI subset LULESH uses (pulled in by `lulesh.h`
  under `-DUSE_FMI=1`); `lulesh-fmi.cc` implements it on one global
  `FMI::Communicator`. `MPI_Datatype` values encode the element size.
- Non-blocking p2p (`MPI_Isend/Irecv/Wait*`) is emulated by deferring sends/recvs and
  flushing them in a deadlock-free order on top of FMI's blocking API — the 26-neighbour
  halo exchange in `lulesh-comm.cc` runs unchanged.
- The shim contains one demo-only, env-gated hook: with `FMI_MIGRATE_AT_CYCLE=C`, the
  C-th per-cycle dt allreduce prints a window marker and holds `FMI_MIGRATE_WINDOW_MS`
  before entering the collective. That allreduce is the quiesce point (no in-flight
  p2p; all ranks meet), which is what makes a clean CRIU cut possible. Unset, it's a no-op.

## Gotchas

- A stale `tcpunchd` holding the rendezvous port with leftover state is the classic
  cause of a pairing hang; the launchers reuse-or-restart it, but check for one first
  when debugging.
- Direct pairing can time out transiently under load. The demos retry up to
  `MAX_ATTEMPTS` times under a fresh `comm_name`; a wrong-but-present energy is a real
  state bug and is never retried.
- CRIU restores a process under its **original PID** — proof of migration is the epoch
  bump (`current_epoch` 0→1), the rank-agent result, and the image tree, not a new PID.
- CRIU runs must be single-threaded: build with `-DWITH_OPENMP=OFF`; the drivers also
  set `OMP_NUM_THREADS=1`.
- `migration-demo-multihost.sh` duplicates FMI's `promote_epoch` Lua and criu flag
  sets **verbatim** (pointers to `ControlPlane.cpp` / `LocalRankAgent`); re-sync the
  script whenever FMI's epoch protocol or criu flags change.
- Bumping FMI means committing in the FMI repo (branch `feat/criu-state-transfer`),
  then updating the submodule pointer here in its own commit.

## Git commits

- Commit after every meaningful, self-contained change; each commit should leave the
  tree working. Stage only the files relevant to the change.
- Write short, clear messages describing what changed (see `git log` for the style).

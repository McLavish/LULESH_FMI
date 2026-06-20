# CRIU rank-migration demo (LULESH on FMI)

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
- the supervisor printed `migrated_rank=0 promoted_epoch=1` and exited 0;
- a CRIU image tree (`core-*.img`, `files.img`, `dump.log`, …) was produced.

> CRIU restores a process under its **original PID**, so the PID does not change —
> the evidence that migration physically happened is the epoch bump, the supervisor
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
`build-fmi-criu/extern/fmi/tools/fmi-migration-supervisor`. Build single-threaded
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
supervisor                   = rc=0  migrated_rank=0 promoted_epoch=1
epoch (meta current_epoch)   = 1
...
[demo] PASS
[demo]   => the migrated rank's in-memory physics state survived the migration.
```

Tunables (env): `N`, `NX`, `ITERS`, `MIGRATE_RANK`, `MIGRATE_CYCLE`, `WINDOW_MS`,
`COMM_NAME`, `FMI_DIRECT_PORT`, `IMAGES_DIR`, `FMI_CRIU_EXTRA_ARGS`, and the path
overrides `BUILD_DIR`/`LULESH_EXE`/`SUPERVISOR`/`FT_CONFIG`/`NOFT_CONFIG`. Per-rank
logs are written under a fresh temp dir printed at startup. The CRIU fault-tolerance
config is `fmi-lulesh-ft.json` (Direct data plane + Redis control plane,
`state_transfer="criu"`).

## v1 limitations (inherited from FMI's CRIU path)

- Same host only (criu restores on the host that dumped).
- `Direct` (TCP) is the only supported data backend; Redis is the control plane.
- One targeted rank per migration; in-flight collectives are not preserved — hence
  the allreduce quiesce point above.
- Single-threaded for a clean checkpoint.

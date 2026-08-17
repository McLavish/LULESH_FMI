# LULESH across four machines, migrated mid-run (neighborhood drain)

The cross-host sequel to the single-host drain validation. Same subject, same oracle — rank 0's
`Final Origin Energy`, bit-deterministic for a given (binary, size, rank count) — but the eight
ranks are spread over four machines and the migrations move a rank, or a whole machine's ranks,
**from one machine to another** while the simulation runs.

The protocol is FMI's **neighborhood drain** (`DrainTCP`, `"drain": true`), not the epoch-era
control plane `MIGRATION.md` describes: nothing in LULESH or in the shim participates. A rank is
asked to leave by a signal (or by a `migrate` event for a batch), it drains its links, ends up
holding **zero sockets**, and criu images it with no `--tcp-close`, no `--tcp-established` and no
`--shell-job` on either leg.

## What was run

| | |
| --- | --- |
| date / trees | 2026-08-14; FMI `feat/neighborhood-drain` @ `91b3b31`, this tree's then-uncommitted port. That FMI hash is **pre-rewrite and no longer reachable**: the branch was merged into `dev` and its history rebuilt, and the commit carrying this code is now `dev` @ `8fe6b69`, which is what the `extern/fmi` gitlink pins. Re-verified against it on 2026-08-17 (below). |
| cluster | criu-testing `10.164.0.3` (T, 8c), criu-node-2 `.4` (N2), criu-node-1 `.5` (N1), criu-node-3 `.6` (N3); Rocky 9.8, criu 3.19 under `sudo`, shared `/scratch` |
| build | `/scratch/LULESH_FMI`, `extern/fmi` a symlink to the shared `/scratch/fmi`; `cmake -DWITH_FMI=ON -DWITH_OPENMP=OFF -DCMAKE_BUILD_TYPE=Release` — the single-host build's options exactly, because the golden energy came from them |
| binary | `build-fmi-drain/lulesh2.0`, sha256 `8545f08ee26c1cd4ac1c799dcf3400fe910ff78db0a195611947f03ec6fd7bfe`, identical on all four nodes |
| config | `fmi-lulesh-drain-multihost.json` — DrainTCP alone, `drain: true`, `trigger: both`, registry `10.164.0.3:6380`, `bind_host 0.0.0.0`, `advertise_host ""`, `migration_max_ms 120000` |
| job | 8 ranks (a perfect cube), 2 per machine, block placement r0,r1@T r2,r3@N2 r4,r5@N1 r6,r7@N3; `-s 30`, run to completion at 2031 cycles |
| driver | `lulesh-multihost-drain.py` |

## Result

**11 trials, 11 passed. 23 cross-host migrations — 9 by signal, 14 inside 5 single cuts — every
one dumped on one machine and restored on another. Every run, clean and migrated, printed
`Final Origin Energy = 7.130703e+05` and `Iteration count = 2031`: the four-machine clean run
reproduces the single-host golden, and every migration preserves it bit for bit.**

| trial | what moves | energy | cycles | cut at cycle | window |
| --- | --- | --- | --- | --- | --- |
| D0 clean | — | `7.130703e+05` | 2031 | — | — |
| D1a | r3 N2→N3 | `7.130703e+05` | 2031 | 271 | 978 ms |
| D1b | r6 N3→T | `7.130703e+05` | 2031 | 266 | 971 ms |
| D1c | **r0** T→N1 | `7.130703e+05` | 2031 | 500 | 1060 ms |
| D2a | r2 N2→T, r5 N1→N2, r7 N3→N1 (sequential) | `7.130703e+05` | 2031 | 468 / 722 / 1049 | 958 / 945 / 937 ms |
| D2b | **r0** T→N3, r3 N2→N1, r6 N3→T (sequential) | `7.130703e+05` | 2031 | 373 / 392\* / 392\* | 1041 / 948 / 983 ms |
| D3a | cut: N2 evacuated (r2, r3) → T, N1 | `7.130703e+05` | 2031 | 282 | 1902 ms |
| D3b | cut: N1 evacuated (r4, r5) → T, N2 | `7.130703e+05` | 2031 | 504 | 1919 ms |
| D3c | cut: **T** evacuated (r0, r1) → N2, N1 | `7.130703e+05` | 2031 | 484 | 2029 ms |
| D4a | cut k=4: N1+N3 (r4…r7) → T, N2 | `7.130703e+05` | 2031 | 302 | 3786 ms |
| D4b | cut k=4: T+N2 (r0…r3) → N1, N3 | `7.130703e+05` | 2031 | 525 | 3857 ms |

"window" is `leaving → restored` for a single rank, and first `sealed` → last `restored` for a
cut. The drain itself — half-closing seven links carrying tens of megabytes each and reading
them to EOF — takes **1–20 ms**, whatever k is and whatever the traffic.

\* The cut cycle is read from rank 0's log, and in D2b rank 0 had just moved to N3, so the two
later readings are a stale NFS view (392) rather than where the simulation actually was. It only
ever makes the "progressed past the cut" check conservative — the run finished at 2031 — and the
migration windows, which come from the Redis event stream, are unaffected.

Also checked per trial, by the campaign driver's own code: `fds=5 sockets=0` at state `T` for
every migrated rank; no `inetsk.img` / `unixsk.img` / `tcp-stream.img` / `sk-queues.img` /
`packetsk.img` / `netlinksk.img` in any of the 23 image directories; no socket line in any
`dump.log`; both criu legs rc 0; `leaving(e) → sealed(e) → restored(e+1)` with the expected batch
id; every member of a cut sealed before any member restored; `sealed[a].sent.b ==
sealed[b].received.a` for all 15 pairs inside cuts; the restored pid still in its launch node's
band; and the rank actually on a different machine (the two criu logs name the two hosts).

**LULESH's own elapsed time is a placement effect, not a migration cost**: it is
bulk-synchronous, so it runs at the speed of the busiest machine. Sorted by ranks-per-core on
the busiest node after the moves — 0.5 → 26–29 s, 0.75 → 35–39 s, 1.0 → 41 s (D4, which ends
with four ranks on one 4-core box). The clean run is 26 s.

## Re-verified against the pinned submodule (2026-08-17)

The campaign above predates the FMI history rewrite. Rebuilt from `extern/fmi` @ `8fe6b69` and
re-run on the same four machines, one smoke per protocol, both `PASS` with the golden energy:

| check | result |
| --- | --- |
| D1a, drain (`lulesh-multihost-drain.py`) | r3 `10.164.0.4`→`10.164.0.6`; `fds=5 sockets=0`, dump rc 0 / 0 socket lines, restore rc 0, window 1.18 s; `7.130703e+05`, 2031 cycles |
| D1a, sequenced (`lulesh-multihost-sequenced.py`) | r3 `10.164.0.4`→`10.164.0.6` (with `--tcp-close --shell-job`); dump 0.19 s, restore 0.16 s, window 0.63 s; `7.130703e+05`, 2031 cycles |

Single-host, same build: the clean 8-rank run, one drain migration and one sequenced criu
dump+restore all printed `7.130703e+05` at 2031 cycles too. Note the nodes' pid bands had been
lost to a reboot and had to be re-seeded (`/proc/sys/kernel/ns_last_pid`) before the preflight
would pass — see `CLAUDE.md`'s cluster prerequisites.

## Running it

```bash
# on the cluster head, with the tree at the SAME absolute path on every node
cd /scratch/LULESH_FMI
python3 lulesh-multihost-drain.py --nodes 10.164.0.3 10.164.0.4 10.164.0.5 10.164.0.6 --dry-run
python3 lulesh-multihost-drain.py --nodes … --only D0 --keep          # establishes the golden
python3 lulesh-multihost-drain.py --nodes … --only D3a,D3b,D3c --golden-energy 7.130703e+05
```

The driver **imports** `runbooks/drain-migration/cluster.py` and `multihost_drain.py` from the
FMI checkout — the `extern/fmi` submodule resolved relative to the driver file, or
`$FMI_RUNBOOKS` / `$FMI_DRAIN_RUNBOOK` — and reuses their orchestration verbatim —
`migrate_one`, `evacuate`, the criu legs, the batch lease, the event reader, the socket
evidence. What is LULESH's own is only: the launch (the four FMI parameters go through
`FMI_RANK` / `FMI_WORLD_SIZE` / `FMI_CONFIG` / `FMI_COMM_NAME`, not argv), `stdbuf -oL` so the
per-cycle counter is readable while the job runs, rank 0's `cycle = N` as the progress oracle
(one global allreduce per cycle makes it a statement about all eight ranks), and the energy
comparison. Evidence for the runs above: `/scratch/LULESH_FMI/drain-runs/mh*/`.

Three things to know before re-running:

* **`-s 30` and 8 ranks are the golden's parameters.** Change either and `7.130703e+05` no
  longer applies; establish a fresh golden first.
* **`pkill` cannot be scoped to a communicator here** — every rank has an identical command line
  and the name is in the environment — so cleanup kills on `[l]ulesh2.0` across all nodes. Run
  one job at a time.
* **Ranks 1–7 print nothing** in a healthy run, so "finished" is read from the process, not the
  log: one `wait-gone` per rank, which also guarantees the NFS write-back before rank 0's energy
  is read.

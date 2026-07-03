# LULESH CRIU node evacuation → serverless (Kubernetes + Knative)

Evacuate **all the LULESH ranks co-located on one machine, mid-simulation, in a single
consistent CRIU cut, onto scale-from-zero Knative instances — with their in-memory state
preserved bit-for-bit**. This is the FMI `runbooks/k8s-criu-node-evacuation` demo with a real
HPC workload instead of a synthetic worker: LULESH (via the repo's FMI MPI shim) computing a
shock hydrodynamics simulation whose final answer proves whether the migrated memory survived.

The scenario: an 8-rank LULESH run (`-s 15 -i 200`, ranks in a 2×cube layout) computes **4
ranks on each of two machines** (nodes). At cycle 100 every rank opens a 20 s migration window
(prints a marker and holds *before* the cycle's dt allreduce). An orchestrator Job:

1. releases the **start gate** — the machine pods sit idle on a Redis key until the
   orchestrator exists, so the run can't race ahead of it — then waits for all 8 ranks ACTIVE;
2. waits for the **window marker** in machine A's log, then execs
   `fmi-rank-agent evacuate-local` in that pod: machine A's 4 ranks are quiesced together,
   criu-dumped in one cut, and their packed images **staged in Redis**;
3. scales machine A's Deployment to **zero** — the node is gone;
4. POSTs one `/restore` per rank to the **`lulesh-restore` Knative Service**
   (scale-from-zero, `containerConcurrency: 1`): four cold-started pods each fetch a staged
   image, `criu restore` their rank, and hold the request while the restored LULESH computes
   cycles 100..200;
5. once the CRIU registry shows every rank RUNNING on a `lulesh-restore-*` host, promotes the
   epoch **once**; survivors on machine B (parked at the allreduce) and the restored ranks
   rebuild channels at epoch 1 and finish the simulation together;
6. verifies: every `/restore` response reports `exit_code: 0`, every survivor logs
   `rank=<r> exit=0`, and — run **last**, as an independent measurement — a golden
   non-migrated LULESH's `Final Origin Energy` is **string-identical** to the migrated run's.

Why the energy check is the whole point: LULESH's final energy is a bit-deterministic function
of (binary, `-s`, `-i`, rank count). 100 post-restore cycles of nonlinear hydrodynamics amplify
any corrupted byte of restored state into a divergent energy; an identical string is end-to-end
proof the checkpoint/restore was exact.

## Timing discipline (what's different from the synthetic FMI demo)

LULESH computes for real from the moment its ranks connect — there is no "big sleep" to hide
setup latency in. Three mechanisms keep the cut correct and race-free:

- **The start gate.** Machine supervisors idle until the orchestrator SETs
  `fmi:ft:{comm}:demo:start`. Cluster preflights can take minutes; without the gate the run
  could pass cycle 100 (or finish) before the orchestrator ever looked.
- **The migration window is mandatory for correctness, not just determinism.** A cut requested
  at an arbitrary time quiesces its targets at their next operation boundary — but a
  *survivor* already blocked in a `recv` from a target (whose sockets are released before the
  dump) would be stranded. During the held window no rank is inside any operation: targets
  quiesce at the allreduce, survivors park cleanly. The orchestrator therefore waits for the
  `FMI_MIGRATE: window open` marker before cutting.
- **The golden run comes last.** It's an independent measurement, so it doesn't need to precede
  anything — and running it first would eat the window budget. (The local gate runs it first
  instead, safely: there the machines aren't started yet.)

## Components

| File | Role |
|------|------|
| `Dockerfile` | One image for every role: `lulesh2.0` (FMI shim, CRIU-enabled), `fmi-rank-agent`, a robust `tcpunchd`, `criu` 4.2 from source, and the python entrypoints. Build context = repo root. |
| `supervisor.py` | Per-machine pod entrypoint: renders the config, waits on the start gate, sets the pid band, launches this machine's LULESH rank slice to restorable log files, reports every rank's exit (`rank=<r> exit=<rc>`). |
| `restore_server.py` | Knative container entrypoint: `POST /restore {"rank": R}` runs `fmi-rank-agent restore-remote` and holds the request while the restored rank runs, answering with its exit code and log tail (rank 0's carries the energy). |
| `golden_run.py` | The non-migrated reference: N local ranks over a private tcpunchd, FT disabled; prints `GOLDEN_ENERGY=<v>`. Importable (orchestrator) + CLI (local gate). |
| `orchestrator.py` | Kubernetes Job: gate → window → evacuate-local → scale to 0 → parallel `/restore` → registry-verified relocation → promote → exits verified → golden energy comparison. |
| `fmi-lulesh.json.tmpl` | FT config template (Direct data plane, per-role `criu.host_id`). |
| `fmi-lulesh-golden.json.tmpl` | Non-FT config for the golden run (private tcpunchd endpoint). |
| `k8s/*.yaml` | namespace, redis, tcpunch (headless), templated machine Deployment, RBAC, the `lulesh-restore` Knative Service, orchestrator Job. |
| `local-gate.sh` | Cluster-free dress rehearsal: machine A is a container that gets **stopped** after staging; four fresh containers restore its ranks over HTTP; PASS = bit-identical energy at epoch 1. |
| `tests/test_orchestrator.py` | Unit tests for the parsing/verification logic. |

The tcpunchd robustness patch is reused in place from the FMI submodule
(`extern/fmi/runbooks/k8s-criu-node-evacuation/patches/`), applied during the image build.

## Prerequisites

- Local gates: `docker`, submodules initialized (`git submodule update --init --recursive`
  at the repo root — TCPunch is nested inside `extern/fmi`).
- Cluster: a multi-node cluster with **Knative Serving**, nodes that allow privileged pods
  (the machine pods) and can run `criu` (recent kernel), pod-to-pod TCP between nodes, a
  registry your nodes can pull from, and **cluster-admin on the `knative-serving` namespace**
  (one ConfigMap patch, below). `docker`, `kubectl`, `envsubst` on your workstation.

## 1. Verify locally first (no cluster)

These are the fast gates; run them before touching the cluster. From the repo root:

```bash
# (a) orchestrator + restore-server logic unit tests
python3 k8s-criu-demo/tests/test_orchestrator.py

# (b) build the image (context = repo root; needs initialized submodules)
docker build -f k8s-criu-demo/Dockerfile -t lulesh-criu-evac:dev .

# (c) the dress rehearsal: golden run, then machine-a is STOPPED after staging and its ranks
#     restored over HTTP into four fresh containers (distinct hostnames/IPs, fresh pid ns)
bash k8s-criu-demo/local-gate.sh
```

(c) ends with
`PASS: machine-a's 4 LULESH ranks were evacuated mid-run ... Final Origin Energy bit-identical to the golden run`.
This exercises everything the cluster path needs except Knative itself.

## 2. Run on the cluster

Set the shared variables once. `LULESH_IMAGE` must be reachable from your nodes (push the
image built above to your registry).

```bash
export LULESH_IMAGE=<your-registry>/lulesh-criu-evac:v1
export IMAGE_PULL_POLICY=Always
export COMM_NAME=lulesh-evac-1
export NUM_PEERS=8            # must be a cube × machines layout: 8 = 2×4, LULESH needs a cube
export LULESH_SIZE=15
export LULESH_ITERS=200
export MIGRATE_AT_CYCLE=100
export MIGRATE_WINDOW_MS=20000
export RESTORE_MAX_SCALE=4    # = ranks per machine
```

`NUM_PEERS` must be a perfect cube (8, 27, ...) — a LULESH constraint, not an FMI one.

### 2a. Enable the Knative feature gates (once, cluster-admin)

criu restore needs root + ptrace/admin capabilities inside the restore pods; Knative blocks
securityContext fields, added capabilities, and emptyDir volumes unless these gates are on:

```bash
kubectl patch configmap/config-features -n knative-serving --type merge -p \
  '{"data":{"kubernetes.podspec-securitycontext":"enabled",
            "kubernetes.containerspec-addcapabilities":"enabled",
            "kubernetes.podspec-volumes-emptydir":"enabled"}}'
```

### 2b. Label the two nodes that will host the machines

```bash
kubectl get nodes
kubectl label node <NODE-A> lulesh-machine=a --overwrite
kubectl label node <NODE-B> lulesh-machine=b --overwrite
```

### 2c. Base infra

```bash
cd k8s-criu-demo
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/redis.yaml
kubectl -n lulesh-criu rollout status deploy/lulesh-redis --timeout=120s
envsubst '${LULESH_IMAGE} ${IMAGE_PULL_POLICY}' < k8s/tcpunch.yaml | kubectl apply -f -
kubectl -n lulesh-criu rollout status deploy/lulesh-tcpunch --timeout=120s
kubectl apply -f k8s/rbac.yaml
```

### 2d. The restore Knative Service and the two machines

```bash
envsubst '${LULESH_IMAGE} ${IMAGE_PULL_POLICY} ${COMM_NAME} ${NUM_PEERS} ${RESTORE_MAX_SCALE}' \
  < k8s/knative-restore-service.yaml | kubectl apply -f -
kubectl -n lulesh-criu wait ksvc/lulesh-restore --for=condition=Ready --timeout=180s

MACHINE_ID=a NODE_VALUE=a BASE_PEER_ID=0 RANKS_HERE=4 \
  envsubst < k8s/machine-deployment.yaml | kubectl apply -f -
MACHINE_ID=b NODE_VALUE=b BASE_PEER_ID=4 RANKS_HERE=4 \
  envsubst < k8s/machine-deployment.yaml | kubectl apply -f -
kubectl -n lulesh-criu rollout status deploy/lulesh-machine-a --timeout=180s
kubectl -n lulesh-criu rollout status deploy/lulesh-machine-b --timeout=180s
```

The machine pods come up **idle at the start gate** (`waiting for start gate` in their logs) —
LULESH has not started; take your time with the preflights.

### 2e. Preflights (before the orchestrator)

```bash
# (1) criu can checkpoint inside a machine pod
kubectl -n lulesh-criu exec deploy/lulesh-machine-a -- criu check --all

# (2) criu can RESTORE inside a Knative pod — the highest-risk piece of the whole demo.
#     Temporarily scale the service to one warm instance and run criu check in it:
kubectl -n lulesh-criu patch ksvc lulesh-restore --type merge -p \
  '{"spec":{"template":{"metadata":{"annotations":{"autoscaling.knative.dev/min-scale":"1"}}}}}'
kubectl -n lulesh-criu wait ksvc/lulesh-restore --for=condition=Ready --timeout=120s
POD=$(kubectl -n lulesh-criu get pod -l serving.knative.dev/service=lulesh-restore \
      -o jsonpath='{.items[0].metadata.name}')
kubectl -n lulesh-criu exec "$POD" -c user-container -- criu check --all
kubectl -n lulesh-criu patch ksvc lulesh-restore --type merge -p \
  '{"spec":{"template":{"metadata":{"annotations":{"autoscaling.knative.dev/min-scale":"0"}}}}}'

# (3) the rendezvous is reachable and pod-to-pod TCP works across the two nodes
kubectl -n lulesh-criu exec deploy/lulesh-machine-a -- sh -c \
  'getent hosts lulesh-tcpunch && nc -z -w3 lulesh-tcpunch 10000 && echo tcpunch-ok'
B_IP=$(kubectl -n lulesh-criu get pod -l lulesh-machine=b -o jsonpath='{.items[0].status.podIP}')
kubectl -n lulesh-criu exec deploy/lulesh-machine-a -- ping -c1 -W2 "$B_IP"

# (4) both machine pods are Running and parked at the gate (ranks NOT launched yet —
#     epoch:0:states stays empty until the orchestrator releases the gate; that's correct)
kubectl -n lulesh-criu logs deploy/lulesh-machine-a --tail=3   # "waiting for start gate ..."
```

If preflight (2) fails: the ksvc pod could not get its capabilities — check that the 2a patch
took effect (`kubectl get cm config-features -n knative-serving -o yaml`), that no PodSecurity
admission policy on the namespace blocks it, and that your container runtime accepts the
`CHECKPOINT_RESTORE` capability (drop it from `k8s/knative-restore-service.yaml` on kernels/
runtimes that predate it — `SYS_ADMIN` covers it there). If the cross-node `ping`/`nc` fails, a
NetworkPolicy or CNI is blocking pod-to-pod traffic — the Direct data plane needs it.

### 2f. Evacuate machine A to serverless and verify

The Job releases the gate, drives everything, and ends with the golden comparison:

```bash
envsubst '${LULESH_IMAGE} ${IMAGE_PULL_POLICY} ${COMM_NAME} ${NUM_PEERS} ${LULESH_SIZE} ${LULESH_ITERS}' \
  < k8s/orchestrator-job.yaml | kubectl apply -f -
kubectl -n lulesh-criu logs -f job/lulesh-orchestrator
```

Expected:

```
[orchestrator] released the start gate (fmi:ft:lulesh-evac-1:demo:start)
[orchestrator] --- epoch 0 directory ---
[orchestrator]   rank 0..7: state=ACTIVE placement=machine-a|machine-b
[orchestrator] waiting for the migration window marker in pod 'lulesh-machine-a-...'
[orchestrator] window open — staging machine-A ranks: fmi-rank-agent evacuate-local ...
[orchestrator]   [agent] staged_epoch=1 ranks=0,1,2,3
[orchestrator] scaling deploy/lulesh-machine-a to 0 (machine A is evacuated)
[orchestrator] POSTing 4 parallel /restore requests to http://lulesh-restore...
[orchestrator]   rank 0: machine-a -> lulesh-restore-00001-deployment-...
[orchestrator]   [agent] promoted_epoch=1
[orchestrator] --- epoch 1 directory ---
[orchestrator]   rank 0 on lulesh-restore-...: exit_code=0 (finished on serverless)
[orchestrator]   rank 4: exit=0 (survivor)
[orchestrator] running the golden reference: 8 local ranks, -s 15 -i 200, no migration
[orchestrator] energy check: migrated=2.720531e+04 matches golden
[orchestrator] PASSED: machine A evacuated to serverless in one criu cut — 4 LULESH ranks ...
```

You can watch the four Knative pods cold-start during the restore step:
`kubectl -n lulesh-criu get pods -l serving.knative.dev/service=lulesh-restore -w`.

## Cleanup

```bash
kubectl delete namespace lulesh-criu
kubectl label node <NODE-A> lulesh-machine- ; kubectl label node <NODE-B> lulesh-machine-
# optionally revert the knative-serving feature gates from 2a
```

## Troubleshooting / knobs

- **criu check fails in a pod**: see the preflight (2) notes above (feature gates, PodSecurity,
  `CHECKPOINT_RESTORE` on kernel < 5.9). For rootless local experiments,
  `FMI_CRIU_EXTRA_ARGS=--unprivileged` is honored by the agent.
- **Slower clusters**: the timing knobs are `MIGRATE_AT_CYCLE` (how much compute happens before
  the cut; each early cycle is fast — cycle 100 arrives in well under a minute at `-s 15`),
  `MIGRATE_WINDOW_MS` (how long the ranks hold), and the orchestrator's
  `ACTIVE_TIMEOUT_S`/`WINDOW_TIMEOUT_S`/`RELOCATE_TIMEOUT_S`/`RESTORE_HTTP_TIMEOUT_S` envs.
  If the exec regularly lands after the window closes, raise `MIGRATE_WINDOW_MS` — the hold
  only costs wall-clock time.
- **`GOLDEN`**: `local` (default) runs the reference inside the orchestrator pod after the
  migrated run. `skip` disables the energy comparison (e.g. heterogeneous-ISA clusters, where
  bit-identity across nodes isn't guaranteed). Any other value is used as the expected energy
  string — valid **only for the exact image build** that produced it, since the energy depends
  on the binary.
- **Problem size**: keep `LULESH_SIZE` ≲ 30 with the default resource limits — memory per rank
  and the staged CRIU blobs in Redis grow with the cube of it (bump the Redis/machine limits
  and `RESTORE_HTTP_TIMEOUT_S`/`timeoutSeconds` together if you go bigger).
- **Scaling the layout**: apply `machine-deployment.yaml` per machine with the right
  `MACHINE_ID`/`NODE_VALUE`/`BASE_PEER_ID`, set `NUM_PEERS` to the (cubic) total,
  `RESTORE_MAX_SCALE` to the largest per-machine rank count, and point `EVACUATE_SELECTOR`/
  `EVACUATE_DEPLOYMENT` at the machine to evacuate. One machine (one epoch cut) at a time.

## Verification status

Verified on this host: the unit tests, the image build, and `local-gate.sh` (golden run;
machine-a container stopped after staging; four fresh privileged containers each restored one
rank via `restore_server.py` over HTTP; epoch 1; migrated energy string-identical to golden).
The cluster steps (§2) are the deployment procedure for a real multi-node Knative cluster; the
highest-risk piece they add over the local gate is **criu restore inside a Knative pod**
(capabilities via the 2a feature gates) — covered first by preflight 2e(2).

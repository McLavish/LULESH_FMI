#!/usr/bin/env python3
"""Orchestrator for the LULESH CRIU node-evacuation-to-serverless demo.

Runs as a one-shot Kubernetes Job and drives the whole cross-host migration:

  1. release the start gate (a Redis key the machine supervisors poll before launching their
     LULESH ranks — nothing computes while the cluster is still being set up), then wait until
     all ranks are ACTIVE at epoch 0;
  2. wait for the migration window: at cycle MIGRATE_AT_CYCLE every rank prints
     `FMI_MIGRATE: window open` and holds for MIGRATE_WINDOW_MS before entering the dt
     allreduce. The cut MUST land inside this window — it is a correctness requirement, not
     cosmetics: a cut at an arbitrary time quiesces its targets at their next operation
     boundary, and a survivor already blocked in a recv from a target (whose sockets are
     released before the dump) would be stranded. During the hold no rank is inside any
     operation, so targets quiesce at the allreduce and survivors park cleanly;
  3. `kubectl exec` `fmi-rank-agent evacuate-local` inside the machine-A pod: its ranks are
     criu-dumped in one consistent cut and the packed images staged in Redis;
  4. scale machine A's Deployment to zero — the node is now truly evacuated;
  5. POST one /restore per evacuated rank to the lulesh-restore Knative Service, in parallel:
     each request cold-starts an instance that criu-restores its rank and holds the request
     while the restored worker runs;
  6. wait until the CRIU registry shows every evacuated rank RUNNING on a NEW host, then
     promote the epoch once; survivors and restored ranks rebuild channels at epoch 1 and
     compute cycles MIGRATE_AT_CYCLE..LULESH_ITERS to completion;
  7. verify and print PASSED/FAIL: every /restore response reports a clean exit, every
     survivor rank logs `rank=<r> exit=0` (LULESH ranks other than 0 print nothing on
     success — their exit code is their result), and — last, as an independent measurement —
     a golden non-migrated run's Final Origin Energy is string-identical to the migrated
     run's. The golden run comes LAST because LULESH computes from the moment its ranks
     connect; running it first would eat the migration-window budget.

State is read straight from the Redis control plane (same key schema the runbook scripts
use), so this image needs no FMI Python binding; the one control-plane WRITE (epoch
promotion) goes through the fmi-rank-agent binary shipped in this same image.
"""
import os
import sys
import time

from golden_run import extract_energy  # noqa: F401  (re-exported for the unit tests)


# ----- pure helpers (unit-tested in tests/test_orchestrator.py) -----

def all_active_from_states(states, num_peers):
    """True iff ranks 0..num_peers-1 are all ACTIVE in a Redis states hash."""
    return all(states.get(str(i)) == "ACTIVE" for i in range(num_peers))


def parse_evacuation_output(text):
    """Extract (staged_epoch, [ranks]) from evacuate-local's `staged_epoch=1 ranks=0,1,2,3`."""
    for line in text.splitlines():
        if line.startswith("staged_epoch="):
            fields = dict(part.split("=", 1) for part in line.split() if "=" in part)
            ranks = [int(r) for r in fields["ranks"].split(",") if r != ""]
            return int(fields["staged_epoch"]), ranks
    raise ValueError(f"no staged_epoch line in evacuate-local output: {text!r}")


def parse_survivor_exits(log_text, ranks):
    """Map rank -> exit code from the supervisor's reaper lines (`rank=<r> exit=<rc>` /
    `rank=<r> killed=signal-<n>`), restricted to the given ranks. Exit codes are ints; a
    signalled death is kept as its `signal-<n>` string (never equal to 0)."""
    wanted = set(ranks)
    exits = {}
    for line in log_text.splitlines():
        line = line.strip()
        if line.startswith("[supervisor] "):
            line = line[len("[supervisor] "):]
        if not line.startswith("rank="):
            continue
        fields = dict(part.split("=", 1) for part in line.split() if "=" in part)
        try:
            rank = int(fields["rank"])
        except (KeyError, ValueError):
            continue
        if rank not in wanted:
            continue
        if "exit" in fields:
            try:
                exits[rank] = int(fields["exit"])
            except ValueError:
                continue
        elif "killed" in fields:
            exits[rank] = fields["killed"]
    return exits


def verify_relocations(original_hosts, registry_entries, ranks):
    """Failure strings unless every evacuated rank is RUNNING on a host different from the one
    it was dumped on. original_hosts / registry_entries map rank -> pre-dump host_id / registry
    hash ({'state': ..., 'host_id': ...})."""
    failures = []
    for rank in ranks:
        entry = registry_entries.get(rank) or {}
        state, host = entry.get("state"), entry.get("host_id", "")
        if state != "RUNNING":
            failures.append(f"rank {rank}: registry state is {state}, expected RUNNING")
        elif not host or host == original_hosts.get(rank):
            failures.append(f"rank {rank}: still on '{host}' (dumped on "
                            f"'{original_hosts.get(rank)}'), did not relocate")
    return failures


def verify_restore_responses(responses, ranks):
    """Failure strings unless every /restore response reports status=ok and a clean exit.
    An exit_code of None WITH status ok is accepted: the server could not reap the worker
    (it was not the parent) and fell back to verifying the log."""
    failures = []
    for rank in ranks:
        response = responses.get(rank)
        if not isinstance(response, dict):
            failures.append(f"rank {rank}: no /restore response ({response!r})")
            continue
        if response.get("status") != "ok":
            failures.append(f"rank {rank}: /restore status={response.get('status')!r} "
                            f"exit_code={response.get('exit_code')!r} "
                            f"(log tail: {response.get('log_tail', '')[-200:]!r})")
        elif response.get("exit_code") not in (0, None):
            failures.append(f"rank {rank}: /restore status ok but "
                            f"exit_code={response.get('exit_code')!r}")
    return failures


# ----- runtime (skipped during unit tests) -----

def main():
    import concurrent.futures
    import json
    import subprocess
    import urllib.error
    import urllib.request
    from string import Template

    import redis
    from kubernetes import client, config
    from kubernetes.stream import stream

    import golden_run

    ns = os.environ.get("NAMESPACE", "lulesh-criu")
    comm = os.environ["COMM_NAME"]
    num_peers = int(os.environ["NUM_PEERS"])
    redis_host = os.environ.get("REDIS_HOST", "lulesh-redis")
    redis_port = int(os.environ.get("REDIS_PORT", "6379"))
    tcpunch_host = os.environ.get("TCPUNCH_HOST", "lulesh-tcpunch")
    evacuate_selector = os.environ.get("EVACUATE_SELECTOR",
                                       "app=lulesh-machine,lulesh-machine=a")
    evacuate_deployment = os.environ.get("EVACUATE_DEPLOYMENT", "lulesh-machine-a")
    all_selector = os.environ.get("ALL_MACHINES_SELECTOR", "app=lulesh-machine")
    container = os.environ.get("WORKER_CONTAINER", "lulesh-machine")
    agent_config = os.environ.get("AGENT_CONFIG", "/tmp/fmi.json")
    agent_bin = os.environ.get("FMI_AGENT_BIN", "/opt/fmi/bin/fmi-rank-agent")
    restore_url = os.environ.get(
        "RESTORE_URL", "http://lulesh-restore.lulesh-criu.svc.cluster.local/restore")
    template_path = os.environ.get("CONFIG_TEMPLATE", "/opt/fmi/app/fmi-lulesh.json.tmpl")
    lulesh_size = os.environ.get("LULESH_SIZE", "15")
    lulesh_iters = os.environ.get("LULESH_ITERS", "200")
    # GOLDEN: "local" = run the reference here after the migrated run; "skip" = no energy
    # comparison; anything else = a pinned expected energy (valid only per image build).
    golden = os.environ.get("GOLDEN", "local")
    golden_timeout_s = int(os.environ.get("GOLDEN_TIMEOUT_S", "300"))
    active_timeout_s = int(os.environ.get("ACTIVE_TIMEOUT_S", "300"))
    window_timeout_s = int(os.environ.get("WINDOW_TIMEOUT_S", "600"))
    epoch_timeout_s = int(os.environ.get("EPOCH_TIMEOUT_S", "300"))
    relocate_timeout_s = int(os.environ.get("RELOCATE_TIMEOUT_S", "300"))
    survivor_timeout_s = int(os.environ.get("SURVIVOR_TIMEOUT_S", "120"))
    restore_http_timeout_s = int(os.environ.get("RESTORE_HTTP_TIMEOUT_S", "600"))

    prefix = f"fmi:ft:{comm}:"
    r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)

    def log(msg):
        print(f"[orchestrator] {msg}", flush=True)

    def fail(msg):
        print(f"[orchestrator] FAIL: {msg}", file=sys.stderr, flush=True)
        sys.exit(1)

    def current_epoch():
        val = r.hget(f"{prefix}meta", "current_epoch")
        return int(val) if val is not None else 0

    def states(epoch):
        return r.hgetall(f"{prefix}epoch:{epoch}:states")

    def registry_entry(rank):
        return r.hgetall(f"{prefix}criu:rank:{rank}")

    def wait_until(predicate, timeout_s, what):
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            if predicate():
                return
            time.sleep(0.5)
        fail(f"timed out after {timeout_s}s waiting for {what}")

    def print_directory(epoch):
        st = states(epoch)
        pl = r.hgetall(f"{prefix}epoch:{epoch}:placement")
        log(f"--- epoch {epoch} directory ---")
        for i in range(num_peers):
            log(f"  rank {i}: state={st.get(str(i), '-')} placement={pl.get(str(i), '-')}")

    def find_pod(core, selector):
        pods = core.list_namespaced_pod(ns, label_selector=selector).items
        running = [p for p in pods if p.status.phase == "Running"]
        if not running:
            fail(f"no Running pod matches selector '{selector}' in namespace {ns}")
        return running[0].metadata.name

    def pod_log(core, name, tail_lines=None):
        try:
            return core.read_namespaced_pod_log(name, ns, container=container,
                                                tail_lines=tail_lines)
        except client.ApiException as e:
            log(f"could not read logs of {name}: {e.reason}")
            return ""

    def post_restore(rank):
        request = urllib.request.Request(
            restore_url,
            data=json.dumps({"rank": rank}).encode(),
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=restore_http_timeout_s) as response:
                return json.loads(response.read().decode("utf-8", errors="replace"))
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            try:
                return json.loads(body)  # restore-server errors carry a structured body
            except json.JSONDecodeError:
                return {"status": f"http-{exc.code}", "log_tail": body}
        except urllib.error.URLError as exc:
            return {"status": "unreachable", "log_tail": str(exc.reason)}

    config.load_incluster_config()
    core = client.CoreV1Api()
    apps = client.AppsV1Api()

    log(f"comm={comm} num_peers={num_peers} namespace={ns}")

    # 0. Release the start gate: the machine supervisors idle on this key, so the LULESH run
    #    begins only now that its orchestrator exists and is watching.
    r.set(f"{prefix}demo:start", "1")
    log(f"released the start gate ({prefix}demo:start)")

    log("waiting for all ranks ACTIVE at epoch 0")
    wait_until(lambda: all_active_from_states(states(0), num_peers), active_timeout_s,
               "all ranks ACTIVE at epoch 0")
    print_directory(0)

    # 1. Wait for the migration window to open (the supervisor mirrors the rank logs, so the
    #    marker shows up in the machine-A pod log), then cut immediately.
    pod = find_pod(core, evacuate_selector)
    log(f"waiting for the migration window marker in pod '{pod}'")
    wait_until(lambda: "FMI_MIGRATE: window open" in pod_log(core, pod, tail_lines=400),
               window_timeout_s, "the FMI_MIGRATE window-open marker")

    # 2. Evacuate machine A inside the held window: dump its ranks in one cut and stage the
    #    images in Redis.
    log(f"window open — staging machine-A ranks: fmi-rank-agent evacuate-local in pod '{pod}'")
    cmd = " ".join([agent_bin, "evacuate-local", comm, str(num_peers), agent_config]) \
        + '; echo "__EXIT__=$?"'
    out = stream(core.connect_get_namespaced_pod_exec, pod, ns,
                 command=["sh", "-c", cmd], container=container,
                 stderr=True, stdin=False, stdout=True, tty=False, _preload_content=True)
    for line in out.splitlines():
        log(f"  [agent] {line}")
    if "__EXIT__=0" not in out:
        fail(f"evacuate-local did not exit cleanly:\n{out}")
    staged_epoch, evac_ranks = parse_evacuation_output(out)
    log(f"staged epoch {staged_epoch} for ranks {evac_ranks}")
    original_hosts = {rank: registry_entry(rank).get("host_id", "") for rank in evac_ranks}

    # 3. The dumped processes are dead; retire the machine itself. From here on nothing of
    #    the evacuated ranks exists on machine A — the restores must succeed somewhere else.
    log(f"scaling deploy/{evacuate_deployment} to 0 (machine A is evacuated)")
    apps.patch_namespaced_deployment_scale(evacuate_deployment, ns, {"spec": {"replicas": 0}})
    wait_until(lambda: not core.list_namespaced_pod(ns, label_selector=evacuate_selector).items,
               active_timeout_s, "machine-A pod to terminate")

    # 4. One /restore per rank, in parallel: Knative cold-starts one instance per request
    #    (containerConcurrency=1) and each request stays open while its restored worker runs.
    log(f"POSTing {len(evac_ranks)} parallel /restore requests to {restore_url}")
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=len(evac_ranks))
    futures = {rank: pool.submit(post_restore, rank) for rank in evac_ranks}

    # 5. Restores are confirmed via the control plane (the responses only arrive after the
    #    workers FINISH, which needs the promotion below — don't wait on them here).
    def all_relocated():
        entries = {rank: registry_entry(rank) for rank in evac_ranks}
        return not verify_relocations(original_hosts, entries, evac_ranks)

    log("waiting for every evacuated rank to be RUNNING on a new host")
    wait_until(all_relocated, relocate_timeout_s,
               "all evacuated ranks RUNNING on a restore host")
    for rank in evac_ranks:
        log(f"  rank {rank}: {original_hosts[rank]} -> {registry_entry(rank).get('host_id')}")

    # 6. Promote the epoch: survivors and the restored ranks rebuild channels at epoch N+1.
    with open(template_path) as f:
        rendered = Template(f.read()).safe_substitute(
            REDIS_HOST=redis_host, TCPUNCH_HOST=tcpunch_host, HOST_ID="orchestrator")
    with open("/tmp/fmi-orchestrator.json", "w") as f:
        f.write(rendered)
    promote = subprocess.run(
        [agent_bin, "promote", comm, str(num_peers), "/tmp/fmi-orchestrator.json"],
        capture_output=True, text=True)
    for line in (promote.stdout + promote.stderr).splitlines():
        log(f"  [agent] {line}")
    if promote.returncode != 0:
        fail(f"promote failed with exit code {promote.returncode}")

    log(f"waiting for epoch {staged_epoch} with all ranks ACTIVE")
    wait_until(lambda: current_epoch() >= staged_epoch
               and all_active_from_states(states(staged_epoch), num_peers),
               epoch_timeout_s, f"epoch {staged_epoch} with all ranks ACTIVE")
    print_directory(staged_epoch)

    # 7a. Evacuated ranks: the held responses complete when the restored workers exit.
    responses = {rank: future.result(timeout=restore_http_timeout_s)
                 for rank, future in futures.items()}
    pool.shutdown()
    response_failures = verify_restore_responses(responses, evac_ranks)
    if response_failures:
        fail("serverless restores did not complete cleanly:\n  " +
             "\n  ".join(response_failures))
    for rank in evac_ranks:
        log(f"  rank {rank} on {responses[rank].get('host')}: "
            f"exit_code={responses[rank].get('exit_code')} (finished on serverless)")

    # 7b. Survivor ranks: their success signal is exit 0, reported by their supervisor as
    #     `rank=<r> exit=<rc>`. They finish within seconds of the restored ranks (the
    #     collectives couple them), but the log mirroring lags the exits — poll.
    survivor_ranks = [i for i in range(num_peers) if i not in set(evac_ranks)]

    def survivor_exits():
        exits = {}
        for p in core.list_namespaced_pod(ns, label_selector=all_selector).items:
            exits.update(parse_survivor_exits(pod_log(core, p.metadata.name), survivor_ranks))
        return exits

    log(f"waiting for survivor ranks {survivor_ranks} to report their exit")
    wait_until(lambda: len(survivor_exits()) == len(survivor_ranks), survivor_timeout_s,
               "every survivor rank's exit line in the machine logs")
    bad = {rank: rc for rank, rc in survivor_exits().items() if rc != 0}
    if bad:
        fail(f"survivor ranks did not exit cleanly: {bad}")
    for rank in survivor_ranks:
        log(f"  rank {rank}: exit=0 (survivor)")

    # 7c. Energy check, last: an independent golden measurement against the migrated run.
    #     Rank 0 prints the energy wherever it ended up — in its /restore log_tail if it was
    #     evacuated (the default topology), else in a surviving machine pod's log.
    if 0 in evac_ranks:
        migrated = extract_energy(responses[0].get("log_tail", ""))
    else:
        migrated = None
        for p in core.list_namespaced_pod(ns, label_selector=all_selector).items:
            migrated = migrated or extract_energy(pod_log(core, p.metadata.name))
    if not migrated:
        fail("could not extract the migrated run's Final Origin Energy from rank 0")

    if golden == "skip":
        log(f"migrated Final Origin Energy = {migrated} (GOLDEN=skip: not compared)")
    else:
        if golden == "local":
            log(f"running the golden reference: {num_peers} local ranks, "
                f"-s {lulesh_size} -i {lulesh_iters}, no migration")
            expected = golden_run.run_golden(num_peers=num_peers, comm_name=comm,
                                             size=lulesh_size, iters=lulesh_iters,
                                             timeout_s=golden_timeout_s)
        else:
            expected = golden
        if migrated != expected:
            fail(f"energy mismatch: migrated={migrated} golden={expected} — "
                 f"restored state diverged")
        log(f"energy check: migrated={migrated} matches golden")

    log(f"PASSED: machine A evacuated to serverless in one criu cut — {len(evac_ranks)} LULESH "
        f"ranks relocated to lulesh-restore pods, all {num_peers} ranks finished at epoch "
        f"{current_epoch()}, Final Origin Energy = {migrated}"
        + ("" if golden == "skip" else " (golden-verified)"))


if __name__ == "__main__":
    main()

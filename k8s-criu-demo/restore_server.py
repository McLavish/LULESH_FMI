#!/usr/bin/env python3
"""Serverless restore endpoint for the LULESH CRIU node-evacuation demo.

Runs as the container entrypoint of the `lulesh-restore` Knative Service (and of the
fake-Knative containers in local-gate.sh). Each instance restores ONE evacuated rank per
request (containerConcurrency=1), turning a staged checkpoint into a live LULESH process in
this pod:

  GET  /healthz        readiness (lets Knative route to a cold-started instance).
  POST /restore        body {"rank": R}: render the FMI config (HOST_ID left empty, so the
                       agent's host identity is THIS pod's hostname), run
                       `fmi-rank-agent restore-remote`, then HOLD the request while the restored
                       worker runs — the response is what keeps a scale-from-zero instance alive
                       until its rank finishes — and answer with the worker's outcome.

The restored worker is detached by criu, so it is re-parented to this server (PID 1 in the
container); its exit code — the only success signal a non-zero LULESH rank produces — is
detected via /proc (a zombie counts as exited) and collected by reaping it. The response also
carries the tail of the rank's (shipped + reopened) log file: for rank 0 that includes the
Final Origin Energy line the orchestrator compares against the golden run.
"""
import json
import os
import socket
import subprocess
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from string import Template


class Settings:
    """Environment-derived configuration, read in main() (not at import, so the pure helpers
    below stay importable by the unit tests)."""

    def __init__(self):
        def env(name, default=None, required=False):
            val = os.environ.get(name, default)
            if required and (val is None or val == ""):
                sys.exit(f"[restore-server] missing required env {name}")
            return val

        self.comm_name = env("COMM_NAME", required=True)
        self.num_peers = env("NUM_PEERS", required=True)
        self.redis_host = env("REDIS_HOST", "lulesh-redis")
        self.tcpunch_host = env("TCPUNCH_HOST", "lulesh-tcpunch")
        self.template_path = env("CONFIG_TEMPLATE", "/opt/fmi/app/fmi-lulesh.json.tmpl")
        self.config_path = env("CONFIG_PATH", "/tmp/fmi.json")
        self.agent_bin = env("FMI_AGENT_BIN", "/opt/fmi/bin/fmi-rank-agent")
        self.log_dir = env("LOG_DIR", "/tmp")
        self.port = int(env("PORT", "8080"))
        # Knative buys us timeoutSeconds per request; give up slightly earlier so the client
        # gets a structured timeout instead of a severed connection.
        self.hold_timeout_s = int(env("HOLD_TIMEOUT_S", "570"))


SETTINGS = None  # populated by main()


def render_config(settings):
    """HOST_ID is deliberately empty: resolve_host_id then falls back to gethostname(), giving
    every restore instance its own identity — the control-plane proof that the rank moved."""
    with open(settings.template_path) as f:
        rendered = Template(f.read()).safe_substitute(
            REDIS_HOST=settings.redis_host, TCPUNCH_HOST=settings.tcpunch_host, HOST_ID="")
    with open(settings.config_path, "w") as f:
        f.write(rendered)


def parse_agent_output(text):
    """Extract pid/host from `restored_rank=R pid=P host=H` (unit-tested)."""
    for line in text.splitlines():
        if line.startswith("restored_rank="):
            fields = dict(part.split("=", 1) for part in line.split() if "=" in part)
            return int(fields["pid"]), fields.get("host", "")
    raise ValueError(f"no restored_rank line in agent output: {text!r}")


def worker_outcome(exit_code, log_text):
    """Terminal verdict for a restored LULESH rank (unit-tested). The exit code is
    authoritative when known — this server is PID 1 of its container and the criu-detached
    worker re-parents to it, so normally it is. The log fallback exists for harnesses where
    the worker was not our child: only rank 0 prints anything on success (the Final Origin
    Energy block), every rank prints a `fatal:`/Abort line on failure."""
    if exit_code == 0:
        return "ok"
    if exit_code is not None:
        return "failed"
    energy_seen = False
    for line in log_text.splitlines():
        if "fatal:" in line or "Abort" in line:
            return "failed"
        if "Final Origin Energy" in line:
            energy_seen = True
    return "ok" if energy_seen else "unknown"


def process_exited(pid):
    """True when pid is gone or a zombie (exited, not yet reaped)."""
    try:
        with open(f"/proc/{pid}/stat") as f:
            # Field 3 is the state; the comm field can contain spaces but is parenthesised.
            state = f.read().rpartition(")")[2].split()[0]
        return state == "Z"
    except (FileNotFoundError, ProcessLookupError):
        return True


def wait_for_exit(pid, timeout_s):
    """Wait until pid exits. Returns (exited, exit_code); exit_code is None when unknowable
    (the worker was not our child, so there was nothing to reap)."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if process_exited(pid):
            exit_code = None
            try:
                wpid, status = os.waitpid(pid, os.WNOHANG)
                # Reaped iff wpid == pid. A clean exit is status 0 — never truth-test status.
                if wpid == pid:
                    if os.WIFEXITED(status):
                        exit_code = os.WEXITSTATUS(status)
                    elif os.WIFSIGNALED(status):
                        exit_code = 128 + os.WTERMSIG(status)
            except ChildProcessError:
                pass
            return True, exit_code
        time.sleep(0.5)
    return False, None


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        print(f"[restore-server] {self.address_string()} {fmt % args}", flush=True)

    def _respond(self, code, payload):
        body = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/healthz":
            self._respond(200, {"status": "ok", "host": socket.gethostname()})
        else:
            self._respond(404, {"error": f"unknown path {self.path}"})

    def do_POST(self):
        if self.path != "/restore":
            self._respond(404, {"error": f"unknown path {self.path}"})
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            rank = int(json.loads(self.rfile.read(length) or b"{}")["rank"])
        except (KeyError, ValueError, json.JSONDecodeError) as exc:
            self._respond(400, {"error": f"body must be JSON with an integer 'rank': {exc}"})
            return

        host = socket.gethostname()
        print(f"[restore-server] restoring rank {rank} on {host}", flush=True)
        agent = subprocess.run(
            [SETTINGS.agent_bin, "restore-remote", SETTINGS.comm_name, SETTINGS.num_peers,
             SETTINGS.config_path, str(rank)],
            capture_output=True, text=True)
        if agent.returncode != 0:
            print(f"[restore-server] restore-remote failed rc={agent.returncode}\n"
                  f"{agent.stdout}{agent.stderr}", flush=True)
            self._respond(500, {"status": "restore-failed", "rank": rank, "host": host,
                                "agent_stdout": agent.stdout, "agent_stderr": agent.stderr})
            return
        try:
            pid, _ = parse_agent_output(agent.stdout)
        except ValueError as exc:
            self._respond(500, {"status": "restore-failed", "rank": rank, "host": host,
                                "error": str(exc)})
            return

        # Hold the request while the restored worker runs: for a scale-from-zero Knative
        # instance this open request IS the pod's reason to exist.
        print(f"[restore-server] rank {rank} restored as pid {pid}; holding until it exits",
              flush=True)
        exited, exit_code = wait_for_exit(pid, SETTINGS.hold_timeout_s)

        log_path = os.path.join(SETTINGS.log_dir, f"rank-{rank}.log")
        try:
            with open(log_path) as f:
                log_tail = f.read()[-4000:]
        except OSError:
            log_tail = ""

        status = worker_outcome(exit_code, log_tail) if exited else "timeout"
        print(f"[restore-server] rank {rank} finished status={status} exit_code={exit_code}",
              flush=True)
        self._respond(200 if status == "ok" else 500,
                      {"status": status, "rank": rank, "pid": pid, "host": host,
                       "exit_code": exit_code, "log_tail": log_tail})


def main():
    global SETTINGS
    SETTINGS = Settings()
    render_config(SETTINGS)
    print(f"[restore-server] comm={SETTINGS.comm_name} num_peers={SETTINGS.num_peers} "
          f"redis={SETTINGS.redis_host} tcpunch={SETTINGS.tcpunch_host} port={SETTINGS.port} "
          f"host={socket.gethostname()}", flush=True)
    ThreadingHTTPServer(("", SETTINGS.port), Handler).serve_forever()


if __name__ == "__main__":
    main()

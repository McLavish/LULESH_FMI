#!/usr/bin/env python3
"""Unit tests for the orchestrator's and restore server's pure verification logic.

These cover the parts that decide PASS/FAIL without needing a cluster, Redis, or criu: rank
state scanning, agent output parsing, relocation verification, restore-response verification,
survivor-exit parsing, and energy extraction. Run with `python3 tests/test_orchestrator.py`
(no pytest needed).
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from golden_run import extract_energy  # noqa: E402
from orchestrator import (  # noqa: E402
    all_active_from_states,
    parse_evacuation_output,
    parse_survivor_exits,
    verify_relocations,
    verify_restore_responses,
)
from restore_server import parse_agent_output, worker_outcome  # noqa: E402


def test_all_active_true_when_every_rank_active():
    states = {str(i): "ACTIVE" for i in range(8)}
    assert all_active_from_states(states, 8)


def test_all_active_false_when_one_quiesced():
    states = {str(i): "ACTIVE" for i in range(8)}
    states["3"] = "QUIESCED"
    assert not all_active_from_states(states, 8)


def test_all_active_false_when_rank_missing():
    states = {str(i): "ACTIVE" for i in range(7)}  # rank 7 not present yet
    assert not all_active_from_states(states, 8)


def test_parse_evacuation_output_extracts_epoch_and_ranks():
    out = "\n".join([
        "some agent noise",
        "staged_epoch=1 ranks=0,1,2,3",
        "__EXIT__=0",
    ])
    assert parse_evacuation_output(out) == (1, [0, 1, 2, 3])


def test_parse_evacuation_output_rejects_missing_line():
    try:
        parse_evacuation_output("fatal: nothing staged\n__EXIT__=1")
    except ValueError:
        return
    raise AssertionError("expected ValueError for missing staged_epoch line")


def test_parse_survivor_exits_collects_exit_codes():
    log = "\n".join([
        "[supervisor] launched rank 4 (pid 3004) -> /tmp/rank-4.log",
        "FMI_MIGRATE: window open rank=5 allreduce_call=100 holding_ms=20000 ...",
        "[supervisor] rank=4 exit=0",
        "[supervisor] rank=5 exit=1",
        "[supervisor] rank=6 killed=signal-9 (expected if criu-dumped)",
        "some unrelated line",
    ])
    exits = parse_survivor_exits(log, [4, 5, 6, 7])
    assert exits[4] == 0
    assert exits[5] == 1
    assert exits[6] == "signal-9"
    assert 7 not in exits  # rank 7 has not reported yet


def test_parse_survivor_exits_restricts_to_given_ranks():
    log = "[supervisor] rank=0 exit=0\n[supervisor] rank=4 exit=0"
    assert parse_survivor_exits(log, [4]) == {4: 0}


def test_parse_survivor_exits_ignores_window_markers():
    # The FMI_MIGRATE marker also contains `rank=`; it must not read as an exit report.
    log = "FMI_MIGRATE: window open rank=4 allreduce_call=100 holding_ms=20000"
    assert parse_survivor_exits(log, [4]) == {}


def test_verify_relocations_passes_when_all_moved():
    original = {0: "machine-a", 1: "machine-a"}
    entries = {
        0: {"state": "RUNNING", "host_id": "lulesh-restore-00001-deployment-abc"},
        1: {"state": "RUNNING", "host_id": "lulesh-restore-00001-deployment-def"},
    }
    assert verify_relocations(original, entries, [0, 1]) == []


def test_verify_relocations_flags_unmoved_and_unrestored_ranks():
    original = {0: "machine-a", 1: "machine-a", 2: "machine-a"}
    entries = {
        0: {"state": "RUNNING", "host_id": "machine-a"},   # restored in place — not a relocation
        1: {"state": "QUIESCED", "host_id": "machine-a"},  # never restored
        # rank 2 missing entirely
    }
    failures = verify_relocations(original, entries, [0, 1, 2])
    assert len(failures) == 3
    assert any("rank 0" in f and "did not relocate" in f for f in failures)
    assert any("rank 1" in f and "QUIESCED" in f for f in failures)
    assert any("rank 2" in f for f in failures)


def test_verify_restore_responses_passes_on_clean_exits():
    responses = {
        0: {"status": "ok", "exit_code": 0, "host": "pod-a",
            "log_tail": "   Final Origin Energy =  2.720531e+04"},
        1: {"status": "ok", "exit_code": None, "host": "pod-b", "log_tail": ""},  # log fallback
    }
    assert verify_restore_responses(responses, [0, 1]) == []


def test_verify_restore_responses_flags_bad_status_and_bad_exit():
    responses = {
        0: {"status": "restore-failed", "log_tail": ""},
        1: {"status": "ok", "exit_code": 1, "log_tail": ""},
        2: {"status": "failed", "exit_code": 1, "log_tail": "fatal: allreduce failed"},
        # rank 3: no response at all
    }
    failures = verify_restore_responses(responses, [0, 1, 2, 3])
    assert len(failures) == 4
    assert any("rank 0" in f and "restore-failed" in f for f in failures)
    assert any("rank 1" in f and "exit_code=1" in f for f in failures)
    assert any("rank 2" in f and "'failed'" in f for f in failures)
    assert any("rank 3" in f and "no /restore response" in f for f in failures)


def test_extract_energy_reads_the_real_lulesh_format():
    log = "\n".join([
        "Run completed:",
        "   Problem size        =  15",
        "   MPI tasks           =  8",
        "   Iteration count     =  200",
        "   Final Origin Energy =  2.720531e+04",
        "   Testing Plane 0 of Energy Array on rank 0:",
    ])
    assert extract_energy(log) == "2.720531e+04"


def test_extract_energy_returns_none_on_garbage():
    assert extract_energy("") is None
    assert extract_energy("fatal: allreduce failed") is None
    assert extract_energy("Final Origin Energy =  ") is None


def test_restore_server_parses_agent_output():
    out = "restored_rank=2 pid=3004 host=lulesh-restore-00001-deployment-abc\n"
    assert parse_agent_output(out) == (3004, "lulesh-restore-00001-deployment-abc")


def test_restore_server_rejects_agent_output_without_marker():
    try:
        parse_agent_output("fatal: No staged CRIU image for rank 2 at epoch 1")
    except ValueError:
        return
    raise AssertionError("expected ValueError for missing restored_rank line")


def test_restore_server_worker_outcome_exit_code_is_authoritative():
    assert worker_outcome(0, "") == "ok"
    assert worker_outcome(0, "fatal: some stale line") == "ok"          # rc wins over log
    assert worker_outcome(1, "   Final Origin Energy =  2.7e+04") == "failed"  # rc wins
    assert worker_outcome(137, "") == "failed"


def test_restore_server_worker_outcome_log_fallback_when_unreaped():
    assert worker_outcome(None, "   Final Origin Energy =  2.720531e+04") == "ok"
    assert worker_outcome(None, "[FMI rank 2] fatal: allreduce failed") == "failed"
    assert worker_outcome(None, "MPI_Abort(code=1)") == "failed"
    assert worker_outcome(None, "cycle = 150") == "unknown"  # non-zero rank: silent success


def main():
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for t in tests:
        t()
        print(f"  ok  {t.__name__}")
    print(f"PASSED {len(tests)} orchestrator unit tests")


if __name__ == "__main__":
    main()

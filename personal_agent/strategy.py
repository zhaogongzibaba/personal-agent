from __future__ import annotations

from typing import Any


def simulate_rsi_strategy() -> dict[str, Any]:
    """Replay the same failing A workload with and without a derived yield rule."""
    scenario = {
        "file": "data/repeated_market.json",
        "failures_before_success": 3,
        "b_waiting": True,
    }
    failure_records = [
        {"task": "A", "file": scenario["file"], "attempt": attempt, "result": "read_error"}
        for attempt in range(1, scenario["failures_before_success"] + 1)
    ]
    same_file_failures = [
        record
        for record in failure_records
        if record["file"] == scenario["file"] and record["result"] == "read_error"
    ]
    yield_after_failures = (
        1 if scenario["b_waiting"] and len(same_file_failures) >= 2 else None
    )
    strategy = {
        "rule": (
            f"同一文件连续读取失败 {yield_after_failures} 次且 B 正在排队时，让出执行槽给 B。"
            if yield_after_failures is not None
            else "当前失败记录不足以调整调度策略，保持原策略。"
        ),
        "evidence": (
            f"A 对同一文件出现 {len(same_file_failures)} 条连续失败记录，"
            f"B 在此期间{'一直等待' if scenario['b_waiting'] else '未排队'}。"
        ),
        "adopted": yield_after_failures is not None,
    }

    before = _replay(scenario, yield_after_failures=None)
    after = _replay(scenario, yield_after_failures=yield_after_failures)
    return {
        "scenario": scenario,
        "failure_records": failure_records,
        "strategy": strategy,
        "before": before,
        "after": after,
        "comparison": {
            "b_start_after_a_reads_before": before["a_reads_before_b"],
            "b_start_after_a_reads_after": after["a_reads_before_b"],
            "same_file_reads_before_b_reduced_by": (
                before["a_reads_before_b"] - after["a_reads_before_b"]
            ),
            "a_total_reads_before": before["a_total_reads"],
            "a_total_reads_after": after["a_total_reads"],
        },
    }


def _replay(scenario: dict[str, Any], yield_after_failures: int | None) -> dict[str, Any]:
    failures = scenario["failures_before_success"]
    reads = 0
    b_started_after: int | None = None
    trace: list[dict[str, Any]] = []

    while reads <= failures:
        reads += 1
        if reads <= failures:
            trace.append(
                {
                    "step": len(trace) + 1,
                    "task": "A",
                    "action": "read_same_file",
                    "attempt": reads,
                    "result": "read_error",
                }
            )
            if (
                scenario["b_waiting"]
                and yield_after_failures is not None
                and reads >= yield_after_failures
                and b_started_after is None
            ):
                b_started_after = reads
                trace.append(
                    {
                        "step": len(trace) + 1,
                        "task": "B",
                        "action": "start_check",
                        "reason": "A 命中重复文件失败让出规则。",
                    }
                )
        else:
            trace.append(
                {
                    "step": len(trace) + 1,
                    "task": "A",
                    "action": "read_same_file",
                    "attempt": reads,
                    "result": "success",
                }
            )

    if b_started_after is None:
        b_started_after = reads
        trace.append(
            {
                "step": len(trace) + 1,
                "task": "B",
                "action": "start_check",
                "reason": "A 完成当前文件读取后，调度器处理排队任务 B。",
            }
        )

    return {
        "policy": "baseline" if yield_after_failures is None else "adopted_yield_after_one_failure",
        "b_start_after_a_attempt": b_started_after,
        "a_reads_before_b": b_started_after,
        "a_total_reads": reads,
        "a_duplicate_reads_before_b": max(0, b_started_after - 1),
        "trace": trace,
    }

from __future__ import annotations

import json
from pathlib import Path
from uuid import uuid4

from .engine import AgentEngine, SimulatedCrash
from .storage import timestamp
from .strategy import simulate_rsi_strategy


def run_demo(project_root: Path) -> Path:
    run_id = f"{timestamp()[:10]}-{uuid4().hex[:8]}"
    workdir = project_root / ".personal_agent" / "demo" / run_id
    data_dir = project_root / "data"
    engine = AgentEngine(workdir, data_dir)
    engine.create_workflow(recipient="张三")
    engine.prepare_tasks(inject_missing_unit=True)

    print("\n[日报质量门]")
    print("首次草稿故意漏掉一项价格单位；系统拦截发送、修正日报并复检通过。")
    print("[调度]")
    print("A 等待批准时释放执行槽，B 随后读取本地文件并完成检查。")

    engine.approve("张三")
    engine.set_recipient("李四")
    blocked = not engine.send()
    print("[收件人批准门]")
    print(f"批准张三后改为李四，旧批准拦截：{'是' if blocked else '否'}；当前发送记录数：{len(engine.store.outbox())}。")

    engine.approve("李四")
    try:
        engine.send(simulate_crash_after_write=True)
    except SimulatedCrash:
        print("[重启接续]")
        print("模拟发送记录落盘后进程退出，回执尚未返回。")

    restarted_engine = AgentEngine(workdir, data_dir)
    restarted_engine.recover()
    print(f"重启后核对完成；同一动作的发送记录数：{len(restarted_engine.store.outbox())}。")

    rsi = simulate_rsi_strategy()
    (workdir / "rsi-comparison.json").write_text(
        json.dumps(rsi, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    _write_strategy_summary(workdir, rsi)

    print("\n[RSI 策略对照]")
    print(rsi["strategy"]["evidence"])
    print(rsi["strategy"]["rule"])
    print(
        "B 开始前 A 对同一文件的读取次数："
        f"{rsi['before']['a_reads_before_b']} -> {rsi['after']['a_reads_before_b']}；"
        f"A 总读取次数：{rsi['before']['a_total_reads']} -> {rsi['after']['a_total_reads']}。"
    )
    print(f"采用策略：{'是' if rsi['strategy']['adopted'] else '否'}。")

    _print_audit(restarted_engine.store.events())
    print("\n[输出文件]")
    for name in (
        "b-check-result.md",
        "send-log.jsonl",
        "events.jsonl",
        "rsi-comparison.md",
        "rsi-comparison.json",
        "state.json",
    ):
        print(workdir / name)
    print(workdir / restarted_engine.state["task_a"]["report_path"])
    return workdir


def _write_strategy_summary(workdir: Path, result: dict[str, object]) -> None:
    before = result["before"]
    after = result["after"]
    strategy = result["strategy"]
    comparison = result["comparison"]
    content = "\n".join(
        [
            "# RSI 调度策略复跑对照",
            "",
            f"场景：A 对 `{result['scenario']['file']}` 连续读取失败，B 一直排队。",
            f"失败证据：{strategy['evidence']}",
            f"提出策略：{strategy['rule']}",
            f"采用策略：{'是' if strategy['adopted'] else '否'}。",
            "",
            "| 指标 | 改进前 | 改进后 |",
            "| --- | ---: | ---: |",
            f"| B 开始前 A 的同文件读取次数 | {before['a_reads_before_b']} | {after['a_reads_before_b']} |",
            f"| A 总读取次数 | {before['a_total_reads']} | {after['a_total_reads']} |",
            f"| B 开始前的重复读取次数 | {before['a_duplicate_reads_before_b']} | {after['a_duplicate_reads_before_b']} |",
            "",
            f"改进后 B 提前获得执行槽，A 总共减少等待期间的重复读取 {comparison['same_file_reads_before_b_reduced_by']} 次；A 后续仍可继续自己的重试。",
            "",
            "## 改进前事件",
            "",
            *[
                f"- 第 {event['step']} 步：任务 {event['task']}，{event['action']}"
                + (f"，结果 {event['result']}" if "result" in event else "")
                for event in before["trace"]
            ],
            "",
            "## 改进后事件",
            "",
            *[
                f"- 第 {event['step']} 步：任务 {event['task']}，{event['action']}"
                + (f"，结果 {event['result']}" if "result" in event else "")
                for event in after["trace"]
            ],
            "",
        ]
    )
    (workdir / "rsi-comparison.md").write_text(content, encoding="utf-8")


def _print_audit(events: list[dict[str, object]]) -> None:
    print("\n[决策与执行审计]")
    for event in events:
        task = f"任务 {event['task_id']}" if event.get("task_id") else "系统"
        print(f"{event['decision']:5} {task} / {event['action']}：{event['reason']}")

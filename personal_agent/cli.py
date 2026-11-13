from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .demo import run_demo
from .engine import AgentEngine


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    project_root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="本地 Personal Agent 决策治理原型")
    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("demo", help="复跑日报、批准门、任务调度、重启接续和 RSI 对照")
    subparsers.add_parser("run", help="执行日报与文件检查，停在发送批准门")
    subparsers.add_parser("status", help="查看当前任务状态")

    recipient_parser = subparsers.add_parser("set-recipient", help="修改当前日报收件人")
    recipient_parser.add_argument("recipient")

    approval_parser = subparsers.add_parser("approve", help="批准发送给当前收件人")
    approval_parser.add_argument("--recipient", required=True)

    subparsers.add_parser("send", help="通过批准检查后写入本地模拟发送记录")
    subparsers.add_parser("recover", help="核对未确认的发送动作并恢复回执")

    args = parser.parse_args()
    if args.command in (None, "demo"):
        run_demo(project_root)
        return

    engine = AgentEngine(project_root / ".personal_agent" / "interactive", project_root / "data")
    if args.command == "run":
        state = engine.status()
        if (
            state is None
            or state["task_a"]["status"] in {"COMPLETED", "FAILED"}
            or state["task_b"]["status"] == "FAILED"
        ):
            engine.create_workflow()
        state = engine.status()
        if state["task_a"]["status"] == "QUEUED" or state["task_b"]["status"] == "QUEUED":
            engine.prepare_tasks()
        state = engine.status()
        task_a = state["task_a"]
        task_b = state["task_b"]
        print(f"任务状态：A={task_a['status']}，B={task_b['status']}")
        report_path = engine.store.root / task_a["report_path"]
        if report_path.exists():
            print(f"日报：{report_path}")
        if task_a["status"] == "WAITING_APPROVAL":
            print(f"发送尚未执行；请先明确批准当前收件人 {task_a['recipient']}。")
        elif task_a["status"] == "WAITING_RECEIPT":
            print("发送记录等待核对；运行 `python main.py recover` 恢复回执。")
        elif task_a["status"] == "FAILED":
            print(f"A 任务失败：{task_a['error']}")
        if task_b["status"] == "FAILED":
            print(f"B 任务失败：{task_b['error']}")
    elif args.command == "status":
        state = engine.status()
        if state is None:
            print("当前没有任务。运行 `python main.py run` 开始。")
        else:
            print(json.dumps(state, ensure_ascii=False, indent=2))
    elif args.command == "set-recipient":
        engine.set_recipient(args.recipient)
        print(f"当前收件人：{engine.state['task_a']['recipient']}")
    elif args.command == "approve":
        if engine.approve(args.recipient):
            print(f"已批准发送给 {args.recipient}。")
        else:
            print("批准未生效，详情见审计记录。")
    elif args.command == "send":
        if engine.send():
            print(f"本地模拟发送完成：{engine.store.outbox_path}")
        else:
            print("发送被拦截，详情见审计记录。")
    elif args.command == "recover":
        if engine.recover():
            print("已根据原发送记录恢复回执；没有重复写入。")
        else:
            print("没有待恢复的发送动作。")


if __name__ == "__main__":
    main()

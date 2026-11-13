from __future__ import annotations

import json
import shutil
import tempfile
import threading
import unittest
from pathlib import Path

from personal_agent.engine import AgentEngine, SimulatedCrash
from personal_agent.strategy import simulate_rsi_strategy


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class AgentWorkflowTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.workdir = Path(self.tempdir.name) / "agent"
        self.engine = AgentEngine(self.workdir, PROJECT_ROOT / "data")
        self.engine.create_workflow()

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_report_gate_correction_and_b_uses_released_slot(self) -> None:
        self.engine.prepare_tasks(inject_missing_unit=True)
        state = self.engine.status()
        self.assertEqual(state["task_a"]["status"], "WAITING_APPROVAL")
        self.assertTrue(state["task_a"]["report_valid"])
        self.assertEqual(state["task_b"]["status"], "COMPLETED")
        self.assertEqual(state["task_b"]["findings"][0]["record_id"], "Q-002")

        decisions = [event["decision"] for event in self.engine.store.events()]
        self.assertIn("BLOCK", decisions)
        quality_gate = next(
            event for event in self.engine.store.events()
            if event["action"] == "report_quality_gate" and event["decision"] == "BLOCK"
        )
        self.assertIn("价格单位", quality_gate["reason"])
        b_start = next(
            event for event in self.engine.store.events()
            if event["action"] == "check_market_file"
        )
        self.assertIn("任务 A 正等待用户批准", b_start["reason"])
        a_wait = next(
            event for event in self.engine.store.events()
            if event["action"] == "approval_gate"
        )
        self.assertLess(
            self.engine.store.events().index(a_wait),
            self.engine.store.events().index(b_start),
        )

    def test_recipient_change_invalidates_approval_and_restart_deduplicates(self) -> None:
        self.engine.prepare_tasks()
        self.assertTrue(self.engine.approve("张三"))
        self.engine.set_recipient("李四")
        self.assertFalse(self.engine.send())
        self.assertEqual(self.engine.store.outbox(), [])

        self.assertTrue(self.engine.approve("李四"))
        with self.assertRaises(SimulatedCrash):
            self.engine.send(simulate_crash_after_write=True)
        self.assertEqual(len(self.engine.store.outbox()), 1)

        restarted = AgentEngine(self.workdir, PROJECT_ROOT / "data")
        self.assertTrue(restarted.recover())
        self.assertEqual(len(restarted.store.outbox()), 1)
        self.assertEqual(restarted.state["task_a"]["status"], "COMPLETED")
        self.assertTrue(restarted.state["task_a"]["receipt"]["recovered_after_restart"])

    def test_rsi_replay_uses_failure_records_and_improves_b_start(self) -> None:
        result = simulate_rsi_strategy()
        self.assertEqual(len(result["failure_records"]), 3)
        self.assertEqual(result["before"]["a_reads_before_b"], 4)
        self.assertEqual(result["after"]["a_reads_before_b"], 1)
        self.assertEqual(result["before"]["a_total_reads"], result["after"]["a_total_reads"])
        self.assertTrue(result["strategy"]["adopted"])

    def test_invalid_json_input_is_recorded_as_failed_and_b_still_runs(self) -> None:
        self.engine.prepare_tasks()
        previous_report = self.workdir / self.engine.status()["task_a"]["report_path"]
        self.assertTrue(previous_report.exists())

        data_dir = Path(self.tempdir.name) / "bad-source"
        data_dir.mkdir()
        (data_dir / "market_snapshot.json").write_text("{bad json", encoding="utf-8")
        shutil.copy(PROJECT_ROOT / "data" / "market_check.csv", data_dir / "market_check.csv")
        engine = AgentEngine(self.workdir, data_dir)
        engine.create_workflow()

        engine.prepare_tasks()

        self.assertEqual(engine.status()["task_a"]["status"], "FAILED")
        self.assertIn("无法读取", engine.status()["task_a"]["error"])
        self.assertNotEqual(engine.status()["task_a"]["report_path"], previous_report.name)
        self.assertFalse((self.workdir / engine.status()["task_a"]["report_path"]).exists())
        self.assertEqual(engine.status()["task_b"]["status"], "COMPLETED")
        failure = next(event for event in engine.store.events() if event["action"] == "task_failed")
        self.assertEqual(failure["task_id"], "A")

    def test_missing_csv_column_fails_instead_of_reporting_success(self) -> None:
        data_dir = Path(self.tempdir.name) / "bad-csv"
        data_dir.mkdir()
        shutil.copy(PROJECT_ROOT / "data" / "market_snapshot.json", data_dir / "market_snapshot.json")
        (data_dir / "market_check.csv").write_text(
            "record_id,symbol,name,price,date,source\nQ-1,ABC,Example,10.00,2026-11-13,test\n",
            encoding="utf-8",
        )
        engine = AgentEngine(self.workdir, data_dir)
        engine.create_workflow()

        engine.prepare_tasks()

        self.assertEqual(engine.status()["task_b"]["status"], "FAILED")
        self.assertIn("unit", engine.status()["task_b"]["error"])
        result = (self.workdir / "b-check-result.md").read_text(encoding="utf-8")
        self.assertIn("检查失败", result)
        self.assertNotIn("未发现", result)

    def test_price_and_unit_problems_are_reported_as_separate_issue_types(self) -> None:
        data_dir = Path(self.tempdir.name) / "price-issues"
        data_dir.mkdir()
        shutil.copy(PROJECT_ROOT / "data" / "market_snapshot.json", data_dir / "market_snapshot.json")
        (data_dir / "market_check.csv").write_text(
            "record_id,symbol,name,price,unit,date,source\n"
            "Q-1,ABC,Missing price,,,2026-11-13,test\n"
            "Q-2,XYZ,Missing unit,10.00,,2026-11-13,test\n",
            encoding="utf-8",
        )
        engine = AgentEngine(self.workdir, data_dir)
        engine.create_workflow()

        engine.prepare_tasks()

        findings = engine.status()["task_b"]["findings"]
        self.assertEqual(findings[0]["issue"], "价格缺失")
        self.assertEqual(findings[1]["issue"], "缺少价格单位")

    def test_two_engine_instances_cannot_write_duplicate_send_records(self) -> None:
        self.engine.prepare_tasks()
        self.assertTrue(self.engine.approve("张三"))
        second_engine = AgentEngine(self.workdir, PROJECT_ROOT / "data")
        barrier = threading.Barrier(2)
        results: list[bool] = []

        def send(engine: AgentEngine) -> None:
            barrier.wait()
            results.append(engine.send())

        first = threading.Thread(target=send, args=(self.engine,))
        second = threading.Thread(target=send, args=(second_engine,))
        first.start()
        second.start()
        first.join(timeout=5)
        second.join(timeout=5)

        self.assertFalse(first.is_alive())
        self.assertFalse(second.is_alive())
        self.assertEqual(sorted(results), [False, True])
        self.assertEqual(len(self.engine.store.outbox()), 1)

    def test_recipient_cannot_change_after_send_is_pending(self) -> None:
        self.engine.prepare_tasks()
        self.assertTrue(self.engine.approve("张三"))
        with self.assertRaises(SimulatedCrash):
            self.engine.send(simulate_crash_after_write=True)

        with self.assertRaisesRegex(ValueError, "不能再修改"):
            self.engine.set_recipient("李四")

        self.assertEqual(self.engine.status()["task_a"]["recipient"], "张三")


if __name__ == "__main__":
    unittest.main()

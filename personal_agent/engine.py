from __future__ import annotations

import csv
import hashlib
import json
from datetime import date
from contextlib import contextmanager
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, Iterator, TypeVar
from uuid import uuid4

from .storage import LocalStore, timestamp

T = TypeVar("T")


class SimulatedCrash(RuntimeError):
    """Raised by the demo after the local send record is durable."""


class InputDataError(ValueError):
    """Raised when a configured local input cannot be safely interpreted."""


class ExecutionSlot:
    """Serializes task steps and records why the slot was granted or released."""

    def __init__(self, event: Callable[..., None]) -> None:
        self._event = event
        self.owner: str | None = None

    @contextmanager
    def claim(self, task_id: str, action: str, reason: str) -> Iterator[None]:
        if self.owner is not None:
            self._event(
                "BLOCK",
                "slot_request",
                f"执行槽正由任务 {self.owner} 使用，拒绝任务 {task_id} 的请求。",
                task_id=task_id,
            )
            raise RuntimeError("execution slot is already occupied")
        self.owner = task_id
        self._event("ALLOW", action, reason, task_id=task_id)
        try:
            yield
        finally:
            self.owner = None
            self._event(
                "INFO",
                "slot_released",
                f"任务 {task_id} 的当前步骤结束，执行槽已释放。",
                task_id=task_id,
            )


class AgentEngine:
    def __init__(self, workdir: Path, data_dir: Path) -> None:
        self.store = LocalStore(workdir)
        self.data_dir = data_dir
        self.state = self.store.load_state()
        self.slot = ExecutionSlot(self._event)

    def create_workflow(self, recipient: str = "张三") -> dict[str, Any]:
        with self._exclusive():
            return self._create_workflow_locked(recipient)

    def _create_workflow_locked(self, recipient: str) -> dict[str, Any]:
        workflow_id = f"{timestamp()[:10]}-{uuid4().hex[:8]}"
        self.state = {
            "workflow_id": workflow_id,
            "created_at": timestamp(),
            "task_a": {
                "status": "QUEUED",
                "recipient": recipient,
                "report_path": f"report-{workflow_id}.md",
                "report_sha256": None,
                "report_valid": False,
                "approval": None,
                "pending_send": None,
                "receipt": None,
            },
            "task_b": {"status": "QUEUED", "findings": []},
        }
        self._save()
        self._event(
            "INFO",
            "workflow_created",
            "创建 A 日报与 B 文件检查任务；同一时刻最多执行一个步骤。",
        )
        return self.state

    def prepare_tasks(self, *, inject_missing_unit: bool = False) -> None:
        with self._exclusive():
            self._prepare_tasks_locked(inject_missing_unit=inject_missing_unit)

    def _prepare_tasks_locked(self, *, inject_missing_unit: bool) -> None:
        self._require_state()
        task_a = self.state["task_a"]
        if task_a["status"] == "QUEUED":
            try:
                with self.slot.claim("A", "read_market_data", "本地行情和新闻读取规则允许自动执行。"):
                    source = self._read_market_source()
                    quotes = source["quotes"]
                    news = source["news"]
                    as_of = source["as_of"]

                with self.slot.claim("A", "generate_report", "日报是本地产物，可以自动生成。"):
                    report = self._build_report(
                        quotes,
                        news,
                        as_of,
                        omit_first_unit=inject_missing_unit,
                    )
                    self._save_report(report)

                with self.slot.claim("A", "check_report", "生成后的日报按必填字段规则自动检查。"):
                    defects = self._validate_report(report, quotes, news, as_of)
                    if defects:
                        task_a["status"] = "CORRECTION_REQUIRED"
                        task_a["report_valid"] = False
                        self._save()
                        self._event(
                            "BLOCK",
                            "report_quality_gate",
                            "日报缺少必填信息，拦截后续发送；" + "；".join(defects),
                            task_id="A",
                            metadata={"defects": defects},
                        )
                    else:
                        self._mark_report_valid(report)

                if defects:
                    with self.slot.claim(
                        "A",
                        "correct_report",
                        "质量门发现缺项后，允许自动修正文档并重新检查。",
                    ):
                        corrected = self._build_report(quotes, news, as_of, omit_first_unit=False)
                        self._save_report(corrected)
                        self._event(
                            "INFO",
                            "report_corrected",
                            "已补齐缺失价格单位，等待重新检查。",
                            task_id="A",
                        )
                    with self.slot.claim("A", "recheck_report", "修正后的日报必须通过同一套必填字段检查。"):
                        defects = self._validate_report(corrected, quotes, news, as_of)
                        if defects:
                            task_a["status"] = "CORRECTION_REQUIRED"
                            self._save()
                            self._event(
                                "BLOCK",
                                "report_recheck",
                                "修正后仍未通过检查：" + "；".join(defects),
                                task_id="A",
                                metadata={"defects": defects},
                            )
                            return
                        self._mark_report_valid(corrected)

                task_a["status"] = "WAITING_APPROVAL"
                self._save()
                self._event(
                    "WAIT",
                    "approval_gate",
                    "日报检查通过，但发送属于外部影响；等待用户批准当前收件人和报告版本。",
                    task_id="A",
                    metadata={"recipient": task_a["recipient"], "report_sha256": task_a["report_sha256"]},
                )
            except InputDataError as exc:
                self._fail_task("A", str(exc))

        if self.state["task_b"]["status"] == "QUEUED":
            self._run_task_b()

    def approve(self, recipient: str) -> bool:
        with self._exclusive():
            return self._approve_locked(recipient)

    def _approve_locked(self, recipient: str) -> bool:
        self._require_state()
        task_a = self.state["task_a"]
        if task_a["status"] != "WAITING_APPROVAL":
            self._event(
                "BLOCK",
                "approval_request",
                f"任务 A 当前状态为 {task_a['status']}，不接受新的发送批准。",
                task_id="A",
            )
            return False
        if recipient != task_a["recipient"]:
            self._event(
                "BLOCK",
                "approval_request",
                f"批准对象是 {recipient}，当前收件人是 {task_a['recipient']}；批准不匹配。",
                task_id="A",
                metadata={"requested_recipient": recipient, "current_recipient": task_a["recipient"]},
            )
            return False
        if not task_a["report_valid"]:
            self._event("BLOCK", "approval_request", "日报尚未通过检查，不能批准发送。", task_id="A")
            return False

        task_a["approval"] = {
            "recipient": recipient,
            "report_sha256": task_a["report_sha256"],
            "approved_at": timestamp(),
        }
        self._save()
        self._event(
            "ALLOW",
            "approval_recorded",
            f"已记录对收件人 {recipient} 和当前日报版本的明确批准。",
            task_id="A",
            metadata={"recipient": recipient, "report_sha256": task_a["report_sha256"]},
        )
        return True

    def set_recipient(self, recipient: str) -> None:
        with self._exclusive():
            self._set_recipient_locked(recipient)

    def _set_recipient_locked(self, recipient: str) -> None:
        self._require_state()
        if not recipient.strip():
            raise ValueError("recipient cannot be blank")
        task_a = self.state["task_a"]
        if task_a["status"] not in {"QUEUED", "WAITING_APPROVAL"}:
            reason = f"任务 A 当前状态为 {task_a['status']}，不能再修改本次发送的收件人。"
            self._event("BLOCK", "recipient_change", reason, task_id="A")
            raise ValueError(reason)
        previous = task_a["recipient"]
        task_a["recipient"] = recipient.strip()
        self._save()
        approval = task_a.get("approval")
        if approval and approval.get("recipient") != task_a["recipient"]:
            self._event(
                "BLOCK",
                "approval_invalidated",
                f"收件人从 {previous} 改为 {task_a['recipient']}；原批准不再适用，必须重新批准。",
                task_id="A",
                metadata={"old_recipient": previous, "new_recipient": task_a["recipient"]},
            )
        else:
            self._event(
                "INFO",
                "recipient_changed",
                f"收件人已从 {previous} 改为 {task_a['recipient']}。",
                task_id="A",
            )

    def send(self, *, simulate_crash_after_write: bool = False) -> bool:
        with self._exclusive():
            return self._send_locked(simulate_crash_after_write=simulate_crash_after_write)

    def _send_locked(self, *, simulate_crash_after_write: bool) -> bool:
        self._require_state()
        task_a = self.state["task_a"]
        reason = self._send_block_reason()
        if reason:
            self._event("BLOCK", "send_gate", reason, task_id="A")
            return False

        action_id = self._action_id(task_a)
        report = (self.store.root / task_a["report_path"]).read_text(encoding="utf-8")
        record = {
            "action_id": action_id,
            "workflow_id": self.state["workflow_id"],
            "recipient": task_a["recipient"],
            "report_sha256": task_a["report_sha256"],
            "report": report,
            "recorded_at": timestamp(),
            "mode": "local_simulation",
        }
        task_a["pending_send"] = {"action_id": action_id, "record": record}
        task_a["status"] = "WAITING_RECEIPT"
        self._save()

        with self.slot.claim("A", "write_local_send_record", "批准匹配后只写入本地模拟发送记录，不连接外部服务。"):
            stored_record, created = self.store.append_outbox_once(record)
            self._event(
                "ALLOW",
                "simulated_send_recorded",
                "本地发送记录已写入。" if created else "发现同一动作的本地记录，复用已有结果。",
                task_id="A",
                metadata={"action_id": stored_record["action_id"], "created": created},
            )

        if simulate_crash_after_write:
            self._event(
                "INFO",
                "simulated_process_exit",
                "发送记录已持久化，但回执尚未更新；现在模拟进程退出。",
                task_id="A",
                metadata={"action_id": action_id},
            )
            raise SimulatedCrash("simulated exit after durable send record")

        self._complete_from_record(stored_record, recovered=False)
        return True

    def recover(self) -> bool:
        with self._exclusive():
            return self._recover_locked()

    def _recover_locked(self) -> bool:
        self._require_state()
        task_a = self.state["task_a"]
        pending = task_a.get("pending_send")
        if not pending:
            self._event("INFO", "recovery_check", "没有待核对的发送动作。", task_id="A")
            return False

        with self.slot.claim(
            "A",
            "recover_pending_send",
            "任务 A 从发送等待态恢复，核对持久化记录后再确认回执。",
        ):
            record = self.store.find_outbox(pending["action_id"])
            if record is None:
                record, created = self.store.append_outbox_once(pending["record"])
                self._event(
                    "INFO",
                    "recovery_retry",
                    "尚无本地发送记录，按同一动作编号补写一次。",
                    task_id="A",
                    metadata={"action_id": record["action_id"], "created": created},
                )
            else:
                self._event(
                    "ALLOW",
                    "recovery_reconcile",
                    "找到原发送记录，复用它更新回执，不重复写入。",
                    task_id="A",
                    metadata={"action_id": record["action_id"]},
                )
            self._complete_from_record(record, recovered=True)
        return True

    def status(self) -> dict[str, Any] | None:
        with self._exclusive():
            return self.state

    def _run_task_b(self) -> None:
        check_path = self.data_dir / "market_check.csv"
        if self.state["task_a"]["status"] == "WAITING_APPROVAL":
            reason = "任务 A 正等待用户批准并已释放唯一执行槽；本地文件检查规则允许 B 自动执行。"
        else:
            reason = "本地文件检查规则允许 B 自动执行。"
        with self.slot.claim("B", "check_market_file", reason):
            try:
                with check_path.open("r", encoding="utf-8-sig", newline="") as stream:
                    reader = csv.DictReader(stream, strict=True)
                    rows = list(reader)
                    headers = reader.fieldnames or []
            except (OSError, UnicodeError, csv.Error) as exc:
                self._fail_task("B", f"无法读取 {check_path.name}：{exc}")
                return

            required_headers = {"record_id", "symbol", "name", "price", "unit", "date", "source"}
            missing_headers = sorted(required_headers - set(headers))
            if not headers or missing_headers:
                self._fail_task(
                    "B",
                    f"{check_path.name} 缺少必需列：{', '.join(missing_headers) or '表头'}。",
                )
                return
            if len(set(headers)) != len(headers):
                self._fail_task("B", f"{check_path.name} 存在重复列名，无法可靠检查。")
                return

            findings: list[dict[str, Any]] = []
            malformed_rows: list[int] = []
            for row_number, row in enumerate(rows, start=2):
                if None in row or any(value is None for value in row.values()):
                    malformed_rows.append(row_number)
                    continue
                record_id = row.get("record_id", "").strip()
                symbol = row.get("symbol", "").strip()
                name = row.get("name", "").strip()
                price = row.get("price", "").strip()
                unit = row.get("unit", "").strip()
                row_findings: list[str] = []
                if not record_id:
                    row_findings.append("缺少记录编号")
                if not symbol:
                    row_findings.append("缺少标的代码")
                if not name:
                    row_findings.append("缺少名称")

                parsed_price: Decimal | None = None
                if not price:
                    row_findings.append("价格缺失")
                else:
                    try:
                        parsed_price = Decimal(price)
                    except InvalidOperation:
                        row_findings.append("价格无效")
                    else:
                        if not parsed_price.is_finite() or parsed_price < 0:
                            parsed_price = None
                            row_findings.append("价格无效")
                if parsed_price is not None and not unit:
                    row_findings.append("缺少价格单位")

                row_date = row.get("date", "").strip()
                if not row_date:
                    row_findings.append("日期缺失")
                elif not self._is_iso_date(row_date):
                    row_findings.append("日期格式无效")

                if row_findings:
                    findings.append(
                        {
                            "record_id": record_id or f"第 {row_number} 行",
                            "symbol": symbol,
                            "name": name,
                            "issue": "、".join(row_findings),
                        }
                    )

            if malformed_rows:
                self._fail_task(
                    "B",
                    f"{check_path.name} 第 {', '.join(map(str, malformed_rows))} 行的列数与表头不一致；"
                    "检查不完整，未将部分结果标记为成功。",
                    metadata={"malformed_rows": malformed_rows},
                )
                return

            self.state["task_b"] = {
                "status": "COMPLETED",
                "checked_rows": len(rows),
                "findings": findings,
            }
            self._save()
            lines = ["# 行情文件检查结果", "", f"检查文件：`{check_path.name}`", ""]
            if findings:
                lines.extend(
                    f"- {item['record_id']} {item['symbol']} {item['name']}：{item['issue']}。"
                    for item in findings
                )
            else:
                lines.append("- 未发现价格单位缺失或其他已配置的数据问题。")
            self.store.write_output("b-check-result.md", "\n".join(lines) + "\n")
            self._event(
                "INFO",
                "check_result",
                f"B 检查完成 {len(rows)} 条记录，发现 {len(findings)} 条存在数据问题。",
                task_id="B",
                metadata={"checked_rows": len(rows), "findings": findings},
            )

    def _read_market_source(self) -> dict[str, Any]:
        source_path = self.data_dir / "market_snapshot.json"
        try:
            with source_path.open("r", encoding="utf-8") as stream:
                source = json.load(stream)
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise InputDataError(f"无法读取 {source_path.name}：{exc}") from exc
        if not isinstance(source, dict):
            raise InputDataError(f"{source_path.name} 顶层必须是 JSON 对象。")
        if not self._is_iso_date(str(source.get("as_of", ""))):
            raise InputDataError(f"{source_path.name} 的 as_of 缺失或不是 YYYY-MM-DD 日期。")
        quotes = source.get("quotes")
        news = source.get("news")
        if not isinstance(quotes, list) or not quotes:
            raise InputDataError(f"{source_path.name} 的 quotes 必须是非空数组。")
        if not isinstance(news, list) or not news:
            raise InputDataError(f"{source_path.name} 的 news 必须是非空数组。")

        quote_fields = ("symbol", "name", "unit", "date", "source")
        for index, quote in enumerate(quotes, start=1):
            label = f"{source_path.name} 的第 {index} 条行情"
            if not isinstance(quote, dict):
                raise InputDataError(f"{label} 必须是 JSON 对象。")
            missing = [
                field
                for field in quote_fields
                if not isinstance(quote.get(field), str) or not quote[field].strip()
            ]
            if missing:
                raise InputDataError(f"{label} 缺少必需字段：{', '.join(missing)}。")
            if not self._is_iso_date(str(quote["date"])):
                raise InputDataError(f"{label} 的 date 不是 YYYY-MM-DD 日期。")
            try:
                price = Decimal(str(quote.get("price", "")))
            except InvalidOperation as exc:
                raise InputDataError(f"{label} 的 price 不是有效数字。") from exc
            if not price.is_finite() or price < 0:
                raise InputDataError(f"{label} 的 price 必须是有限且非负的数字。")
            quote["price"] = price

        news_fields = ("title", "source", "impact")
        for index, item in enumerate(news, start=1):
            label = f"{source_path.name} 的第 {index} 条新闻"
            if not isinstance(item, dict):
                raise InputDataError(f"{label} 必须是 JSON 对象。")
            missing = [
                field
                for field in news_fields
                if not isinstance(item.get(field), str) or not item[field].strip()
            ]
            if missing:
                raise InputDataError(f"{label} 缺少必需字段：{', '.join(missing)}。")
        return source

    @staticmethod
    def _is_iso_date(value: str) -> bool:
        try:
            return date.fromisoformat(value).isoformat() == value
        except ValueError:
            return False

    @staticmethod
    def _build_report(
        quotes: list[dict[str, Any]],
        news: list[dict[str, Any]],
        as_of: str,
        *,
        omit_first_unit: bool,
    ) -> str:
        rows = [
            "# Personal Agent 行情与新闻日报",
            "",
            f"日期：{as_of}",
            "",
            "## 行情",
            "",
            "| 标的 | 名称 | 价格 | 单位 | 日期 | 来源 |",
            "| --- | --- | ---: | --- | --- | --- |",
        ]
        for index, quote in enumerate(quotes):
            unit = "" if omit_first_unit and index == 0 else quote["unit"]
            rows.append(
                f"| {quote['symbol']} | {quote['name']} | {quote['price']:.2f} | "
                f"{unit or '待补'} | {quote['date']} | {quote['source']} |"
            )
        rows.extend(["", "## 新闻", ""])
        rows.extend(
            f"- {item['title']}（来源：{item['source']}；影响：{item['impact']}）"
            for item in news
        )
        rows.append("")
        return "\n".join(rows)

    @staticmethod
    def _validate_report(
        report: str,
        quotes: list[dict[str, Any]],
        news: list[dict[str, Any]],
        as_of: str,
    ) -> list[str]:
        defects: list[str] = []
        if f"日期：{as_of}" not in report:
            defects.append(f"缺少日报日期 {as_of}")
        for quote in quotes:
            expected = (
                quote["symbol"],
                quote["name"],
                f"{quote['price']:.2f}",
                quote["unit"],
                quote["date"],
                quote["source"],
            )
            line = next(
                (item for item in report.splitlines() if item.startswith(f"| {quote['symbol']} |")),
                None,
            )
            actual = [cell.strip() for cell in line.strip("|").split("|")] if line else []
            if actual == list(expected):
                continue
            if len(actual) == len(expected) and actual[3] != expected[3]:
                defects.append(
                    f"{quote['symbol']} 行情记录缺少或写错价格单位（应为 {quote['unit']}）"
                )
            else:
                defects.append(
                    f"{quote['symbol']} 行情行不完整；价格、单位、日期和来源必须逐项匹配"
                )
        for item in news:
            if item["title"] not in report or item["source"] not in report:
                defects.append(f"新闻 {item['title']} 缺少标题或来源")
        return defects

    def _save_report(self, report: str) -> None:
        self.store.write_output(self.state["task_a"]["report_path"], report)

    def _mark_report_valid(self, report: str) -> None:
        task_a = self.state["task_a"]
        task_a["report_valid"] = True
        task_a["report_sha256"] = hashlib.sha256(report.encode("utf-8")).hexdigest()
        task_a["status"] = "WAITING_APPROVAL"
        self._save()
        self._event(
            "ALLOW",
            "report_quality_gate",
            "日报必填字段齐全，允许进入批准阶段。",
            task_id="A",
            metadata={"report_sha256": task_a["report_sha256"]},
        )

    def _send_block_reason(self) -> str | None:
        task_a = self.state["task_a"]
        if task_a["status"] != "WAITING_APPROVAL":
            return f"任务 A 当前状态为 {task_a['status']}，不在可发送阶段。"
        if not task_a["report_valid"]:
            return "日报未通过质量检查，发送被拦截。"
        try:
            current_report = (self.store.root / task_a["report_path"]).read_text(encoding="utf-8")
        except (OSError, UnicodeError) as exc:
            return f"无法读取已检查的日报，发送被拦截：{exc}"
        current_hash = hashlib.sha256(current_report.encode("utf-8")).hexdigest()
        if current_hash != task_a["report_sha256"]:
            return "日报内容在批准后发生变化，旧批准失效；必须重新检查并批准。"
        approval = task_a.get("approval")
        if not approval:
            return "没有用户批准，发送被拦截。"
        if approval.get("recipient") != task_a["recipient"]:
            return (
                f"批准对象是 {approval.get('recipient')}，当前收件人是 {task_a['recipient']}；"
                "旧批准不适用，且没有写入发送记录。"
            )
        if approval.get("report_sha256") != current_hash:
            return "批准对应的日报版本已变化，必须重新批准。"
        return None

    def _complete_from_record(self, record: dict[str, Any], *, recovered: bool) -> None:
        task_a = self.state["task_a"]
        task_a["status"] = "COMPLETED"
        task_a["receipt"] = {
            "action_id": record["action_id"],
            "recipient": record["recipient"],
            "recorded_at": record["recorded_at"],
            "recovered_after_restart": recovered,
        }
        self._save()
        self._event(
            "ALLOW",
            "receipt_confirmed",
            "已确认本地模拟发送结果。" + ("这是重启后对原记录的核对。" if recovered else ""),
            task_id="A",
            metadata={"action_id": record["action_id"], "recovered": recovered},
        )

    def _action_id(self, task_a: dict[str, Any]) -> str:
        seed = "|".join(
            (
                self.state["workflow_id"],
                "A",
                task_a["recipient"],
                task_a["report_sha256"],
            )
        )
        return hashlib.sha256(seed.encode("utf-8")).hexdigest()

    def _event(
        self,
        decision: str,
        action: str,
        reason: str,
        *,
        task_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        self.store.append_event(
            {
                "at": timestamp(),
                "workflow_id": self.state.get("workflow_id") if self.state else None,
                "task_id": task_id,
                "decision": decision,
                "action": action,
                "reason": reason,
                "metadata": metadata or {},
            }
        )

    @contextmanager
    def _exclusive(self) -> Iterator[None]:
        with self.store.workflow_lock():
            self.state = self.store.load_state()
            yield

    def _fail_task(
        self,
        task_id: str,
        reason: str,
        *,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        task = self.state["task_a" if task_id == "A" else "task_b"]
        task["status"] = "FAILED"
        task["error"] = reason
        if task_id == "A":
            task["report_valid"] = False
            task["approval"] = None
        self._save()
        self._event(
            "BLOCK",
            "task_failed",
            reason,
            task_id=task_id,
            metadata={"error": reason, **(metadata or {})},
        )
        if task_id == "B":
            self.store.write_output(
                "b-check-result.md",
                "\n".join(
                    [
                        "# 行情文件检查结果",
                        "",
                        "- 检查失败，未将不完整结果标记为成功。",
                        f"- 原因：{reason}",
                        "",
                    ]
                ),
            )

    def _save(self) -> None:
        self._require_state()
        self.store.save_state(self.state)

    def _require_state(self) -> None:
        if self.state is None:
            raise RuntimeError("no workflow exists; run `python main.py run` first")

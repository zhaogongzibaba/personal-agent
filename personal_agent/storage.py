from __future__ import annotations

import json
import os
import uuid
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterator


def timestamp() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


class LocalStore:
    """Small JSON/JSONL store for the local simulation; no external services."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        self.state_path = root / "state.json"
        self.events_path = root / "events.jsonl"
        self.outbox_path = root / "send-log.jsonl"
        self.lock_path = root / "workflow.lock"

    @contextmanager
    def workflow_lock(self) -> Iterator[None]:
        """Hold one cross-process lock across a workflow state transition."""
        with self.lock_path.open("a+b") as lock_file:
            lock_file.seek(0, os.SEEK_END)
            if lock_file.tell() == 0:
                lock_file.write(b"0")
                lock_file.flush()
            lock_file.seek(0)
            if os.name == "nt":
                import errno
                import msvcrt
                import time

                while True:
                    lock_file.seek(0)
                    try:
                        msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
                        break
                    except OSError as exc:
                        if exc.errno not in {errno.EACCES, errno.EDEADLK}:
                            raise
                        time.sleep(0.05)
            else:
                import fcntl

                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                lock_file.seek(0)
                if os.name == "nt":
                    msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

    def load_state(self) -> dict[str, Any] | None:
        if not self.state_path.exists():
            return None
        return json.loads(self.state_path.read_text(encoding="utf-8"))

    def save_state(self, state: dict[str, Any]) -> None:
        temp_path = self.root / f"state-{uuid.uuid4().hex}.tmp"
        temp_path.write_text(
            json.dumps(state, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        os.replace(temp_path, self.state_path)

    def append_event(self, event: dict[str, Any]) -> None:
        self._append_jsonl(self.events_path, event)

    def events(self) -> list[dict[str, Any]]:
        return self._read_jsonl(self.events_path)

    def outbox(self) -> list[dict[str, Any]]:
        return self._read_jsonl(self.outbox_path)

    def find_outbox(self, action_id: str) -> dict[str, Any] | None:
        return next(
            (record for record in self.outbox() if record.get("action_id") == action_id),
            None,
        )

    def append_outbox_once(self, record: dict[str, Any]) -> tuple[dict[str, Any], bool]:
        existing = self.find_outbox(record["action_id"])
        if existing is not None:
            return existing, False
        self._append_jsonl(self.outbox_path, record)
        return record, True

    def write_output(self, name: str, content: str) -> Path:
        path = self.root / name
        path.write_text(content, encoding="utf-8")
        return path

    @staticmethod
    def _append_jsonl(path: Path, item: dict[str, Any]) -> None:
        with path.open("a", encoding="utf-8", newline="\n") as stream:
            stream.write(json.dumps(item, ensure_ascii=False, sort_keys=True) + "\n")
            stream.flush()
            os.fsync(stream.fileno())

    @staticmethod
    def _read_jsonl(path: Path) -> list[dict[str, Any]]:
        if not path.exists():
            return []
        records: list[dict[str, Any]] = []
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                records.append(json.loads(line))
        return records

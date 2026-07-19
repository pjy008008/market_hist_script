"""Persistent, atomic checkpoints for the daily market-data pipeline."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_STATE_PATH = PROJECT_ROOT / "pipeline_state" / "daily_pipeline_state.json"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class PipelineStateStore:
    def __init__(self, path: Path = DEFAULT_STATE_PATH) -> None:
        self.path = path
        self.data = self._load()

    def _load(self) -> dict[str, Any]:
        if not self.path.is_file():
            return {"version": 1, "checkpoints": {}}
        try:
            loaded = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"파이프라인 상태 파일을 읽을 수 없습니다: {exc}") from exc
        if not isinstance(loaded, dict) or loaded.get("version") != 1:
            raise RuntimeError("지원하지 않는 파이프라인 상태 파일 형식입니다.")
        loaded.setdefault("checkpoints", {})
        return loaded

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary_path = self.path.with_name(f".{self.path.name}.tmp")
        self.data["updated_at_utc"] = utc_now_iso()
        try:
            temporary_path.write_text(
                json.dumps(self.data, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
            os.replace(temporary_path, self.path)
        finally:
            temporary_path.unlink(missing_ok=True)

    def begin_run(self, storage_format: str, target_session_utc: str) -> None:
        self.data["last_run"] = {
            "status": "running",
            "storage_format": storage_format,
            "target_session_utc": target_session_utc,
            "started_at_utc": utc_now_iso(),
        }
        self.save()

    def finish_run(self, status: str, failures: list[dict[str, Any]]) -> None:
        run = self.data.setdefault("last_run", {})
        run["status"] = status
        run["finished_at_utc"] = utc_now_iso()
        run["failure_count"] = len(failures)
        self.data["last_failures"] = failures
        self.save()

    def _entry(
        self,
        storage_format: str,
        symbol: str,
        stage: str,
        create: bool = False,
    ) -> dict[str, Any]:
        checkpoints = self.data.setdefault("checkpoints", {})
        if create:
            return (
                checkpoints.setdefault(storage_format, {})
                .setdefault(symbol, {})
                .setdefault(stage, {})
            )
        return (
            checkpoints.get(storage_format, {})
            .get(symbol, {})
            .get(stage, {})
        )

    def is_complete(
        self,
        storage_format: str,
        symbol: str,
        stage: str,
        target_session_utc: str,
        output_path: Path,
    ) -> bool:
        entry = self._entry(storage_format, symbol, stage)
        return (
            output_path.is_file()
            and entry.get("status") == "success"
            and entry.get("target_session_utc") == target_session_utc
        )

    def mark_stage(
        self,
        storage_format: str,
        symbol: str,
        stage: str,
        status: str,
        target_session_utc: str,
        attempt: int,
        error: str = "",
        details: dict[str, Any] | None = None,
    ) -> None:
        entry = self._entry(storage_format, symbol, stage, create=True)
        entry.update(
            {
                "status": status,
                "target_session_utc": target_session_utc,
                "attempt": attempt,
                "updated_at_utc": utc_now_iso(),
                "error": error,
            }
        )
        if details:
            entry.update(details)
        self.save()


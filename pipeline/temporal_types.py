from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class TemporalActivityInput:
    run_id: str
    config_path: str | None = None
    root: str | None = None
    fresh: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class TemporalActivityResult:
    run_id: str
    status: str
    note: str
    last_error: str = ""
    sendable_rows: int = 0
    auto_send: bool | None = None
    outcome: str = "success"
    current_stage: str = ""
    retry_count: int = 0
    metadata: dict[str, Any] | None = None

    @classmethod
    def from_record(cls, record: dict[str, Any], **overrides: Any) -> "TemporalActivityResult":
        payload = {
            "run_id": str(record["id"]),
            "status": str(record.get("status") or ""),
            "note": str(record.get("note") or ""),
            "last_error": str(record.get("last_error") or ""),
            "retry_count": int(record.get("retry_count", 0) or 0),
            **overrides,
        }
        return cls(**payload)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class TemporalWorkflowInput:
    run_id: str
    config_path: str | None = None
    root: str | None = None
    fresh: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class TemporalWorkflowResult:
    run_id: str
    status: str
    note: str
    task_queue: str
    workflow_id: str
    current_stage: str = ""
    linkedin_retry_count: int = 0
    rocketreach_retry_count: int = 0
    email_retry_count: int = 0
    metadata: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class TemporalStartResult:
    run_id: str
    workflow_id: str
    task_queue: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

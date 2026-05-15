from __future__ import annotations

import json
from pathlib import Path

from .config import load_automation_summary


def build_manifest(record: dict) -> dict:
    return {
        "run_id": record["id"],
        "status": record["status"],
        "config_path": record.get("config_path") or "",
        "created_at": record.get("created_at") or "",
        "updated_at": record.get("updated_at") or "",
        "stage_started_at": record.get("stage_started_at") or "",
        "stage_finished_at": record.get("stage_finished_at") or "",
        "retry_count": record.get("retry_count", 0),
        "email_stats": {
            "total": record.get("email_total", 0),
            "sent": record.get("email_sent", 0),
            "failed": record.get("email_failed", 0),
        },
        "enrichment_stats": {
            "provider_success_count": record.get("provider_success_count", 0),
            "no_email_count": record.get("no_email_count", 0),
            "provider_retry_count": record.get("provider_retry_count", 0),
        },
        "workflow_recovery": {
            "workflow_retry_count": record.get("workflow_retry_count", 0),
            "last_workflow_rerun_reason": record.get("last_workflow_rerun_reason") or "",
            "last_failed_stage": record.get("last_failed_stage") or "",
        },
        "temporal": {
            "workflow_id": record.get("temporal_workflow_id") or "",
            "task_queue": record.get("temporal_task_queue") or "",
            "backend": record.get("orchestration_backend") or "",
        },
        "note": record.get("note") or "",
        "last_error": record.get("last_error") or "",
        "live_status": record.get("live_status", {}),
        "automation": load_automation_summary(record.get("config_path") or None),
        "paths": {
            "run_dir": record["run_dir"],
            "applied_csv": record["applied_csv_path"],
            "external_jobs_csv": record["external_jobs_csv_path"],
            "recruiters_csv": record["recruiters_csv_path"],
            "send_report_csv": record["send_report_path"],
            "manifest_json": record["manifest_path"],
            "logs_dir": record["log_dir"],
            "linkedin_stdout_log": record["linkedin_stdout_log"],
            "linkedin_stderr_log": record["linkedin_stderr_log"],
            "rocketreach_stdout_log": record["rocketreach_stdout_log"],
            "rocketreach_stderr_log": record["rocketreach_stderr_log"],
        },
        "artifacts": {
            "applied_csv_exists": Path(record["applied_csv_path"]).exists(),
            "external_jobs_csv_exists": Path(record["external_jobs_csv_path"]).exists(),
            "recruiters_csv_exists": Path(record["recruiters_csv_path"]).exists(),
            "send_report_exists": Path(record["send_report_path"]).exists(),
        },
    }


def write_manifest(record: dict) -> None:
    manifest_path = Path(record["manifest_path"])
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(build_manifest(record), indent=2),
        encoding="utf-8",
    )

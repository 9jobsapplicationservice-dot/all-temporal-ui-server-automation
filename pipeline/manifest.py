from __future__ import annotations

import json
import os
from pathlib import Path

from .config import load_automation_summary


def resolve_data_dir(data_dir: str | None = None) -> Path:
    if data_dir:
        return Path(data_dir).resolve()
    
    # Priority matches constants.py but as a standalone helper here
    env_data = os.environ.get("PIPELINE_DATA_DIR", "").strip()
    if env_data:
        return Path(env_data).resolve()
    
    if os.path.exists("/app/data/pipeline"):
        return Path("/app/data/pipeline")
        
    return Path(__file__).parent.parent.resolve()


def safe_read_manifest(run_id: str, data_dir: str | None = None) -> dict:
    root = resolve_data_dir(data_dir)
    
    # Try multiple possible locations for backward compatibility
    candidates = [
        root / "meta" / f"{run_id}.json",
        root / run_id / "manifest.json",
        root / "runs" / run_id / "manifest.json"
    ]
    
    for path in candidates:
        if path.exists():
            try:
                return json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
                
    # Default safe manifest if none found
    return {
        "id": run_id,
        "runId": run_id,
        "run_id": run_id,
        "status": "starting",
        "stage": "linkedin",
        "appliedRows": 0,
        "recruiterRows": 0,
        "readyToSend": 0,
        "emailSent": 0,
        "latestLog": "Manifest not found; default created.",
        "error": None,
        "artifacts": {
            "applied_csv": None,
            "recruiter_csv": None,
            "email_log_csv": None,
            "applied_csv_exists": False,
            "external_jobs_csv_exists": False,
            "recruiters_csv_exists": False,
            "send_report_exists": False
        },
        "liveStatus": {},
        "paths": {
            "run_dir": str(root / run_id),
            "manifest_json": str(root / run_id / "manifest.json"),
            "applied_csv_path": str(root / run_id / "job_applied" / "applied_jobs.csv"),
            "external_jobs_csv_path": str(root / run_id / "external" / "external_jobs.csv"),
            "recruiters_csv_path": str(root / run_id / "rocket_enrich" / "recruiters_enriched.csv"),
            "send_report_path": str(root / run_id / "reports" / f"{run_id}.csv"),
            "log_dir": str(root / run_id / "logs"),
            "linkedin_stdout_log": str(root / run_id / "logs" / "linkedin.stdout.log"),
            "linkedin_stderr_log": str(root / run_id / "logs" / "linkedin.stderr.log"),
            "rocketreach_stdout_log": str(root / run_id / "logs" / "rocketreach.stdout.log"),
            "rocketreach_stderr_log": str(root / run_id / "logs" / "rocketreach.stderr.log"),
        }
    }


def safe_write_manifest(run_id: str, manifest: dict, data_dir: str | None = None) -> None:
    root = resolve_data_dir(data_dir)
    manifest_path = root / "meta" / f"{run_id}.json"
    write_manifest_file(manifest_path, manifest)


def write_manifest_file(manifest_path: Path, manifest: dict) -> None:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)

    temp_path = manifest_path.with_suffix(".tmp")
    try:
        temp_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        temp_path.replace(manifest_path)
    except Exception as e:
        if temp_path.exists():
            temp_path.unlink()
        raise e


def build_manifest(record: dict) -> dict:
    run_id = record.get("id") or record.get("runId") or record.get("run_id") or "unknown"
    return {
        "id": run_id,
        "runId": run_id,
        "run_id": run_id,
        "status": record.get("status") or "starting",
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
        "note": record.get("note") or "",
        "last_error": record.get("last_error") or "",
        "live_status": record.get("live_status", {}),
        "automation": load_automation_summary(record.get("config_path") or None),
        "paths": {
            "run_dir": record.get("run_dir") or "",
            "applied_csv": record.get("applied_csv_path") or "",
            "external_jobs_csv": record.get("external_jobs_csv_path") or "",
            "recruiters_csv": record.get("recruiters_csv_path") or "",
            "send_report_csv": record.get("send_report_path") or "",
            "manifest_json": record.get("manifest_path") or "",
            "logs_dir": record.get("log_dir") or "",
            "linkedin_stdout_log": record.get("linkedin_stdout_log") or "",
            "linkedin_stderr_log": record.get("linkedin_stderr_log") or "",
            "rocketreach_stdout_log": record.get("rocketreach_stdout_log") or "",
            "rocketreach_stderr_log": record.get("rocketreach_stderr_log") or "",
        },
        "artifacts": {
            "applied_csv": record.get("applied_csv_path"),
            "recruiter_csv": record.get("recruiters_csv_path"),
            "email_log_csv": record.get("send_report_path"),
            "applied_csv_exists": Path(record.get("applied_csv_path") or ".").exists() if record.get("applied_csv_path") else False,
            "external_jobs_csv_exists": Path(record.get("external_jobs_csv_path") or ".").exists() if record.get("external_jobs_csv_path") else False,
            "recruiters_csv_exists": Path(record.get("recruiters_csv_path") or ".").exists() if record.get("recruiters_csv_path") else False,
            "send_report_exists": Path(record.get("send_report_path") or ".").exists() if record.get("send_report_path") else False,
        },
    }


def write_manifest(record: dict) -> None:
    manifest = build_manifest(record)
    run_id = record.get("id") or manifest.get("run_id") or ""
    manifest_path = record.get("manifest_path")
    if manifest_path:
        write_manifest_file(Path(manifest_path), manifest)
        return
    safe_write_manifest(run_id, manifest)

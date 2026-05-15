from __future__ import annotations

import argparse
import asyncio
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from .config import AutomationConfigError, load_automation_config
from .storage import PipelineStore
from .utils import utc_now_iso

TERMINAL_STATUSES = {"completed", "failed", "waiting_login", "waiting_review", "blocked_runtime"}
FRESH_REUSE_WINDOW_SECONDS = 120.0


def _runner_log_dir(root: str | None) -> Path:
    store = PipelineStore(root)
    log_dir = store.paths.logs_root / "launcher"
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir


def _open_log(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    return path.open("w", encoding="utf-8")


# Temporal logic removed for Direct Execution migration
# Temporal logic removed for Direct Execution migration


def _resolve_effective_fresh_mode(config_path: str | None, requested_fresh: bool) -> bool:
    if requested_fresh:
        return True
    try:
        config = load_automation_config(config_path)
    except AutomationConfigError:
        raw = os.environ.get("PIPELINE_RUN_ONCE_ALWAYS_FRESH", "").strip().lower()
        return raw in {"1", "true", "yes", "on"}
    if getattr(config, "run_once_always_fresh", False):
        print(
            "PIPELINE_RUN_ONCE_ALWAYS_FRESH=true, so this command will start from the LinkedIn job apply stage "
            "even though --resume was requested."
        )
        return True
    return False


def _parse_utc_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _looks_like_recent_fresh_run(record: dict) -> bool:
    note = str(record.get("note") or "").lower()
    if "fresh run" not in note and "reopening chrome" not in note and "starting linkedin from scratch" not in note:
        return False
    timestamp = _parse_utc_timestamp(str(record.get("updated_at") or record.get("created_at") or ""))
    if timestamp is None:
        return False
    age_seconds = (datetime.now(timezone.utc) - timestamp).total_seconds()
    return age_seconds <= FRESH_REUSE_WINDOW_SECONDS


def _looks_like_recovered_interrupted_run(record: dict | None) -> bool:
    if not record:
        return False
    status = str(record.get("status") or "").strip().lower()
    note = str(record.get("note") or "").strip().lower()
    return status == "queued" and note.startswith("recovered interrupted ")


def _print_status(record: dict, previous_status: str | None, previous_note: str | None) -> None:
    status = str(record.get("status") or "")
    note = str(record.get("note") or "")
    if status != previous_status or note != previous_note:
        print(f"[{record['id']}] status={status} note={note}")


def _final_artifact_lines(record: dict) -> list[str]:
    return [
        f"manifest: {record['manifest_path']}",
        f"logs: {record['log_dir']}",
        f"applied_csv: {record['applied_csv_path']}",
        f"recruiters_csv: {record['recruiters_csv_path']}",
        f"send_report: {record['send_report_path']}",
    ]


async def _start_or_attach_workflow(
    store: PipelineStore,
    *,
    run_id: str | None,
    config_path: str | None,
    root: str | None,
    fresh: bool = False,
) -> TemporalStartResult:
    if fresh:
        if run_id:
            print(f"Fresh run requested for explicit run {run_id}. Restarting LinkedIn from scratch for that run.")
            return await start_temporal_workflow(run_id=run_id, config_path=config_path, root=root, fresh=True)
        print("Fresh run requested. Clearing conflicting active runs and starting LinkedIn from scratch.")
        return await start_temporal_workflow(run_id=None, config_path=config_path, root=root, fresh=True)

    if run_id:
        try:
            existing = store.get_run(run_id)
        except KeyError:
            existing = None
        if existing and str(existing.get("temporal_workflow_id") or "").strip():
            workflow_id = str(existing["temporal_workflow_id"])
            task_queue = str(existing.get("temporal_task_queue") or "")
            workflow_status = await get_temporal_workflow_status(workflow_id)
            if workflow_status == "running":
                if _looks_like_recovered_interrupted_run(existing):
                    print(
                        f"Run {existing['id']} was locally recovered after an interrupted stage, "
                        "so its stored running workflow will be restarted instead of reattached."
                    )
                    return await start_temporal_workflow(
                        run_id=existing["id"],
                        config_path=config_path,
                        root=root,
                        force_restart=True,
                    )
                print(f"Attaching to still-running workflow for run {existing['id']}.")
                return TemporalStartResult(run_id=existing["id"], workflow_id=workflow_id, task_queue=task_queue)
            print(
                f"Stored workflow for run {existing['id']} is {workflow_status or 'closed/unavailable'}. "
                "Restarting the same run instead of attaching to stale execution."
            )
        return await start_temporal_workflow(run_id=run_id, config_path=config_path, root=root)

    active_run = store.get_active_live_run()
    if active_run is not None:
        workflow_id = str(active_run.get("temporal_workflow_id") or "").strip()
        task_queue = str(active_run.get("temporal_task_queue") or "").strip()
        if workflow_id:
            workflow_status = await get_temporal_workflow_status(workflow_id)
            if workflow_status == "running":
                if _looks_like_recovered_interrupted_run(active_run):
                    print(
                        f"Active run {active_run['id']} was locally recovered after an interrupted stage, "
                        "so its stored running workflow will be restarted instead of reattached."
                    )
                    return await start_temporal_workflow(
                        run_id=active_run["id"],
                        config_path=config_path,
                        root=root,
                        force_restart=True,
                    )
                print(f"Attaching to active run {active_run['id']} because its workflow is still running.")
                return TemporalStartResult(run_id=active_run["id"], workflow_id=workflow_id, task_queue=task_queue)
            print(
                f"Active run {active_run['id']} points to a {workflow_status or 'closed/unavailable'} workflow. "
                "Restarting the same run instead of creating a new one."
            )
            return await start_temporal_workflow(run_id=active_run["id"], config_path=config_path, root=root)
        print(f"Reusing active run {active_run['id']} and starting its Temporal workflow.")
        return await start_temporal_workflow(run_id=active_run["id"], config_path=config_path, root=root)

    return await start_temporal_workflow(run_id=run_id, config_path=config_path, root=root)


async def run_once(
    *,
    config_path: str | None = None,
    run_id: str | None = None,
    root: str | None = None,
    fresh: bool = False,
) -> int:
    print("SCRIPT_STARTED: pipeline.run_once", flush=True)
    print(f"RUN_ID={run_id or 'unknown'}", flush=True)

    # Storage Validation
    store = PipelineStore(root)
    data_dir = str(store.paths.root)
    run_dir = str(store.paths.for_run(run_id or "unknown").run_dir)
    print(f"PIPELINE_DATA_DIR={data_dir}", flush=True)
    print(f"RUN_DIR={run_dir}", flush=True)
    print(f"CONFIG_PATH={config_path or 'default'}", flush=True)

    # Ensure run record exists before proceeding
    if run_id:
        try:
            store.get_run(run_id)
            print("RUN_RECORD_EXISTS=true", flush=True)
            print("MANIFEST_LOADED=true", flush=True)
        except KeyError:
            print(f"RUN_RECORD_EXISTS=false. Creating run {run_id}...", flush=True)
            store.create_run(run_id=run_id, config_path=config_path, allow_active_conflict=True)
            print("MANIFEST_LOADED=true", flush=True)

    # Render Startup Check
    if run_id:
        record = store.get_run(run_id)
        applied_path = Path(record.get("applied_csv_path") or "")
        print(f"APPLIED_CSV_EXISTS={str(applied_path.exists()).lower()}", flush=True)

    # DIRECT EXECUTION MODE
    try:
        from .stage_manager import PipelineStageManager
        manager = PipelineStageManager(store)
        
        # If fresh, reset artifacts
        if fresh and run_id:
            store.reset_fresh_artifacts_for_run(run_id)

        # Run the stage(s)
        manager.process_run(run_id or "")
        
        # Final status check
        record = store.get_run(run_id or "")
        status = str(record.get("status") or "")
        print(f"final_status={status}")
        if record.get("last_error"):
            print(f"last_error={record['last_error']}")
        for line in _final_artifact_lines(record):
            print(line)
        return 0 if status in {"completed", "waiting_review"} else 1
    except Exception as e:
        print(f"Direct execution failed: {e}")
        if run_id:
            store.update_run(
                run_id,
                status="failed",
                note=f"Direct execution failed.",
                last_error=str(e),
                stage_finished_at=utc_now_iso()
            )
        return 1
    finally:
        pass


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the Direct Execution pipeline end-to-end from one terminal."
    )
    parser.add_argument("--config", help="Optional config file to copy into the run folder.", default=None)
    parser.add_argument("--run-id", help="Optional explicit run id.", default=None)
    parser.add_argument("--root", help="Optional pipeline root override.", default=None)
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument("--fresh", dest="fresh", action="store_true", help="Create a new LinkedIn-first run and clear stale shared artifacts.")
    mode_group.add_argument("--resume", dest="fresh", action="store_false", help="Resume or attach to an existing recoverable run instead of starting fresh.")
    parser.set_defaults(fresh=False)
    args = parser.parse_args()
    raise SystemExit(asyncio.run(run_once(config_path=args.config, run_id=args.run_id, root=args.root, fresh=args.fresh)))


if __name__ == "__main__":
    main()

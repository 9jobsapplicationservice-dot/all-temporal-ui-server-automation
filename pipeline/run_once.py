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
from .start_workflow import start_temporal_workflow
from .storage import PipelineStore
from .utils import utc_now_iso
from .temporal_config import find_temporal_cli, get_temporal_workflow_status, temporal_server_is_reachable
from .temporal_types import TemporalStartResult

TERMINAL_STATUSES = {"completed", "failed", "waiting_login", "waiting_review", "blocked_runtime"}
TEMPORAL_UI_URL = "http://localhost:8233"
FRESH_REUSE_WINDOW_SECONDS = 120.0


def _runner_log_dir(root: str | None) -> Path:
    store = PipelineStore(root)
    log_dir = store.paths.logs_root / "launcher"
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir


def _open_log(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    return path.open("w", encoding="utf-8")


def _spawn_temporal_server(root: str | None = None) -> subprocess.Popen[str]:
    temporal_cli = find_temporal_cli()
    if not temporal_cli:
        raise RuntimeError(
            "Temporal CLI not found. Install it or set TEMPORAL_CLI_PATH to temporal.exe."
        )

    log_dir = _runner_log_dir(root)
    stdout_handle = _open_log(log_dir / "temporal-server.stdout.log")
    stderr_handle = _open_log(log_dir / "temporal-server.stderr.log")
    process = subprocess.Popen(
        [temporal_cli, "server", "start-dev", "--db-filename", "temporal.db"],
        cwd=str(Path.cwd()),
        stdout=stdout_handle,
        stderr=stderr_handle,
        text=True,
        creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0) if os.name == "nt" else 0,
        start_new_session=os.name != "nt",
    )
    process._stdout_handle = stdout_handle  # type: ignore[attr-defined]
    process._stderr_handle = stderr_handle  # type: ignore[attr-defined]
    return process


def _spawn_worker(root: str | None = None) -> subprocess.Popen[str]:
    log_dir = _runner_log_dir(root)
    stdout_handle = _open_log(log_dir / "temporal-worker.stdout.log")
    stderr_handle = _open_log(log_dir / "temporal-worker.stderr.log")
    process = subprocess.Popen(
        [sys.executable, "-m", "pipeline.temporal_worker"],
        cwd=str(Path.cwd()),
        stdout=stdout_handle,
        stderr=stderr_handle,
        text=True,
        env=os.environ.copy(),
        creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0) if os.name == "nt" else 0,
        start_new_session=os.name != "nt",
    )
    process._stdout_handle = stdout_handle  # type: ignore[attr-defined]
    process._stderr_handle = stderr_handle  # type: ignore[attr-defined]
    return process


def _close_process_logs(process: subprocess.Popen[str]) -> None:
    for attribute_name in ("_stdout_handle", "_stderr_handle"):
        handle = getattr(process, attribute_name, None)
        if handle is not None:
            try:
                handle.close()
            except OSError:
                pass


def _stop_process(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        _close_process_logs(process)
        return
    try:
        if os.name == "nt":
            subprocess.run(
                ["taskkill", "/PID", str(process.pid), "/T", "/F"],
                capture_output=True,
                text=True,
                check=False,
            )
        else:
            process.terminate()
    except OSError:
        pass
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        try:
            process.kill()
        except OSError:
            pass
    _close_process_logs(process)


def _wait_for_temporal_server(
    process: subprocess.Popen[str] | None,
    *,
    timeout_seconds: float = 30.0,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if temporal_server_is_reachable():
            return
        if process is not None and process.poll() is not None:
            raise RuntimeError(
                "Temporal dev server exited before it became ready. "
                "Check pipeline/logs/launcher/temporal-server.stderr.log."
            )
        time.sleep(1)
    raise RuntimeError("Temporal dev server did not become ready on localhost:7233.")


def _wait_for_worker_start(process: subprocess.Popen[str], *, timeout_seconds: float = 10.0) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(
                "Temporal worker exited during startup. "
                "Check pipeline/logs/launcher/temporal-worker.stderr.log."
            )
        time.sleep(0.5)


def _resolve_auto_start(config_path: str | None) -> bool:
    try:
        return load_automation_config(config_path).temporal_auto_start
    except AutomationConfigError:
        raw = os.environ.get("PIPELINE_TEMPORAL_AUTO_START", "").strip().lower()
        if raw in {"0", "false", "no", "off"}:
            return False
        return True


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
    print(f"CONFIG_PATH={config_path or 'default'}", flush=True)

    # Storage Validation
    store = PipelineStore(root)
    data_dir = store.paths.root
    record_path = store.paths.meta_dir / f"{run_id}.json"
    record_exists = record_path.exists()
    print(f"PIPELINE_DATA_DIR={data_dir}", flush=True)
    print(f"RUN_RECORD_PATH={record_path}", flush=True)
    print(f"RUN_RECORD_EXISTS={str(record_exists).lower()}", flush=True)

    owned_server: subprocess.Popen[str] | None = None
    owned_worker: subprocess.Popen[str] | None = None
    auto_start = _resolve_auto_start(config_path)
    effective_fresh = _resolve_effective_fresh_mode(config_path, fresh)

    try:
        if temporal_server_is_reachable():
            print("Reusing Temporal dev server already running on localhost:7233.")
        elif auto_start:
            print("Starting local Temporal dev server...")
            owned_server = _spawn_temporal_server(root)
            _wait_for_temporal_server(owned_server)
            print("Temporal dev server is ready.")
        else:
            raise RuntimeError(
                "Temporal server is not reachable and PIPELINE_TEMPORAL_AUTO_START=false."
            )
        print(f"Temporal UI: {TEMPORAL_UI_URL}")

        print("Starting Temporal worker...")
        owned_worker = _spawn_worker(root)
        _wait_for_worker_start(owned_worker)
        print("Temporal worker is running.")

        print("Starting workflow...")
        result = await _start_or_attach_workflow(
            store=PipelineStore(root),
            run_id=run_id,
            config_path=config_path,
            root=root,
            fresh=effective_fresh,
        )
        print(f"run_id={result.run_id}")
        print(f"workflow_id={result.workflow_id}")
        print(f"task_queue={result.task_queue}")
        print(f"temporal_ui={TEMPORAL_UI_URL}")

        store = PipelineStore(root)
        previous_status: str | None = None
        previous_note: str | None = None
        start_time = time.monotonic()
        queued_timeout = 30.0 # 30 seconds to start or we fallback/fail
        
        while True:
            record = store.get_run(result.run_id)
            status = str(record.get("status") or "")
            _print_status(record, previous_status, previous_note)
            previous_status = status
            previous_note = str(record.get("note") or "")
            
            if status in TERMINAL_STATUSES:
                print(f"final_status={status}")
                if record.get("last_error"):
                    print(f"last_error={record['last_error']}")
                for line in _final_artifact_lines(record):
                    print(line)
                return 0 if status in {"completed", "waiting_review"} else 1
            
            # Check for stuck queued state
            if status == "queued" and (time.monotonic() - start_time) > queued_timeout:
                print(f"Workflow stuck in queued state for >{queued_timeout}s. Falling back to direct execution...")
                # Fallback: Run stage manager directly
                try:
                    from .stage_manager import PipelineStageManager
                    manager = PipelineStageManager(store)
                    manager.process_run(result.run_id)
                    # Reset start time to allow the direct execution to proceed without immediately timing out again
                    start_time = time.monotonic()
                except Exception as e:
                    print(f"Direct execution fallback failed: {e}")
                    store.update_run(
                        result.run_id,
                        status="failed",
                        note=f"Workflow stuck in queued state and direct fallback failed.",
                        last_error=str(e),
                        stage_finished_at=utc_now_iso()
                    )
                    return 1

            if owned_worker is not None and owned_worker.poll() is not None:
                # If the worker we started died, we should probably fallback to direct execution too
                print("Local Temporal worker died. Attempting direct execution...")
                try:
                    from .stage_manager import PipelineStageManager
                    manager = PipelineStageManager(store)
                    manager.process_run(result.run_id)
                except Exception as e:
                    print(f"Direct execution fallback (after worker death) failed: {e}")
                    return 1

            await asyncio.sleep(2)
    finally:
        if owned_worker is not None:
            _stop_process(owned_worker)
        if owned_server is not None:
            _close_process_logs(owned_server)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the Temporal-backed pipeline end-to-end from one terminal."
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

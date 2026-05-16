from __future__ import annotations

import argparse
import asyncio
import os
import subprocess
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

from .config import AutomationConfigError, load_automation_config
from .storage import PipelineStore
from .utils import utc_now_iso

TERMINAL_STATUSES = {"completed", "failed", "waiting_login", "waiting_review", "blocked_runtime"}


def _final_artifact_lines(record: dict) -> list[str]:
    return [
        f"manifest: {record['manifest_path']}",
        f"logs: {record['log_dir']}",
        f"applied_csv: {record['applied_csv_path']}",
        f"recruiters_csv: {record['recruiters_csv_path']}",
        f"send_report: {record['send_report_path']}",
    ]


async def run_once(
    *,
    config_path: str | None = None,
    run_id: str | None = None,
    root: str | None = None,
    fresh: bool = False,
) -> int:
    print("DEBUG: Entered run_once function", flush=True)
    try:
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
        print(f"final_status={status}", flush=True)
        if record.get("last_error"):
            print(f"last_error={record['last_error']}", flush=True)
        for line in _final_artifact_lines(record):
            print(line, flush=True)
        return 0 if status in {"completed", "waiting_review"} else 1
    except Exception as e:
        print(f"CRITICAL_ERROR: Pipeline execution failed: {e}", flush=True)
        traceback.print_exc()
        if run_id:
            try:
                # Re-initialize store in case it was the source of failure
                store = PipelineStore(root)
                store.update_run(
                    run_id,
                    status="failed",
                    note="Direct execution failed during initialization.",
                    last_error=str(e),
                    stage_finished_at=utc_now_iso()
                )
            except:
                pass
        return 1


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
    
    try:
        exit_code = asyncio.run(run_once(config_path=args.config, run_id=args.run_id, root=args.root, fresh=args.fresh))
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n[Pipeline] Interrupted by user.")
        sys.exit(130)
    except Exception as e:
        print(f"\n[Pipeline] Fatal error: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

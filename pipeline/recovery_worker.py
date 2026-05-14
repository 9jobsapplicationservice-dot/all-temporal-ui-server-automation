from __future__ import annotations

import argparse
import asyncio
import time

from .config import load_automation_config
from .start_workflow import start_temporal_workflow
from .storage import PipelineStore
from .temporal_config import get_temporal_workflow_status
from .utils import utc_now_iso


TRANSIENT_FAILURE_MARKERS = (
    "timeout",
    "temporarily",
    "quota",
    "rate limit",
    "connection",
    "503",
    "504",
    "429",
    "retryable",
)


def _is_retry_eligible(record: dict) -> bool:
    if str(record.get("status") or "") != "failed":
        return False
    note = str(record.get("note") or "").lower()
    last_error = str(record.get("last_error") or "").lower()
    return any(marker in note or marker in last_error for marker in TRANSIENT_FAILURE_MARKERS)


async def recover_failed_workflows(root: str | None = None) -> int:
    store = PipelineStore(root)
    rerun_count = 0
    for record in store.list_runs():
        if not _is_retry_eligible(record):
            continue
        config = load_automation_config(record.get("config_path") or None)
        max_reruns = int(config.workflow_max_reruns or 0)
        current_reruns = int(record.get("workflow_retry_count", 0) or 0)
        if current_reruns >= max_reruns:
            continue
        workflow_id = str(record.get("temporal_workflow_id") or "").strip()
        workflow_status = await get_temporal_workflow_status(workflow_id) if workflow_id else None
        if workflow_status == "running":
            continue
        reason = f"Auto-rerun after failed workflow ({workflow_status or 'closed/unavailable'}) due to retry-eligible error."
        store.update_run(
            record["id"],
            status="queued",
            workflow_retry_count=current_reruns + 1,
            last_workflow_rerun_reason=reason,
            note=reason,
            last_error="",
            stage_finished_at=utc_now_iso(),
        )
        await start_temporal_workflow(
            run_id=record["id"],
            config_path=record.get("config_path") or None,
            root=root,
            fresh=False,
            force_restart=True,
        )
        rerun_count += 1
    return rerun_count


async def _main_async(args: argparse.Namespace) -> int:
    if args.watch:
        while True:
            rerun_count = await recover_failed_workflows(args.root)
            if rerun_count:
                print(f"Auto-reran {rerun_count} failed workflow(s).")
            time.sleep(args.poll_interval)
    rerun_count = await recover_failed_workflows(args.root)
    print(f"Auto-reran {rerun_count} failed workflow(s).")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Detect and auto-rerun retry-eligible failed Temporal workflows.")
    parser.add_argument("--root", help="Optional pipeline root override.", default=None)
    parser.add_argument("--watch", action="store_true", help="Keep polling for retry-eligible failed workflows.")
    parser.add_argument("--poll-interval", type=float, default=15.0)
    raise SystemExit(asyncio.run(_main_async(parser.parse_args())))


if __name__ == "__main__":
    main()

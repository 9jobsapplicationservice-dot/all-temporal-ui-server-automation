from __future__ import annotations

import argparse
import asyncio
import json
import time

from .storage import PipelineStore
from .temporal_config import connect_temporal_client, get_temporal_workflow_status, temporal_task_queue, terminate_temporal_workflow
from .temporal_types import TemporalStartResult, TemporalWorkflowInput
from .temporal_workflow import AutomationPipelineWorkflow
from .utils import csv_has_expected_header, csv_row_count, recruiter_csv_is_placeholder, recruiter_sendable_row_count, utc_now_iso
from .constants import APPLIED_JOBS_HEADERS, ENRICHED_RECRUITER_HEADERS


def ensure_temporal_run(store: PipelineStore, run_id: str | None, config_path: str | None, *, allow_active_conflict: bool = False) -> dict:
    if run_id:
        try:
            return store.get_run(run_id)
        except KeyError:
            pass
    return store.create_run(run_id=run_id, config_path=config_path, allow_active_conflict=allow_active_conflict)


def _base_workflow_id(run_id: str) -> str:
    return f"pipeline-{run_id}"


def _restart_workflow_id(run_id: str) -> str:
    return f"{_base_workflow_id(run_id)}-restart-{int(time.time())}"


def _prepare_run_for_restart(store: PipelineStore, record: dict) -> dict:
    recruiters_csv_path = record["recruiters_csv_path"]
    applied_csv_path = record["applied_csv_path"]

    if csv_has_expected_header(recruiters_csv_path, ENRICHED_RECRUITER_HEADERS):
        if recruiter_csv_is_placeholder(recruiters_csv_path):
            return store.update_run(
                record["id"],
                status="queued",
                note="Stale closed workflow detected. LinkedIn already completed; restarting this run from RocketReach without reopening Chrome.",
                last_error="",
            )
        sendable_rows = recruiter_sendable_row_count(recruiters_csv_path)
        if sendable_rows > 0:
            return store.update_run(
                record["id"],
                status="queued",
                note="Stale closed workflow detected. Recruiter enrichment already exists; restarting this run from email/review without reopening Chrome.",
                last_error="",
            )
        return store.update_run(
            record["id"],
            status="queued",
            note="Stale closed workflow detected. Recruiter enrichment already exists with no sendable contacts; workflow will finalize without reopening Chrome.",
            last_error="",
        )

    if csv_has_expected_header(applied_csv_path, APPLIED_JOBS_HEADERS):
        applied_rows = csv_row_count(applied_csv_path)
        if applied_rows > 0:
            return store.update_run(
                record["id"],
                status="queued",
                note=(
                    "Stale closed workflow detected. "
                    f"LinkedIn already completed with {applied_rows} applied row(s); restarting this run from RocketReach without reopening Chrome."
                ),
                last_error="",
            )

    return store.update_run(
        record["id"],
        status="queued",
        note="Stale closed workflow detected. Restarting this run from LinkedIn; Chrome will open again.",
        last_error="",
    )


def _prepare_run_for_fresh_restart(store: PipelineStore, record: dict) -> dict:
    store.reset_fresh_artifacts_for_run(record["id"])
    return store.update_run(
        record["id"],
        status="queued",
        note="Fresh run starting LinkedIn in Chrome. Old shared artifacts were cleared for this run.",
        last_error="",
        stage_started_at="",
        stage_finished_at="",
        retry_count=0,
        email_total=0,
        email_sent=0,
        email_failed=0,
        provider_success_count=0,
        no_email_count=0,
        provider_retry_count=0,
    )


async def _clear_conflicting_fresh_runs(
    store: PipelineStore,
    *,
    keep_run_id: str | None = None,
) -> list[dict]:
    conflicting_runs = store.list_active_live_runs(exclude_run_id=keep_run_id)
    if not conflicting_runs:
        return []

    client = await connect_temporal_client()
    cleared_runs: list[dict] = []
    try:
        for record in conflicting_runs:
            workflow_id = str(record.get("temporal_workflow_id") or "").strip()
            workflow_status = await get_temporal_workflow_status(workflow_id, client=client) if workflow_id else None
            terminated = False
            if workflow_status == "running" and workflow_id:
                terminated = await terminate_temporal_workflow(
                    workflow_id,
                    reason="Superseded by a newer fresh LinkedIn-first run.",
                    client=client,
                )
                if terminated:
                    workflow_status = "terminated"

            status_label = workflow_status or "closed/unavailable"
            note = (
                "Superseded by a newer fresh run. "
                f"Previous workflow status: {status_label}."
            )
            if terminated:
                note = (
                    "Superseded by a newer fresh run after terminating the previous Temporal workflow. "
                    f"Previous workflow status: {status_label}."
                )
            cleared_runs.append(
                store.update_run(
                    record["id"],
                    status="terminated",
                    note=note,
                    last_error="A newer fresh LinkedIn-first run replaced this shared-folder run.",
                    stage_finished_at=utc_now_iso(),
                )
            )
    finally:
        if hasattr(client, "close"):
            close_result = client.close()
            if hasattr(close_result, "__await__"):
                await close_result

    return cleared_runs


async def start_temporal_workflow(
    *,
    run_id: str | None = None,
    config_path: str | None = None,
    root: str | None = None,
    fresh: bool = False,
    force_restart: bool = False,
) -> TemporalStartResult:
    store = PipelineStore(root)
    if not fresh and not run_id:
        active_record = store.get_active_live_run()
        if active_record is not None:
            run_id = str(active_record["id"])
    if fresh:
        await _clear_conflicting_fresh_runs(store, keep_run_id=run_id)
    record = ensure_temporal_run(store, run_id, config_path, allow_active_conflict=fresh)
    queue = temporal_task_queue()
    workflow_id = _base_workflow_id(record["id"])
    client = await connect_temporal_client()

    if fresh:
        record = _prepare_run_for_fresh_restart(store, record)

    existing_workflow_id = str(record.get("temporal_workflow_id") or "").strip()
    existing_workflow_status = None
    if existing_workflow_id:
        existing_workflow_status = await get_temporal_workflow_status(existing_workflow_id, client=client)
        if existing_workflow_status == "running" and not fresh and not force_restart:
            return TemporalStartResult(run_id=record["id"], workflow_id=existing_workflow_id, task_queue=queue)
        if not fresh:
            record = _prepare_run_for_restart(store, record)
        workflow_id = _restart_workflow_id(record["id"])
    elif fresh:
        record = store.update_run(
            record["id"],
            note=record.get("note") or "Fresh run starting LinkedIn in Chrome.",
            last_error="",
            retry_count=0,
            email_total=0,
            email_sent=0,
            email_failed=0,
            provider_success_count=0,
            no_email_count=0,
            provider_retry_count=0,
            stage_started_at="",
            stage_finished_at="",
        )

    store.update_run(
        record["id"],
        temporal_workflow_id=workflow_id,
        temporal_task_queue=queue,
        orchestration_backend="temporal",
        note=record.get("note") or "Temporal workflow queued.",
    )

    await client.start_workflow(
        AutomationPipelineWorkflow.run,
        TemporalWorkflowInput(
            run_id=record["id"],
            config_path=record.get("config_path") or config_path,
            root=root,
            fresh=fresh,
        ),
        id=workflow_id,
        task_queue=queue,
    )
    return TemporalStartResult(run_id=record["id"], workflow_id=workflow_id, task_queue=queue)


async def _main_async(args: argparse.Namespace) -> int:
    result = await start_temporal_workflow(run_id=args.run_id, config_path=args.config, root=args.root, fresh=args.fresh)
    print(json.dumps(result.to_dict(), indent=2))
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Start a Temporal-backed automation workflow.")
    parser.add_argument("--config", help="Optional config file to copy into the run folder.", default=None)
    parser.add_argument("--run-id", help="Optional explicit run id.", default=None)
    parser.add_argument("--root", help="Optional pipeline root override.", default=None)
    parser.add_argument("--fresh", action="store_true", help="Force a new LinkedIn-first run.")
    args = parser.parse_args()
    raise SystemExit(asyncio.run(_main_async(args)))


if __name__ == "__main__":
    main()

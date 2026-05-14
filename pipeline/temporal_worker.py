from __future__ import annotations

import argparse
import asyncio
from concurrent.futures import ThreadPoolExecutor

from .core.sentry_config import init_sentry
from .temporal_activities import email_activity, linkedin_activity, rocketreach_activity
from .temporal_config import (
    connect_temporal_client,
    find_temporal_cli,
    temporal_address,
    temporal_task_queue,
    temporal_server_is_reachable,
)
from .temporal_sdk import Worker
from .temporal_interceptors import SentryTemporalInterceptor
from .temporal_workflow import AutomationPipelineWorkflow


async def run_temporal_worker() -> None:
    if not temporal_server_is_reachable():
        address = temporal_address()
        cli_hint = (
            "Run `temporal server start-dev --db-filename temporal.db` in another terminal first."
            if find_temporal_cli()
            else "Start the server with `python -m pipeline.run_once --config pipeline/automation.env`, which can auto-start the local Temporal dev server."
        )
        raise RuntimeError(
            f"Temporal server is not reachable at {address}. "
            f"{cli_hint}"
        )
    init_sentry()
    client = await connect_temporal_client()
    activity_executor = ThreadPoolExecutor(max_workers=3)
    worker_kwargs = {
        "task_queue": temporal_task_queue(),
        "workflows": [AutomationPipelineWorkflow],
        "activities": [linkedin_activity, rocketreach_activity, email_activity],
        "activity_executor": activity_executor,
        "interceptors": [SentryTemporalInterceptor()],
    }

    worker = Worker(
        client,
        **worker_kwargs,
    )
    try:
        await worker.run()
    finally:
        activity_executor.shutdown(wait=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Temporal worker for the automation pipeline.")
    parser.parse_args()
    asyncio.run(run_temporal_worker())


if __name__ == "__main__":
    main()

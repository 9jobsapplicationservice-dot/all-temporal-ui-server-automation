from __future__ import annotations

import argparse
import time

from .storage import PipelineStore
from .worker import PipelineWorker


TERMINAL_STATUSES = {"completed", "failed", "blocked_runtime", "waiting_login", "waiting_review", "manual_review", "sending"}


def main() -> None:
    parser = argparse.ArgumentParser(description="Process a single pipeline run until it reaches a handoff or terminal state.")
    parser.add_argument("--run-id", required=True, help="Pipeline run id to process.")
    parser.add_argument("--root", help="Optional pipeline root override.", default=None)
    parser.add_argument("--poll-interval", type=float, default=2.0)
    args = parser.parse_args()

    worker = PipelineWorker(root=args.root, poll_interval=args.poll_interval)
    store = PipelineStore(args.root)

    while True:
        record = store.get_run(args.run_id)
        if record["status"] in TERMINAL_STATUSES:
            return

        processed = worker.process_available_runs_once()
        record = store.get_run(args.run_id)
        if record["status"] in TERMINAL_STATUSES:
            return
        if processed == 0:
            time.sleep(args.poll_interval)


if __name__ == "__main__":
    main()

from __future__ import annotations

import argparse

from .storage import PipelineStore


def main() -> None:
    parser = argparse.ArgumentParser(description="Retry a recoverable pipeline run.")
    parser.add_argument("--run-id", required=True, help="Pipeline run id.")
    parser.add_argument("--root", help="Optional pipeline root override.", default=None)
    args = parser.parse_args()

    store = PipelineStore(args.root)
    record = store.get_run(args.run_id)
    if record["status"] != "waiting_login":
        raise SystemExit(f"Run {args.run_id} is not eligible for retry from status {record['status']}.")

    updated = store.update_run(
        args.run_id,
        status="queued",
        note="Retry requested from dashboard. Reopening LinkedIn stage.",
        last_error="",
        stage_finished_at="",
    )
    print(f"{updated['id']} -> {updated['status']}")


if __name__ == "__main__":
    main()

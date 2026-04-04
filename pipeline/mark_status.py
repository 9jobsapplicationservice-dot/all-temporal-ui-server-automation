from __future__ import annotations

import argparse

from .storage import PipelineStore
from .utils import utc_now_iso


def main() -> None:
    parser = argparse.ArgumentParser(description="Update pipeline run status from external tooling.")
    parser.add_argument("--run-id", required=True, help="Pipeline run id.")
    parser.add_argument("--status", required=True, choices=["waiting_review", "sending", "completed", "failed"])
    parser.add_argument("--note", default="", help="Optional status note.")
    parser.add_argument("--root", help="Optional pipeline root override.", default=None)
    args = parser.parse_args()

    store = PipelineStore(args.root)
    changes = {
        "status": args.status,
        "note": args.note,
    }
    if args.status == "sending":
        changes["stage_started_at"] = utc_now_iso()
    else:
        changes["stage_finished_at"] = utc_now_iso()

    record = store.update_run(args.run_id, **changes)
    print(f"{record['id']} -> {record['status']}")


if __name__ == "__main__":
    main()

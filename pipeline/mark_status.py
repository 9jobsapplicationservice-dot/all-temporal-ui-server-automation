from __future__ import annotations

import argparse

from .storage import PipelineStore
from .utils import utc_now_iso


def main() -> None:
    parser = argparse.ArgumentParser(description="Update pipeline run status from external tooling.")
    parser.add_argument("--run-id", required=True, help="Pipeline run id.")
    parser.add_argument("--status", required=True, choices=["queued", "running", "waiting_review", "sending", "email_running", "completed", "failed"])
    parser.add_argument("--note", default="", help="Optional status note.")
    parser.add_argument("--root", help="Optional pipeline root override.", default=None)
    parser.add_argument("--email-total", type=int, default=None)
    parser.add_argument("--email-sent", type=int, default=None)
    parser.add_argument("--email-failed", type=int, default=None)
    args = parser.parse_args()

    store = PipelineStore(args.root)
    changes = {
        "status": args.status,
        "note": args.note,
    }
    if args.email_total is not None:
        changes["email_total"] = args.email_total
    if args.email_sent is not None:
        changes["email_sent"] = args.email_sent
    if args.email_failed is not None:
        changes["email_failed"] = args.email_failed
    if args.status in {"sending", "email_running"}:
        changes["stage_started_at"] = utc_now_iso()
    else:
        changes["stage_finished_at"] = utc_now_iso()

    record = store.update_run(args.run_id, **changes)
    print(f"{record['id']} -> {record['status']}")


if __name__ == "__main__":
    main()

from __future__ import annotations

import argparse

from .storage import PipelineStore


def main() -> None:
    parser = argparse.ArgumentParser(description="Show local pipeline run statuses.")
    parser.add_argument("--root", help="Optional pipeline root override.", default=None)
    parser.add_argument("--limit", type=int, default=20, help="Maximum runs to display.")
    args = parser.parse_args()

    store = PipelineStore(args.root)
    records = store.list_runs(limit=args.limit)

    if not records:
        print("No pipeline runs found.")
        return

    print("RUN ID           STATUS              RETRIES  UPDATED AT                  NOTE")
    print("-" * 100)
    for record in records:
        print(
            f"{record['id'][:16]:16} "
            f"{record['status'][:18]:18} "
            f"{record['retry_count']:<8} "
            f"{record['updated_at'][:26]:26} "
            f"{(record.get('note') or '')[:40]}"
        )


if __name__ == "__main__":
    main()

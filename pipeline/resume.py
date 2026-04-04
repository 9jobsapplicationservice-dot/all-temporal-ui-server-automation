from __future__ import annotations

import argparse

from .worker import PipelineWorker


def main() -> None:
    parser = argparse.ArgumentParser(description="Recover interrupted runs and resume queued work.")
    parser.add_argument("--root", help="Optional pipeline root override.", default=None)
    parser.add_argument("--watch", action="store_true", help="Keep polling for new runs after recovery.")
    args = parser.parse_args()

    worker = PipelineWorker(root=args.root)
    recovered = worker.recover()
    if recovered:
        print(f"Recovered {len(recovered)} interrupted run(s).")
    else:
        print("No interrupted runs needed recovery.")

    if args.watch:
        worker.run_forever()
        return

    processed = worker.process_available_runs_once()
    print(f"Processed {processed} queued run(s).")


if __name__ == "__main__":
    main()

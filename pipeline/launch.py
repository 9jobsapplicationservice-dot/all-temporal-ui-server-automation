from __future__ import annotations

import argparse
import os
import subprocess
import time
from pathlib import Path

from .config import AutomationConfigError, load_automation_config
from .storage import PipelineStore
from .worker import PipelineWorker


WORKSPACE_ROOT = Path(__file__).resolve().parent.parent
EMAIL_APP_ROOT = WORKSPACE_ROOT / "sendemailwith-code" / "email-automation-nodejs"


def npm_command() -> list[str]:
    return ["npm.cmd", "run", "dev"] if os.name == "nt" else ["npm", "run", "dev"]


def main() -> None:
    parser = argparse.ArgumentParser(description="Launch the local pipeline worker or run a single end-to-end automated pipeline.")
    parser.add_argument("--root", help="Optional pipeline root override.", default=None)
    parser.add_argument("--poll-interval", type=float, default=5.0)
    parser.add_argument("--no-ui", action="store_true", help="Skip launching the Next.js email review UI.")
    parser.add_argument("--auto-run", action="store_true", help="Enqueue one run and wait for it to finish automatically.")
    parser.add_argument("--config", help="Automation config file (.env/.json) for auto-send and optional pipeline settings.", default=None)
    parser.add_argument("--run-id", help="Optional explicit run id for --auto-run.", default=None)
    args = parser.parse_args()

    ui_process: subprocess.Popen[str] | None = None
    worker = PipelineWorker(root=args.root, poll_interval=args.poll_interval)
    recovered = worker.recover()

    if recovered:
        print(f"Recovered {len(recovered)} interrupted run(s).")

    if args.auto_run:
        try:
            config = load_automation_config(args.config)
            print(f"Loaded automation config from {config.source}. auto_send={config.auto_send}")
        except AutomationConfigError as error:
            raise SystemExit(f"Invalid automation config: {error}") from error

    if not args.auto_run and not args.no_ui and EMAIL_APP_ROOT.exists():
        print(f"Starting email review UI in {EMAIL_APP_ROOT} ...")
        ui_process = subprocess.Popen(
            npm_command(),
            cwd=EMAIL_APP_ROOT,
            text=True,
        )

    if args.auto_run:
        store = PipelineStore(args.root)
        try:
            record = store.create_run(run_id=args.run_id, config_path=args.config)
        except RuntimeError as error:
            raise SystemExit(str(error)) from error
        print(f"Enqueued run {record['id']}. Starting end-to-end automation...")
        terminal_statuses = {"completed", "failed", "blocked_runtime", "waiting_review"}
        while True:
            processed = worker.process_available_runs_once()
            current = store.get_run(record["id"])
            print(f"[{current['id']}] status={current['status']} note={current.get('note') or ''}")
            if current["status"] in terminal_statuses:
                if current["status"] == "waiting_review":
                    print("Automatic sending is disabled for this run; manual review is still required.")
                break
            if processed == 0:
                time.sleep(args.poll_interval)
        raise SystemExit(0 if current["status"] == "completed" else 1)

    print("Pipeline worker running. Enqueue a run with: python -m pipeline.enqueue")
    try:
        worker.run_forever()
    except KeyboardInterrupt:
        print("\nStopping pipeline launcher...")
    finally:
        if ui_process is not None and ui_process.poll() is None:
            ui_process.terminate()
            try:
                ui_process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                ui_process.kill()


if __name__ == "__main__":
    main()

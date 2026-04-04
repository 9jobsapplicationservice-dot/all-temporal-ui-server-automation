from __future__ import annotations

import argparse
import os
import subprocess
from pathlib import Path

from .worker import PipelineWorker


WORKSPACE_ROOT = Path(__file__).resolve().parent.parent
EMAIL_APP_ROOT = WORKSPACE_ROOT / "sendeamilwith code" / "email-automation-nodejs"


def npm_command() -> list[str]:
    return ["npm.cmd", "run", "dev"] if os.name == "nt" else ["npm", "run", "dev"]


def main() -> None:
    parser = argparse.ArgumentParser(description="Launch the local queue pipeline worker and email review UI.")
    parser.add_argument("--root", help="Optional pipeline root override.", default=None)
    parser.add_argument("--poll-interval", type=float, default=5.0)
    parser.add_argument("--no-ui", action="store_true", help="Skip launching the Next.js email review UI.")
    args = parser.parse_args()

    ui_process: subprocess.Popen[str] | None = None
    worker = PipelineWorker(root=args.root, poll_interval=args.poll_interval)
    recovered = worker.recover()

    if recovered:
        print(f"Recovered {len(recovered)} interrupted run(s).")

    if not args.no_ui and EMAIL_APP_ROOT.exists():
        print(f"Starting email review UI in {EMAIL_APP_ROOT} ...")
        ui_process = subprocess.Popen(
            npm_command(),
            cwd=EMAIL_APP_ROOT,
            text=True,
        )

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

from __future__ import annotations

import argparse
import json

from .storage import PipelineStore


def main() -> None:
    parser = argparse.ArgumentParser(description="Enqueue a new local pipeline run.")
    parser.add_argument("--config", help="Optional config file to copy into the run folder.", default=None)
    parser.add_argument("--run-id", help="Optional explicit run id.", default=None)
    parser.add_argument("--root", help="Optional pipeline root override.", default=None)
    args = parser.parse_args()

    store = PipelineStore(args.root)
    try:
        record = store.create_run(run_id=args.run_id, config_path=args.config)
    except RuntimeError as error:
        print(json.dumps({"error": str(error)}, indent=2))
        raise SystemExit(1) from error
    print(json.dumps(record, indent=2))


if __name__ == "__main__":
    main()

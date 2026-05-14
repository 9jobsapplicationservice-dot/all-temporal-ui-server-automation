from __future__ import annotations

import json
import os
import sys
import traceback


if __name__ == "__main__":
    run_id = os.environ.get("PIPELINE_RUN_ID", "").strip() or "manual"
    output_dir = os.environ.get("PIPELINE_OUTPUT_DIR", "").strip() or os.getcwd()
    print(f"[pipeline_entry] run_id={run_id} output_dir={output_dir}")
    try:
        from runAiBot import main

        result = main()
    except Exception as error:
        print(f"[pipeline_entry] startup failure: {error}", file=sys.stderr)
        traceback.print_exc()
        raise SystemExit(1) from error

    payload = {
        "run_id": run_id,
        "output_dir": output_dir,
        **result,
    }
    print(json.dumps(payload, ensure_ascii=True))
    raise SystemExit(int(payload.get("exit_code", 1)))

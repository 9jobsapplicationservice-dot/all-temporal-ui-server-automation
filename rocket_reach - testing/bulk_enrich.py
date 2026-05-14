from __future__ import annotations

import argparse
import json

from rocketreach_bulk import bulk_enrich


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run RocketReach bulk enrichment from a CSV file.')
    parser.add_argument('--input', required=True, help='Input CSV path.')
    parser.add_argument('--output', required=True, help='Output CSV path.')
    args = parser.parse_args()

    stats = bulk_enrich(args.input, args.output)
    print(json.dumps(stats, indent=2))

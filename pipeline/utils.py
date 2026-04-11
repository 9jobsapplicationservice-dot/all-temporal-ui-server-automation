from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def csv_has_expected_header(csv_path: str | Path, expected_header: list[str]) -> bool:
    path = Path(csv_path)
    if not path.exists():
        return False

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        header = next(reader, None)
    return list(header or []) == list(expected_header)


def read_log_tail(log_path: str | Path, line_count: int = 20) -> str:
    path = Path(log_path)
    if not path.exists():
        return ""

    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    if not lines:
        return ""
    return "\n".join(lines[-line_count:])



def csv_row_count(csv_path: str | Path) -> int:
    path = Path(csv_path)
    if not path.exists():
        return 0

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        next(reader, None)
        return sum(1 for row in reader if any(cell.strip() for cell in row))



def recruiter_sendable_row_count(csv_path: str | Path) -> int:
    path = Path(csv_path)
    if not path.exists():
        return 0

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        sendable_rows = 0
        for row in reader:
            if not any(str(value or "").strip() for value in row.values()):
                continue
            primary = (row.get("HR Email") or "").strip()
            secondary = (row.get("HR Secondary Email") or "").strip()
            if "@" in primary or "@" in secondary:
                sendable_rows += 1
        return sendable_rows


def recruiter_csv_is_placeholder(csv_path: str | Path) -> bool:
    path = Path(csv_path)
    if not path.exists():
        return False

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        saw_row = False
        for row in reader:
            if not any(str(value or "").strip() for value in row.values()):
                continue
            saw_row = True
            status = (row.get("RocketReach Status") or "").strip().lower()
            if status != "pending_enrichment":
                return False
        return saw_row


def ensure_placeholder_recruiter_csv(
    applied_csv_path: str | Path,
    recruiters_csv_path: str | Path,
    enriched_header: list[str],
    status: str = "pending_enrichment",
) -> int:
    applied_path = Path(applied_csv_path)
    recruiters_path = Path(recruiters_csv_path)
    if not applied_path.exists():
        return 0

    recruiters_path.parent.mkdir(parents=True, exist_ok=True)
    row_count = 0
    with applied_path.open("r", encoding="utf-8-sig", newline="") as source, recruiters_path.open(
        "w",
        encoding="utf-8-sig",
        newline="",
    ) as target:
        reader = csv.DictReader(source)
        writer = csv.DictWriter(target, fieldnames=enriched_header)
        writer.writeheader()
        for row in reader:
            clean_row = {
                "Date": (row.get("Date") or "").strip(),
                "Company Name": (row.get("Company Name") or "").strip(),
                "Position": (row.get("Position") or "").strip(),
                "Job Link": (row.get("Job Link") or "").strip(),
                "Submitted": (row.get("Submitted") or "").strip(),
                "HR Name": (row.get("HR Name") or "").strip(),
                "HR Position": (row.get("HR Position") or "").strip(),
                "HR Profile Link": (row.get("HR Profile Link") or "").strip(),
                "HR Email": "",
                "HR Secondary Email": "",
                "HR Email Preview": "",
                "HR Contact": "",
                "HR Contact Preview": "",
                "RocketReach Status": status,
            }
            writer.writerow(clean_row)
            row_count += 1
    return row_count


def read_last_json_object(log_path: str | Path) -> dict:
    path = Path(log_path)
    if not path.exists():
        return {}

    text = path.read_text(encoding="utf-8", errors="replace").strip()
    if not text:
        return {}

    decoder = json.JSONDecoder()
    last_payload: dict = {}
    for index, char in enumerate(text):
        if char != '{':
            continue
        try:
            payload, end_index = decoder.raw_decode(text[index:])
        except json.JSONDecodeError:
            continue
        remainder = text[index + end_index:].strip()
        if isinstance(payload, dict) and not remainder:
            last_payload = payload
        elif isinstance(payload, dict):
            last_payload = payload
    return last_payload

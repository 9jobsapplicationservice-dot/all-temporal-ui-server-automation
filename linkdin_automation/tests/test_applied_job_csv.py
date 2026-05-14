import ast
import csv
import os
import re
import shutil
import unittest
import uuid
from datetime import datetime
from pathlib import Path
from typing import Literal


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_PATH = PROJECT_ROOT / "runAiBot.py"
TMP_ROOT = PROJECT_ROOT / ".tmp-tests"


def truncate_for_csv(value, max_length: int = 131000, suffix: str = "...[TRUNCATED]"):
    text = str(value)
    if len(text) <= max_length:
        return text
    return text[: max_length - len(suffix)] + suffix


def load_target_namespace():
    source = SOURCE_PATH.read_text(encoding="utf-8-sig")
    tree = ast.parse(source, filename=str(SOURCE_PATH))
    namespace = {
        "csv": csv,
        "datetime": datetime,
        "Literal": Literal,
        "os": os,
        "re": re,
        "truncate_for_csv": truncate_for_csv,
        "print_lg": lambda *args, **kwargs: None,
        "show_alert": lambda *args, **kwargs: "OK",
        "applied_csv_lock_warned": False,
        "rows_written_to_applied_csv": 0,
        "rows_missing_hr_profile": 0,
        "sync_recruiter_csv_after_application": lambda: None,
        "APPLIED_JOBS_FIELDNAMES": [
            "Date",
            "Company Name",
            "Position",
            "Job Link",
            "Submitted",
            "HR Name",
            "HR Position",
            "HR Profile Link",
        ],
    }

    target_functions = {
        "format_applied_date",
        "clean_csv_text",
        "get_logged_at_value",
        "warn_applied_csv_locked",
        "ensure_applied_jobs_csv_schema",
        "merge_csv_history",
        "merge_applied_job_rows",
        "upsert_applied_job_row",
        "submitted_jobs",
    }

    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name in target_functions:
            module = ast.Module(body=[node], type_ignores=[])
            ast.fix_missing_locations(module)
            exec(compile(module, filename=str(SOURCE_PATH), mode="exec"), namespace)

    return namespace


class AppliedJobCsvTests(unittest.TestCase):
    def setUp(self) -> None:
        self.namespace = load_target_namespace()
        TMP_ROOT.mkdir(parents=True, exist_ok=True)
        self.temp_dir = TMP_ROOT / f"applied-job-csv-{uuid.uuid4().hex}"
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self.addCleanup(lambda: shutil.rmtree(self.temp_dir, ignore_errors=True))
        self.applied_csv = self.temp_dir / "applied_jobs_history.csv"
        self.namespace["file_name"] = str(self.applied_csv)

    def test_submitted_jobs_creates_row(self):
        saved = self.namespace["submitted_jobs"](
            "123",
            "Business Analyst",
            "TCS",
            "Perth",
            "On-site",
            "desc",
            3,
            ["Python"],
            "Jane Recruiter",
            "https://www.linkedin.com/in/jane",
            "Recruiter",
            "resume.pdf",
            False,
            datetime(2026, 4, 12),
            datetime(2026, 4, 12),
            "https://www.linkedin.com/jobs/view/123",
            "Easy Applied",
            set(),
            "In Development",
        )

        self.assertTrue(saved)
        with self.applied_csv.open("r", encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["Date"], "12/04/2026")
        self.assertEqual(rows[0]["Submitted"], "Applied")

    def test_submitted_jobs_updates_same_row_with_new_date_history(self):
        job_link = "https://www.linkedin.com/jobs/view/123"

        first_saved = self.namespace["submitted_jobs"](
            "123",
            "Business Analyst",
            "TCS",
            "Perth",
            "On-site",
            "desc",
            3,
            ["Python"],
            "Jane Recruiter",
            "https://www.linkedin.com/in/jane",
            "Recruiter",
            "resume.pdf",
            False,
            datetime(2026, 4, 12),
            datetime(2026, 4, 12),
            job_link,
            "Easy Applied",
            set(),
            "In Development",
        )
        second_saved = self.namespace["submitted_jobs"](
            "123",
            "Business Analyst",
            "TCS",
            "Perth",
            "On-site",
            "desc",
            3,
            ["Python"],
            "Jane Recruiter",
            "https://www.linkedin.com/in/jane",
            "Senior Recruiter",
            "resume.pdf",
            False,
            datetime(2026, 4, 13),
            datetime(2026, 4, 13),
            job_link,
            "Easy Applied",
            set(),
            "In Development",
        )
        third_saved = self.namespace["submitted_jobs"](
            "123",
            "Business Analyst",
            "TCS",
            "Perth",
            "On-site",
            "desc",
            3,
            ["Python"],
            "Jane Recruiter",
            "https://www.linkedin.com/in/jane",
            "Lead Recruiter",
            "resume.pdf",
            False,
            datetime(2026, 4, 14),
            datetime(2026, 4, 14),
            job_link,
            "Easy Applied",
            set(),
            "In Development",
        )

        self.assertTrue(first_saved)
        self.assertTrue(second_saved)
        self.assertTrue(third_saved)

        with self.applied_csv.open("r", encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["Date"], "12/04/2026, 13/04/2026, 14/04/2026")
        self.assertEqual(rows[0]["Submitted"], "Applied")
        self.assertEqual(rows[0]["HR Position"], "Lead Recruiter")


if __name__ == "__main__":
    unittest.main()

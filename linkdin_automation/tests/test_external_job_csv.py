import ast
import csv
import os
import re
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from typing import Literal
from unittest import mock
import shutil


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_PATH = PROJECT_ROOT / "runAiBot.py"


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
        "external_csv_lock_warned": False,
        "rows_written_to_external_csv": 0,
        "EXTERNAL_JOBS_FIELDNAMES": [
            "Date",
            "Company Name",
            "Position",
            "External Job Link",
            "HR Name",
            "HR Profile Link",
        ],
        "logged_external_job_links": set(),
    }

    target_functions = {
        "format_applied_date",
        "clean_csv_text",
        "get_logged_at_value",
        "warn_external_jobs_csv_locked",
        "ensure_external_jobs_csv_schema",
        "external_job_already_logged",
        "log_external_job",
    }

    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name in target_functions:
            module = ast.Module(body=[node], type_ignores=[])
            ast.fix_missing_locations(module)
            exec(compile(module, filename=str(SOURCE_PATH), mode="exec"), namespace)

    return namespace


class ExternalJobCsvTests(unittest.TestCase):
    def setUp(self) -> None:
        self.namespace = load_target_namespace()
        self.temp_dir = Path(tempfile.mkdtemp(dir=PROJECT_ROOT))
        self.addCleanup(lambda: shutil.rmtree(self.temp_dir, ignore_errors=True))
        self.external_csv = self.temp_dir / "external_jobs_history.csv"
        self.namespace["external_jobs_file_name"] = str(self.external_csv)

    def test_ensure_schema_creates_external_csv_with_expected_header(self):
        result = self.namespace["ensure_external_jobs_csv_schema"]()

        self.assertTrue(result)
        self.assertTrue(self.external_csv.exists())
        with self.external_csv.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.reader(handle)
            header = next(reader)
        self.assertEqual(header, self.namespace["EXTERNAL_JOBS_FIELDNAMES"])

    def test_log_external_job_writes_new_row(self):
        logged = self.namespace["log_external_job"](
            "OpenAI",
            "Automation Engineer",
            "https://www.linkedin.com/jobs/view/123456789/",
            "https://jobs.openai.com/apply/automation-engineer",
            "Jane Recruiter",
            "https://www.linkedin.com/in/recruiter-1",
        )

        self.assertTrue(logged)
        self.assertEqual(self.namespace["rows_written_to_external_csv"], 1)
        with self.external_csv.open("r", encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["Company Name"], "OpenAI")
        self.assertEqual(rows[0]["Position"], "Automation Engineer")
        self.assertEqual(rows[0]["External Job Link"], "https://jobs.openai.com/apply/automation-engineer")
        self.assertEqual(rows[0]["HR Name"], "Jane Recruiter")
        self.assertEqual(rows[0]["HR Profile Link"], "https://www.linkedin.com/in/recruiter-1")
        self.assertTrue(rows[0]["Date"])

    def test_log_external_job_skips_duplicate_job_link(self):
        first = self.namespace["log_external_job"](
            "OpenAI",
            "Automation Engineer",
            "https://www.linkedin.com/jobs/view/123456789/",
            "https://jobs.openai.com/apply/automation-engineer",
            "Jane Recruiter",
            "https://www.linkedin.com/in/recruiter-1",
        )
        second = self.namespace["log_external_job"](
            "OpenAI",
            "Automation Engineer",
            "https://www.linkedin.com/jobs/view/123456789/",
            "https://jobs.openai.com/apply/automation-engineer-v2",
            "Jane Recruiter",
            "https://www.linkedin.com/in/recruiter-1",
        )

        self.assertTrue(first)
        self.assertFalse(second)
        self.assertEqual(self.namespace["rows_written_to_external_csv"], 1)
        with self.external_csv.open("r", encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))
        self.assertEqual(len(rows), 1)

    def test_log_external_job_keeps_blank_hr_profile_link(self):
        logged = self.namespace["log_external_job"](
            "OpenAI",
            "Automation Engineer",
            "https://www.linkedin.com/jobs/view/123456789/",
            "https://jobs.openai.com/apply/automation-engineer",
            "",
            "",
        )

        self.assertTrue(logged)
        with self.external_csv.open("r", encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))
        self.assertEqual(rows[0]["HR Profile Link"], "")

    def test_locked_external_csv_warns_once_and_returns_false(self):
        warn = self.namespace["warn_external_jobs_csv_locked"]
        ensure_schema = self.namespace["ensure_external_jobs_csv_schema"]

        with mock.patch.dict(ensure_schema.__globals__, {"print_lg": lambda *args, **kwargs: None}), \
             mock.patch.dict(warn.__globals__, {"print_lg": lambda *args, **kwargs: None}), \
             mock.patch.dict(warn.__globals__, {"show_alert": mock.Mock(return_value="OK")}):
            show_alert = warn.__globals__["show_alert"]
            with mock.patch("builtins.open", side_effect=PermissionError("locked")):
                self.assertFalse(ensure_schema())
                self.assertFalse(ensure_schema())

        self.assertEqual(show_alert.call_count, 1)


if __name__ == "__main__":
    unittest.main()

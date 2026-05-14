import ast
import os
import pathlib
import re
import shutil
import unittest
import uuid
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_PATH = PROJECT_ROOT / "runAiBot.py"
TMP_ROOT = PROJECT_ROOT / ".tmp-tests"


def load_target_namespace():
    source = SOURCE_PATH.read_text(encoding="utf-8-sig")
    tree = ast.parse(source, filename=str(SOURCE_PATH))
    namespace = {
        "os": os,
        "pathlib": pathlib,
        "re": re,
        "pipeline_mode": False,
        "logs_folder_path": "logs/",
        "screenshot_folder_path": "logs/screenshots",
    }

    target_functions = {
        "resolve_screenshot_directory",
        "screenshot_directory_is_run_local",
        "normalize_screenshot_label",
        "sanitize_screenshot_token",
        "next_screenshot_serial",
        "build_serial_screenshot_name",
    }

    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name in target_functions:
            module = ast.Module(body=[node], type_ignores=[])
            ast.fix_missing_locations(module)
            exec(compile(module, filename=str(SOURCE_PATH), mode="exec"), namespace)

    return namespace


class ScreenshotNamingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.namespace = load_target_namespace()
        TMP_ROOT.mkdir(parents=True, exist_ok=True)
        self.temp_dir = TMP_ROOT / f"screenshot-naming-{uuid.uuid4().hex}"
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self.addCleanup(lambda: shutil.rmtree(self.temp_dir, ignore_errors=True))

    def test_build_serial_screenshot_name_starts_with_first_global_counter(self):
        filename = self.namespace["build_serial_screenshot_name"](self.temp_dir, "4398842697", "Before Apply")

        self.assertEqual(filename, "0001_before_apply_4398842697.png")

    def test_build_serial_screenshot_name_increments_from_existing_files(self):
        (self.temp_dir / "0001_before_apply_4398842697.png").write_text("", encoding="utf-8")
        (self.temp_dir / "0002_after_submitted_4398842697.png").write_text("", encoding="utf-8")

        filename = self.namespace["build_serial_screenshot_name"](self.temp_dir, "4397042958", "After Submitted")

        self.assertEqual(filename, "0003_after_submitted_4397042958.png")

    def test_build_serial_screenshot_name_sanitizes_tokens(self):
        filename = self.namespace["build_serial_screenshot_name"](self.temp_dir, "job:123/abc", "Before   Apply!!!")

        self.assertEqual(filename, "0001_before_apply_job_123_abc.png")

    def test_resolve_screenshot_directory_uses_shared_pipeline_dir_from_env(self):
        self.namespace["pipeline_mode"] = True
        self.namespace["logs_folder_path"] = str(self.temp_dir / "logs" / "run-123")
        self.namespace["os"].environ["PIPELINE_SCREENSHOTS_DIR"] = str(self.temp_dir / "logs" / "screenshots")
        self.addCleanup(lambda: self.namespace["os"].environ.pop("PIPELINE_SCREENSHOTS_DIR", None))

        resolved = self.namespace["resolve_screenshot_directory"]()

        self.assertEqual(resolved, self.temp_dir / "logs" / "screenshots")
        self.assertFalse(self.namespace["screenshot_directory_is_run_local"](resolved))

    def test_resolve_screenshot_directory_never_falls_back_to_run_local_folder_in_pipeline_mode(self):
        self.namespace["pipeline_mode"] = True
        self.namespace["logs_folder_path"] = str(self.temp_dir / "logs" / "run-123")
        self.namespace["os"].environ.pop("PIPELINE_SCREENSHOTS_DIR", None)

        resolved = self.namespace["resolve_screenshot_directory"]()

        self.assertEqual(resolved, self.temp_dir / "logs" / "screenshots")
        self.assertFalse(self.namespace["screenshot_directory_is_run_local"](resolved))
        self.assertNotEqual(resolved, self.temp_dir / "logs" / "run-123" / "screenshots")


if __name__ == "__main__":
    unittest.main()

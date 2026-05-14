import importlib
import io
import os
import sys
import unittest
from pathlib import Path
from unittest import mock


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


class HelpersRegressionTests(unittest.TestCase):
    def setUp(self) -> None:
        self._previous_pipeline_mode = os.environ.get("PIPELINE_MODE")
        self._previous_pipeline_enable_popups = os.environ.get("PIPELINE_ENABLE_POPUPS")

    def tearDown(self) -> None:
        if self._previous_pipeline_mode is None:
            os.environ.pop("PIPELINE_MODE", None)
        else:
            os.environ["PIPELINE_MODE"] = self._previous_pipeline_mode
        if self._previous_pipeline_enable_popups is None:
            os.environ.pop("PIPELINE_ENABLE_POPUPS", None)
        else:
            os.environ["PIPELINE_ENABLE_POPUPS"] = self._previous_pipeline_enable_popups
        sys.modules.pop("modules.helpers", None)

    def _load_helpers(self, pipeline_mode: str = "1", pipeline_enable_popups: str = "0"):
        os.environ["PIPELINE_MODE"] = pipeline_mode
        os.environ["PIPELINE_ENABLE_POPUPS"] = pipeline_enable_popups
        sys.modules.pop("modules.helpers", None)
        helpers = importlib.import_module("modules.helpers")
        return importlib.reload(helpers)

    def test_show_alert_accepts_exception_objects_in_pipeline_mode(self):
        helpers = self._load_helpers("1")

        result = helpers.show_alert(Exception("boom"), title=ValueError("title"), button="Continue")

        self.assertEqual(result, "Continue")

    def test_show_confirm_uses_deterministic_pipeline_fallback(self):
        helpers = self._load_helpers("1")

        result = helpers.show_confirm(
            Exception("review this"),
            title="Confirm your information",
            buttons=["Disable Pause", "Discard Application", "Submit Application"],
        )

        self.assertEqual(result, "Disable Pause")

    def test_show_alert_uses_pyautogui_when_pipeline_popups_enabled(self):
        helpers = self._load_helpers("1", "1")

        with mock.patch("pyautogui.alert", return_value="Continue") as pyautogui_alert:
            result = helpers.show_alert("boom", title="title", button="Continue")

        self.assertEqual(result, "Continue")
        pyautogui_alert.assert_called_once_with("boom", "title", "Continue")

    def test_show_confirm_uses_pyautogui_when_pipeline_popups_enabled(self):
        helpers = self._load_helpers("1", "true")

        with mock.patch("pyautogui.confirm", return_value="Submit Application") as pyautogui_confirm:
            result = helpers.show_confirm(
                "review this",
                title="Confirm your information",
                buttons=["Disable Pause", "Discard Application", "Submit Application"],
            )

        self.assertEqual(result, "Submit Application")
        pyautogui_confirm.assert_called_once_with(
            "review this",
            "Confirm your information",
            ["Disable Pause", "Discard Application", "Submit Application"],
        )

    def test_show_alert_uses_pyautogui_when_not_in_pipeline_mode(self):
        helpers = self._load_helpers("0", "0")

        with mock.patch("pyautogui.alert", return_value="OK") as pyautogui_alert:
            result = helpers.show_alert("manual", title="Info", button="OK")

        self.assertEqual(result, "OK")
        pyautogui_alert.assert_called_once_with("manual", "Info", "OK")

    def test_emit_console_line_handles_unicode_encoding_failures(self):
        helpers = self._load_helpers("1")

        class FakeStream:
            encoding = "cp1252"

            def __init__(self) -> None:
                self.buffer = io.BytesIO()

            def write(self, value: str) -> int:
                raise UnicodeEncodeError("charmap", value, 0, 1, "character maps to <undefined>")

            def flush(self) -> None:
                return None

        fake_stream = FakeStream()
        with mock.patch.object(helpers.sys, "stdout", fake_stream):
            helpers._emit_console_line("emoji test ??", flush=True)

        output = fake_stream.buffer.getvalue()
        self.assertTrue(output)
        self.assertIn(b"emoji test ", output)
        self.assertTrue(output.endswith(b"\n"))

    def test_print_lg_warns_once_on_locked_log_file_and_continues(self):
        helpers = self._load_helpers("1")
        helpers.__log_file_warning_shown = False

        with mock.patch.object(helpers, "_ensure_log_parent"),              mock.patch.object(helpers, "_emit_console_line"),              mock.patch.object(helpers, "show_alert", return_value="OK") as show_alert,              mock.patch("builtins.open", side_effect=PermissionError("file is locked")):
            helpers.print_lg("first")
            helpers.print_lg("second")

        self.assertEqual(show_alert.call_count, 1)

    def test_manual_login_retry_waits_in_pipeline_mode_until_login_is_detected(self):
        helpers = self._load_helpers("1", "0")

        login_checks = iter([False, False, True])
        fake_time_values = iter([0, 1, 2, 3, 4, 5])

        with mock.patch.dict(helpers.os.environ, {"PIPELINE_MANUAL_LOGIN_TIMEOUT_SECONDS": "10"}, clear=False), \
             mock.patch.object(helpers, "sleep"), \
             mock.patch.object(helpers.time, "time", side_effect=lambda: next(fake_time_values)):
            result = helpers.manual_login_retry(lambda: next(login_checks))

        self.assertTrue(result)


if __name__ == "__main__":
    unittest.main()

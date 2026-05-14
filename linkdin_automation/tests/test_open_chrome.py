import importlib
import sys
import unittest
from pathlib import Path
from unittest import mock


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


class OpenChromeFallbackTests(unittest.TestCase):
    def setUp(self) -> None:
        sys.modules.pop("modules.open_chrome", None)
        self.chrome = importlib.import_module("modules.open_chrome")
        self.chrome = importlib.reload(self.chrome)

    def tearDown(self) -> None:
        sys.modules.pop("modules.open_chrome", None)

    def test_prefers_stable_chrome_for_manual_single_job_mode(self) -> None:
        self.chrome.target_job_link = "https://www.linkedin.com/jobs/view/123456789"
        self.chrome.linkedin_auto_login = False
        self.chrome.run_in_background = False

        self.assertTrue(self.chrome._should_prefer_stable_chrome())

    def test_initialize_chrome_session_uses_next_fallback_after_failure(self) -> None:
        created_driver = object()

        def fake_create(is_retry=False, force_stable=False):
            if not is_retry and force_stable:
                raise RuntimeError("first stable launch failed")
            return "options", created_driver, "actions", "wait"

        with mock.patch.object(self.chrome, "_should_prefer_stable_chrome", return_value=True), \
             mock.patch.object(self.chrome, "createChromeSession", side_effect=fake_create) as create_session:
            result = self.chrome.initializeChromeSession()

        self.assertEqual(result, ("options", created_driver, "actions", "wait"))
        self.assertEqual(create_session.call_args_list[0].args, (False,))
        self.assertEqual(create_session.call_args_list[0].kwargs, {"force_stable": True})
        self.assertEqual(create_session.call_args_list[1].args, (True,))
        self.assertEqual(create_session.call_args_list[1].kwargs, {"force_stable": True})


if __name__ == "__main__":
    unittest.main()

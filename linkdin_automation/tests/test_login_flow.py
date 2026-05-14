import importlib
import sys
import types
import unittest
from pathlib import Path
from unittest import mock


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _install_ai_stubs() -> None:
    fake_openai = types.ModuleType("openai")
    fake_openai.OpenAI = object

    fake_openai_types = types.ModuleType("openai.types")
    fake_openai_types_model = types.ModuleType("openai.types.model")
    fake_openai_types_model.Model = object
    fake_openai_types_chat = types.ModuleType("openai.types.chat")
    fake_openai_types_chat.ChatCompletion = object
    fake_openai_types_chat.ChatCompletionChunk = object

    fake_google = types.ModuleType("google")
    fake_google_generativeai = types.ModuleType("google.generativeai")
    fake_google_generativeai.configure = lambda **kwargs: None
    fake_google_generativeai.list_models = lambda: []
    fake_google_generativeai.GenerativeModel = object
    fake_google.generativeai = fake_google_generativeai

    sys.modules["openai"] = fake_openai
    sys.modules["openai.types"] = fake_openai_types
    sys.modules["openai.types.model"] = fake_openai_types_model
    sys.modules["openai.types.chat"] = fake_openai_types_chat
    sys.modules["google"] = fake_google
    sys.modules["google.generativeai"] = fake_google_generativeai


class LinkedInLoginFlowTests(unittest.TestCase):
    def setUp(self) -> None:
        _install_ai_stubs()
        sys.modules.pop("runAiBot", None)
        self.bot = importlib.import_module("runAiBot")
        self.bot = importlib.reload(self.bot)

    def tearDown(self) -> None:
        sys.modules.pop("runAiBot", None)

    def test_auto_login_disabled_uses_manual_confirmation_flow(self) -> None:
        self.bot.linkedin_auto_login = False

        with mock.patch.object(self.bot, "show_alert", return_value="OK") as show_alert, \
             mock.patch.object(self.bot, "manual_login_retry", return_value=True) as manual_login_retry:
            result = self.bot.login_LN()

        self.assertTrue(result)
        show_alert.assert_called_once()
        manual_login_retry.assert_called_once_with(self.bot.is_logged_in_LN, 2)

    def test_missing_credentials_uses_manual_confirmation_flow(self) -> None:
        self.bot.linkedin_auto_login = True
        self.bot.username = ""
        self.bot.password = ""

        with mock.patch.object(self.bot, "show_alert", return_value="OK") as show_alert, \
             mock.patch.object(self.bot, "manual_login_retry", return_value=False) as manual_login_retry:
            result = self.bot.login_LN()

        self.assertFalse(result)
        show_alert.assert_called_once()
        manual_login_retry.assert_called_once_with(self.bot.is_logged_in_LN, 2)

    def test_failed_auto_login_falls_back_to_manual_confirmation(self) -> None:
        self.bot.linkedin_auto_login = True
        self.bot.username = "user@example.com"
        self.bot.password = "wrong-password"
        self.bot.driver = mock.Mock()
        self.bot.driver.get = mock.Mock()

        with mock.patch.object(self.bot, "buffer"), \
             mock.patch.object(self.bot, "is_logged_in_LN", side_effect=[False, False, False]), \
             mock.patch.object(self.bot, "find_first_visible_login_element", side_effect=RuntimeError("missing form")), \
             mock.patch.object(self.bot, "find_by_class", side_effect=RuntimeError("no profile button")), \
             mock.patch.object(self.bot, "WebDriverWait") as wait_class, \
             mock.patch.object(self.bot, "show_alert", return_value="OK") as show_alert, \
             mock.patch.object(self.bot, "manual_login_retry", return_value=True) as manual_login_retry:
            wait_class.return_value.until.side_effect = TimeoutError("login timeout")
            result = self.bot.login_LN()

        self.assertTrue(result)
        show_alert.assert_called_once()
        manual_login_retry.assert_called_once()
        self.assertEqual(manual_login_retry.call_args.args[1], 2)

    def test_pipeline_mode_auto_login_failure_raises_manual_login_reason(self) -> None:
        self.bot.linkedin_auto_login = True
        self.bot.pipeline_mode = True
        self.bot.username = "user@example.com"
        self.bot.password = "wrong-password"
        self.bot.driver = mock.Mock()
        self.bot.driver.get = mock.Mock()

        with mock.patch.object(self.bot, "buffer"), \
             mock.patch.object(self.bot, "is_logged_in_LN", side_effect=[False, False, False]), \
             mock.patch.object(self.bot, "find_first_visible_login_element", side_effect=RuntimeError("missing form")), \
             mock.patch.object(self.bot, "find_by_class", side_effect=RuntimeError("no profile button")), \
             mock.patch.object(self.bot, "WebDriverWait") as wait_class:
            wait_class.return_value.until.side_effect = TimeoutError("login timeout")
            with self.assertRaises(RuntimeError) as error_context:
                self.bot.login_LN()

        self.assertIn("Complete manual login in Chrome and keep the browser window open.", str(error_context.exception))

    def test_prompt_manual_linkedin_login_returns_false_when_not_confirmed(self) -> None:
        with mock.patch.object(self.bot, "show_alert", return_value="OK") as show_alert, \
             mock.patch.object(self.bot, "manual_login_retry", return_value=False) as manual_login_retry:
            result = self.bot.prompt_manual_linkedin_login("Please login manually.")

        self.assertFalse(result)
        show_alert.assert_called_once()
        manual_login_retry.assert_called_once_with(self.bot.is_logged_in_LN, 2)


if __name__ == "__main__":
    unittest.main()

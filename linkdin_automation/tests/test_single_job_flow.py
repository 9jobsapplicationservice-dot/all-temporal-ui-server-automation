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


class SingleJobFlowTests(unittest.TestCase):
    def setUp(self) -> None:
        _install_ai_stubs()
        sys.modules.pop("runAiBot", None)
        self.bot = importlib.import_module("runAiBot")
        self.bot = importlib.reload(self.bot)

    def tearDown(self) -> None:
        sys.modules.pop("runAiBot", None)

    def test_validates_linkedin_job_urls(self) -> None:
        self.assertTrue(self.bot.is_valid_linkedin_job_link("https://www.linkedin.com/jobs/view/123456789"))
        self.assertFalse(self.bot.is_valid_linkedin_job_link("https://www.linkedin.com/in/some-profile"))
        self.assertFalse(self.bot.is_valid_linkedin_job_link("https://example.com/jobs/view/123"))

    def test_run_uses_target_job_flow_when_configured(self) -> None:
        self.bot.target_job_link = "https://www.linkedin.com/jobs/view/123456789"
        self.bot.dailyEasyApplyLimitReached = False

        with mock.patch.object(self.bot, "apply_to_target_job") as apply_to_target_job, \
             mock.patch.object(self.bot, "apply_to_jobs") as apply_to_jobs, \
             mock.patch.object(self.bot, "buffer"):
            total_runs = self.bot.run(1)

        self.assertEqual(total_runs, 2)
        apply_to_target_job.assert_called_once_with("https://www.linkedin.com/jobs/view/123456789")
        apply_to_jobs.assert_not_called()

    def test_apply_to_target_job_rejects_invalid_url(self) -> None:
        with self.assertRaises(ValueError):
            self.bot.apply_to_target_job("https://www.linkedin.com/in/not-a-job")

    def test_apply_to_target_job_skips_cleanly_without_easy_apply(self) -> None:
        self.bot.skip_count = 0
        self.bot.use_AI = False

        with mock.patch.object(self.bot, "get_applied_job_ids", return_value=set()), \
             mock.patch.object(self.bot, "navigate_to_target_job"), \
             mock.patch.object(self.bot, "extract_current_job_page_details", return_value=("123456789", "Backend Engineer", "Acme", "Remote", "Remote")), \
             mock.patch.object(self.bot, "try_find_by_classes", side_effect=ValueError("marker missing")), \
             mock.patch.object(self.bot, "extract_hr_details", return_value=("", "", "")), \
             mock.patch.object(self.bot, "get_job_description", return_value=("Description", 2, False, None, None)), \
             mock.patch.object(self.bot, "try_xp", return_value=False):
            self.bot.apply_to_target_job("https://www.linkedin.com/jobs/view/123456789")

        self.assertEqual(self.bot.skip_count, 1)

    def test_launch_easy_apply_captures_before_clicking(self) -> None:
        events: list[str] = []

        def capture_side_effect(job_id: str, label: str) -> str:
            events.append(f"capture:{label}:{job_id}")
            return "before.png"

        def click_side_effect(_driver, xpath: str, click: bool = True):
            if click:
                events.append(f"click:{xpath}")
                return True
            return object()

        with mock.patch.object(self.bot, "capture_application_screenshot", side_effect=capture_side_effect), \
             mock.patch.object(self.bot, "try_xp", side_effect=click_side_effect), \
             mock.patch.object(self.bot, "get_active_easy_apply_modal", return_value="modal"):
            modal, screenshot_name = self.bot.launch_easy_apply("123456789")

        self.assertEqual(modal, "modal")
        self.assertEqual(screenshot_name, "before.png")
        self.assertEqual(
            events,
            [
                "capture:Before Apply:123456789",
                f"click:{self.bot.EASY_APPLY_BUTTON_XPATH}",
            ],
        )

    def test_apply_to_target_job_captures_before_then_after_submitted(self) -> None:
        self.bot.skip_count = 0
        self.bot.easy_applied_count = 0
        self.bot.use_AI = False
        events: list[str] = []

        def try_xp_side_effect(_driver, xpath: str, click: bool = True):
            if xpath == self.bot.EASY_APPLY_BUTTON_XPATH and not click:
                events.append("checked_easy_apply_button")
                return object()
            if xpath == './/span[contains(normalize-space(), " ago")]' and not click:
                return None
            if xpath == self.bot.EASY_APPLY_BUTTON_XPATH and click:
                events.append("clicked_easy_apply_button")
                return True
            return False

        def capture_side_effect(job_id: str, label: str) -> str:
            events.append(f"capture:{label}:{job_id}")
            return f"{label}.png"

        def click_easy_apply_button_side_effect(button_texts, timeout=2.0, scroll_top=False):
            if button_texts == ["Review"]:
                return False
            if button_texts == ["Submit application"]:
                events.append("clicked_submit")
                return True
            return False

        with mock.patch.object(self.bot, "get_applied_job_ids", return_value=set()), \
             mock.patch.object(self.bot, "navigate_to_target_job"), \
             mock.patch.object(self.bot, "extract_current_job_page_details", return_value=("123456789", "Backend Engineer", "Acme", "Remote", "Remote")), \
             mock.patch.object(self.bot, "try_find_by_classes", side_effect=ValueError("marker missing")), \
             mock.patch.object(self.bot, "extract_hr_details", return_value=("", "", "")), \
             mock.patch.object(self.bot, "get_job_description", return_value=("Description", 2, False, None, None)), \
             mock.patch.object(self.bot, "try_xp", side_effect=try_xp_side_effect), \
             mock.patch.object(self.bot, "capture_application_screenshot", side_effect=capture_side_effect), \
             mock.patch.object(self.bot, "get_active_easy_apply_modal", return_value=mock.Mock()), \
             mock.patch.object(self.bot, "answer_questions", return_value=set()), \
             mock.patch.object(self.bot, "upload_resume", return_value=(False, "Previous resume")), \
             mock.patch.object(self.bot, "easy_apply_step_buffer"), \
             mock.patch.object(self.bot, "detect_manual_form_needs", return_value=(False, "")), \
             mock.patch.object(self.bot, "click_easy_apply_button", side_effect=click_easy_apply_button_side_effect), \
             mock.patch.object(self.bot, "find_easy_apply_button", return_value=object()), \
             mock.patch.object(self.bot, "application_sent_confirmation_present", return_value=False), \
             mock.patch.object(self.bot, "follow_company"), \
             mock.patch.object(self.bot, "confirmed_easy_apply_submission", return_value=True), \
             mock.patch.object(self.bot, "close_easy_apply_success_dialog"), \
             mock.patch.object(self.bot, "submitted_jobs", return_value=True):
            self.bot.apply_to_target_job("https://www.linkedin.com/jobs/view/123456789")

        self.assertEqual(self.bot.easy_applied_count, 1)
        self.assertEqual(
            [event for event in events if event.startswith("capture:")],
            [
                "capture:Before Apply:123456789",
                "capture:After Submitted:123456789",
            ],
        )
        self.assertLess(events.index("capture:Before Apply:123456789"), events.index("clicked_easy_apply_button"))
        self.assertLess(events.index("clicked_submit"), events.index("capture:After Submitted:123456789"))


if __name__ == "__main__":
    unittest.main()

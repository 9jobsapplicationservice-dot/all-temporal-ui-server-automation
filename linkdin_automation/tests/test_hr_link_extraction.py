import ast
import sys
import types
import unittest
from pathlib import Path
from urllib.parse import urlparse


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_PATH = PROJECT_ROOT / "runAiBot.py"


def clean_csv_text(value):
    if value is None:
        return ""
    return " ".join(str(value).strip().split())


class By:
    XPATH = "xpath"


def load_target_functions():
    source = SOURCE_PATH.read_text(encoding="utf-8-sig")
    tree = ast.parse(source, filename=str(SOURCE_PATH))
    namespace = {
        "By": By,
        "WebElement": object,
        "clean_csv_text": clean_csv_text,
        "urlparse": urlparse,
    }

    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name in {
            "normalize_linkedin_profile_link",
            "extract_hr_details_from_card",
        }:
            module = ast.Module(body=[node], type_ignores=[])
            ast.fix_missing_locations(module)
            exec(compile(module, filename=str(SOURCE_PATH), mode="exec"), namespace)

    namespace["extract_hr_position"] = lambda card, name: ""
    return namespace


class FakeAnchor:
    def __init__(self, href: str, text: str) -> None:
        self._href = href
        self.text = text

    def get_attribute(self, name: str) -> str:
        if name == "href":
            return self._href
        raise KeyError(name)


class FakeCard:
    def __init__(self, text: str, anchors: list[FakeAnchor]) -> None:
        self.text = text
        self._anchors = anchors

    def find_elements(self, by: str, selector: str):
        return list(self._anchors)


class HrLinkExtractionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.namespace = load_target_functions()
        cls.normalize_linkedin_profile_link = staticmethod(cls.namespace["normalize_linkedin_profile_link"])
        cls.extract_hr_details_from_card = staticmethod(cls.namespace["extract_hr_details_from_card"])

    def test_accepts_only_linkedin_person_profile_urls(self):
        normalize = self.normalize_linkedin_profile_link

        self.assertEqual(
            normalize("https://www.linkedin.com/in/jane-doe-123"),
            "https://www.linkedin.com/in/jane-doe-123",
        )
        self.assertEqual(
            normalize("https://www.linkedin.com/pub/jane-doe/12/34/567"),
            "https://www.linkedin.com/pub/jane-doe/12/34/567",
        )
        self.assertEqual(normalize("https://www.linkedin.com/jobs/view/123"), "")
        self.assertEqual(normalize("https://www.linkedin.com/company/openai"), "")
        self.assertEqual(normalize("https://www.linkedin.com/in/copyright"), "")
        self.assertEqual(normalize("https://www.linkedin.com/recruiter/profile"), "")

    def test_name_only_card_keeps_blank_profile_link(self):
        card = FakeCard(
            text="Rahul Rameez Nazir\nTalent Acquisition Specialist\nMessage",
            anchors=[],
        )

        hr_name, hr_link, hr_position = self.extract_hr_details_from_card(card)

        self.assertEqual(hr_name, "Rahul Rameez Nazir")
        self.assertEqual(hr_link, "")
        self.assertEqual(hr_position, "")

    def test_action_links_are_ignored_when_extracting_profile(self):
        card = FakeCard(
            text="Jane Doe\nRecruiter\nConnect",
            anchors=[
                FakeAnchor("https://www.linkedin.com/company/openai", "OpenAI"),
                FakeAnchor("https://www.linkedin.com/in/jane-doe-123", "Jane Doe"),
            ],
        )

        hr_name, hr_link, _ = self.extract_hr_details_from_card(card)

        self.assertEqual(hr_name, "Jane Doe")
        self.assertEqual(hr_link, "https://www.linkedin.com/in/jane-doe-123")


if __name__ == "__main__":
    unittest.main()

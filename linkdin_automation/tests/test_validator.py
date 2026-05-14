import importlib
import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


class SecretsValidationTests(unittest.TestCase):
    def setUp(self) -> None:
        sys.modules.pop("modules.validator", None)
        self.validator = importlib.import_module("modules.validator")
        self.validator = importlib.reload(self.validator)

    def tearDown(self) -> None:
        sys.modules.pop("modules.validator", None)

    def test_validate_secrets_allows_blank_credentials_when_auto_login_disabled(self):
        self.validator.linkedin_auto_login = False
        self.validator.username = ""
        self.validator.password = ""

        self.validator.validate_secrets()

    def test_validate_secrets_requires_credentials_when_auto_login_enabled(self):
        self.validator.linkedin_auto_login = True
        self.validator.username = ""
        self.validator.password = ""

        with self.assertRaises(ValueError):
            self.validator.validate_secrets()


if __name__ == "__main__":
    unittest.main()

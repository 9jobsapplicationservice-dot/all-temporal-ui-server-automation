from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


class SaasDeploymentConfigTest(unittest.TestCase):
    def test_dockerfile_packages_next_python_chrome_and_temporal(self) -> None:
        dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")

        self.assertIn("node:20-bookworm", dockerfile)
        self.assertIn("temporalio/temporal", dockerfile)
        self.assertIn("chromium", dockerfile)
        self.assertIn("xvfb", dockerfile)
        self.assertIn("npm run build", dockerfile)
        self.assertIn("scripts/start-saas.sh", dockerfile)

    def test_start_script_runs_next_on_hosted_port_with_persistent_paths(self) -> None:
        start_script = (ROOT / "scripts" / "start-saas.sh").read_text(encoding="utf-8")

        self.assertIn("PIPELINE_WORKSPACE_ROOT", start_script)
        self.assertIn("PIPELINE_ROOT", start_script)
        self.assertIn("Xvfb", start_script)
        self.assertIn("npm --prefix", start_script)
        self.assertIn("email-automation-nodejs", start_script)

    def test_compose_exposes_single_saas_url_and_persistent_volume(self) -> None:
        compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")

        self.assertIn("3000:3000", compose)
        self.assertIn("PIPELINE_TEMPORAL_AUTO_START=true", compose)
        self.assertIn("pipeline-data:/data/pipeline", compose)


if __name__ == "__main__":
    unittest.main()

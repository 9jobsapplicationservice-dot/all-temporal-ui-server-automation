from __future__ import annotations

import os
from pathlib import Path


# Keep module import Temporal-workflow-sandbox safe by avoiding Path.resolve at import time.
PACKAGE_ROOT = Path(__file__).parent
DEFAULT_PIPELINE_ROOT = PACKAGE_ROOT
DEFAULT_POLL_INTERVAL_SECONDS = 5.0
MAX_ROCKETREACH_RETRIES = 5
MAX_EMAIL_STAGE_RETRIES = 3
DEFAULT_LINKEDIN_STAGE_TIMEOUT_SECONDS = 3600
DEFAULT_LINKEDIN_IDLE_TIMEOUT_SECONDS = 600
TEMPORAL_DEFAULT_ADDRESS = "localhost:7233"
TEMPORAL_DEFAULT_NAMESPACE = "default"
TEMPORAL_DEFAULT_TASK_QUEUE = "automation-pipeline"
DEFAULT_PROVIDER_RATE_LIMIT_PER_MINUTE = 2
MAX_PROVIDER_RATE_LIMIT_PER_MINUTE = 3
DEFAULT_WORKFLOW_MAX_RERUNS = 3

RUN_STATUSES = (
    "queued",
    "starting",
    "running",
    "browser_launched",
    "linkedin_loaded",
    "applying",
    "waiting_review",
    "sending",
    "completed",
    "failed",
    "terminated",
)

APPLIED_JOBS_HEADERS = [
    "Date",
    "Company Name",
    "Position",
    "Job Link",
    "Submitted",
    "HR Name",
    "HR Position",
    "HR Profile Link",
]

ENRICHED_RECRUITER_HEADERS = [
    "Date",
    "Company Name",
    "Position",
    "Job Link",
    "Submitted",
    "HR Name",
    "HR Position",
    "HR Profile Link",
    "HR Email",
    "HR Secondary Email",
    "HR Email Preview",
    "HR Contact",
    "HR Contact Preview",
    "RocketReach Status",
    "Email Source",
    "Email Lookup Status",
    "Lookup Attempts",
    "Last Provider Error",
]


def resolve_pipeline_root(explicit_root: str | Path | None = None) -> Path:
    if explicit_root:
        return Path(explicit_root).expanduser().resolve()

    # Strict priority for Render/Production
    if os.environ.get("RENDER") or os.path.exists("/app/data/pipeline"):
        return Path("/app/data/pipeline")

    data_dir = os.environ.get("PIPELINE_DATA_DIR", "").strip()
    if data_dir:
        return Path(data_dir).expanduser().resolve()

    env_root = os.environ.get("PIPELINE_ROOT", "").strip()
    if env_root:
        return Path(env_root).expanduser().resolve()

    return DEFAULT_PIPELINE_ROOT

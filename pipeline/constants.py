from __future__ import annotations

import os
from pathlib import Path


PACKAGE_ROOT = Path(__file__).resolve().parent
DEFAULT_PIPELINE_ROOT = PACKAGE_ROOT
DEFAULT_POLL_INTERVAL_SECONDS = 5.0
MAX_ROCKETREACH_RETRIES = 3

RUN_STATUSES = (
    "queued",
    "blocked_runtime",
    "waiting_login",
    "linkedin_running",
    "rocketreach_running",
    "email_running",
    "waiting_review",
    "sending",
    "completed",
    "failed",
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
]


def resolve_pipeline_root(explicit_root: str | Path | None = None) -> Path:
    if explicit_root:
        return Path(explicit_root).expanduser().resolve()

    env_root = os.environ.get("PIPELINE_ROOT", "").strip()
    if env_root:
        return Path(env_root).expanduser().resolve()

    return DEFAULT_PIPELINE_ROOT

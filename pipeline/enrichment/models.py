from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class EnrichmentContact:
    fingerprint: str
    date: str
    company_name: str
    position: str
    job_link: str
    submitted: str
    hr_name: str
    hr_position: str
    hr_profile_link: str
    company_domain: str = ""


@dataclass(frozen=True)
class ProviderLookupResult:
    provider: str
    status: str
    email: str = ""
    secondary_email: str = ""
    email_preview: str = ""
    contact: str = ""
    contact_preview: str = ""
    normalized_profile_link: str = ""
    resolved_name: str = ""
    resolved_position: str = ""
    last_error: str = ""
    raw_payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ProviderAttemptLog:
    provider: str
    attempt_number: int
    recruiter_fingerprint: str
    status: str
    retryable: bool
    error: str = ""


@dataclass(frozen=True)
class EnrichmentRunStats:
    total: int = 0
    matched: int = 0
    preview_match: int = 0
    failed: int = 0
    skipped: int = 0
    no_match: int = 0
    missing_hr_link: int = 0
    invalid_hr_link: int = 0
    profile_only: int = 0
    lookup_quota_reached: int = 0
    authentication_failed: int = 0
    sendable_rows: int = 0
    provider_success_count: int = 0
    no_email_count: int = 0
    provider_retry_count: int = 0

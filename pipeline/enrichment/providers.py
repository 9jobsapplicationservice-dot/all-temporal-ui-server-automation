from __future__ import annotations

import importlib.util
import json
import logging
import os
import pathlib
from typing import Any

import requests

from .errors import (
    MissingProfileDataError,
    NoEmailFoundError,
    NonRetryableProviderError,
    QuotaExceededError,
    RateLimitError,
    RetryableProviderError,
)
from .models import EnrichmentContact, ProviderLookupResult
from .rate_limit import ProviderRateLimiter
from ..core.sentry_config import build_pipeline_tags, capture_exception_with_context

logger = logging.getLogger(__name__)

_ROCKETREACH_MODULE = None

_ROCKETREACH_AUTH_MARKERS = (
    "verify your email",
    "authentication failed",
    "unauthorized",
    "forbidden",
    "invalid api key",
    "invalid token",
    "invalid credentials",
    "account verification",
    "email verification",
    "access denied",
)

_ROCKETREACH_TRANSIENT_MARKERS = (
    "rate limit",
    "temporarily unavailable",
    "timed out",
    "timeout",
    "connection reset",
    "connection aborted",
    "connection refused",
    "service unavailable",
    "bad gateway",
    "gateway timeout",
    "too many requests",
    "429",
    "500",
    "502",
    "503",
    "504",
)


def _workspace_root() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parents[2]


def load_rocketreach_module():
    global _ROCKETREACH_MODULE
    if _ROCKETREACH_MODULE is not None:
        return _ROCKETREACH_MODULE
    module_path = _workspace_root() / "rocket_reach - testing" / "rocketreach_bulk.py"
    spec = importlib.util.spec_from_file_location("_local_rocketreach_bulk_pipeline", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load RocketReach module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    _ROCKETREACH_MODULE = module
    return module


def _is_rocketreach_auth_error(message: str) -> bool:
    lowered = (message or "").lower()
    return any(marker in lowered for marker in _ROCKETREACH_AUTH_MARKERS)


def _is_rocketreach_transient_error(message: str) -> bool:
    lowered = (message or "").lower()
    return any(marker in lowered for marker in _ROCKETREACH_TRANSIENT_MARKERS)


def _raise_for_http_error(provider: str, response: requests.Response, payload: Any) -> None:
    status_code = int(response.status_code)
    message = ""
    if isinstance(payload, dict):
        message = str(payload.get("message") or payload.get("error") or payload.get("detail") or "")
    if not message:
        message = response.text.strip()
    lowered = message.lower()
    if status_code in {408, 425, 429, 500, 502, 503, 504}:
        if status_code == 429:
            raise RateLimitError(message or f"{provider} rate limit exceeded.", provider=provider)
        raise RetryableProviderError(message or f"{provider} temporary HTTP error {status_code}.", provider=provider)
    if "quota" in lowered or "credit" in lowered or "rate limit" in lowered:
        raise QuotaExceededError(message or f"{provider} quota exhausted.", provider=provider)
    if status_code in {400, 401, 403, 404, 422}:
        raise NonRetryableProviderError(message or f"{provider} rejected the request.", provider=provider)
    raise RetryableProviderError(message or f"{provider} request failed.", provider=provider)


def _capture_provider_exception(
    provider: str,
    error: BaseException,
    *,
    contact: EnrichmentContact,
    extra: dict[str, object] | None = None,
) -> None:
    logger.exception(
        "Provider request failed. provider=%s recruiter=%s company=%s",
        provider,
        contact.hr_name,
        contact.company_name,
    )
    payload = {
        "recruiter_name": contact.hr_name,
        "company_name": contact.company_name,
        "position": contact.position,
        "job_link": contact.job_link,
        "profile_link": contact.hr_profile_link,
        **(extra or {}),
    }
    capture_exception_with_context(
        error,
        message="provider request failed",
        tags=build_pipeline_tags(provider=provider, stage="rocketreach"),
        extras=payload,
    )


class ProviderClient:
    provider_name = "provider"

    def lookup(self, contact: EnrichmentContact, limiter: ProviderRateLimiter) -> ProviderLookupResult:
        raise NotImplementedError


class RocketReachClient(ProviderClient):
    provider_name = "rocketreach"

    def __init__(self) -> None:
        rr = load_rocketreach_module()
        self._rr = rr
        self._headers = rr.build_rocketreach_headers(rr.ENV_PATH)

    @property
    def is_usable(self) -> bool:
        return True

    def lookup(self, contact: EnrichmentContact, limiter: ProviderRateLimiter) -> ProviderLookupResult:
        if not contact.hr_profile_link and not (contact.hr_name and contact.company_name):
            raise MissingProfileDataError(
                "RocketReach lookup requires recruiter LinkedIn URL or recruiter name plus company name.",
                provider=self.provider_name,
            )
        row = {
            "Date": contact.date,
            "Company Name": contact.company_name,
            "Position": contact.position,
            "Job Link": contact.job_link,
            "Submitted": contact.submitted,
            "HR Name": contact.hr_name,
            "HR Position": contact.hr_position,
            "HR Profile Link": contact.hr_profile_link,
        }
        try:
            result = self._rr.lookup_then_search(contact.hr_profile_link, self._headers, row=row)
        except requests.Timeout as error:
            _capture_provider_exception(self.provider_name, error, contact=contact)
            raise RetryableProviderError(f"RocketReach timeout: {error}", provider=self.provider_name) from error
        except requests.ConnectionError as error:
            _capture_provider_exception(self.provider_name, error, contact=contact)
            raise RetryableProviderError(f"RocketReach connection error: {error}", provider=self.provider_name) from error
        except requests.RequestException as error:
            _capture_provider_exception(self.provider_name, error, contact=contact)
            raise RetryableProviderError(f"RocketReach request error: {error}", provider=self.provider_name) from error
        except Exception as error:
            _capture_provider_exception(self.provider_name, error, contact=contact)
            message = str(error).strip() or "RocketReach lookup failed."
            if _is_rocketreach_auth_error(message):
                raise NonRetryableProviderError(
                    f"RocketReach authentication failed: {message}",
                    provider=self.provider_name,
                ) from error
            if _is_rocketreach_transient_error(message):
                raise RetryableProviderError(
                    f"RocketReach temporary failure: {message}",
                    provider=self.provider_name,
                ) from error
            raise NonRetryableProviderError(
                f"RocketReach lookup failed: {message}",
                provider=self.provider_name,
            ) from error
        body = dict(result.get("body") or {})
        clean_row = self._rr.clean_output_row(row, body)
        status = clean_row.get("RocketReach Status", "")
        if status == "matched":
            return ProviderLookupResult(
                provider=self.provider_name,
                status=status,
                email=str(clean_row.get("HR Email") or ""),
                secondary_email=str(clean_row.get("HR Secondary Email") or ""),
                email_preview=str(clean_row.get("HR Email Preview") or ""),
                contact=str(clean_row.get("HR Contact") or ""),
                contact_preview=str(clean_row.get("HR Contact Preview") or ""),
                normalized_profile_link=str(clean_row.get("HR Profile Link") or ""),
                resolved_name=str(clean_row.get("HR Name") or ""),
                resolved_position=str(clean_row.get("HR Position") or ""),
                raw_payload=body,
            )
        if status == "preview_match":
            return ProviderLookupResult(
                provider=self.provider_name,
                status=status,
                email_preview=str(clean_row.get("HR Email Preview") or ""),
                contact_preview=str(clean_row.get("HR Contact Preview") or ""),
                normalized_profile_link=str(clean_row.get("HR Profile Link") or ""),
                resolved_name=str(clean_row.get("HR Name") or ""),
                resolved_position=str(clean_row.get("HR Position") or ""),
                raw_payload=body,
            )
        if status == "lookup_quota_reached":
            raise QuotaExceededError("RocketReach quota or rate limit blocked lookup.", provider=self.provider_name)
        if status in {"missing_hr_link", "invalid_hr_link"}:
            raise MissingProfileDataError(f"RocketReach could not use recruiter profile input: {status}.", provider=self.provider_name)
        if status in {"profile_only", "no_match"}:
            raise NoEmailFoundError(f"RocketReach returned {status}.", provider=self.provider_name)
        raise NonRetryableProviderError(f"RocketReach returned unsupported status {status!r}.", provider=self.provider_name)


class HunterClient(ProviderClient):
    provider_name = "hunter"

    def __init__(self, api_key: str, base_url: str | None = None, timeout_seconds: int = 30) -> None:
        self.api_key = api_key.strip()
        self.base_url = (base_url or "https://api.hunter.io/v2").rstrip("/")
        self.timeout_seconds = max(1, int(timeout_seconds))
        self.session = requests.Session()

    @property
    def is_usable(self) -> bool:
        return bool(self.api_key)

    def lookup(self, contact: EnrichmentContact, limiter: ProviderRateLimiter) -> ProviderLookupResult:
        if not self.api_key:
            raise MissingProfileDataError("Hunter API key is not configured.", provider=self.provider_name)
        if not contact.company_domain:
            raise MissingProfileDataError("Hunter lookup requires company domain.", provider=self.provider_name)
        if not contact.hr_name or " " not in contact.hr_name.strip():
            raise MissingProfileDataError("Hunter lookup requires recruiter first and last name.", provider=self.provider_name)
        parts = [part for part in contact.hr_name.strip().split() if part]
        first_name = parts[0]
        last_name = parts[-1]
        try:
            response = self.session.get(
                f"{self.base_url}/email-finder",
                params={
                    "api_key": self.api_key,
                    "domain": contact.company_domain,
                    "first_name": first_name,
                    "last_name": last_name,
                },
                timeout=self.timeout_seconds,
            )
        except requests.RequestException as error:
            _capture_provider_exception(self.provider_name, error, contact=contact)
            raise RetryableProviderError(f"Hunter request failed: {error}", provider=self.provider_name) from error
        try:
            payload = response.json()
        except ValueError:
            payload = {"message": response.text}
        if not response.ok:
            _raise_for_http_error(self.provider_name, response, payload)
        data = payload.get("data") or {}
        email = str(data.get("email") or "").strip()
        if not email:
            raise NoEmailFoundError("Hunter did not find a deliverable email.", provider=self.provider_name)
        return ProviderLookupResult(
            provider=self.provider_name,
            status="matched",
            email=email,
            normalized_profile_link=contact.hr_profile_link,
            resolved_name=contact.hr_name,
            resolved_position=contact.hr_position,
            raw_payload=payload,
        )


class ApolloClient(ProviderClient):
    provider_name = "apollo"

    def __init__(self, api_key: str, base_url: str | None = None, timeout_seconds: int = 30) -> None:
        self.api_key = api_key.strip()
        if not self.api_key:
            raise ValueError("Apollo API key missing")
        self.base_url = (base_url or "https://api.apollo.io").rstrip("/")
        self.timeout_seconds = max(1, int(timeout_seconds))
        self.session = requests.Session()
        self._health_checked = False
        self._health_error = ""

    @property
    def is_usable(self) -> bool:
        return bool(self.api_key)

    def _api_root(self) -> str:
        root = self.base_url.rstrip("/")
        for suffix in ("/api/v1", "/v1"):
            if root.endswith(suffix):
                return root[:-len(suffix)]
        return root

    def _health_check_url(self) -> str:
        return f"{self._api_root()}/v1/auth/health"

    def _people_search_url(self) -> str:
        return f"{self._api_root()}/api/v1/mixed_people/api_search"

    def _headers(self) -> dict[str, str]:
        return {
            "Content-Type": "application/json",
            "Cache-Control": "no-cache",
            "Accept": "application/json",
            "X-Api-Key": self.api_key,
        }

    def _ensure_healthy(self, contact: EnrichmentContact) -> None:
        if self._health_checked:
            if self._health_error:
                raise NonRetryableProviderError(self._health_error, provider=self.provider_name)
            return
        try:
            response = self.session.get(
                self._health_check_url(),
                headers=self._headers(),
                timeout=self.timeout_seconds,
            )
        except requests.RequestException as error:
            _capture_provider_exception(
                self.provider_name,
                error,
                contact=contact,
                extra={"apollo_health_check": True},
            )
            self._health_checked = True
            self._health_error = f"Apollo health-check failed: {error}"
            logger.error("Apollo health-check failed, continuing to hunter. reason=%s", error)
            raise NonRetryableProviderError(self._health_error, provider=self.provider_name) from error

        self._health_checked = True
        payload: dict[str, Any]
        try:
            payload = response.json() if response.text else {}
        except ValueError:
            payload = {"message": response.text}
        if response.status_code != 200:
            self._health_error = (
                f"Apollo health-check failed with status {response.status_code}: "
                f"{payload.get('message') or response.text.strip() or 'unknown error'}"
            )
            logger.error("Apollo health-check failed, continuing to hunter. reason=%s", self._health_error)
            raise NonRetryableProviderError(self._health_error, provider=self.provider_name)
        if any(isinstance(value, bool) and value is False for value in payload.values()):
            self._health_error = f"Apollo health-check returned unhealthy response: {payload}"
            logger.error("Apollo health-check failed, continuing to hunter. reason=%s", self._health_error)
            raise NonRetryableProviderError(self._health_error, provider=self.provider_name)

    def lookup(self, contact: EnrichmentContact, limiter: ProviderRateLimiter) -> ProviderLookupResult:
        if not contact.hr_name or not contact.company_name:
            raise MissingProfileDataError(
                "Apollo lookup requires recruiter name and company name.",
                provider=self.provider_name,
            )
        self._ensure_healthy(contact)
        payload = {
            "q_organization_name": contact.company_name,
            "q_keywords": contact.hr_name,
            "page": 1,
            "per_page": 5,
        }
        if contact.hr_profile_link:
            payload["person_linkedin_url"] = contact.hr_profile_link
        try:
            response = self.session.post(
                self._people_search_url(),
                headers=self._headers(),
                json=payload,
                timeout=self.timeout_seconds,
            )
        except requests.RequestException as error:
            _capture_provider_exception(self.provider_name, error, contact=contact)
            raise RetryableProviderError(f"Apollo request failed: {error}", provider=self.provider_name) from error
        try:
            body = response.json()
        except ValueError:
            body = {"message": response.text}
        if not response.ok:
            _raise_for_http_error(self.provider_name, response, body)
        people = body.get("people") or body.get("contacts") or []
        if not people:
            raise NoEmailFoundError("Apollo returned no matching people.", provider=self.provider_name)
        candidate = people[0]
        email = str(candidate.get("email") or candidate.get("email_address") or "").strip()
        if not email:
            raise NoEmailFoundError("Apollo did not return a usable email.", provider=self.provider_name)
        return ProviderLookupResult(
            provider=self.provider_name,
            status="matched",
            email=email,
            normalized_profile_link=str(candidate.get("linkedin_url") or contact.hr_profile_link or ""),
            resolved_name=str(candidate.get("name") or contact.hr_name or ""),
            resolved_position=str(candidate.get("title") or contact.hr_position or ""),
            raw_payload=body if isinstance(body, dict) else {"body": body},
        )

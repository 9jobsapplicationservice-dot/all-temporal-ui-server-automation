from __future__ import annotations

import csv
import hashlib
import io
import json
import os
from pathlib import Path
from typing import Any

from ..config import bootstrap_runtime_environment, load_automation_config, load_runtime_env_values
from ..constants import ENRICHED_RECRUITER_HEADERS
from ..storage import PipelineStore
from ..utils import utc_now_iso
from .errors import (
    MissingProfileDataError,
    NoEmailFoundError,
    NonRetryableProviderError,
    ProviderError,
    QuotaExceededError,
    RateLimitError,
    RetryableProviderError,
)
from .models import EnrichmentContact, ProviderAttemptLog, ProviderLookupResult
from .providers import ApolloClient, HunterClient, RocketReachClient, load_rocketreach_module
from .rate_limit import ProviderRateLimiter


def _structured_log(event: str, **fields: Any) -> None:
    payload = {"event": event, **fields}
    print(json.dumps(payload, sort_keys=True))


def _row_has_values(row: dict[str, Any]) -> bool:
    return any(str(value or "").strip() for value in row.values())


def _build_fingerprint(run_id: str, row: dict[str, str]) -> str:
    parts = [
        run_id,
        str(row.get("Job Link") or "").strip().lower(),
        str(row.get("HR Profile Link") or "").strip().lower(),
        str(row.get("HR Name") or "").strip().lower(),
        str(row.get("Company Name") or "").strip().lower(),
    ]
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def _company_domain_from_row(row: dict[str, str]) -> str:
    for key in ("Company Domain", "Company Website", "Website", "Domain"):
        value = str(row.get(key) or "").strip().lower()
        if value:
            return value.removeprefix("https://").removeprefix("http://").split("/", 1)[0]
    return ""


def _build_contact(run_id: str, row: dict[str, str]) -> EnrichmentContact:
    return EnrichmentContact(
        fingerprint=_build_fingerprint(run_id, row),
        date=str(row.get("Date") or "").strip(),
        company_name=str(row.get("Company Name") or "").strip(),
        position=str(row.get("Position") or "").strip(),
        job_link=str(row.get("Job Link") or "").strip(),
        submitted=str(row.get("Submitted") or "").strip(),
        hr_name=str(row.get("HR Name") or "").strip(),
        hr_position=str(row.get("HR Position") or "").strip(),
        hr_profile_link=str(row.get("HR Profile Link") or "").strip(),
        company_domain=_company_domain_from_row(row),
    )


def _build_provider_clients(config_path: str | None) -> list:
    env_bootstrap = bootstrap_runtime_environment(config_path)
    runtime_values = load_automation_config(config_path)
    env_values = load_runtime_env_values(config_path)
    env_values.update({
        key: value
        for key, value in os.environ.items()
        if key.startswith("ROCKETREACH_") or key.startswith("HUNTER_") or key.startswith("APOLLO_")
    })
    rocketreach_api_key = os.getenv("ROCKETREACH_API_KEY", "").strip()
    apollo_api_key = os.getenv("APOLLO_API_KEY", "").strip()
    hunter_api_key = os.getenv("HUNTER_API_KEY", "").strip()
    rr = RocketReachClient()
    try:
        apollo = ApolloClient(
            api_key=apollo_api_key,
            base_url=str(env_values.get("APOLLO_BASE_URL") or "https://api.apollo.io"),
            timeout_seconds=runtime_values.linkedin_idle_timeout_seconds if runtime_values.linkedin_idle_timeout_seconds else 30,
        )
    except ValueError as error:
        apollo = type(
            "DisabledApolloProvider",
            (),
            {
                "provider_name": "apollo",
                "is_usable": False,
                "disabled_reason": str(error),
            },
        )()
    hunter = HunterClient(
        api_key=hunter_api_key,
        base_url=str(env_values.get("HUNTER_BASE_URL") or "https://api.hunter.io/v2"),
        timeout_seconds=runtime_values.linkedin_idle_timeout_seconds if runtime_values.linkedin_idle_timeout_seconds else 30,
    )
    providers = [rr, apollo, hunter]
    _structured_log(
        "provider_env_bootstrap",
        config_path=config_path or "",
        loaded_sources=env_bootstrap.get("sources", []),
        stale_apollo_snapshot=bool(env_bootstrap.get("stale_apollo_snapshot")),
        apollo_key_present=bool(apollo_api_key),
        hunter_key_present=bool(hunter_api_key),
        rocketreach_key_present=bool(rocketreach_api_key),
    )
    _structured_log(
        "provider_chain_initialized",
        provider_order=[provider.provider_name for provider in providers],
        usable_providers=[provider.provider_name for provider in providers if _provider_is_usable(provider)],
        config_path=config_path or "",
    )
    return providers


def _lookup_from_cache(store: PipelineStore, contact: EnrichmentContact) -> ProviderLookupResult | None:
    cached = store.get_enrichment_cache(contact.fingerprint)
    if not cached:
        return None
    status = str(cached.get("lookup_status") or "").strip()
    if status not in {"matched", "no_email_found", "preview_match"}:
        return None
    return ProviderLookupResult(
        provider=str(cached.get("provider") or ""),
        status=status,
        email=str(cached.get("email") or ""),
        secondary_email=str(cached.get("secondary_email") or ""),
        contact=str(cached.get("contact") or ""),
        last_error=str(cached.get("last_provider_error") or ""),
        raw_payload={},
    )


def _update_cache_from_result(store: PipelineStore, run_id: str, contact: EnrichmentContact, result: ProviderLookupResult, attempts: int, provider_retry_count: int) -> None:
    store.upsert_enrichment_cache(
        contact.fingerprint,
        run_id=run_id,
        provider=result.provider,
        lookup_status=result.status,
        email=result.email,
        secondary_email=result.secondary_email,
        contact=result.contact,
        provider_attempts=attempts,
        provider_retry_count=provider_retry_count,
        last_provider_error=result.last_error,
        raw_payload=json.dumps(result.raw_payload),
    )


def _result_to_row(base_row: dict[str, str], result: ProviderLookupResult, attempts: int) -> dict[str, str]:
    rr = load_rocketreach_module()
    row = rr.clean_output_row(base_row, {})
    row["HR Name"] = result.resolved_name or row.get("HR Name") or ""
    row["HR Position"] = result.resolved_position or row.get("HR Position") or ""
    row["HR Profile Link"] = result.normalized_profile_link or row.get("HR Profile Link") or ""
    row["HR Email"] = result.email
    row["HR Secondary Email"] = result.secondary_email
    row["HR Email Preview"] = result.email_preview
    row["HR Contact"] = result.contact
    row["HR Contact Preview"] = result.contact_preview
    row["RocketReach Status"] = result.status
    row["Email Source"] = result.provider
    row["Email Lookup Status"] = result.status
    row["Lookup Attempts"] = str(attempts)
    row["Last Provider Error"] = result.last_error
    return row


def _final_result_for_error(provider: str, status: str, error: str) -> ProviderLookupResult:
    return ProviderLookupResult(
        provider=provider,
        status=status,
        last_error=error,
    )


def _provider_is_usable(provider: object) -> bool:
    return bool(getattr(provider, "is_usable", True))


def _is_authentication_error_message(message: str) -> bool:
    lowered = (message or "").lower()
    markers = (
        "authentication failed",
        "verify your email",
        "account verification",
        "email verification",
        "unauthorized",
        "forbidden",
        "invalid api key",
        "invalid credentials",
        "access denied",
    )
    return any(marker in lowered for marker in markers)


def _is_provider_configuration_error_message(message: str) -> bool:
    lowered = (message or "").lower()
    markers = (
        "api key is not configured",
        "credentials are not configured",
        "smtp configuration is missing",
        "provider credentials are not configured",
    )
    return any(marker in lowered for marker in markers)


def _increment_result_counters(stats: dict[str, Any], result: ProviderLookupResult) -> None:
    status = str(result.status or "").strip()
    if result.email or result.secondary_email:
        stats["matched"] += 1
        stats["sendable_rows"] += 1
        stats["provider_success_count"] += 1
        return
    if status == "preview_match":
        stats["preview_match"] += 1
    elif status == "missing_hr_link":
        stats["missing_hr_link"] += 1
    elif status == "invalid_hr_link":
        stats["invalid_hr_link"] += 1
    elif status == "profile_only":
        stats["profile_only"] += 1
    elif status == "lookup_quota_reached":
        stats["lookup_quota_reached"] += 1
    elif status == "authentication_failed":
        stats["authentication_failed"] += 1
    elif status == "provider_configuration_missing":
        stats["provider_configuration_blocked"] += 1
    else:
        stats["no_match"] += 1
    stats["no_email_count"] += 1


def _resolve_contact(
    contact: EnrichmentContact,
    providers: list,
    limiter: ProviderRateLimiter,
    *,
    run_id: str,
) -> tuple[ProviderLookupResult, list[ProviderAttemptLog], dict[str, Any]]:
    attempts: list[ProviderAttemptLog] = []
    saw_retryable = False
    saw_quota_failure = False
    saw_authentication_failure = False
    saw_configuration_block = False
    usable_provider_names = [provider.provider_name for provider in providers if _provider_is_usable(provider)]
    fallback_provider_names = [
        provider.provider_name
        for provider in providers
        if getattr(provider, "provider_name", "") != "rocketreach" and _provider_is_usable(provider)
    ]
    for index, provider in enumerate(providers, start=1):
        if not _provider_is_usable(provider):
            message = str(
                getattr(provider, "disabled_reason", "")
                or f"{provider.provider_name.title()} provider credentials are not configured."
            )
            attempts.append(
                ProviderAttemptLog(
                    provider=provider.provider_name,
                    attempt_number=index,
                    recruiter_fingerprint=contact.fingerprint,
                    status="provider_unconfigured",
                    retryable=False,
                    error=message,
                )
            )
            _structured_log(
                "provider_lookup_skipped_unconfigured",
                run_id=run_id,
                provider=provider.provider_name,
                recruiter_fingerprint=contact.fingerprint,
                attempt_number=index,
                error=message,
            )
            saw_configuration_block = True
            continue
        _structured_log(
            "provider_selected",
            run_id=run_id,
            provider=provider.provider_name,
            provider_class=provider.__class__.__name__,
            recruiter_fingerprint=contact.fingerprint,
        )
        try:
            wait_seconds = limiter.wait(provider.provider_name)
            if wait_seconds > 0:
                _structured_log(
                    "provider_rate_limit_wait",
                    run_id=run_id,
                    provider=provider.provider_name,
                    recruiter_fingerprint=contact.fingerprint,
                    wait_seconds=round(wait_seconds, 3),
                )
            result = provider.lookup(contact, limiter=limiter)
            attempts.append(
                ProviderAttemptLog(
                    provider=provider.provider_name,
                    attempt_number=index,
                    recruiter_fingerprint=contact.fingerprint,
                    status=result.status,
                    retryable=False,
                )
            )
            _structured_log(
                "provider_lookup_success",
                run_id=run_id,
                provider=provider.provider_name,
                recruiter_fingerprint=contact.fingerprint,
                attempt_number=index,
                final_status=result.status,
            )
            return result, attempts, {
                "saw_retryable": saw_retryable,
                "quota_blocked_no_fallback": False,
                "authentication_failed": False,
                "configuration_blocked": False,
                "usable_provider_names": usable_provider_names,
                "fallback_provider_names": fallback_provider_names,
            }
        except RetryableProviderError as error:
            saw_retryable = True
            if isinstance(error, (QuotaExceededError, RateLimitError)):
                saw_quota_failure = True
            attempts.append(
                ProviderAttemptLog(
                    provider=provider.provider_name,
                    attempt_number=index,
                    recruiter_fingerprint=contact.fingerprint,
                    status="retryable_error",
                    retryable=True,
                    error=str(error),
                )
            )
            _structured_log(
                "provider_lookup_retryable_error",
                run_id=run_id,
                provider=provider.provider_name,
                recruiter_fingerprint=contact.fingerprint,
                attempt_number=index,
                error=str(error),
            )
            continue
        except (MissingProfileDataError, NoEmailFoundError, NonRetryableProviderError) as error:
            error_message = str(error)
            if _is_authentication_error_message(error_message):
                saw_authentication_failure = True
            if _is_provider_configuration_error_message(error_message):
                saw_configuration_block = True
            attempts.append(
                ProviderAttemptLog(
                    provider=provider.provider_name,
                    attempt_number=index,
                    recruiter_fingerprint=contact.fingerprint,
                    status="non_retryable_error",
                    retryable=False,
                    error=str(error),
                )
            )
            _structured_log(
                "provider_lookup_non_retryable_error",
                run_id=run_id,
                provider=provider.provider_name,
                recruiter_fingerprint=contact.fingerprint,
                attempt_number=index,
                error=str(error),
            )
            continue
    resolution = {
        "saw_retryable": saw_retryable,
        "quota_blocked_no_fallback": False,
        "authentication_failed": saw_authentication_failure,
        "configuration_blocked": saw_configuration_block or not usable_provider_names,
        "usable_provider_names": usable_provider_names,
        "fallback_provider_names": fallback_provider_names,
    }
    if saw_quota_failure and not fallback_provider_names:
        resolution["quota_blocked_no_fallback"] = True
        resolution["configuration_blocked"] = True
        return (
            _final_result_for_error(
                "provider_fallback_chain",
                "lookup_quota_reached",
                (
                    "RocketReach quota/credits were exhausted and no Hunter or Apollo fallback API keys are configured. "
                    "Add RocketReach credits or configure HUNTER_API_KEY / APOLLO_API_KEY to continue."
                ),
            ),
            attempts,
            resolution,
        )
    if saw_authentication_failure:
        return (
            _final_result_for_error(
                "provider_fallback_chain",
                "authentication_failed",
                "Recruiter lookup stopped because provider authentication/account verification failed.",
            ),
            attempts,
            resolution,
        )
    if saw_retryable and usable_provider_names:
        return (
            _final_result_for_error(
                "provider_fallback_chain",
                "no_email_found",
                "All configured enrichment providers failed. Continuing without recruiter email.",
            ),
            attempts,
            resolution,
        )
    if saw_configuration_block and not usable_provider_names:
        return (
            _final_result_for_error(
                "provider_fallback_chain",
                "provider_configuration_missing",
                "Recruiter lookup needs at least one configured enrichment provider credential.",
            ),
            attempts,
            resolution,
        )
    return (
        _final_result_for_error(
            "provider_fallback_chain",
            "no_email_found",
            "No provider returned a deliverable email.",
        ),
        attempts,
        resolution,
    )


def enrich_contacts(
    record: dict,
    store: PipelineStore,
    *,
    finalize_retryable_failures: bool = False,
) -> dict[str, Any]:
    config = load_automation_config(record.get("config_path") or None)
    if not config.enrichment_sequential:
        raise RuntimeError("Only sequential enrichment is supported in this pipeline version.")
    providers = _build_provider_clients(record.get("config_path") or None)
    limiter = ProviderRateLimiter(config.provider_rate_limit_per_minute)
    input_path = Path(record["applied_csv_path"])
    text = input_path.read_text(encoding="utf-8-sig")
    reader = csv.DictReader(io.StringIO(text))
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=ENRICHED_RECRUITER_HEADERS, extrasaction="ignore")
    writer.writeheader()
    stats = {
        "total": 0,
        "matched": 0,
        "preview_match": 0,
        "failed": 0,
        "skipped": 0,
        "no_match": 0,
        "missing_hr_link": 0,
        "invalid_hr_link": 0,
        "profile_only": 0,
        "lookup_quota_reached": 0,
        "authentication_failed": 0,
        "sendable_rows": 0,
        "provider_success_count": 0,
        "no_email_count": 0,
        "provider_retry_count": 0,
        "provider_configuration_blocked": 0,
        "final_status": "",
        "final_reason": "",
        "attempt_logs": [],
    }
    retryable_pending = False
    for row in reader:
        if not _row_has_values(row):
            continue
        stats["total"] += 1
        contact = _build_contact(record["id"], row)
        cached = _lookup_from_cache(store, contact)
        if cached is not None:
            cached_attempts = int((store.get_enrichment_cache(contact.fingerprint) or {}).get("provider_attempts", 0) or 0)
            writer.writerow(_result_to_row(row, cached, cached_attempts))
            _increment_result_counters(stats, cached)
            continue
        resolved, attempts, resolution = _resolve_contact(contact, providers, limiter, run_id=record["id"])
        attempt_count = max(len(attempts), 1)
        provider_retry_count = sum(1 for attempt in attempts if attempt.retryable)
        stats["provider_retry_count"] += provider_retry_count
        _update_cache_from_result(
            store,
            record["id"],
            contact,
            resolved,
            attempt_count,
            provider_retry_count,
        )
        writer.writerow(_result_to_row(row, resolved, attempt_count))
        stats["attempt_logs"].extend([attempt.__dict__ for attempt in attempts])
        _increment_result_counters(stats, resolved)
        if not stats["final_status"]:
            if bool(resolution.get("authentication_failed")):
                stats["final_status"] = "waiting_review"
                stats["final_reason"] = (
                    "Recruiter lookup needs valid RocketReach/provider credentials before recruiter emails can be enriched."
                )
            elif bool(resolution.get("quota_blocked_no_fallback")) and int(stats.get("sendable_rows", 0) or 0) == 0:
                stats["final_status"] = "waiting_review"
                stats["final_reason"] = (
                    "RocketReach quota/credits were exhausted and Hunter/Apollo fallback credentials are missing. "
                    "Add RocketReach credits or configure HUNTER_API_KEY / APOLLO_API_KEY, then retry."
                )
            elif bool(resolution.get("configuration_blocked")) and int(stats.get("sendable_rows", 0) or 0) == 0:
                stats["final_status"] = "waiting_review"
                stats["final_reason"] = (
                    "Recruiter lookup could not continue because no fallback provider credentials are configured."
                )

    rr = load_rocketreach_module()
    written_path, output_note = rr.write_output_csv(record["recruiters_csv_path"], output.getvalue())
    stats["recruiters_csv_path"] = str(written_path)
    if output_note:
        stats["output_note"] = output_note
    if not stats["final_status"]:
        stats["final_status"] = "completed"
        if int(stats.get("sendable_rows", 0) or 0) == 0:
            stats["final_reason"] = "Recruiter enrichment finished without any sendable recruiter emails."
    stats["last_processed_at"] = utc_now_iso()
    return stats

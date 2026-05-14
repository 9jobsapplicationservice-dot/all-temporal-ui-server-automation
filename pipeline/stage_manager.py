from __future__ import annotations

from .adapters import LinkedInRuntimeUnavailableError, StageError, TransientStageError, preflight_linkedin_runtime, run_linkedin_stage, run_rocketreach_stage
from .config import AutomationConfig, AutomationConfigError, load_automation_config, load_automation_summary
from .constants import APPLIED_JOBS_HEADERS, ENRICHED_RECRUITER_HEADERS, MAX_ROCKETREACH_RETRIES
from .core.sentry_config import log_and_capture_error
from .emailer import send_run_emails
from .storage import PipelineStore
from .utils import csv_has_expected_header, csv_row_count, ensure_placeholder_recruiter_csv, recruiter_csv_is_placeholder, recruiter_sendable_row_count, utc_now_iso


def build_linkedin_note(summary: dict) -> str:
    jobs_applied = int(summary.get("jobs_applied", 0) or 0)
    rows_written = int(summary.get("rows_written_to_applied_csv", 0) or 0)
    external_links = int(summary.get("external_links_logged", 0) or 0)
    missing_hr = int(summary.get("rows_missing_hr_profile", 0) or 0)
    return (
        "LinkedIn stage completed. "
        f"jobs_applied={jobs_applied} rows_written={rows_written} "
        f"external_links={external_links} rows_missing_hr_profile={missing_hr}. "
        "Queued for RocketReach enrichment."
    )


def build_rocketreach_note(stats: dict) -> str:
    total = int(stats.get("total", 0) or 0)
    matched = int(stats.get("matched", 0) or 0)
    preview = int(stats.get("preview_match", 0) or 0)
    sendable = int(stats.get("sendable_rows", 0) or 0)
    missing = int(stats.get("missing_hr_link", 0) or 0)
    invalid = int(stats.get("invalid_hr_link", 0) or 0)
    profile_only = int(stats.get("profile_only", 0) or 0)
    no_match = int(stats.get("no_match", 0) or 0)
    quota = int(stats.get("lookup_quota_reached", 0) or 0)
    auth_failed = int(stats.get("authentication_failed", 0) or 0)
    provider_success = int(stats.get("provider_success_count", 0) or 0)
    no_email_count = int(stats.get("no_email_count", 0) or 0)
    provider_retry_count = int(stats.get("provider_retry_count", 0) or 0)
    provider_configuration_blocked = int(stats.get("provider_configuration_blocked", 0) or 0)
    final_reason = str(stats.get("final_reason", "") or "").strip()
    output_note = str(stats.get("output_note", "") or "").strip()
    if auth_failed > 0:
        note = (
            "Recruiter CSV generated without RocketReach contacts because authentication failed. "
            f"total={total} authentication_failed={auth_failed}."
        )
        if output_note:
            note = f"{note} {output_note}"
        return note
    if sendable == 0:
        if quota > 0:
            skip_reason = "RocketReach lookup quota/credit/account verification blocked email lookup."
        elif provider_configuration_blocked > 0:
            skip_reason = "No usable fallback provider credentials were configured."
        elif missing > 0 and no_match == 0 and profile_only == 0 and preview == 0:
            skip_reason = "LinkedIn did not provide recruiter profile links for enrichment."
        elif no_match > 0:
            skip_reason = "RocketReach returned no matching recruiter emails."
        elif preview > 0:
            skip_reason = "RocketReach returned preview/masked contacts only."
        elif profile_only > 0:
            skip_reason = "RocketReach returned profiles without usable emails."
        else:
            skip_reason = "RocketReach returned no sendable emails."
        note = (
            f"Contacts enriched with no sendable emails. {skip_reason} "
            f"total={total} matched={matched} missing_hr_link={missing} invalid_hr_link={invalid} "
            f"preview_match={preview} profile_only={profile_only} no_match={no_match} lookup_quota_reached={quota} "
            f"provider_success_count={provider_success} no_email_count={no_email_count} provider_retry_count={provider_retry_count} "
            f"provider_configuration_blocked={provider_configuration_blocked}."
        )
        if final_reason:
            note = f"{note} {final_reason}"
        if output_note:
            note = f"{note} {output_note}"
        return note
    note = (
        "Contacts enriched successfully. Ready for automated email sending. "
        f"total={total} matched={matched} sendable_rows={sendable} missing_hr_link={missing} "
        f"invalid_hr_link={invalid} preview_match={preview} profile_only={profile_only} no_match={no_match} lookup_quota_reached={quota} "
        f"provider_success_count={provider_success} no_email_count={no_email_count} provider_retry_count={provider_retry_count}."
    )
    if output_note:
        note = f"{note} {output_note}"
    return note


def build_email_note(stats: dict[str, object]) -> str:
    total = int(stats.get("email_total", 0) or 0)
    sent = int(stats.get("email_sent", 0) or 0)
    failed = int(stats.get("email_failed", 0) or 0)
    if failed > 0:
        return f"Automated email stage finished with failures. total={total} sent={sent} failed={failed}."
    return f"Automated email stage completed successfully. total={total} sent={sent} failed={failed}."


def build_email_waiting_review_note(stats: dict[str, object], reason: str | None = None) -> str:
    total = int(stats.get("email_total", 0) or 0)
    sent = int(stats.get("email_sent", 0) or 0)
    failed = int(stats.get("email_failed", 0) or 0)
    if reason:
        return f"{reason} total={total} sent={sent} failed={failed}."
    return f"Automatic sending paused. total={total} sent={sent} failed={failed}."


def is_waiting_login_error(message: str) -> bool:
    lowered = (message or "").lower()
    markers = (
        "linkedin login was not confirmed",
        "browser window closed or session became invalid",
        "complete manual login in chrome and keep the browser window open",
        "chrome default profile crashed while linkedin was opening",
        "chrome startup needs manual recovery",
        "automatic linkedin login did not complete successfully",
        "session was blocked by linkedin",
        "captcha",
        "checkpoint",
        "2fa",
        "login page/form was unavailable",
    )
    return any(marker in lowered for marker in markers)


def build_waiting_login_note(automation_summary: dict | None, last_error: str | None = None) -> str:
    linkedin_summary = (automation_summary or {}).get("linkedin") if isinstance(automation_summary, dict) else None
    mode = linkedin_summary.get("mode") if isinstance(linkedin_summary, dict) else None
    lowered = (last_error or "").lower()
    if "captcha" in lowered or "checkpoint" in lowered or "2fa" in lowered or "blocked by linkedin" in lowered:
        return "LinkedIn needs manual verification in Chrome. Complete the captcha/checkpoint/2FA, keep the browser open, then resume this run."
    if "automatic linkedin login did not complete successfully" in lowered or mode == "auto_login":
        return "LinkedIn auto-login needs manual confirmation in Chrome. Complete login there and keep the browser window open."
    return "Chrome opened with your default profile. Log into LinkedIn there and keep the browser window open."


class PipelineStageManager:
    def __init__(self, store: PipelineStore) -> None:
        self.store = store

    def process_run(self, run_id: str) -> dict:
        try:
            record = self.store.get_run(run_id)
            has_started_stage = bool((record.get("stage_started_at") or "").strip())

            if has_started_stage and csv_has_expected_header(record["recruiters_csv_path"], ENRICHED_RECRUITER_HEADERS):
                if recruiter_csv_is_placeholder(record["recruiters_csv_path"]):
                    return self.run_rocketreach(run_id)
                return self.resume_from_recruiters(run_id)

            if has_started_stage and csv_has_expected_header(record["applied_csv_path"], APPLIED_JOBS_HEADERS):
                if csv_row_count(record["applied_csv_path"]) == 0:
                    self.store.update_run(
                        run_id,
                        status="failed",
                        note="LinkedIn output exists but contains zero saved applied rows.",
                        last_error="No applied jobs were written to applied_jobs.csv.",
                        stage_finished_at=utc_now_iso(),
                    )
                    log_and_capture_error(
                        StageError("No applied jobs were written to applied_jobs.csv."),
                        message="LinkedIn output exists but contains zero saved applied rows.",
                        tags={"run_id": run_id, "stage": "linkedin"},
                    )
                return self.run_rocketreach(run_id)

            linkedin_result = self.run_linkedin(run_id)
            if linkedin_result["status"] in {"failed", "blocked_runtime", "waiting_login", "completed"}:
                return linkedin_result
            return self.run_rocketreach(run_id)
        except PermissionError as error:
            locked_path = getattr(error, "filename", "") or "pipeline CSV file"
            self.store.update_run(
                run_id,
                status="failed",
                note="Pipeline file is locked.",
                last_error=f"Close Excel or any other app using '{locked_path}' and retry the run.",
                stage_finished_at=utc_now_iso(),
            )
            log_and_capture_error(
                error,
                message="Pipeline file is locked.",
                tags={"run_id": run_id, "stage": "pipeline"},
                extras={"locked_path": locked_path},
            )

    def resume_blocked_runtime_runs(self) -> list[dict]:
        preflight = preflight_linkedin_runtime()
        if not preflight.is_available:
            return []

        resumed: list[dict] = []
        for record in self.store.list_runs():
            if record["status"] != "blocked_runtime":
                continue
            resumed.append(
                self.store.update_run(
                    record["id"],
                    status="queued",
                    note="LinkedIn runtime is now available. Requeued automatically.",
                    last_error="",
                    stage_finished_at="",
                )
            )
        return resumed

    def run_linkedin(self, run_id: str) -> dict:
        preflight = preflight_linkedin_runtime()
        automation_summary = load_automation_summary(self.store.get_run(run_id).get("config_path") or None)
        if not preflight.is_available:
            blocked_reason = preflight.blocked_reason or "LinkedIn runtime setup required."
            self.store.update_run(
                run_id,
                status="blocked_runtime",
                note="LinkedIn runtime setup required.",
                last_error=blocked_reason,
                stage_finished_at=utc_now_iso(),
            )
            log_and_capture_error(
                LinkedInRuntimeUnavailableError(blocked_reason),
                message="LinkedIn runtime setup required.",
                tags={"run_id": run_id, "stage": "linkedin"},
            )

        self.store.reset_live_artifacts_for_run(run_id)
        record = self.store.update_run(
            run_id,
            status="linkedin_running",
            note="Running LinkedIn job application stage.",
            last_error="",
            stage_started_at=utc_now_iso(),
            stage_finished_at="",
        )
        try:
            try:
                linkedin_stats = run_linkedin_stage(record, python_executable=preflight.executable)
            except TypeError as error:
                # Preserve compatibility with tests or callers that patch the stage with a simpler signature.
                if "python_executable" not in str(error):
                    raise
                linkedin_stats = run_linkedin_stage(record)
        except StageError as error:
            if csv_has_expected_header(record["applied_csv_path"], APPLIED_JOBS_HEADERS):
                recovered_rows = csv_row_count(record["applied_csv_path"])
                if recovered_rows > 0:
                    ensure_placeholder_recruiter_csv(
                        record["applied_csv_path"],
                        record["recruiters_csv_path"],
                        ENRICHED_RECRUITER_HEADERS,
                        status="pending_enrichment",
                    )
                    self.store.update_run(
                        run_id,
                        status="queued",
                        note=(
                            "LinkedIn stage ended with an error after saving applied jobs. "
                            f"Recovered {recovered_rows} applied row(s) and queued RocketReach enrichment."
                        ),
                        last_error=str(error),
                        retry_count=0,
                        stage_finished_at=utc_now_iso(),
                    )
                    log_and_capture_error(
                        error,
                        message="LinkedIn stage failed after saving applied jobs.",
                        tags={"run_id": run_id, "stage": "linkedin"},
                        extras={"recovered_rows": recovered_rows},
                    )
            if is_waiting_login_error(str(error)):
                self.store.update_run(
                    run_id,
                    status="waiting_login",
                    note=build_waiting_login_note(automation_summary, str(error)),
                    last_error=str(error),
                    retry_count=0,
                    stage_finished_at=utc_now_iso(),
                )
                log_and_capture_error(
                    error,
                    message="LinkedIn stage is waiting for login/manual verification.",
                    tags={"run_id": run_id, "stage": "linkedin"},
                )
            self.store.update_run(
                run_id,
                status="failed",
                note="LinkedIn stage failed.",
                last_error=str(error),
                stage_finished_at=utc_now_iso(),
            )
            log_and_capture_error(
                error,
                message="LinkedIn stage failed.",
                tags={"run_id": run_id, "stage": "linkedin"},
            )

        rows_written = int(linkedin_stats.get("rows_written_to_applied_csv", 0) or 0)
        jobs_applied = int(linkedin_stats.get("jobs_applied", 0) or 0)
        failed_jobs = int(linkedin_stats.get("failed_jobs", 0) or 0)
        if rows_written == 0:
            if jobs_applied == 0:
                if failed_jobs > 0:
                    self.store.update_run(
                        run_id,
                        status="failed",
                        note=(
                            "LinkedIn stage completed without a confirmed submission. "
                            f"Skipped {failed_jobs} failed Easy Apply attempt(s); no jobs were queued for enrichment."
                        ),
                        last_error=(
                            f"LinkedIn automation finished with 0 submitted jobs and {failed_jobs} failed Easy Apply attempt(s). "
                            "The failed jobs were logged and the run was allowed to continue instead of crashing the whole pipeline."
                        ),
                        retry_count=0,
                        stage_finished_at=utc_now_iso(),
                    )
                    log_and_capture_error(
                        StageError(f"LinkedIn automation finished with 0 submitted jobs and {failed_jobs} failed Easy Apply attempt(s)."),
                        message="LinkedIn Easy Apply attempts failed without confirmed submissions.",
                        tags={"run_id": run_id, "stage": "linkedin"},
                        extras={"failed_jobs": failed_jobs},
                    )
                return self.store.update_run(
                    run_id,
                    status="completed",
                    note="LinkedIn stage completed with no confirmed Easy Apply submissions. Nothing was queued for enrichment.",
                    last_error="",
                    retry_count=0,
                    stage_finished_at=utc_now_iso(),
                )

            self.store.update_run(
                run_id,
                status="failed",
                note="LinkedIn stage submitted jobs but wrote zero rows to applied_jobs.csv.",
                last_error="Confirmed applications were not persisted to applied_jobs.csv.",
                retry_count=0,
                stage_finished_at=utc_now_iso(),
            )
            log_and_capture_error(
                StageError("Confirmed applications were not persisted to applied_jobs.csv."),
                message="LinkedIn stage submitted jobs but wrote zero rows to applied_jobs.csv.",
                tags={"run_id": run_id, "stage": "linkedin"},
                extras={"jobs_applied": jobs_applied},
            )

        ensure_placeholder_recruiter_csv(
            record["applied_csv_path"],
            record["recruiters_csv_path"],
            ENRICHED_RECRUITER_HEADERS,
            status="pending_enrichment",
        )

        return self.store.update_run(
            run_id,
            status="queued",
            note=build_linkedin_note(linkedin_stats),
            last_error="",
            retry_count=0,
            stage_finished_at=utc_now_iso(),
        )

    def run_rocketreach(self, run_id: str) -> dict:
        record = self.store.update_run(
            run_id,
            status="rocketreach_running",
            note="Running RocketReach enrichment stage.",
            last_error="",
            stage_started_at=utc_now_iso(),
            stage_finished_at="",
        )
        try:
            rocketreach_stats = run_rocketreach_stage(record)
        except TransientStageError as error:
            updated_retry_count = record["retry_count"] + 1
            if updated_retry_count <= MAX_ROCKETREACH_RETRIES:
                self.store.update_run(
                    run_id,
                    status="queued",
                    retry_count=updated_retry_count,
                    note=f"RocketReach transient failure. Retry {updated_retry_count}/{MAX_ROCKETREACH_RETRIES} queued.",
                    last_error=str(error),
                    stage_finished_at=utc_now_iso(),
                )
                log_and_capture_error(
                    error,
                    message="RocketReach transient failure.",
                    tags={"run_id": run_id, "stage": "rocketreach"},
                    extras={"retry_count": updated_retry_count},
                )
            try:
                rocketreach_stats = run_rocketreach_stage(
                    self.store.get_run(run_id),
                    finalize_retryable_failures=True,
                )
            except StageError as final_error:
                self.store.update_run(
                    run_id,
                    status="failed",
                    retry_count=updated_retry_count,
                    note="RocketReach retries exhausted.",
                    last_error=str(final_error),
                    stage_finished_at=utc_now_iso(),
                )
                log_and_capture_error(
                    final_error,
                    message="RocketReach retries exhausted.",
                    tags={"run_id": run_id, "stage": "rocketreach"},
                    extras={"retry_count": updated_retry_count},
                )
            finalized_record = self.store.update_run(
                run_id,
                retry_count=updated_retry_count,
                last_error=str(error),
            )
            return self._finalize_rocketreach_success(run_id, finalized_record, rocketreach_stats)
        except StageError as error:
            self.store.update_run(
                run_id,
                status="failed",
                note="RocketReach stage failed.",
                last_error=str(error),
                stage_finished_at=utc_now_iso(),
            )
            log_and_capture_error(
                error,
                message="RocketReach stage failed.",
                tags={"run_id": run_id, "stage": "rocketreach"},
            )

        return self._finalize_rocketreach_success(run_id, record, rocketreach_stats)

    def handle_rocketreach_success(self, run_id: str, rocketreach_stats: dict, config: AutomationConfig | None = None) -> dict:
        record = self.store.get_run(run_id)
        return self._finalize_rocketreach_success(run_id, record, rocketreach_stats, config=config)

    def _finalize_rocketreach_success(
        self,
        run_id: str,
        record: dict,
        rocketreach_stats: dict,
        config: AutomationConfig | None = None,
    ) -> dict:
        recruiters_csv_path = rocketreach_stats.get("recruiters_csv_path")
        if recruiters_csv_path and recruiters_csv_path != record["recruiters_csv_path"]:
            record = self.store.update_run(
                run_id,
                recruiters_csv_path=recruiters_csv_path,
            )

        if int(rocketreach_stats.get("sendable_rows", 0) or 0) == 0:
            final_status = str(rocketreach_stats.get("final_status") or "completed").strip() or "completed"
            final_reason = str(rocketreach_stats.get("final_reason") or "").strip()
            final_note = build_rocketreach_note(rocketreach_stats)
            if final_reason and final_reason not in final_note:
                final_note = f"{final_note} {final_reason}"
            elif not final_reason:
                final_note = (
                    f"{final_note} Automatic email sending was skipped because no recruiter emails were sendable."
                )
            return self.store.update_run(
                run_id,
                status="waiting_review" if final_status == "waiting_review" else "completed",
                note=final_note,
                last_error="",
                retry_count=0,
                email_total=0,
                email_sent=0,
                email_failed=0,
                provider_success_count=int(rocketreach_stats.get("provider_success_count", 0) or 0),
                no_email_count=int(rocketreach_stats.get("no_email_count", 0) or 0),
                provider_retry_count=int(rocketreach_stats.get("provider_retry_count", 0) or 0),
                stage_finished_at=utc_now_iso(),
            )

        self.store.update_run(
            run_id,
            status="queued",
            note=build_rocketreach_note(rocketreach_stats),
            last_error="",
            retry_count=0,
            provider_success_count=int(rocketreach_stats.get("provider_success_count", 0) or 0),
            no_email_count=int(rocketreach_stats.get("no_email_count", 0) or 0),
            provider_retry_count=int(rocketreach_stats.get("provider_retry_count", 0) or 0),
            stage_finished_at=utc_now_iso(),
        )
        return self.run_email(run_id, config=config)

    def resume_from_recruiters(self, run_id: str) -> dict:
        record = self.store.get_run(run_id)
        recruiters_rows = csv_row_count(record["recruiters_csv_path"])
        if recruiters_rows == 0:
            return self.store.update_run(
                run_id,
                status="completed",
                note="Recruiter enrichment file exists but has zero data rows. Nothing to send.",
                last_error="",
                email_total=0,
                email_sent=0,
                email_failed=0,
                stage_finished_at=utc_now_iso(),
            )

        sendable_rows = recruiter_sendable_row_count(record["recruiters_csv_path"])
        if sendable_rows == 0:
            return self.store.update_run(
                run_id,
                status="completed",
                note="Recruiter enrichment already present but no sendable contacts were found. Automatic email sending was skipped.",
                last_error="",
                email_total=0,
                email_sent=0,
                email_failed=0,
                stage_finished_at=utc_now_iso(),
            )

        try:
            config = load_automation_config(record.get("config_path") or None)
        except AutomationConfigError as error:
            self.store.update_run(
                run_id,
                status="failed",
                note="Automation config is invalid.",
                last_error=str(error),
                stage_finished_at=utc_now_iso(),
            )
            log_and_capture_error(
                error,
                message="Automation config is invalid.",
                tags={"run_id": run_id, "stage": "email"},
            )

        if not config.auto_send:
            return self.store.update_run(
                run_id,
                status="waiting_review",
                note=(config.auto_send_reason or "Automatic sending is disabled. Waiting for manual email review."),
                last_error="",
                stage_finished_at=utc_now_iso(),
            )
        return self.run_email(run_id, config=config)

    def run_email(self, run_id: str, config: AutomationConfig | None = None) -> dict:
        record = self.store.get_run(run_id)
        try:
            resolved_config = config or load_automation_config(record.get("config_path") or None)
        except AutomationConfigError as error:
            self.store.update_run(
                run_id,
                status="failed",
                note="Automation config is invalid.",
                last_error=str(error),
                stage_finished_at=utc_now_iso(),
            )
            log_and_capture_error(
                error,
                message="Automation config is invalid.",
                tags={"run_id": run_id, "stage": "email"},
            )

        if not resolved_config.auto_send:
            return self.store.update_run(
                run_id,
                status="waiting_review",
                note=(resolved_config.auto_send_reason or "Automatic sending is disabled. Waiting for manual email review."),
                last_error="",
                stage_finished_at=utc_now_iso(),
            )

        record = self.store.update_run(
            run_id,
            status="email_running",
            note="Running automated email stage.",
            last_error="",
            stage_started_at=utc_now_iso(),
            stage_finished_at="",
        )
        try:
            result = send_run_emails(record, resolved_config)
        except Exception as error:
            self.store.update_run(
                run_id,
                status="failed",
                note="Email stage failed.",
                last_error=str(error),
                stage_finished_at=utc_now_iso(),
            )
            log_and_capture_error(
                error,
                message="Email stage failed.",
                tags={"run_id": run_id, "stage": "email"},
            )
        auth_failures = int(result.get("auth_failure_count", 0) or 0)
        if auth_failures > 0:
            self.store.update_run(
                run_id,
                status="waiting_review",
                note=build_email_waiting_review_note(
                    result,
                    reason="SMTP authentication failed. Automatic sending has been paused until credentials are fixed.",
                ),
                last_error="",
                email_total=int(result["email_total"]),
                email_sent=int(result["email_sent"]),
                email_failed=int(result["email_failed"]),
                stage_finished_at=utc_now_iso(),
            )
            log_and_capture_error(
                StageError("SMTP authentication failed."),
                message="SMTP authentication failed.",
                tags={"run_id": run_id, "stage": "email"},
                extras={"auth_failure_count": auth_failures},
            )
        next_status = "failed" if int(result["email_failed"]) > 0 else "completed"
        final = self.store.update_run(
            run_id,
            status=next_status,
            note=build_email_note(result),
            last_error="" if next_status == "completed" else "One or more automated emails failed.",
            email_total=int(result["email_total"]),
            email_sent=int(result["email_sent"]),
            email_failed=int(result["email_failed"]),
            stage_finished_at=utc_now_iso(),
        )
        if next_status == "failed":
            log_and_capture_error(
                StageError("One or more automated emails failed."),
                message="Email stage finished with failures.",
                tags={"run_id": run_id, "stage": "email"},
                extras={"email_failed": int(result["email_failed"])},
            )
        return final

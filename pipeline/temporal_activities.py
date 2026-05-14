from __future__ import annotations

import logging

from .adapters import StageError, TransientStageError, run_rocketreach_stage
from .config import AutomationConfigError, load_automation_config
from .constants import APPLIED_JOBS_HEADERS, ENRICHED_RECRUITER_HEADERS, MAX_EMAIL_STAGE_RETRIES, MAX_ROCKETREACH_RETRIES
from .core.sentry_config import build_temporal_tags, capture_exception_with_context, capture_live_message, log_and_capture_error
from .emailer import send_run_emails
from .stage_manager import PipelineStageManager, build_email_note, build_email_waiting_review_note
from .storage import PipelineStore
from .temporal_sdk import ApplicationError, activity
from .temporal_types import TemporalActivityInput, TemporalActivityResult
from .utils import csv_has_expected_header, csv_row_count, recruiter_csv_is_placeholder, recruiter_sendable_row_count, utc_now_iso

logger = logging.getLogger(__name__)


def _store(root: str | None) -> PipelineStore:
    return PipelineStore(root)


def _manager(root: str | None) -> PipelineStageManager:
    return PipelineStageManager(_store(root))


def _activity_retry_count(record: dict, increment: bool = False) -> int:
    current = int(record.get("retry_count", 0) or 0)
    return current + 1 if increment else current


def _activity_attempt() -> int:
    try:
        return int(getattr(activity.info(), "attempt", 1) or 1)
    except RuntimeError:
        return 1


def _activity_tags(payload: TemporalActivityInput, stage: str) -> dict[str, str]:
    try:
        info = activity.info()
    except RuntimeError:
        return build_temporal_tags(run_id=payload.run_id, stage=stage)
    return build_temporal_tags(
        workflow_id=getattr(info, "workflow_id", None),
        workflow_type=getattr(info, "workflow_type", None),
        task_queue=getattr(info, "task_queue", None),
        activity_name=getattr(info, "activity_type", None),
        attempt=getattr(info, "attempt", None),
        run_id=payload.run_id,
        stage=stage,
    )


@activity.defn
def linkedin_activity(payload: TemporalActivityInput) -> TemporalActivityResult:
    tags = _activity_tags(payload, "linkedin")
    capture_live_message("Stage started", level="info", tags=tags)
    store = _store(payload.root)
    record = store.get_run(payload.run_id)
    if not payload.fresh and csv_has_expected_header(record["applied_csv_path"], APPLIED_JOBS_HEADERS):
        applied_rows = csv_row_count(record["applied_csv_path"])
        if applied_rows > 0:
            resumed = store.update_run(
                payload.run_id,
                status="queued",
                note=f"LinkedIn already completed for this run. Resuming RocketReach with {applied_rows} applied row(s); Chrome will not reopen.",
                last_error="",
            )
            return TemporalActivityResult.from_record(resumed, outcome="success", current_stage="linkedin")

    manager = PipelineStageManager(store)
    record = manager.run_linkedin(payload.run_id)
    result = TemporalActivityResult.from_record(record)
    if result.status in {"blocked_runtime", "waiting_login", "failed"}:
        log_and_capture_error(
            StageError(result.last_error or result.note or "LinkedIn activity failed."),
            message="LinkedIn activity failed.",
            tags=tags,
            extras={"status": result.status, "note": result.note},
        )
    return TemporalActivityResult.from_record(record, outcome="success", current_stage="linkedin")


@activity.defn
def rocketreach_activity(payload: TemporalActivityInput) -> TemporalActivityResult:
    tags = _activity_tags(payload, "rocketreach")
    capture_live_message("Stage started", level="info", tags=tags)
    store = _store(payload.root)
    manager = PipelineStageManager(store)
    attempt = _activity_attempt()
    existing_record = store.get_run(payload.run_id)
    if csv_has_expected_header(existing_record["recruiters_csv_path"], ENRICHED_RECRUITER_HEADERS):
        if recruiter_csv_is_placeholder(existing_record["recruiters_csv_path"]):
            store.update_run(
                payload.run_id,
                status="queued",
                note="LinkedIn artifacts are ready. Running RocketReach enrichment now.",
                last_error="",
            )
        else:
            sendable_rows = recruiter_sendable_row_count(existing_record["recruiters_csv_path"])
            if sendable_rows > 0:
                resumed = store.update_run(
                    payload.run_id,
                    status="queued",
                    note="Recruiter enrichment already exists. Resuming email/review stage without rerunning RocketReach.",
                    last_error="",
                    stage_finished_at=utc_now_iso(),
                )
                return TemporalActivityResult.from_record(
                    resumed,
                    sendable_rows=sendable_rows,
                    outcome="success",
                    current_stage="rocketreach",
                )
            completed = store.update_run(
                payload.run_id,
                status="completed",
                note="Recruiter enrichment already exists with no sendable contacts. Automatic email sending remains skipped.",
                last_error="",
                email_total=0,
                email_sent=0,
                email_failed=0,
                stage_finished_at=utc_now_iso(),
            )
            return TemporalActivityResult.from_record(
                completed,
                sendable_rows=0,
                outcome="success",
                current_stage="rocketreach",
            )

    record = store.update_run(
        payload.run_id,
        status="rocketreach_running",
        note="Running RocketReach enrichment stage.",
        last_error="",
        retry_count=attempt,
        stage_started_at=utc_now_iso(),
        stage_finished_at="",
        last_failed_stage="rocketreach",
    )
    try:
        stats = run_rocketreach_stage(record)
    except TransientStageError as error:
        logger.warning(
            "RocketReach activity retryable failure. run_id=%s attempt=%s reason=%s",
            payload.run_id,
            attempt,
            error,
        )
        if attempt < MAX_ROCKETREACH_RETRIES:
            store.update_run(
                payload.run_id,
                status="queued",
                retry_count=attempt,
                note=f"RocketReach retryable failure. Temporal retry {attempt}/{MAX_ROCKETREACH_RETRIES} scheduled.",
                last_error=str(error),
                provider_retry_count=attempt,
                stage_finished_at=utc_now_iso(),
                last_failed_stage="rocketreach",
            )
            log_and_capture_error(
                error,
                message="RocketReach activity retryable failure.",
                tags=tags,
                extras={"attempt": attempt, "max_retries": MAX_ROCKETREACH_RETRIES},
            )
        stats = run_rocketreach_stage(record, finalize_retryable_failures=True)
    except StageError as error:
        status = "waiting_review" if "credential" in str(error).lower() or "authentication" in str(error).lower() else "completed"
        store.update_run(
            payload.run_id,
            status=status,
            note="Email enrichment finished without a deliverable email because providers returned terminal failures.",
            last_error=str(error),
            no_email_count=max(int(existing_record.get("no_email_count", 0) or 0), 1),
            stage_finished_at=utc_now_iso(),
            last_failed_stage="rocketreach",
        )
        log_and_capture_error(
            error,
            message="RocketReach activity failed.",
            tags=tags,
            extras={"config_path": payload.config_path or "", "fresh": payload.fresh, "attempt": attempt},
        )

    record = manager.handle_rocketreach_success(payload.run_id, stats)
    return TemporalActivityResult.from_record(
        record,
        sendable_rows=int(stats.get("sendable_rows", 0) or 0),
        outcome="success",
        current_stage="rocketreach",
        retry_count=attempt,
        metadata={
            "provider_success_count": int(stats.get("provider_success_count", 0) or 0),
            "no_email_count": int(stats.get("no_email_count", 0) or 0),
            "provider_retry_count": int(stats.get("provider_retry_count", 0) or 0),
        },
    )


@activity.defn
def email_activity(payload: TemporalActivityInput) -> TemporalActivityResult:
    tags = _activity_tags(payload, "email")
    capture_live_message("Stage started", level="info", tags=tags)
    store = _store(payload.root)
    manager = PipelineStageManager(store)
    record = store.get_run(payload.run_id)
    attempt = _activity_attempt()
    try:
        config = load_automation_config(payload.config_path or record.get("config_path") or None)
    except AutomationConfigError as error:
        store.update_run(
            payload.run_id,
            status="failed",
            note="Automation config is invalid.",
            last_error=str(error),
            stage_finished_at=utc_now_iso(),
        )
        log_and_capture_error(
            error,
            message="Automation config is invalid.",
            tags=tags,
        )

    if not config.auto_send:
        waiting = store.update_run(
            payload.run_id,
            status="waiting_review",
            note=(config.auto_send_reason or "Automatic sending is disabled. Waiting for manual email review."),
            last_error="",
            stage_finished_at=utc_now_iso(),
        )
        return TemporalActivityResult.from_record(
            waiting,
            auto_send=False,
            outcome="success",
            current_stage="email",
        )

    retry_count = _activity_retry_count(record)
    running = store.update_run(
        payload.run_id,
        status="email_running",
        note="Running automated email stage.",
        last_error="",
        retry_count=attempt,
        stage_started_at=utc_now_iso(),
        stage_finished_at="",
        last_failed_stage="email",
    )
    email_result = send_run_emails(running, config)
    auth_failures = int(email_result.get("auth_failure_count", 0) or 0)
    if auth_failures > 0:
        store.update_run(
            payload.run_id,
            status="waiting_review",
            note=build_email_waiting_review_note(
                email_result,
                reason="SMTP authentication failed. Automatic sending has been paused until credentials are fixed.",
            ),
            last_error="",
            email_total=int(email_result["email_total"]),
            email_sent=int(email_result["email_sent"]),
            email_failed=int(email_result["email_failed"]),
            retry_count=attempt,
            stage_finished_at=utc_now_iso(),
        )
        log_and_capture_error(
            StageError("SMTP authentication failed."),
            message="SMTP authentication failed.",
            tags=tags,
            extras={"auth_failure_count": auth_failures, "attempt": attempt},
        )
    transient_only = (
        int(email_result.get("email_failed", 0) or 0) > 0
        and int(email_result.get("transient_failure_count", 0) or 0) > 0
        and int(email_result.get("permanent_failure_count", 0) or 0) == 0
    )
    if transient_only and attempt < MAX_EMAIL_STAGE_RETRIES:
        logger.error(
            "Email activity transient failure. run_id=%s attempt=%s",
            payload.run_id,
            attempt,
        )
        store.update_run(
            payload.run_id,
            status="queued",
            retry_count=attempt,
            note=f"Email stage transient failure. Temporal retry {attempt}/{MAX_EMAIL_STAGE_RETRIES} scheduled.",
            last_error="One or more automated emails failed.",
            stage_finished_at=utc_now_iso(),
        )
        log_and_capture_error(
            ApplicationError("Email stage transient failure."),
            message="Email activity transient failure.",
            tags=tags,
            extras={"attempt": attempt, "max_retries": MAX_EMAIL_STAGE_RETRIES},
        )

    if transient_only and attempt >= MAX_EMAIL_STAGE_RETRIES:
        store.update_run(
            payload.run_id,
            status="waiting_review",
            retry_count=attempt,
            note="Email retries exhausted. Waiting for manual review.",
            last_error="",
            stage_finished_at=utc_now_iso(),
        )
        log_and_capture_error(
            StageError("Email retries exhausted."),
            message="Email retries exhausted.",
            tags=tags,
            extras={"attempt": attempt, "max_retries": MAX_EMAIL_STAGE_RETRIES},
        )

    next_status = "failed" if int(email_result.get("email_failed", 0) or 0) > 0 else "completed"
    final = store.update_run(
        payload.run_id,
        status=next_status,
        note=build_email_note(email_result),
        last_error="" if next_status == "completed" else "One or more automated emails failed.",
        email_total=int(email_result["email_total"]),
        email_sent=int(email_result["email_sent"]),
        email_failed=int(email_result["email_failed"]),
        retry_count=0 if next_status == "completed" else attempt,
        stage_finished_at=utc_now_iso(),
    )
    outcome = "success" if next_status in {"completed", "waiting_review"} else "terminal_failure"
    if next_status == "failed":
        log_and_capture_error(
            StageError("One or more automated emails failed."),
            message="Email activity failed.",
            tags=tags,
            extras={"email_failed": int(email_result["email_failed"]), "attempt": attempt},
        )
    return TemporalActivityResult.from_record(
        final,
        auto_send=True,
        outcome=outcome,
        current_stage="email",
    )

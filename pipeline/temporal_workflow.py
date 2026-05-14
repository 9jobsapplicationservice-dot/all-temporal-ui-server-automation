from __future__ import annotations

from datetime import timedelta

from .constants import MAX_EMAIL_STAGE_RETRIES, MAX_ROCKETREACH_RETRIES
from .temporal_sdk import RetryPolicy, workflow
from .temporal_types import TemporalActivityInput, TemporalActivityResult, TemporalWorkflowInput, TemporalWorkflowResult


# Keep workflow imports sandbox-safe by avoiding runtime-heavy pipeline modules here.
LINKEDIN_ACTIVITY_NAME = "linkedin_activity"
ROCKETREACH_ACTIVITY_NAME = "rocketreach_activity"
EMAIL_ACTIVITY_NAME = "email_activity"
MAX_LINKEDIN_STAGE_RETRIES = 2
RETRYABLE_STAGE_INITIAL_INTERVAL_SECONDS = 10
RETRYABLE_STAGE_BACKOFF_COEFFICIENT = 3.0
RETRYABLE_STAGE_MAXIMUM_INTERVAL_MINUTES = 2


def _activity_result(payload: object) -> TemporalActivityResult:
    if isinstance(payload, TemporalActivityResult):
        return payload
    if isinstance(payload, dict):
        return TemporalActivityResult(
            run_id=str(payload.get("run_id") or ""),
            status=str(payload.get("status") or ""),
            note=str(payload.get("note") or ""),
            last_error=str(payload.get("last_error") or ""),
            sendable_rows=int(payload.get("sendable_rows", 0) or 0),
            auto_send=payload.get("auto_send"),
            outcome=str(payload.get("outcome") or "success"),
            current_stage=str(payload.get("current_stage") or ""),
            retry_count=int(payload.get("retry_count", 0) or 0),
            metadata=payload.get("metadata") if isinstance(payload.get("metadata"), dict) else None,
        )
    raise TypeError(f"Unsupported Temporal activity result type: {type(payload)!r}")


def _workflow_result(
    payload: TemporalWorkflowInput,
    workflow_info: object,
    *,
    status: str,
    note: str,
    current_stage: str,
    retry_counts: dict[str, int],
) -> TemporalWorkflowResult:
    return TemporalWorkflowResult(
        run_id=payload.run_id,
        status=status,
        note=note,
        task_queue=str(getattr(workflow_info, "task_queue", "") or ""),
        workflow_id=str(getattr(workflow_info, "workflow_id", "") or ""),
        current_stage=current_stage,
        linkedin_retry_count=int(retry_counts.get("linkedin", 0) or 0),
        rocketreach_retry_count=int(retry_counts.get("rocketreach", 0) or 0),
        email_retry_count=int(retry_counts.get("email", 0) or 0),
        metadata=None,
    )


def _stage_retry_policy(stage: str) -> RetryPolicy:
    if stage == "linkedin":
        return RetryPolicy(maximum_attempts=1)
    max_attempts = {
        "rocketreach": MAX_ROCKETREACH_RETRIES,
        "email": MAX_EMAIL_STAGE_RETRIES,
    }[stage]
    return RetryPolicy(
        initial_interval=timedelta(seconds=RETRYABLE_STAGE_INITIAL_INTERVAL_SECONDS),
        backoff_coefficient=RETRYABLE_STAGE_BACKOFF_COEFFICIENT,
        maximum_interval=timedelta(minutes=RETRYABLE_STAGE_MAXIMUM_INTERVAL_MINUTES),
        maximum_attempts=max_attempts,
    )


async def _execute_stage(stage: str, payload: TemporalWorkflowInput) -> TemporalActivityResult:
    activity_name = {
        "linkedin": LINKEDIN_ACTIVITY_NAME,
        "rocketreach": ROCKETREACH_ACTIVITY_NAME,
        "email": EMAIL_ACTIVITY_NAME,
    }[stage]
    timeout = {
        "linkedin": timedelta(hours=2),
        "rocketreach": timedelta(minutes=30),
        "email": timedelta(minutes=30),
    }[stage]
    retry_policy = _stage_retry_policy(stage)
    return _activity_result(await workflow.execute_activity(
        activity_name,
        TemporalActivityInput(
            run_id=payload.run_id,
            config_path=payload.config_path,
            root=payload.root,
            fresh=payload.fresh,
        ),
        start_to_close_timeout=timeout,
        retry_policy=retry_policy,
    ))


@workflow.defn
class AutomationPipelineWorkflow:
    @workflow.run
    async def run(self, payload: TemporalWorkflowInput) -> TemporalWorkflowResult:
        workflow_info = workflow.info()
        retry_counts = {
            "linkedin": 0,
            "rocketreach": 0,
            "email": 0,
        }
        linkedin_result = await _execute_stage("linkedin", payload)
        if linkedin_result.status in {"completed", "waiting_login", "blocked_runtime", "failed"}:
            return _workflow_result(
                payload,
                workflow_info,
                status=linkedin_result.status,
                note=linkedin_result.note,
                current_stage="linkedin",
                retry_counts=retry_counts,
            )

        rocketreach_result = await _execute_stage("rocketreach", payload)
        retry_counts["rocketreach"] = int(rocketreach_result.retry_count or 0)
        if rocketreach_result.status in {"completed", "waiting_review", "failed"}:
            return _workflow_result(
                payload,
                workflow_info,
                status=rocketreach_result.status,
                note=rocketreach_result.note,
                current_stage="rocketreach",
                retry_counts=retry_counts,
            )

        email_result = await _execute_stage("email", payload)
        retry_counts["email"] = int(email_result.retry_count or 0)
        return _workflow_result(
            payload,
            workflow_info,
            status=email_result.status,
            note=email_result.note,
            current_stage="email",
            retry_counts=retry_counts,
        )

from __future__ import annotations

import logging
from typing import Any

from .core.sentry_config import (
    build_activity_context,
    build_temporal_tags,
    capture_exception_with_context,
)
from .temporal_sdk import (
    ActivityInboundInterceptor,
    ExecuteActivityInput,
    ExecuteWorkflowInput,
    Interceptor,
    WorkflowInboundInterceptor,
    WorkflowInterceptorClassInput,
    activity,
    workflow,
)

logger = logging.getLogger(__name__)

with workflow.unsafe.imports_passed_through():
    from .core.sentry_config import workflow_safe_capture_exception


class SentryActivityInboundInterceptor(ActivityInboundInterceptor):
    def __init__(self, next: ActivityInboundInterceptor) -> None:
        self.next = next

    async def execute_activity(self, input: ExecuteActivityInput) -> Any:
        try:
            return await self.next.execute_activity(input)
        except Exception as error:
            info = activity.info()
            payload = input.args[0] if input.args else None
            tags = build_temporal_tags(
                workflow_id=getattr(info, "workflow_id", None),
                workflow_type=getattr(info, "workflow_type", None),
                task_queue=getattr(info, "task_queue", None),
                activity_name=getattr(info, "activity_type", None) or getattr(input.fn, "__name__", ""),
                attempt=getattr(info, "attempt", None),
                run_id=getattr(payload, "run_id", None),
            )
            extras = build_activity_context(payload)
            logger.exception(
                "Temporal activity failed. activity=%s workflow_id=%s task_queue=%s run_id=%s",
                tags.get("activity_name", ""),
                tags.get("workflow_id", ""),
                tags.get("task_queue", ""),
                tags.get("run_id", ""),
            )
            capture_exception_with_context(
                error,
                message="temporal activity failed",
                tags=tags,
                extras=extras,
            )
            raise


class SentryWorkflowInboundInterceptor(WorkflowInboundInterceptor):
    def __init__(self, outbound: Any) -> None:
        super().__init__(outbound)

    async def execute_workflow(self, input: ExecuteWorkflowInput) -> Any:
        try:
            return await super().execute_workflow(input)
        except Exception as error:
            info = workflow.info()
            payload = input.args[0] if input.args else None
            tags = build_temporal_tags(
                workflow_id=getattr(info, "workflow_id", None),
                workflow_type=getattr(info, "workflow_type", None) or getattr(input.type, "__name__", ""),
                task_queue=getattr(info, "task_queue", None),
                run_id=getattr(payload, "run_id", None),
            )
            logger.exception(
                "Temporal workflow failed. workflow_id=%s workflow_type=%s task_queue=%s run_id=%s",
                tags.get("workflow_id", ""),
                tags.get("workflow_type", ""),
                tags.get("task_queue", ""),
                tags.get("run_id", ""),
            )
            workflow_safe_capture_exception(error, tags=tags)
            raise


class SentryTemporalInterceptor(Interceptor):
    def intercept_activity(self, next: ActivityInboundInterceptor) -> ActivityInboundInterceptor:
        return SentryActivityInboundInterceptor(next)

    def workflow_interceptor_class(
        self,
        input: WorkflowInterceptorClassInput,
    ) -> type[WorkflowInboundInterceptor] | None:
        return SentryWorkflowInboundInterceptor

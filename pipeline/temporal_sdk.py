from __future__ import annotations

import asyncio
from dataclasses import dataclass
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any, Callable


TEMPORAL_SDK_AVAILABLE = False
TEMPORAL_IMPORT_ERROR: Exception | None = None

try:
    from temporalio import activity, workflow
    from temporalio.client import Client
    from temporalio.common import RetryPolicy
    from temporalio.exceptions import ActivityError, ApplicationError
    from temporalio.worker import (
        ActivityInboundInterceptor,
        ExecuteActivityInput,
        ExecuteWorkflowInput,
        Interceptor,
        Worker,
        WorkflowInboundInterceptor,
        WorkflowInterceptorClassInput,
    )

    TEMPORAL_SDK_AVAILABLE = True
except Exception as error:  # pragma: no cover
    TEMPORAL_IMPORT_ERROR = error

    class ApplicationError(RuntimeError):
        def __init__(self, message: str, *, non_retryable: bool = False) -> None:
            super().__init__(message)
            self.non_retryable = non_retryable

    class ActivityError(RuntimeError):
        pass

    @dataclass(frozen=True)
    class RetryPolicy:
        initial_interval: Any | None = None
        backoff_coefficient: float | None = None
        maximum_interval: Any | None = None
        maximum_attempts: int | None = None

    class Client:
        @classmethod
        async def connect(cls, *args: Any, **kwargs: Any) -> "Client":
            raise RuntimeError("temporalio is not installed. Run `pip install temporalio` to enable Temporal support.")

        async def execute_workflow(self, *args: Any, **kwargs: Any) -> Any:
            raise RuntimeError("temporalio is not installed. Run `pip install temporalio` to enable Temporal support.")

        async def start_workflow(self, *args: Any, **kwargs: Any) -> Any:
            raise RuntimeError("temporalio is not installed. Run `pip install temporalio` to enable Temporal support.")

    class Worker:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.args = args
            self.kwargs = kwargs

        async def run(self) -> None:
            raise RuntimeError("temporalio is not installed. Run `pip install temporalio` to enable Temporal support.")

    class Interceptor:
        def intercept_activity(self, next: Any) -> Any:
            return next

        def workflow_interceptor_class(self, input: Any) -> type | None:
            return None

    class ActivityInboundInterceptor:
        def __init__(self, next: Any) -> None:
            self.next = next

        async def execute_activity(self, input: Any) -> Any:
            return await self.next.execute_activity(input)

    class WorkflowInboundInterceptor:
        def __init__(self, outbound: Any) -> None:
            self.outbound = outbound

        def init(self, outbound: Any) -> None:
            self.outbound = outbound

        async def execute_workflow(self, input: Any) -> Any:
            return await self.outbound.execute_workflow(input)

    @dataclass(frozen=True)
    class ExecuteActivityInput:
        fn: Callable[..., Any]
        args: tuple[Any, ...]
        executor: Any | None = None
        headers: Any | None = None

    @dataclass(frozen=True)
    class ExecuteWorkflowInput:
        type: type
        run_fn: Callable[..., Any]
        args: tuple[Any, ...]
        headers: Any | None = None

    @dataclass(frozen=True)
    class WorkflowInterceptorClassInput:
        unsafe_extern_functions: dict[str, Callable]

    class _ActivityShim:
        @staticmethod
        def defn(fn: Callable[..., Any] | None = None, **kwargs: Any):
            if fn is None:
                return lambda inner: inner
            return fn

        @staticmethod
        def info() -> Any:
            return SimpleNamespace(attempt=1, workflow_id="", activity_id="", task_queue="")

    class _WorkflowShim:
        @staticmethod
        def defn(cls: type | None = None, **kwargs: Any):
            if cls is None:
                return lambda inner: inner
            return cls

        @staticmethod
        def run(fn: Callable[..., Any] | None = None, **kwargs: Any):
            if fn is None:
                return lambda inner: inner
            return fn

        @staticmethod
        async def execute_activity(*args: Any, **kwargs: Any) -> Any:
            raise RuntimeError("temporalio is not installed. Run `pip install temporalio` to enable Temporal support.")

        @staticmethod
        async def sleep(duration: Any) -> None:
            seconds = duration.total_seconds() if hasattr(duration, "total_seconds") else float(duration)
            await asyncio.sleep(seconds)

        @staticmethod
        def info() -> Any:
            return SimpleNamespace(workflow_id="", task_queue="")

        class unsafe:
            @staticmethod
            @contextmanager
            def imports_passed_through():
                yield

    activity = _ActivityShim()
    workflow = _WorkflowShim()


def ensure_temporal_sdk_available() -> None:
    if TEMPORAL_SDK_AVAILABLE:
        return
    raise RuntimeError("temporalio is not installed. Run `pip install temporalio` to enable Temporal support.")

from __future__ import annotations

import logging
import os
import sys
import traceback
from collections.abc import Mapping
from contextlib import contextmanager
from typing import Any

from ..config import load_runtime_env_values

logger = logging.getLogger(__name__)

_SENTRY_IMPORT_ERROR: Exception | None = None

try:
    import sentry_sdk
    from sentry_sdk.integrations.logging import LoggingIntegration
except Exception as error:  # pragma: no cover - depends on optional dependency
    sentry_sdk = None
    LoggingIntegration = None
    _SENTRY_IMPORT_ERROR = error


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        stream=sys.stdout,
        force=False,
    )


def _load_sentry_env_defaults(config_path: str | None = None) -> None:
    runtime_values = load_runtime_env_values(config_path)
    for key, value in runtime_values.items():
        if key.startswith("SENTRY_") and value and not os.environ.get(key):
            os.environ[key] = value


def sentry_enabled() -> bool:
    return bool(os.environ.get("SENTRY_DSN", "").strip())


def sentry_available() -> bool:
    return sentry_sdk is not None


def init_sentry(config_path: str | None = None) -> bool:
    configure_logging()
    _load_sentry_env_defaults(config_path)
    dsn = os.environ.get("SENTRY_DSN", "").strip()
    if not dsn:
        logger.info("Sentry DSN not configured. Error reporting is disabled.")
        return False
    if sentry_sdk is None:
        logger.warning(
            "Sentry DSN is configured but sentry_sdk is not installed. "
            "Run `pip install sentry-sdk` to enable error reporting."
        )
        if _SENTRY_IMPORT_ERROR is not None:
            logger.debug("sentry_sdk import failed: %s", _SENTRY_IMPORT_ERROR)
        return False
    environment = (
        os.environ.get("SENTRY_ENVIRONMENT", "").strip()
        or os.environ.get("PIPELINE_ENVIRONMENT", "").strip()
        or "local"
    )
    integrations = []
    if LoggingIntegration is not None:
        integrations.append(LoggingIntegration(level=logging.INFO, event_level=logging.ERROR))
    sentry_sdk.init(
        dsn=dsn,
        environment=environment,
        release=(os.environ.get("SENTRY_RELEASE", "").strip() or None),
        traces_sample_rate=1.0,
        integrations=integrations,
    )
    logger.info(
        "Sentry initialized for environment=%s release=%s",
        environment,
        os.environ.get("SENTRY_RELEASE", "").strip() or "unset",
    )
    return True


def _parse_sample_rate(raw_value: str) -> float:
    if not raw_value:
        return 0.0
    try:
        value = float(raw_value)
    except ValueError:
        logger.warning("Invalid SENTRY_TRACES_SAMPLE_RATE=%r. Falling back to 0.0.", raw_value)
        return 0.0
    return min(max(value, 0.0), 1.0)


@contextmanager
def configure_scope(
    *,
    tags: Mapping[str, object] | None = None,
    extras: Mapping[str, object] | None = None,
):
    if sentry_sdk is None:
        yield
        return
    with sentry_sdk.push_scope() as scope:
        for key, value in (tags or {}).items():
            if value is not None and str(value) != "":
                scope.set_tag(key, str(value))
        for key, value in (extras or {}).items():
            if value is not None:
                scope.set_extra(key, value)
        yield


def capture_exception_with_context(
    error: BaseException,
    *,
    message: str | None = None,
    tags: Mapping[str, object] | None = None,
    extras: Mapping[str, object] | None = None,
) -> None:
    if not sentry_enabled() or sentry_sdk is None:
        return
    if message:
        logger.debug("Sending exception to Sentry: %s", message)
    with configure_scope(tags=tags, extras=extras):
        sentry_sdk.capture_exception(error)
        sentry_sdk.flush()


def capture_live_message(
    message: str,
    *,
    level: str = "info",
    tags: Mapping[str, object] | None = None,
    extras: Mapping[str, object] | None = None,
) -> None:
    logger.info("%s", message)
    if not sentry_enabled() or sentry_sdk is None:
        return
    with configure_scope(tags=tags, extras=extras):
        sentry_sdk.capture_message(message, level=level)
        sentry_sdk.flush()


def log_and_capture_error(
    error: BaseException,
    *,
    message: str | None = None,
    tags: Mapping[str, object] | None = None,
    extras: Mapping[str, object] | None = None,
) -> None:
    error_message = message or str(error) or error.__class__.__name__
    traceback.print_exception(type(error), error, error.__traceback__, file=sys.stdout)
    sys.stdout.flush()
    logger.error(error_message, exc_info=(type(error), error, error.__traceback__))
    if sentry_enabled() and sentry_sdk is not None:
        with configure_scope(tags=tags, extras=extras):
            sentry_sdk.capture_exception(error)
            sentry_sdk.flush()
    raise error.with_traceback(error.__traceback__)


def build_temporal_tags(
    *,
    workflow_id: str | None = None,
    workflow_type: str | None = None,
    task_queue: str | None = None,
    activity_name: str | None = None,
    attempt: int | None = None,
    run_id: str | None = None,
    stage: str | None = None,
    provider: str | None = None,
) -> dict[str, str]:
    tags: dict[str, str] = {}
    raw_values = {
        "workflow_id": workflow_id,
        "workflow_type": workflow_type,
        "task_queue": task_queue,
        "activity_name": activity_name,
        "attempt": attempt,
        "run_id": run_id,
        "stage": stage,
        "provider": provider,
    }
    for key, value in raw_values.items():
        if value is None:
            continue
        text = str(value).strip()
        if text:
            tags[key] = text
    return tags


def build_activity_context(payload: object) -> dict[str, object]:
    run_id = getattr(payload, "run_id", None)
    return {"run_id": str(run_id) if run_id else ""}


def workflow_safe_capture_exception(error: BaseException, *, tags: Mapping[str, object] | None = None) -> None:
    # Workflow-side reporting must remain passive. It should never affect workflow
    # control flow or make Temporal decisions depend on Sentry availability.
    capture_exception_with_context(
        error,
        message="workflow execution failed",
        tags=tags,
    )

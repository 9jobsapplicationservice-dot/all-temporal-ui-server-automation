from __future__ import annotations

import os
import shutil
import subprocess
import sys
import time
import json
import logging
from dataclasses import dataclass
from pathlib import Path

from .config import load_automation_config, load_runtime_env_values
from .constants import (
    APPLIED_JOBS_HEADERS,
    DEFAULT_LINKEDIN_IDLE_TIMEOUT_SECONDS,
    DEFAULT_LINKEDIN_STAGE_TIMEOUT_SECONDS,
    ENRICHED_RECRUITER_HEADERS,
)
from .core.sentry_config import build_pipeline_tags, capture_exception_with_context
from .manifest import write_manifest
from .utils import csv_has_expected_header, csv_row_count, read_last_json_object, read_log_tail, recruiter_sendable_row_count
from .storage import PipelineStore
from .enrichment import RetryableProviderError, enrich_contacts


WORKSPACE_ROOT = Path(__file__).resolve().parent.parent
LINKEDIN_PROJECT_ROOT = WORKSPACE_ROOT / "linkdin_automation"
ROCKETREACH_PROJECT_ROOT = WORKSPACE_ROOT / "rocket_reach - testing"
LINKEDIN_PYTHON_ENV_VAR = "PIPELINE_LINKEDIN_PYTHON"
LINKEDIN_POPUPS_ENV_VAR = "PIPELINE_ENABLE_POPUPS"
SUPPORTED_LINKEDIN_PYTHON_MIN = (3, 11)
SUPPORTED_LINKEDIN_PYTHON_MAX_EXCLUSIVE = (3, 16)
logger = logging.getLogger(__name__)


class StageError(RuntimeError):
    pass


class TransientStageError(StageError):
    pass


class LinkedInRuntimeUnavailableError(StageError):
    pass


class StageTimeoutError(StageError):
    pass


@dataclass(frozen=True)
class SubprocessRunResult:
    returncode: int
    started_at: float
    finished_at: float
    last_activity_at: float
    exit_reason: str


@dataclass(frozen=True)
class LinkedInRuntimePreflight:
    executable: str | None
    source: str | None
    blocked_reason: str | None

    @property
    def is_available(self) -> bool:
        return bool(self.executable) and not self.blocked_reason


def _classify_linkedin_tail(stdout_log: Path, stderr_log: Path) -> dict[str, object]:
    combined_tail = ((read_log_tail(stdout_log) or "") + "\n" + (read_log_tail(stderr_log) or "")).strip()
    lowered = combined_tail.lower()
    session_end_reason = ""
    waiting_login = False
    recoverable_apply_failure = False

    if "devtoolsactiveport" in lowered or "failed to create chrome session" in lowered:
        session_end_reason = (
            "Chrome default profile crashed while LinkedIn was opening. "
            "Complete manual login in Chrome and keep the browser window open."
        )
        waiting_login = True
    elif "failed to open chrome reliably" in lowered:
        session_end_reason = (
            "Chrome startup needs manual recovery. "
            "Close extra Chrome windows, reopen LinkedIn in Chrome, and keep the browser window open."
        )
        waiting_login = True
    elif (
        "seems like login attempt failed" in lowered
        or "complete manual login in chrome and keep the browser window open" in lowered
        or "captcha" in lowered
        or "checkpoint" in lowered
        or "2fa" in lowered
    ):
        session_end_reason = "LinkedIn login was not confirmed. Complete manual login in Chrome and keep the browser window open."
        waiting_login = True
    elif (
        "continuous loop of next/review" in lowered
        or "unable to advance easy apply" in lowered
        or "could not find next, review, or submit application" in lowered
        or "easy apply failed i guess" in lowered
    ):
        session_end_reason = "LinkedIn stage could not complete any Easy Apply submissions."
        recoverable_apply_failure = True

    return {
        "combined_tail": combined_tail,
        "session_end_reason": session_end_reason,
        "waiting_login": waiting_login,
        "recoverable_apply_failure": recoverable_apply_failure,
    }


def _discover_windows_supported_python_executables() -> list[str]:
    supported_versions = ("311", "312", "313")
    candidates: list[Path] = []
    local_app_data = os.environ.get("LOCALAPPDATA", "").strip()
    if local_app_data:
        for version in supported_versions:
            candidates.append(Path(local_app_data) / "Programs" / "Python" / f"Python{version}" / "python.exe")

    for env_var in ("ProgramFiles", "ProgramFiles(x86)"):
        base = os.environ.get(env_var, "").strip()
        if base:
            for version in supported_versions:
                candidates.append(Path(base) / f"Python{version}" / "python.exe")

    system_drive = os.environ.get("SystemDrive", "C:").strip() or "C:"
    for version in supported_versions:
        candidates.append(Path(f"{system_drive}\\Python{version}\\python.exe"))

    discovered: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        candidate_str = str(candidate)
        if candidate_str in seen:
            continue
        seen.add(candidate_str)
        if candidate.exists():
            discovered.append(str(candidate.resolve()))
    return discovered


def _probe_python_version(executable: str) -> tuple[int, int]:
    try:
        completed = subprocess.run(
            [executable, "-c", "import sys; print(f'{sys.version_info[0]}.{sys.version_info[1]}')"],
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError as error:
        raise StageError(
            f"LinkedIn stage could not start Python interpreter '{executable}': {error}"
        ) from error

    if completed.returncode != 0:
        stderr = (completed.stderr or completed.stdout or "").strip()
        raise StageError(
            f"LinkedIn stage could not use Python interpreter '{executable}': "
            f"version probe exited with code {completed.returncode}. {stderr}".strip()
        )

    version_text = completed.stdout.strip()
    try:
        major_text, minor_text = version_text.split(".", maxsplit=1)
        return int(major_text), int(minor_text)
    except ValueError as error:
        raise StageError(
            f"LinkedIn stage received an invalid version response from '{executable}': {version_text!r}"
        ) from error


def _validate_linkedin_python_executable(executable: str, source: str) -> str:
    version = _probe_python_version(executable)
    if version < SUPPORTED_LINKEDIN_PYTHON_MIN or version >= SUPPORTED_LINKEDIN_PYTHON_MAX_EXCLUSIVE:
        raise StageError(
            f"LinkedIn stage selected Python interpreter '{executable}' from {source}, "
            f"but Python {version[0]}.{version[1]} is incompatible with the current "
            "LinkedIn automation dependencies because undetected_chromedriver still imports "
            f"distutils. Set {LINKEDIN_PYTHON_ENV_VAR} to a Python 3.11 through 3.15 executable."
        )
    return executable


def _build_runtime_blocked_reason(
    executable: str | None,
    source: str | None,
    detail: str,
) -> str:
    selected = executable or "none found"
    source_text = source or "runtime preflight"
    return (
        "LinkedIn runtime setup required. "
        f"Selected interpreter: {selected} ({source_text}). "
        f"{detail} "
        f"Install Python 3.11, 3.12, or 3.13 or set {LINKEDIN_PYTHON_ENV_VAR} to an absolute compatible path, "
        "for example "
        r"C:\Users\USER\AppData\Local\Programs\Python\Python311\python.exe."
    ).strip()


def preflight_linkedin_runtime() -> LinkedInRuntimePreflight:
    configured_python = os.environ.get(LINKEDIN_PYTHON_ENV_VAR, "").strip()
    if configured_python:
        candidate = Path(configured_python).expanduser()
        resolved = candidate.resolve()
        source = f"environment variable {LINKEDIN_PYTHON_ENV_VAR}"
        if not resolved.exists():
            return LinkedInRuntimePreflight(
                executable=None,
                source=source,
                blocked_reason=_build_runtime_blocked_reason(
                    str(resolved),
                    source,
                    f"Configured path does not exist: {resolved}.",
                ),
            )
        if resolved.is_dir():
            return LinkedInRuntimePreflight(
                executable=None,
                source=source,
                blocked_reason=_build_runtime_blocked_reason(
                    str(resolved),
                    source,
                    f"Configured path is a directory, not a Python executable: {resolved}.",
                ),
            )
        try:
            return LinkedInRuntimePreflight(
                executable=_validate_linkedin_python_executable(str(resolved), source),
                source=source,
                blocked_reason=None,
            )
        except StageError as error:
            return LinkedInRuntimePreflight(
                executable=str(resolved),
                source=source,
                blocked_reason=_build_runtime_blocked_reason(str(resolved), source, str(error)),
            )

    if os.name == "nt":
        for executable in _discover_windows_supported_python_executables():
            source = "Windows Python 3.11+ auto-discovery"
            try:
                return LinkedInRuntimePreflight(
                    executable=_validate_linkedin_python_executable(executable, source),
                    source=source,
                    blocked_reason=None,
                )
            except StageError:
                continue

    current_python = shutil.which(sys.executable) or sys.executable
    source = "the current pipeline Python interpreter"
    try:
        return LinkedInRuntimePreflight(
            executable=_validate_linkedin_python_executable(current_python, source),
            source=source,
            blocked_reason=None,
        )
    except StageError as error:
        return LinkedInRuntimePreflight(
            executable=current_python,
            source=source,
            blocked_reason=_build_runtime_blocked_reason(current_python, source, str(error)),
        )


def resolve_linkedin_python_executable() -> str:
    preflight = preflight_linkedin_runtime()
    if preflight.is_available and preflight.executable:
        return preflight.executable
    raise LinkedInRuntimeUnavailableError(preflight.blocked_reason or "LinkedIn runtime setup required.")


def _env_int(env: dict[str, str], key: str, default: int) -> int:
    raw = (env.get(key) or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value >= 0 else default


def _safe_mtime(path: Path) -> float | None:
    try:
        return path.stat().st_mtime if path.exists() else None
    except OSError:
        return None


def _kill_process_tree(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    try:
        if os.name == "nt":
            subprocess.run(
                ["taskkill", "/PID", str(process.pid), "/T", "/F"],
                capture_output=True,
                text=True,
                check=False,
            )
        else:
            process.terminate()
    except OSError:
        pass
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        try:
            process.kill()
        except OSError:
            pass


def _readable_duration(total_seconds: float) -> str:
    rounded = max(1, int(total_seconds))
    minutes, seconds = divmod(rounded, 60)
    if minutes == 0:
        return f"{seconds} second{'s' if seconds != 1 else ''}"
    if seconds == 0:
        return f"{minutes} minute{'s' if minutes != 1 else ''}"
    return (
        f"{minutes} minute{'s' if minutes != 1 else ''} "
        f"{seconds} second{'s' if seconds != 1 else ''}"
    )


def _new_process_group_kwargs() -> dict[str, object]:
    if os.name == "nt":
        creationflags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        return {"creationflags": creationflags}
    return {"start_new_session": True}


def _run_subprocess(
    command: list[str],
    workdir: Path,
    stdout_log: Path,
    stderr_log: Path,
    env: dict[str, str] | None = None,
    record: dict | None = None,
) -> SubprocessRunResult:
    stdout_log.parent.mkdir(parents=True, exist_ok=True)
    stderr_log.parent.mkdir(parents=True, exist_ok=True)
    merged_env = os.environ.copy()
    if env:
        merged_env.update(env)

    hard_timeout_seconds = _env_int(merged_env, "PIPELINE_LINKEDIN_STAGE_TIMEOUT_SECONDS", DEFAULT_LINKEDIN_STAGE_TIMEOUT_SECONDS)
    idle_timeout_seconds = _env_int(merged_env, "PIPELINE_LINKEDIN_IDLE_TIMEOUT_SECONDS", DEFAULT_LINKEDIN_IDLE_TIMEOUT_SECONDS)

    with stdout_log.open("w", encoding="utf-8") as stdout_handle, stderr_log.open("w", encoding="utf-8") as stderr_handle:
        process = subprocess.Popen(
            command,
            cwd=workdir,
            env=merged_env,
            stdout=stdout_handle,
            stderr=stderr_handle,
            text=True,
            **_new_process_group_kwargs(),
        )
        started_at = time.monotonic()
        last_activity_at = started_at
        last_manifest_update = 0

        while True:
            return_code = process.poll()
            stdout_handle.flush()
            stderr_handle.flush()

            stdout_mtime = _safe_mtime(stdout_log)
            stderr_mtime = _safe_mtime(stderr_log)
            if stdout_mtime is not None or stderr_mtime is not None:
                wall_now = time.time()
                if stdout_mtime is not None and wall_now - stdout_mtime <= 2:
                    last_activity_at = time.monotonic()
                if stderr_mtime is not None and wall_now - stderr_mtime <= 2:
                    last_activity_at = time.monotonic()

            # Parse live status from logs and update manifest periodically
            if record and stdout_log.exists() and time.monotonic() - last_manifest_update > 2:
                tail = read_log_tail(stdout_log, line_count=30)
                live_status = record.get("live_status", {})
                updated = False
                for line in (tail or "").splitlines():
                    if "LINKEDIN_PAGE_LOADED" in line:
                        parts = line.split("|")
                        for p in parts:
                            if "CURRENT_URL=" in p:
                                live_status["current_url"] = p.split("=")[1].strip()
                                updated = True
                            if "PAGE_TITLE=" in p:
                                live_status["page_title"] = p.split("=")[1].strip()
                                updated = True
                    if "LOGIN_REQUIRED=" in line or "CHECKPOINT_REQUIRED=" in line or "JOB_CARDS_FOUND=" in line:
                        parts = line.split("|")
                        for p in parts:
                            p = p.strip()
                            if "LOGIN_REQUIRED=" in p:
                                live_status["login_required"] = "true" in p.lower()
                                updated = True
                            if "CHECKPOINT_REQUIRED=" in p:
                                live_status["checkpoint_required"] = "true" in p.lower()
                                updated = True
                            if "JOB_CARDS_FOUND=" in p:
                                live_status["job_cards_count"] = p.split("=")[1].strip()
                                updated = True
                            if "JOB_DETAILS_FOUND=" in p:
                                live_status["job_details_count"] = p.split("=")[1].strip()
                                updated = True
                            if "EASY_APPLY_BUTTONS_FOUND=" in p:
                                live_status["easy_apply_count"] = p.split("=")[1].strip()
                                updated = True
                    if "SCREENSHOT_PATH=" in line:
                        live_status["last_screenshot"] = line.split("SCREENSHOT_PATH=")[1].strip()
                        updated = True
                
                if updated:
                    record["live_status"] = live_status
                    write_manifest(record)
                    last_manifest_update = time.monotonic()

            if return_code is not None:
                completed = SubprocessRunResult(
                    returncode=return_code,
                    started_at=started_at,
                    finished_at=time.monotonic(),
                    last_activity_at=last_activity_at,
                    exit_reason="completed",
                )
                break

            elapsed = time.monotonic() - started_at
            idle_elapsed = time.monotonic() - last_activity_at
            if hard_timeout_seconds > 0 and elapsed >= hard_timeout_seconds:
                _kill_process_tree(process)
                tail = read_log_tail(stderr_log) or read_log_tail(stdout_log)
                raise StageTimeoutError(
                    (
                        f"LinkedIn stage timed out after {_readable_duration(hard_timeout_seconds)}. "
                        "The browser automation did not finish cleanly, so the process tree was stopped. "
                        + (f"\n{tail}" if tail else "")
                    ).strip()
                )
            if idle_timeout_seconds > 0 and idle_elapsed >= idle_timeout_seconds:
                _kill_process_tree(process)
                tail = read_log_tail(stderr_log) or read_log_tail(stdout_log)
                raise StageTimeoutError(
                    (
                        f"LinkedIn stage stalled with no new output for {_readable_duration(idle_timeout_seconds)} after browser activity. "
                        "The automation appears hung after apply/interview questions. "
                        + (f"\n{tail}" if tail else "")
                    ).strip()
                )
            time.sleep(1)

    if completed.returncode == 0:
        return completed

    tail = read_log_tail(stderr_log) or read_log_tail(stdout_log)
    raise StageError(
        f"Command failed with exit code {completed.returncode}: {' '.join(command)}\n{tail}".strip()
    )


def run_linkedin_stage(record: dict, python_executable: str | None = None) -> dict:
    configured_easy_apply_limit = "50"
    runtime_env = load_runtime_env_values(record.get("config_path") or None)
    try:
        config = load_automation_config(record.get("config_path") or None)
        configured_easy_apply_limit = str(config.max_easy_apply)
    except Exception:
        configured_easy_apply_limit = runtime_env.get("PIPELINE_MAX_EASY_APPLY", "").strip() or os.environ.get("PIPELINE_MAX_EASY_APPLY", "50").strip() or "50"
    pipeline_enable_popups = os.environ.get(LINKEDIN_POPUPS_ENV_VAR, "").strip()

    env = {
        **runtime_env,
        "PIPELINE_MODE": "1",
        "PIPELINE_RUN_ID": record["id"],
        "PIPELINE_OUTPUT_DIR": record["run_dir"],
        "PIPELINE_APPLIED_CSV_PATH": record["applied_csv_path"],
        "PIPELINE_EXTERNAL_CSV_PATH": record["external_jobs_csv_path"],
        "PIPELINE_RECRUITERS_CSV_PATH": record["recruiters_csv_path"],
        "PIPELINE_FAILED_CSV_PATH": str(Path(record["log_dir"]) / "failed_jobs.csv"),
        "PIPELINE_LOGS_DIR": record["log_dir"],
        "PIPELINE_SCREENSHOTS_DIR": str(Path(record["log_dir"]).parent / "screenshots"),
        "PIPELINE_RUN_NON_STOP": "false",
        "PIPELINE_MAX_EASY_APPLY": configured_easy_apply_limit,
        "PIPELINE_RUN_IN_BACKGROUND": "true" if os.name != 'nt' else "false",
    }
    env_log_msg = f"[Pipeline] Launching LinkedIn automation. Run ID: {record['id']}, Mode: {env.get('PIPELINE_MODE')}"
    print(env_log_msg)
    
    command = [python_executable or resolve_linkedin_python_executable(), "pipeline_entry.py"]
    print(f"[Pipeline] Command: {' '.join(command)}")
    
    stdout_log = Path(record["linkedin_stdout_log"])
    stderr_log = Path(record["linkedin_stderr_log"])
    try:
        completed = _run_subprocess(
            command=command,
            workdir=LINKEDIN_PROJECT_ROOT,
            stdout_log=stdout_log,
            stderr_log=stderr_log,
            env=env,
            record=record,
        )
        command_error = None
    except StageError as error:
        completed = None
        command_error = error

    payload = read_last_json_object(stdout_log)
    csv_is_valid = csv_has_expected_header(record["applied_csv_path"], APPLIED_JOBS_HEADERS)
    rows_written = csv_row_count(record["applied_csv_path"]) if csv_is_valid else 0
    session_end_reason = str(payload.get("session_end_reason", "") or "").strip()
    tail_state = _classify_linkedin_tail(stdout_log, stderr_log)
    if (
        not session_end_reason
        or session_end_reason == "Stopped because of an unexpected error."
    ):
        session_end_reason = str(tail_state["session_end_reason"] or "").strip()

    if command_error is not None and not csv_is_valid:
        if session_end_reason:
            raise StageError(session_end_reason) from command_error
        raise command_error
    if not csv_is_valid:
        if session_end_reason:
            raise StageError(session_end_reason)
        raise StageError(
            f"LinkedIn stage did not produce a valid applied jobs CSV at {record['applied_csv_path']}"
        )

    payload.setdefault("jobs_applied", rows_written)
    payload.setdefault("external_links_logged", 0)
    payload.setdefault("rows_written_to_applied_csv", rows_written)
    payload.setdefault("rows_missing_hr_profile", 0)
    payload.setdefault("failed_jobs", 0)
    payload.setdefault("skipped_jobs", 0)
    payload.setdefault("unexpected_failure", False)
    payload.setdefault("exit_code", completed.returncode if completed is not None else 1)
    payload.setdefault("session_end_reason", session_end_reason)

    if completed is not None and not payload:
        raise StageError(
            "LinkedIn stage exited without producing a final summary payload. "
            "This usually means the browser automation hung after applying."
        )

    if command_error is not None:
        if session_end_reason:
            raise StageError(session_end_reason) from command_error
        raise command_error

    if payload["jobs_applied"] > 0 and payload["rows_written_to_applied_csv"] == 0:
        raise StageError(
            "LinkedIn stage submitted jobs but wrote zero rows to applied_jobs.csv."
        )

    if (
        payload["rows_written_to_applied_csv"] == 0
        and bool(tail_state["recoverable_apply_failure"])
        and int(payload.get("failed_jobs", 0) or 0) == 0
    ):
        payload["failed_jobs"] = 1
        payload["unexpected_failure"] = True
        if not payload.get("session_end_reason"):
            payload["session_end_reason"] = str(tail_state["session_end_reason"] or "")

    if (
        payload["rows_written_to_applied_csv"] == 0
        and bool(payload.get("unexpected_failure"))
    ):
        raise StageError(
            str(payload.get("session_end_reason") or "LinkedIn stage ended unexpectedly without saving applied jobs.")
        )

    return payload


def is_transient_rocketreach_error(message: str) -> bool:
    lowered = (message or "").lower()
    transient_markers = (
        "rate limit",
        "quota",
        "temporarily",
        "timeout",
        "timed out",
        "connection aborted",
        "connection reset",
        "request failed",
        "502",
        "503",
        "504",
        "429",
    )
    return any(marker in lowered for marker in transient_markers)


def run_rocketreach_stage(record: dict, *, finalize_retryable_failures: bool = False) -> dict:
    stdout_log = Path(record["rocketreach_stdout_log"])
    stderr_log = Path(record["rocketreach_stderr_log"])
    store_root = Path(record["run_dir"]).resolve().parent
    store = PipelineStore(store_root)
    try:
        stats = enrich_contacts(
            record,
            store,
            finalize_retryable_failures=finalize_retryable_failures,
        )
        stdout_log.parent.mkdir(parents=True, exist_ok=True)
        stdout_log.write_text(json.dumps(stats, indent=2), encoding="utf-8")
        stderr_log.parent.mkdir(parents=True, exist_ok=True)
        if not stderr_log.exists():
            stderr_log.write_text("", encoding="utf-8")
    except RetryableProviderError as error:
        logger.warning(
            "RocketReach enrichment retryable failure. run_id=%s finalize_retryable_failures=%s reason=%s",
            record.get("id", ""),
            finalize_retryable_failures,
            error,
        )
        stdout_log.parent.mkdir(parents=True, exist_ok=True)
        stdout_log.write_text(
            json.dumps(
                {
                    "message": str(error),
                    "provider": getattr(error, "provider", ""),
                    "retryable": True,
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        raise TransientStageError(str(error)) from error
    except Exception as error:
        logger.exception("RocketReach enrichment failed unexpectedly. run_id=%s", record.get("id", ""))
        capture_exception_with_context(
            error,
            message="rocketreach stage unexpected failure",
            tags=build_pipeline_tags(run_id=str(record.get("id", "")), stage="rocketreach"),
            extras={"finalize_retryable_failures": finalize_retryable_failures},
        )
        stderr_log.parent.mkdir(parents=True, exist_ok=True)
        stderr_log.write_text(str(error), encoding="utf-8")
        if is_transient_rocketreach_error(str(error)):
            raise TransientStageError(str(error)) from error
        raise StageError(str(error)) from error

    actual_recruiters_csv_path = stats.get("recruiters_csv_path") or record["recruiters_csv_path"]

    try:
        csv_is_valid = csv_has_expected_header(actual_recruiters_csv_path, ENRICHED_RECRUITER_HEADERS)
    except PermissionError as error:
        locked_path = error.filename or actual_recruiters_csv_path
        raise StageError(
            f"RocketReach stage could not read CSV '{locked_path}' because it is open in another program. Close Excel and retry."
        ) from error

    if not csv_is_valid:
        raise StageError(
            f"RocketReach stage did not produce a valid recruiter CSV at {actual_recruiters_csv_path}"
        )

    try:
        applied_row_count = csv_row_count(record["applied_csv_path"])
        recruiter_row_count = csv_row_count(actual_recruiters_csv_path)
        recruiter_sendable_rows = recruiter_sendable_row_count(actual_recruiters_csv_path)
    except PermissionError as error:
        locked_path = error.filename or actual_recruiters_csv_path
        raise StageError(
            f"RocketReach stage could not read CSV '{locked_path}' because it is open in another program. Close Excel and retry."
        ) from error

    stats.setdefault("total", applied_row_count)
    stats.setdefault("matched", 0)
    stats.setdefault("preview_match", 0)
    stats.setdefault("failed", 0)
    stats.setdefault("skipped", 0)
    stats.setdefault("no_match", 0)
    stats.setdefault("missing_hr_link", 0)
    stats.setdefault("invalid_hr_link", 0)
    stats.setdefault("profile_only", 0)
    stats.setdefault("lookup_quota_reached", 0)
    stats.setdefault("authentication_failed", 0)
    stats.setdefault("sendable_rows", recruiter_sendable_rows)
    stats.setdefault("provider_configuration_blocked", 0)
    stats.setdefault("final_status", "completed")
    stats.setdefault("final_reason", "")

    if stats["total"] > 0 and stats["sendable_rows"] == 0 and stats["matched"] == 0 and stats["missing_hr_link"] == 0 and stats["invalid_hr_link"] == 0 and stats["profile_only"] == 0 and stats["no_match"] == 0 and stats["lookup_quota_reached"] == 0 and stats["authentication_failed"] == 0:
        raise StageError("RocketReach produced a header-only recruiter CSV without reporting row outcomes.")

    stats["applied_csv_path"] = record["applied_csv_path"]
    stats["recruiters_csv_path"] = actual_recruiters_csv_path
    return stats

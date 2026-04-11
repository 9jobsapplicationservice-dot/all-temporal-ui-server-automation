from __future__ import annotations

import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from .config import load_automation_config, load_runtime_env_values
from .constants import APPLIED_JOBS_HEADERS, ENRICHED_RECRUITER_HEADERS
from .utils import csv_has_expected_header, csv_row_count, read_last_json_object, read_log_tail, recruiter_sendable_row_count


WORKSPACE_ROOT = Path(__file__).resolve().parent.parent
LINKEDIN_PROJECT_ROOT = WORKSPACE_ROOT / "linkdin_automation"
ROCKETREACH_PROJECT_ROOT = WORKSPACE_ROOT / "rocket_reach - testing"
LINKEDIN_PYTHON_ENV_VAR = "PIPELINE_LINKEDIN_PYTHON"
LINKEDIN_POPUPS_ENV_VAR = "PIPELINE_ENABLE_POPUPS"
SUPPORTED_LINKEDIN_PYTHON_MIN = (3, 11)
SUPPORTED_LINKEDIN_PYTHON_MAX_EXCLUSIVE = (3, 14)


class StageError(RuntimeError):
    pass


class TransientStageError(StageError):
    pass


class LinkedInRuntimeUnavailableError(StageError):
    pass


@dataclass(frozen=True)
class LinkedInRuntimePreflight:
    executable: str | None
    source: str | None
    blocked_reason: str | None

    @property
    def is_available(self) -> bool:
        return bool(self.executable) and not self.blocked_reason


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
            f"distutils. Set {LINKEDIN_PYTHON_ENV_VAR} to a Python 3.11, 3.12, or 3.13 executable."
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


def _run_subprocess(command: list[str], workdir: Path, stdout_log: Path, stderr_log: Path, env: dict[str, str] | None = None) -> None:
    stdout_log.parent.mkdir(parents=True, exist_ok=True)
    stderr_log.parent.mkdir(parents=True, exist_ok=True)
    merged_env = os.environ.copy()
    if env:
        merged_env.update(env)

    with stdout_log.open("w", encoding="utf-8") as stdout_handle, stderr_log.open("w", encoding="utf-8") as stderr_handle:
        completed = subprocess.run(
            command,
            cwd=workdir,
            env=merged_env,
            stdout=stdout_handle,
            stderr=stderr_handle,
            text=True,
            check=False,
        )

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
        "PIPELINE_RUN_NON_STOP": "false",
        "PIPELINE_MAX_EASY_APPLY": configured_easy_apply_limit,
    }
    if pipeline_enable_popups:
        env[LINKEDIN_POPUPS_ENV_VAR] = pipeline_enable_popups
    command = [python_executable or resolve_linkedin_python_executable(), "pipeline_entry.py"]
    stdout_log = Path(record["linkedin_stdout_log"])
    stderr_log = Path(record["linkedin_stderr_log"])
    try:
        completed = _run_subprocess(
            command=command,
            workdir=LINKEDIN_PROJECT_ROOT,
            stdout_log=stdout_log,
            stderr_log=stderr_log,
            env=env,
        )
        command_error = None
    except StageError as error:
        completed = None
        command_error = error

    payload = read_last_json_object(stdout_log)
    csv_is_valid = csv_has_expected_header(record["applied_csv_path"], APPLIED_JOBS_HEADERS)
    rows_written = csv_row_count(record["applied_csv_path"]) if csv_is_valid else 0
    session_end_reason = str(payload.get("session_end_reason", "") or "").strip()

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

    payload.setdefault("jobs_applied", 0)
    payload.setdefault("external_links_logged", 0)
    payload.setdefault("rows_written_to_applied_csv", rows_written)
    payload.setdefault("rows_missing_hr_profile", 0)
    payload.setdefault("unexpected_failure", False)
    payload.setdefault("exit_code", completed.returncode if completed is not None else 1)
    payload.setdefault("session_end_reason", session_end_reason)

    if payload["jobs_applied"] > 0 and payload["rows_written_to_applied_csv"] == 0:
        raise StageError(
            "LinkedIn stage submitted jobs but wrote zero rows to applied_jobs.csv."
        )

    if command_error is not None and payload["rows_written_to_applied_csv"] == 0:
        if session_end_reason:
            raise StageError(session_end_reason) from command_error
        raise command_error

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


def run_rocketreach_stage(record: dict) -> dict:
    command = [
        sys.executable,
        "bulk_enrich.py",
        "--input",
        record["applied_csv_path"],
        "--output",
        record["recruiters_csv_path"],
    ]
    stdout_log = Path(record["rocketreach_stdout_log"])
    try:
        _run_subprocess(
            command=command,
            workdir=ROCKETREACH_PROJECT_ROOT,
            stdout_log=stdout_log,
            stderr_log=Path(record["rocketreach_stderr_log"]),
        )
    except StageError as error:
        if is_transient_rocketreach_error(str(error)):
            raise TransientStageError(str(error)) from error
        raise

    stats = read_last_json_object(stdout_log)
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

    if stats["total"] > 0 and stats["sendable_rows"] == 0 and stats["matched"] == 0 and stats["missing_hr_link"] == 0 and stats["invalid_hr_link"] == 0 and stats["profile_only"] == 0 and stats["no_match"] == 0 and stats["lookup_quota_reached"] == 0 and stats["authentication_failed"] == 0:
        raise StageError("RocketReach produced a header-only recruiter CSV without reporting row outcomes.")

    stats["applied_csv_path"] = record["applied_csv_path"]
    stats["recruiters_csv_path"] = actual_recruiters_csv_path
    return stats

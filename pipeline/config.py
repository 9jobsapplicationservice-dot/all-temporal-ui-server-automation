from __future__ import annotations

import json
import logging
import os
import ast
from dataclasses import dataclass
from pathlib import Path

from .constants import (
    DEFAULT_PROVIDER_RATE_LIMIT_PER_MINUTE,
    DEFAULT_WORKFLOW_MAX_RERUNS,
    MAX_PROVIDER_RATE_LIMIT_PER_MINUTE,
)


TRUTHY_VALUES = {"1", "true", "yes", "on"}
FALSY_VALUES = {"0", "false", "no", "off"}
WORKSPACE_ROOT = Path(__file__).resolve().parent.parent
EMAIL_APP_ENV_PATH = WORKSPACE_ROOT / "sendemailwith-code" / "email-automation-nodejs" / ".env.local"
PIPELINE_ENV_PATH = WORKSPACE_ROOT / "pipeline" / "automation.env"
ALLOWED_ENV_PREFIXES = ("PIPELINE_", "SMTP_", "ROCKETREACH_", "HUNTER_", "APOLLO_", "SENTRY_")
PYTHON_CONFIG_ENV_ALIASES = {
    "switch_number": "PIPELINE_MAX_EASY_APPLY",
    "linkedin_auto_login": "PIPELINE_LINKEDIN_AUTO_LOGIN",
    "username": "PIPELINE_LINKEDIN_USERNAME",
    "safe_mode": "PIPELINE_LINKEDIN_SAFE_MODE",
    "target_job_link": "PIPELINE_TARGET_JOB_LINK",
}
PYTHON_CONFIG_PREVIEW_KEYS = {
    "search_terms",
    "search_location",
    "sort_by",
    "date_posted",
    "easy_apply_only",
    "experience_level",
    "job_type",
    "on_site",
    "companies",
    "location",
    "bad_words",
    "about_company_bad_words",
    "switch_number",
    "target_job_link",
    "safe_mode",
    "linkedin_auto_login",
}
LINKEDIN_CONFIG_FILES = {
    "personals": {
        "label": "Personal",
        "path": ("linkdin_automation", "config", "personals.py"),
        "fields": {
            "first_name": "text",
            "middle_name": "text",
            "last_name": "text",
            "phone_number": "text",
            "current_city": "text",
            "street": "text",
            "state": "text",
            "zipcode": "text",
            "country": "text",
            "ethnicity": "text",
            "gender": "text",
            "disability_status": "text",
            "veteran_status": "text",
        },
    },
    "questions": {
        "label": "Questions",
        "path": ("linkdin_automation", "config", "questions.py"),
        "fields": {
            "default_resume_path": "text",
            "years_of_experience": "text",
            "require_visa": "text",
            "website": "text",
            "linkedIn": "text",
            "us_citizenship": "text",
            "desired_salary": "number",
            "current_ctc": "number",
            "notice_period": "number",
            "linkedin_headline": "text",
            "linkedin_summary": "textarea",
            "cover_letter": "textarea",
            "user_information_all": "textarea",
            "recent_employer": "text",
            "confidence_level": "text",
            "pause_before_submit": "boolean",
            "pause_at_failed_question": "boolean",
            "overwrite_previous_answers": "boolean",
        },
    },
    "search": {
        "label": "Search",
        "path": ("linkdin_automation", "config", "search.py"),
        "fields": {
            "search_terms": "list",
            "search_location": "text",
            "switch_number": "number",
            "randomize_search_order": "boolean",
            "sort_by": "text",
            "date_posted": "text",
            "salary": "text",
            "easy_apply_only": "boolean",
            "experience_level": "list",
            "job_type": "list",
            "on_site": "list",
            "companies": "list",
            "location": "list",
            "industry": "list",
            "job_function": "list",
            "job_titles": "list",
            "benefits": "list",
            "commitments": "list",
            "under_10_applicants": "boolean",
            "in_your_network": "boolean",
            "fair_chance_employer": "boolean",
            "pause_after_filters": "boolean",
            "about_company_bad_words": "list",
            "about_company_good_words": "list",
            "bad_words": "list",
            "security_clearance": "boolean",
            "did_masters": "boolean",
            "current_experience": "number",
        },
    },
    "secrets": {
        "label": "Secrets",
        "path": ("linkdin_automation", "config", "secrets.py"),
        "fields": {
            "username": "text",
            "password": "password",
            "linkedin_auto_login": "boolean",
            "target_job_link": "text",
            "use_AI": "boolean",
            "ai_provider": "text",
            "llm_api_url": "text",
            "llm_api_key": "password",
            "llm_model": "text",
            "llm_spec": "text",
            "stream_output": "boolean",
        },
    },
    "settings": {
        "label": "Settings",
        "path": ("linkdin_automation", "config", "settings.py"),
        "fields": {
            "close_tabs": "boolean",
            "follow_companies": "boolean",
            "run_non_stop": "boolean",
            "alternate_sortby": "boolean",
            "cycle_date_posted": "boolean",
            "stop_date_cycle_at_24hr": "boolean",
            "pipeline_max_easy_apply": "number",
            "generated_resume_path": "text",
            "file_name": "text",
            "recruiters_file_name": "text",
            "failed_file_name": "text",
            "external_jobs_file_name": "text",
            "logs_folder_path": "text",
            "screenshot_folder_path": "text",
            "click_gap": "number",
            "manual_pause_on_form": "boolean",
            "manual_form_pacing": "number",
            "run_in_background": "boolean",
            "disable_extensions": "boolean",
            "safe_mode": "boolean",
            "smooth_scroll": "boolean",
            "keep_screen_awake": "boolean",
            "stealth_mode": "boolean",
            "showAiErrorAlerts": "boolean",
        },
    },
}
DEFAULT_EMAIL_SUBJECT = "Quick note re: {{position}} at {{company}}"
DEFAULT_EMAIL_BODY = (
    "Hi {{name}},\n\n"
    "I recently applied for the {{position}} role at {{company}} and wanted to introduce myself directly.\n\n"
    "I would love the opportunity to connect and share how I can contribute.\n\n"
    "Best regards,\n"
    "{{sendername}}"
)
logger = logging.getLogger(__name__)
_DOTENV_IMPORT_ERROR: Exception | None = None

try:
    from dotenv import load_dotenv
except Exception as error:  # pragma: no cover - optional dependency in local envs
    load_dotenv = None
    _DOTENV_IMPORT_ERROR = error


class AutomationConfigError(RuntimeError):
    pass


def _linkedIn_login_summary(values: dict[str, str]) -> dict[str, object]:
    auto_login_raw = (values.get("PIPELINE_LINKEDIN_AUTO_LOGIN") or "").strip()
    safe_mode_raw = (values.get("PIPELINE_LINKEDIN_SAFE_MODE") or "").strip()
    username = (values.get("PIPELINE_LINKEDIN_USERNAME") or "").strip()
    mode = "saved_session"
    if auto_login_raw:
        mode = "auto_login" if _parse_bool(auto_login_raw, field_name="PIPELINE_LINKEDIN_AUTO_LOGIN") else "saved_session"
    return {
        "mode": mode,
        "auto_login": _parse_bool(auto_login_raw, field_name="PIPELINE_LINKEDIN_AUTO_LOGIN") if auto_login_raw else False,
        "safe_mode": _parse_bool(safe_mode_raw, field_name="PIPELINE_LINKEDIN_SAFE_MODE") if safe_mode_raw else False,
        "username_configured": bool(username),
        "manual_login_timeout_seconds": _parse_int((values.get("PIPELINE_MANUAL_LOGIN_TIMEOUT_SECONDS") or "").strip() or "180", field_name="PIPELINE_MANUAL_LOGIN_TIMEOUT_SECONDS"),
    }


@dataclass(frozen=True)
class SMTPConfig:
    host: str
    port: int
    secure: bool
    user: str
    password: str
    from_email: str


@dataclass(frozen=True)
class AutomationConfig:
    auto_send: bool
    auto_send_reason: str | None
    max_easy_apply: int
    send_delay_seconds: int
    linkedin_stage_timeout_seconds: int
    linkedin_idle_timeout_seconds: int
    temporal_auto_start: bool
    run_once_always_fresh: bool
    provider_rate_limit_per_minute: int
    enrichment_sequential: bool
    workflow_max_reruns: int
    email_subject: str
    email_body: str
    sender_name: str
    smtp: SMTPConfig | None
    source: str

    def sanitized_summary(self) -> dict[str, object]:
        return {
            "auto_send": self.auto_send,
            "max_easy_apply": self.max_easy_apply,
            "send_delay_seconds": self.send_delay_seconds,
            "linkedin_stage_timeout_seconds": self.linkedin_stage_timeout_seconds,
            "linkedin_idle_timeout_seconds": self.linkedin_idle_timeout_seconds,
            "temporal_auto_start": self.temporal_auto_start,
            "run_once_always_fresh": self.run_once_always_fresh,
            "provider_rate_limit_per_minute": self.provider_rate_limit_per_minute,
            "enrichment_sequential": self.enrichment_sequential,
            "workflow_max_reruns": self.workflow_max_reruns,
            "sender_name": self.sender_name,
            "email_subject": self.email_subject,
            "auto_send_reason": self.auto_send_reason,
            "source": self.source,
            "smtp": {
                "host": self.smtp.host,
                "port": self.smtp.port,
                "secure": self.smtp.secure,
                "user": self.smtp.user,
                "from": self.smtp.from_email,
            } if self.smtp else None,
        }


def _parse_bool(raw_value: str, *, field_name: str) -> bool:
    lowered = raw_value.strip().lower()
    if lowered in TRUTHY_VALUES:
        return True
    if lowered in FALSY_VALUES:
        return False
    raise AutomationConfigError(f"{field_name} must be one of: true/false, yes/no, 1/0.")


def _parse_int(raw_value: str, *, field_name: str) -> int:
    try:
        value = int(raw_value.strip())
    except ValueError as error:
        raise AutomationConfigError(f"{field_name} must be a valid integer.") from error
    if value < 0:
        raise AutomationConfigError(f"{field_name} must be zero or greater.")
    return value


def _read_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        cleaned = value.strip().strip('"').strip("'").replace("\\n", "\n")
        values[key.strip()] = cleaned
    return values


def _literal_to_env_value(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (list, tuple, dict)):
        return json.dumps(value)
    return str(value)


def _read_python_literal_assignments(path: Path) -> dict[str, object]:
    tree = ast.parse(path.read_text(encoding="utf-8-sig"), filename=str(path))
    values: dict[str, object] = {}
    for statement in tree.body:
        if not isinstance(statement, ast.Assign):
            continue
        try:
            value = ast.literal_eval(statement.value)
        except (ValueError, SyntaxError):
            continue
        for target in statement.targets:
            if isinstance(target, ast.Name):
                values[target.id] = value
    return values


def _literal_or_helper_default(node: ast.AST) -> object:
    try:
        return ast.literal_eval(node)
    except (ValueError, SyntaxError):
        pass
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and len(node.args) >= 2:
        if node.func.id in {"_read_str_env", "_read_bool_env", "_read_int_env", "_read_path_env"}:
            try:
                return ast.literal_eval(node.args[1])
            except (ValueError, SyntaxError):
                return ""
    return ""


def _read_python_editable_assignments(path: Path) -> dict[str, object]:
    tree = ast.parse(path.read_text(encoding="utf-8-sig"), filename=str(path))
    values: dict[str, object] = {}
    for statement in tree.body:
        if not isinstance(statement, ast.Assign):
            continue
        value = _literal_or_helper_default(statement.value)
        for target in statement.targets:
            if isinstance(target, ast.Name):
                values[target.id] = value
    return values


def _python_literal(value: object) -> str:
    if isinstance(value, bool):
        return "True" if value else "False"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, list):
        return "[" + ", ".join(_python_literal(item) for item in value) + "]"
    if value is None:
        return "None"
    return repr(str(value))


def _coerce_editable_value(value: object, field_type: str) -> object:
    if field_type == "boolean":
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in TRUTHY_VALUES
    if field_type == "number":
        if isinstance(value, (int, float)):
            return int(value)
        raw = str(value).strip()
        return int(raw or "0")
    if field_type == "list":
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        return [item.strip() for item in str(value).split(",") if item.strip()]
    return "" if value is None else str(value)


def _update_python_assignments(path: Path, updates: dict[str, object], field_types: dict[str, str]) -> None:
    source = path.read_text(encoding="utf-8-sig")
    lines = source.splitlines()
    tree = ast.parse(source, filename=str(path))
    ranges: dict[str, tuple[int, int]] = {}
    for statement in tree.body:
        if not isinstance(statement, ast.Assign):
            continue
        for target in statement.targets:
            if isinstance(target, ast.Name):
                ranges[target.id] = (statement.lineno - 1, (statement.end_lineno or statement.lineno) - 1)

    ordered_updates = sorted(
        updates.items(),
        key=lambda item: ranges.get(item[0], (len(lines), len(lines)))[0],
        reverse=True,
    )
    for name, raw_value in ordered_updates:
        if name not in field_types:
            continue
        value = _coerce_editable_value(raw_value, field_types[name])
        assignment = f"{name} = {_python_literal(value)}"
        if name in ranges:
            start, end = ranges[name]
            lines[start:end + 1] = [assignment]
        else:
            lines.append(assignment)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _workspace_root(root: str | Path | None = None) -> Path:
    return Path(root).expanduser().resolve() if root else WORKSPACE_ROOT


def _config_file_path(root: str | Path | None, section: str) -> Path:
    file_info = LINKEDIN_CONFIG_FILES[section]
    return _workspace_root(root).joinpath(*file_info["path"]).resolve()


def load_editable_linkedin_config(root: str | Path | None = None) -> dict[str, object]:
    files: dict[str, object] = {}
    for section, file_info in LINKEDIN_CONFIG_FILES.items():
        config_path = _config_file_path(root, section)
        values = _read_python_editable_assignments(config_path) if config_path.exists() else {}
        fields = file_info["fields"]
        coerced_values = {
            name: _coerce_editable_value(
                values.get(name, [] if field_type == "list" else False if field_type == "boolean" else 0 if field_type == "number" else ""),
                field_type,
            )
            for name, field_type in fields.items()
        }
        files[section] = {
            "label": file_info["label"],
            "path": str(config_path),
            "fields": [{"name": name, "type": field_type, "value": coerced_values[name]} for name, field_type in fields.items()],
            "values": coerced_values,
        }
    return {"files": files}


def update_editable_linkedin_config(root: str | Path | None, updates: dict[str, object]) -> dict[str, object]:
    for section, raw_section_updates in updates.items():
        if section not in LINKEDIN_CONFIG_FILES or not isinstance(raw_section_updates, dict):
            continue
        config_path = _config_file_path(root, section)
        file_info = LINKEDIN_CONFIG_FILES[section]
        _update_python_assignments(config_path, raw_section_updates, file_info["fields"])
    return load_editable_linkedin_config(root)


def _read_python_config_file(path: Path) -> dict[str, str]:
    literal_values = _read_python_literal_assignments(path)
    values: dict[str, str] = {}
    for key, value in literal_values.items():
        env_key = key if any(key.startswith(prefix) for prefix in ALLOWED_ENV_PREFIXES) else PYTHON_CONFIG_ENV_ALIASES.get(key)
        if env_key:
            values[env_key] = _literal_to_env_value(value)
    return values


def load_config_preview(config_path: str | Path | None = None) -> dict[str, object]:
    if not config_path:
        return {}
    resolved = Path(config_path).expanduser().resolve()
    if resolved.suffix.lower() != ".py" or not resolved.exists():
        return {}
    literal_values = _read_python_literal_assignments(resolved)
    return {
        key: value
        for key, value in literal_values.items()
        if key in PYTHON_CONFIG_PREVIEW_KEYS
    }


def _set_process_environment(values: dict[str, str], *, override: bool) -> None:
    for key, value in values.items():
        if override or key not in os.environ:
            os.environ[key] = value


def _load_dotenv_file(path: Path, *, override: bool) -> bool:
    if not path.exists():
        return False
    if load_dotenv is not None:
        load_dotenv(dotenv_path=path, override=override)
        return True
    _set_process_environment(_read_env_file(path), override=override)
    return True


def bootstrap_runtime_environment(config_path: str | Path | None = None) -> dict[str, object]:
    loaded_sources: list[str] = []
    root_values: dict[str, str] = {}
    run_values: dict[str, str] = {}
    resolved_config: Path | None = None

    if PIPELINE_ENV_PATH.exists():
        root_values = _read_env_file(PIPELINE_ENV_PATH)
        if _load_dotenv_file(PIPELINE_ENV_PATH, override=False):
            loaded_sources.append(str(PIPELINE_ENV_PATH))

    if config_path:
        resolved_config = Path(config_path).expanduser().resolve()
        if resolved_config.exists() and resolved_config != PIPELINE_ENV_PATH:
            if resolved_config.suffix.lower() == ".py":
                run_values = _read_python_config_file(resolved_config)
                _set_process_environment(run_values, override=False)
                loaded_sources.append(str(resolved_config))
            else:
                run_values = _read_env_file(resolved_config)
                if _load_dotenv_file(resolved_config, override=False):
                    loaded_sources.append(str(resolved_config))

    stale_apollo_snapshot = (
        bool(root_values.get("APOLLO_API_KEY", "").strip())
        and bool(run_values)
        and not bool(run_values.get("APOLLO_API_KEY", "").strip())
    )
    if stale_apollo_snapshot:
        logger.info(
            "Run-specific config %s has an empty APOLLO_API_KEY. "
            "Keeping the root pipeline automation.env value already loaded into os.environ.",
            resolved_config,
        )
    if load_dotenv is None and _DOTENV_IMPORT_ERROR is not None:
        logger.warning(
            "python-dotenv is not installed; using built-in fallback env loader. "
            "Install it with `pip install python-dotenv`."
        )
    return {
        "sources": loaded_sources,
        "root_env_path": str(PIPELINE_ENV_PATH),
        "config_path": str(resolved_config) if resolved_config else "",
        "dotenv_available": load_dotenv is not None,
        "apollo_key_present": bool(os.getenv("APOLLO_API_KEY", "").strip()),
        "stale_apollo_snapshot": stale_apollo_snapshot,
    }


def _load_file_values(config_path: Path) -> tuple[dict[str, str], str]:
    suffix = config_path.suffix.lower()
    if suffix == ".json":
        payload = json.loads(config_path.read_text(encoding="utf-8"))
        values = {str(key): "" if value is None else str(value) for key, value in payload.items()}
        return values, f"config file {config_path}"
    if suffix == ".py":
        return _read_python_config_file(config_path), f"config file {config_path}"
    return _read_env_file(config_path), f"config file {config_path}"


def _collect_values(config_path: str | Path | None = None) -> tuple[dict[str, str], str]:
    bootstrap_runtime_environment(config_path)
    values: dict[str, str] = {}
    sources: list[str] = []

    for path in (PIPELINE_ENV_PATH, EMAIL_APP_ENV_PATH):
        if path.exists():
            file_values, source = _load_file_values(path)
            values.update(file_values)
            sources.append(source)

    if config_path:
        resolved = Path(config_path).expanduser().resolve()
        if not resolved.exists():
            raise AutomationConfigError(f"Automation config file not found: {resolved}")
        file_values, source = _load_file_values(resolved)
        values.update(file_values)
        sources.append(source)

    values.update({
        key: value
        for key, value in os.environ.items()
        if any(key.startswith(prefix) for prefix in ALLOWED_ENV_PREFIXES)
    })
    sources.append("environment variables")
    return values, ", ".join(sources)


def load_runtime_env_values(config_path: str | Path | None = None) -> dict[str, str]:
    values, _ = _collect_values(config_path)
    return {
        key: value
        for key, value in values.items()
        if any(key.startswith(prefix) for prefix in ALLOWED_ENV_PREFIXES)
    }


def _guess_sender_name(email_address: str) -> str:
    local_part = (email_address.split("@", 1)[0] if "@" in email_address else email_address).strip()
    if not local_part:
        return ""
    words = [word for word in local_part.replace(".", " ").replace("_", " ").replace("-", " ").split() if word]
    return " ".join(word[:1].upper() + word[1:] for word in words)


def _smtp_config_looks_like_placeholder(config: SMTPConfig | None) -> bool:
    if config is None:
        return False
    placeholder_values = {
        "your-email@gmail.com",
        "your-app-password",
        "your email@gmail.com",
        "your-app-password-here",
    }
    return any(
        value.strip().lower() in placeholder_values
        for value in (config.user, config.password, config.from_email)
    )


def load_automation_config(config_path: str | Path | None = None) -> AutomationConfig:
    values, source = _collect_values(config_path)
    auto_send_raw = (values.get("PIPELINE_AUTO_SEND") or "").strip()
    auto_send = _parse_bool(auto_send_raw, field_name="PIPELINE_AUTO_SEND") if auto_send_raw else False
    max_easy_apply_raw = (values.get("PIPELINE_MAX_EASY_APPLY") or "").strip() or "50"
    max_easy_apply = _parse_int(max_easy_apply_raw, field_name="PIPELINE_MAX_EASY_APPLY")

    delay_raw = (values.get("PIPELINE_SEND_DELAY_SECONDS") or "").strip() or "10"
    send_delay_seconds = _parse_int(delay_raw, field_name="PIPELINE_SEND_DELAY_SECONDS")
    linkedin_stage_timeout_raw = (values.get("PIPELINE_LINKEDIN_STAGE_TIMEOUT_SECONDS") or "").strip() or "1800"
    linkedin_stage_timeout_seconds = _parse_int(linkedin_stage_timeout_raw, field_name="PIPELINE_LINKEDIN_STAGE_TIMEOUT_SECONDS")
    linkedin_idle_timeout_raw = (values.get("PIPELINE_LINKEDIN_IDLE_TIMEOUT_SECONDS") or "").strip() or "300"
    linkedin_idle_timeout_seconds = _parse_int(linkedin_idle_timeout_raw, field_name="PIPELINE_LINKEDIN_IDLE_TIMEOUT_SECONDS")
    temporal_auto_start_raw = (values.get("PIPELINE_TEMPORAL_AUTO_START") or "").strip() or "true"
    temporal_auto_start = _parse_bool(temporal_auto_start_raw, field_name="PIPELINE_TEMPORAL_AUTO_START")
    run_once_always_fresh_raw = (values.get("PIPELINE_RUN_ONCE_ALWAYS_FRESH") or "").strip() or "false"
    run_once_always_fresh = _parse_bool(run_once_always_fresh_raw, field_name="PIPELINE_RUN_ONCE_ALWAYS_FRESH")
    provider_rate_limit_raw = (
        (values.get("PIPELINE_PROVIDER_RATE_LIMIT_PER_MINUTE") or "").strip()
        or str(DEFAULT_PROVIDER_RATE_LIMIT_PER_MINUTE)
    )
    provider_rate_limit_per_minute = _parse_int(
        provider_rate_limit_raw,
        field_name="PIPELINE_PROVIDER_RATE_LIMIT_PER_MINUTE",
    )
    provider_rate_limit_per_minute = max(
        1,
        min(provider_rate_limit_per_minute, MAX_PROVIDER_RATE_LIMIT_PER_MINUTE),
    )
    enrichment_sequential_raw = (values.get("PIPELINE_ENRICHMENT_SEQUENTIAL") or "").strip() or "true"
    enrichment_sequential = _parse_bool(
        enrichment_sequential_raw,
        field_name="PIPELINE_ENRICHMENT_SEQUENTIAL",
    )
    workflow_max_reruns_raw = (values.get("PIPELINE_WORKFLOW_MAX_RERUNS") or "").strip() or str(DEFAULT_WORKFLOW_MAX_RERUNS)
    workflow_max_reruns = _parse_int(
        workflow_max_reruns_raw,
        field_name="PIPELINE_WORKFLOW_MAX_RERUNS",
    )

    email_subject = (values.get("PIPELINE_EMAIL_SUBJECT") or "").strip()
    email_body = values.get("PIPELINE_EMAIL_BODY") or ""
    sender_name = (values.get("PIPELINE_SENDER_NAME") or "").strip()

    smtp_host = (values.get("SMTP_HOST") or "").strip()
    smtp_port_raw = (values.get("SMTP_PORT") or "").strip()
    smtp_secure_raw = (values.get("SMTP_SECURE") or "").strip()
    smtp_user = (values.get("SMTP_USER") or "").strip()
    smtp_pass = values.get("SMTP_PASS") or ""
    smtp_from = (values.get("SMTP_FROM") or "").strip() or smtp_user

    smtp_config: SMTPConfig | None = None
    auto_send_reason: str | None = None
    if any((smtp_host, smtp_port_raw, smtp_secure_raw, smtp_user, smtp_pass, smtp_from)):
        if not smtp_host:
            raise AutomationConfigError("SMTP_HOST is required when SMTP sending is configured.")
        if not smtp_port_raw:
            raise AutomationConfigError("SMTP_PORT is required when SMTP sending is configured.")
        if not smtp_secure_raw:
            raise AutomationConfigError("SMTP_SECURE is required when SMTP sending is configured.")
        if not smtp_user:
            raise AutomationConfigError("SMTP_USER is required when SMTP sending is configured.")
        if not smtp_pass:
            raise AutomationConfigError("SMTP_PASS is required when SMTP sending is configured.")
        smtp_config = SMTPConfig(
            host=smtp_host,
            port=_parse_int(smtp_port_raw, field_name="SMTP_PORT"),
            secure=_parse_bool(smtp_secure_raw, field_name="SMTP_SECURE"),
            user=smtp_user,
            password=smtp_pass,
            from_email=smtp_from,
        )
        if _smtp_config_looks_like_placeholder(smtp_config):
            auto_send = False
            auto_send_reason = (
                "Automatic sending is disabled because SMTP credentials still use placeholder values. "
                "Update SMTP_USER / SMTP_PASS / SMTP_FROM to enable sending."
            )

    if not auto_send and smtp_config is not None and auto_send_reason is None:
        auto_send = True
    if auto_send and not email_subject:
        email_subject = DEFAULT_EMAIL_SUBJECT
    if auto_send and not email_body.strip():
        email_body = DEFAULT_EMAIL_BODY
    if auto_send and not sender_name:
        sender_name = _guess_sender_name(smtp_from or smtp_user)

    if auto_send:
        if not email_subject:
            raise AutomationConfigError("PIPELINE_EMAIL_SUBJECT is required when PIPELINE_AUTO_SEND=true.")
        if not email_body.strip():
            raise AutomationConfigError("PIPELINE_EMAIL_BODY is required when PIPELINE_AUTO_SEND=true.")
        if not sender_name:
            raise AutomationConfigError("PIPELINE_SENDER_NAME is required when PIPELINE_AUTO_SEND=true.")
        if smtp_config is None:
            raise AutomationConfigError("SMTP configuration is required when PIPELINE_AUTO_SEND=true.")

    return AutomationConfig(
        auto_send=auto_send,
        auto_send_reason=auto_send_reason,
        max_easy_apply=max_easy_apply,
        send_delay_seconds=send_delay_seconds,
        linkedin_stage_timeout_seconds=linkedin_stage_timeout_seconds,
        linkedin_idle_timeout_seconds=linkedin_idle_timeout_seconds,
        temporal_auto_start=temporal_auto_start,
        run_once_always_fresh=run_once_always_fresh,
        provider_rate_limit_per_minute=provider_rate_limit_per_minute,
        enrichment_sequential=enrichment_sequential,
        workflow_max_reruns=workflow_max_reruns,
        email_subject=email_subject,
        email_body=email_body,
        sender_name=sender_name,
        smtp=smtp_config,
        source=source,
    )


def load_automation_summary(config_path: str | Path | None = None) -> dict[str, object]:
    try:
        values, _ = _collect_values(config_path)
        summary = load_automation_config(config_path).sanitized_summary()
        summary["linkedin"] = _linkedIn_login_summary(values)
        summary["config_preview"] = load_config_preview(config_path)
        return summary
    except AutomationConfigError as error:
        return {
            "auto_send": False,
            "source": str(config_path) if config_path else "environment variables",
            "error": str(error),
            "config_preview": load_config_preview(config_path),
        }

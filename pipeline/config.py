from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path


TRUTHY_VALUES = {"1", "true", "yes", "on"}
FALSY_VALUES = {"0", "false", "no", "off"}
WORKSPACE_ROOT = Path(__file__).resolve().parent.parent
EMAIL_APP_ENV_PATH = WORKSPACE_ROOT / "sendeamilwith code" / "email-automation-nodejs" / ".env.local"
PIPELINE_ENV_PATH = WORKSPACE_ROOT / "pipeline" / "automation.env"
DEFAULT_EMAIL_SUBJECT = "Quick note re: {{position}} at {{company}}"
DEFAULT_EMAIL_BODY = (
    "Hi {{name}},\n\n"
    "I recently applied for the {{position}} role at {{company}} and wanted to introduce myself directly.\n\n"
    "I would love the opportunity to connect and share how I can contribute.\n\n"
    "Best regards,\n"
    "{{sendername}}"
)


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
    max_easy_apply: int
    send_delay_seconds: int
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
            "sender_name": self.sender_name,
            "email_subject": self.email_subject,
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


def _load_file_values(config_path: Path) -> tuple[dict[str, str], str]:
    suffix = config_path.suffix.lower()
    if suffix == ".json":
        payload = json.loads(config_path.read_text(encoding="utf-8"))
        values = {str(key): "" if value is None else str(value) for key, value in payload.items()}
        return values, f"config file {config_path}"
    return _read_env_file(config_path), f"config file {config_path}"


def _collect_values(config_path: str | Path | None = None) -> tuple[dict[str, str], str]:
    if config_path:
        resolved = Path(config_path).expanduser().resolve()
        if not resolved.exists():
            raise AutomationConfigError(f"Automation config file not found: {resolved}")
        return _load_file_values(resolved)

    values: dict[str, str] = {}
    sources: list[str] = []
    for path in (PIPELINE_ENV_PATH, EMAIL_APP_ENV_PATH):
        if path.exists():
            file_values, source = _load_file_values(path)
            values.update(file_values)
            sources.append(source)

    values.update({
        key: value
        for key, value in os.environ.items()
        if key.startswith("PIPELINE_") or key.startswith("SMTP_")
    })
    sources.append("environment variables")
    return values, ", ".join(sources)


def load_runtime_env_values(config_path: str | Path | None = None) -> dict[str, str]:
    values, _ = _collect_values(config_path)
    return {
        key: value
        for key, value in values.items()
        if key.startswith("PIPELINE_") or key.startswith("SMTP_")
    }


def _guess_sender_name(email_address: str) -> str:
    local_part = (email_address.split("@", 1)[0] if "@" in email_address else email_address).strip()
    if not local_part:
        return ""
    words = [word for word in local_part.replace(".", " ").replace("_", " ").replace("-", " ").split() if word]
    return " ".join(word[:1].upper() + word[1:] for word in words)


def load_automation_config(config_path: str | Path | None = None) -> AutomationConfig:
    values, source = _collect_values(config_path)
    auto_send_raw = (values.get("PIPELINE_AUTO_SEND") or "").strip()
    auto_send = _parse_bool(auto_send_raw, field_name="PIPELINE_AUTO_SEND") if auto_send_raw else False
    max_easy_apply_raw = (values.get("PIPELINE_MAX_EASY_APPLY") or "").strip() or "50"
    max_easy_apply = _parse_int(max_easy_apply_raw, field_name="PIPELINE_MAX_EASY_APPLY")

    delay_raw = (values.get("PIPELINE_SEND_DELAY_SECONDS") or "").strip() or "10"
    send_delay_seconds = _parse_int(delay_raw, field_name="PIPELINE_SEND_DELAY_SECONDS")

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

    if not auto_send and smtp_config is not None:
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
        max_easy_apply=max_easy_apply,
        send_delay_seconds=send_delay_seconds,
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
        return summary
    except AutomationConfigError as error:
        return {
            "auto_send": False,
            "source": str(config_path) if config_path else "environment variables",
            "error": str(error),
        }

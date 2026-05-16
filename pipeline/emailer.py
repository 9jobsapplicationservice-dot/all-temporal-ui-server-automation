from __future__ import annotations

import csv
import logging
import smtplib
import time
from dataclasses import dataclass
from datetime import datetime
from email.message import EmailMessage
from pathlib import Path

from .config import AutomationConfig
from .core.sentry_config import build_pipeline_tags, capture_exception_with_context


TRANSIENT_SMTP_MARKERS = (
    "etimedout",
    "econnreset",
    "econnrefused",
    "connection closed",
    "connection reset",
    "timed out",
    "greeting never received",
    "421",
    "450",
    "451",
    "452",
)

AUTH_SMTP_MARKERS = (
    "badcredentials",
    "username and password not accepted",
    "authentication failed",
    "smtp authentication",
    "5.7.8",
    "535",
)
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EmailContact:
    email: str
    secondary_email: str
    name: str
    company: str
    position: str
    job_link: str


@dataclass(frozen=True)
class EmailLog:
    email: str
    success: bool
    error: str
    timestamp: str
    message_id: str
    name: str
    company: str
    position: str
    job_link: str


class EmailSendError(RuntimeError):
    pass


def read_sendable_contacts(csv_path: str | Path) -> list[EmailContact]:
    contacts: list[EmailContact] = []
    seen_emails: set[str] = set()
    with Path(csv_path).open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if not any(str(value or "").strip() for value in row.values()):
                continue
            primary = (row.get("HR Email") or "").strip()
            secondary = (row.get("HR Secondary Email") or "").strip()
            email = primary if "@" in primary else secondary if "@" in secondary else ""
            if not email:
                continue
            normalized = email.lower()
            if normalized in seen_emails:
                continue
            seen_emails.add(normalized)
            contacts.append(
                EmailContact(
                    email=email,
                    secondary_email=secondary if secondary.lower() != normalized else "",
                    name=(row.get("HR Name") or "").strip() or "Hiring Team",
                    company=(row.get("Company Name") or "").strip(),
                    position=((row.get("Position") or "").strip() or (row.get("HR Position") or "").strip()),
                    job_link=(row.get("Job Link") or "").strip(),
                )
            )
    return contacts


def _replace_template_vars(value: str, contact: EmailContact, sender_name: str) -> str:
    replacements = {
        "name": contact.name,
        "company": contact.company,
        "position": contact.position,
        "joblink": contact.job_link,
        "sendername": sender_name,
    }
    rendered = value
    for key, replacement in replacements.items():
        rendered = rendered.replace(f"{{{{{key}}}}}", replacement or f"{{{{{key}}}}}")
    return rendered


def _is_transient_error(error: Exception) -> bool:
    lowered = str(error).lower()
    return any(marker in lowered for marker in TRANSIENT_SMTP_MARKERS)


def is_transient_email_error_message(message: str) -> bool:
    lowered = (message or "").lower()
    return any(marker in lowered for marker in TRANSIENT_SMTP_MARKERS)


def is_auth_email_error_message(message: str) -> bool:
    lowered = (message or "").lower()
    return any(marker in lowered for marker in AUTH_SMTP_MARKERS)


def _send_single_email(config: AutomationConfig, contact: EmailContact) -> str:
    if config.smtp is None:
        raise EmailSendError("SMTP configuration is missing.")

    subject = _replace_template_vars(config.email_subject, contact, config.sender_name)
    body = _replace_template_vars(config.email_body, contact, config.sender_name)
    message = EmailMessage()
    message["From"] = config.smtp.from_email
    message["To"] = contact.email
    message["Subject"] = subject
    message.set_content(body)

    last_error: Exception | None = None
    for attempt in range(1, 4):
        try:
            if config.smtp.secure:
                with smtplib.SMTP_SSL(config.smtp.host, config.smtp.port, timeout=30) as server:
                    server.login(config.smtp.user, config.smtp.password)
                    return server.send_message(message) or message.get("Message-ID", "")
            with smtplib.SMTP(config.smtp.host, config.smtp.port, timeout=30) as server:
                server.ehlo()
                server.starttls()
                server.ehlo()
                server.login(config.smtp.user, config.smtp.password)
                return server.send_message(message) or message.get("Message-ID", "")
        except Exception as error:  # pragma: no cover - library error surfaces
            last_error = error
            logger.exception(
                "SMTP send attempt failed. email=%s company=%s position=%s attempt=%s",
                contact.email,
                contact.company,
                contact.position,
                attempt,
            )
            capture_exception_with_context(
                error,
                message="smtp send attempt failed",
                tags=build_pipeline_tags(stage="email"),
                extras={
                    "email": contact.email,
                    "company": contact.company,
                    "position": contact.position,
                    "smtp_host": config.smtp.host if config.smtp else "",
                    "attempt": attempt,
                },
            )
            if attempt == 3 or not _is_transient_error(error):
                break
            time.sleep(attempt)

    raise EmailSendError(str(last_error or "Failed to send email."))


def read_existing_successes(report_path: str | Path) -> set[str]:
    path = Path(report_path)
    if not path.exists():
        return set()

    successes: set[str] = set()
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            email = (row.get("Email") or "").strip().lower()
            success = (row.get("Success") or "").strip().lower()
            if email and success == "true":
                successes.add(email)
    return successes


def write_send_report(report_path: str | Path, logs: list[EmailLog]) -> str:
    path = Path(report_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["Timestamp", "Email", "Success", "Error", "Message ID", "Name", "Company", "Position", "Job Link"])
        for log in logs:
            writer.writerow([
                log.timestamp,
                log.email,
                "true" if log.success else "false",
                log.error,
                log.message_id,
                log.name,
                log.company,
                log.position,
                log.job_link,
            ])
    return str(path)


def send_run_emails(record: dict, config: AutomationConfig) -> dict[str, object]:
    contacts = read_sendable_contacts(record["recruiters_csv_path"])
    already_sent = read_existing_successes(record["send_report_path"])
    logs: list[EmailLog] = []

    existing_report = Path(record["send_report_path"])
    if existing_report.exists():
        with existing_report.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                logs.append(
                    EmailLog(
                        email=(row.get("Email") or "").strip(),
                        success=(row.get("Success") or "").strip().lower() == "true",
                        error=(row.get("Error") or "").strip(),
                        timestamp=(row.get("Timestamp") or "").strip(),
                        message_id=(row.get("Message ID") or "").strip(),
                        name=(row.get("Name") or "").strip(),
                        company=(row.get("Company") or "").strip(),
                        position=(row.get("Position") or "").strip(),
                        job_link=(row.get("Job Link") or "").strip(),
                    )
                )

    for index, contact in enumerate(contacts):
        if contact.email.lower() in already_sent:
            continue
        timestamp = datetime.now().isoformat(timespec="seconds")
        try:
            message_id = _send_single_email(config, contact)
            logs.append(
                EmailLog(
                    email=contact.email,
                    success=True,
                    error="",
                    timestamp=timestamp,
                    message_id=str(message_id),
                    name=contact.name,
                    company=contact.company,
                    position=contact.position,
                    job_link=contact.job_link,
                )
            )
        except EmailSendError as error:
            logger.exception(
                "Email send failed for contact. email=%s company=%s position=%s",
                contact.email,
                contact.company,
                contact.position,
            )
            capture_exception_with_context(
                error,
                message="email send failed for contact",
                tags=build_pipeline_tags(stage="email"),
                extras={
                    "email": contact.email,
                    "company": contact.company,
                    "position": contact.position,
                    "job_link": contact.job_link,
                },
            )
            logs.append(
                EmailLog(
                    email=contact.email,
                    success=False,
                    error=str(error),
                    timestamp=timestamp,
                    message_id="",
                    name=contact.name,
                    company=contact.company,
                    position=contact.position,
                    job_link=contact.job_link,
                )
            )
        write_send_report(record["send_report_path"], logs)
        if config.send_delay_seconds > 0 and index < len(contacts) - 1:
            time.sleep(config.send_delay_seconds)

    sent = sum(1 for log in logs if log.success)
    failed = sum(1 for log in logs if not log.success)
    transient_failures = sum(1 for log in logs if not log.success and is_transient_email_error_message(log.error))
    auth_failures = sum(1 for log in logs if not log.success and is_auth_email_error_message(log.error))
    return {
        "contacts": contacts,
        "logs": logs,
        "email_total": len(contacts),
        "email_sent": sent,
        "email_failed": failed,
        "transient_failure_count": transient_failures,
        "auth_failure_count": auth_failures,
        "permanent_failure_count": max(failed - transient_failures, 0),
        "report_path": str(record["send_report_path"]),
    }

from __future__ import annotations

import time
from pathlib import Path

from .adapters import preflight_linkedin_runtime, run_linkedin_stage, run_rocketreach_stage
from .constants import DEFAULT_POLL_INTERVAL_SECONDS
from . import stage_manager as stage_manager_module
from .stage_manager import PipelineStageManager
from .storage import PipelineStore


def build_linkedin_note(summary: dict) -> str:
    jobs_applied = int(summary.get("jobs_applied", 0) or 0)
    rows_written = int(summary.get("rows_written_to_applied_csv", 0) or 0)
    external_links = int(summary.get("external_links_logged", 0) or 0)
    missing_hr = int(summary.get("rows_missing_hr_profile", 0) or 0)
    return (
        "LinkedIn stage completed. "
        f"jobs_applied={jobs_applied} rows_written={rows_written} "
        f"external_links={external_links} rows_missing_hr_profile={missing_hr}. "
        "Queued for RocketReach enrichment."
    )



def build_rocketreach_note(stats: dict) -> str:
    total = int(stats.get("total", 0) or 0)
    matched = int(stats.get("matched", 0) or 0)
    preview = int(stats.get("preview_match", 0) or 0)
    sendable = int(stats.get("sendable_rows", 0) or 0)
    missing = int(stats.get("missing_hr_link", 0) or 0)
    invalid = int(stats.get("invalid_hr_link", 0) or 0)
    profile_only = int(stats.get("profile_only", 0) or 0)
    no_match = int(stats.get("no_match", 0) or 0)
    quota = int(stats.get("lookup_quota_reached", 0) or 0)
    auth_failed = int(stats.get("authentication_failed", 0) or 0)
    output_note = str(stats.get("output_note", "") or "").strip()
    if auth_failed > 0:
        note = (
            "Recruiter CSV generated without RocketReach contacts because authentication failed. "
            f"total={total} authentication_failed={auth_failed}."
        )
        if output_note:
            note = f"{note} {output_note}"
        return note
    if sendable == 0:
        if quota > 0:
            skip_reason = "RocketReach lookup quota/credit/account verification blocked email lookup."
        elif missing > 0 and no_match == 0 and profile_only == 0 and preview == 0:
            skip_reason = "LinkedIn did not provide recruiter profile links for enrichment."
        elif no_match > 0:
            skip_reason = "RocketReach returned no matching recruiter emails."
        elif preview > 0:
            skip_reason = "RocketReach returned preview/masked contacts only."
        elif profile_only > 0:
            skip_reason = "RocketReach returned profiles without usable emails."
        else:
            skip_reason = "RocketReach returned no sendable emails."
        note = (
            f"Contacts enriched with no sendable emails. {skip_reason} "
            f"total={total} matched={matched} missing_hr_link={missing} invalid_hr_link={invalid} "
            f"preview_match={preview} profile_only={profile_only} no_match={no_match} lookup_quota_reached={quota}."
        )
        if output_note:
            note = f"{note} {output_note}"
        return note
    note = (
        "Contacts enriched successfully. Ready for automated email sending. "
        f"total={total} matched={matched} sendable_rows={sendable} missing_hr_link={missing} "
        f"invalid_hr_link={invalid} preview_match={preview} profile_only={profile_only} no_match={no_match} lookup_quota_reached={quota}."
    )
    if output_note:
        note = f"{note} {output_note}"
    return note


def build_email_note(stats: dict[str, object]) -> str:
    total = int(stats.get("email_total", 0) or 0)
    sent = int(stats.get("email_sent", 0) or 0)
    failed = int(stats.get("email_failed", 0) or 0)
    if failed > 0:
        return f"Automated email stage finished with failures. total={total} sent={sent} failed={failed}."
    return f"Automated email stage completed successfully. total={total} sent={sent} failed={failed}."


def is_waiting_login_error(message: str) -> bool:
    lowered = (message or "").lower()
    markers = (
        "linkedin login was not confirmed",
        "browser window closed or session became invalid",
        "complete manual login in chrome and keep the browser window open",
        "chrome default profile crashed while linkedin was opening",
        "chrome startup needs manual recovery",
    )
    return any(marker in lowered for marker in markers)


def build_waiting_login_note(automation_summary: dict | None) -> str:
    linkedin_summary = (automation_summary or {}).get("linkedin") if isinstance(automation_summary, dict) else None
    mode = linkedin_summary.get("mode") if isinstance(linkedin_summary, dict) else None
    if mode == "auto_login":
        return "LinkedIn auto-login needs attention. Check credentials, captcha, 2FA, then retry this run."
    return "Chrome opened with your default profile. Log into LinkedIn there and keep the browser window open."



class PipelineWorker:
    def __init__(self, root: str | Path | None = None, poll_interval: float = DEFAULT_POLL_INTERVAL_SECONDS) -> None:
        self.store = PipelineStore(root)
        self.stage_manager = PipelineStageManager(self.store)
        self.poll_interval = poll_interval

    def recover(self) -> list[dict]:
        self._sync_stage_manager_hooks()
        recovered = self.store.recover_interrupted_runs()
        recovered.extend(self.stage_manager.resume_blocked_runtime_runs())
        return recovered

    def run_forever(self) -> None:
        while True:
            processed = self.process_next_run()
            if not processed:
                time.sleep(self.poll_interval)

    def process_available_runs_once(self) -> int:
        processed_count = 0
        while self.process_next_run():
            processed_count += 1
        return processed_count

    def process_next_run(self) -> bool:
        self._sync_stage_manager_hooks()
        self.stage_manager.resume_blocked_runtime_runs()
        record = self.store.get_next_queued_run()
        if not record:
            return False
        self.process_run(record["id"])
        return True

    def process_run(self, run_id: str) -> dict:
        self._sync_stage_manager_hooks()
        return self.stage_manager.process_run(run_id)

    def _sync_stage_manager_hooks(self) -> None:
        # Keep worker-level monkeypatches effective for existing tests and callers.
        stage_manager_module.preflight_linkedin_runtime = preflight_linkedin_runtime
        stage_manager_module.run_linkedin_stage = run_linkedin_stage
        stage_manager_module.run_rocketreach_stage = run_rocketreach_stage

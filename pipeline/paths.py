from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .constants import resolve_pipeline_root


@dataclass(frozen=True)
class RunPaths:
    run_id: str
    run_dir: Path
    csv_dir: Path
    job_applied_dir: Path
    external_dir: Path
    rocket_enrich_dir: Path
    applied_csv: Path
    external_jobs_csv: Path
    recruiters_csv: Path
    send_report_csv: Path
    manifest_json: Path
    logs_dir: Path
    linkedin_stdout_log: Path
    linkedin_stderr_log: Path
    rocketreach_stdout_log: Path
    rocketreach_stderr_log: Path
    failed_jobs_csv: Path
    config_copy_path: Path

    def ensure_directories(self) -> None:
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.csv_dir.mkdir(parents=True, exist_ok=True)
        self.job_applied_dir.mkdir(parents=True, exist_ok=True)
        self.external_dir.mkdir(parents=True, exist_ok=True)
        self.rocket_enrich_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.manifest_json.parent.mkdir(parents=True, exist_ok=True)
        self.send_report_csv.parent.mkdir(parents=True, exist_ok=True)
        self.config_copy_path.parent.mkdir(parents=True, exist_ok=True)


@dataclass(frozen=True)
class PipelinePaths:
    root: Path
    database: Path
    runs_dir: Path
    meta_dir: Path
    logs_root: Path
    screenshots_dir: Path
    reports_dir: Path
    configs_dir: Path

    @classmethod
    def create(cls, root: str | Path | None = None) -> "PipelinePaths":
        resolved_root = resolve_pipeline_root(root)
        return cls(
            root=resolved_root,
            database=resolved_root / "pipeline.db",
            runs_dir=resolved_root / "runs",
            meta_dir=resolved_root / "meta",
            logs_root=resolved_root / "logs",
            screenshots_dir=resolved_root / "logs" / "screenshots",
            reports_dir=resolved_root / "reports",
            configs_dir=resolved_root / "configs",
        )

    def ensure_directories(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        self.runs_dir.mkdir(parents=True, exist_ok=True)
        self.meta_dir.mkdir(parents=True, exist_ok=True)
        self.logs_root.mkdir(parents=True, exist_ok=True)
        self.screenshots_dir.mkdir(parents=True, exist_ok=True)
        self.reports_dir.mkdir(parents=True, exist_ok=True)
        self.configs_dir.mkdir(parents=True, exist_ok=True)

    def for_run(self, run_id: str, config_name: str | None = None) -> RunPaths:
        run_dir = self.root / run_id
        csv_dir = run_dir / "csv"
        job_applied_dir = run_dir / "job_applied"
        external_dir = run_dir / "external"
        rocket_enrich_dir = run_dir / "rocket_enrich"
        logs_dir = self.logs_root / run_id
        config_filename = config_name or "config.json"
        return RunPaths(
            run_id=run_id,
            run_dir=run_dir,
            csv_dir=csv_dir,
            job_applied_dir=job_applied_dir,
            external_dir=external_dir,
            rocket_enrich_dir=rocket_enrich_dir,
            applied_csv=job_applied_dir / "applied_jobs.csv",
            external_jobs_csv=external_dir / "external_jobs.csv",
            recruiters_csv=rocket_enrich_dir / "recruiters_enriched.csv",
            send_report_csv=self.reports_dir / f"{run_id}.csv",
            manifest_json=self.meta_dir / f"{run_id}.json",
            logs_dir=logs_dir,
            linkedin_stdout_log=logs_dir / "linkedin.stdout.log",
            linkedin_stderr_log=logs_dir / "linkedin.stderr.log",
            rocketreach_stdout_log=logs_dir / "rocketreach.stdout.log",
            rocketreach_stderr_log=logs_dir / "rocketreach.stderr.log",
            failed_jobs_csv=logs_dir / "failed_jobs.csv",
            config_copy_path=self.configs_dir / f"{run_id}-{config_filename}",
        )

from __future__ import annotations

import shutil
import sqlite3
import uuid
from pathlib import Path

from .manifest import write_manifest
from .paths import PipelinePaths
from .utils import recruiter_sendable_row_count, utc_now_iso


CREATE_RUNS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    config_path TEXT,
    run_dir TEXT NOT NULL,
    applied_csv_path TEXT NOT NULL,
    external_jobs_csv_path TEXT NOT NULL,
    recruiters_csv_path TEXT NOT NULL,
    send_report_path TEXT NOT NULL,
    manifest_path TEXT NOT NULL,
    log_dir TEXT NOT NULL,
    linkedin_stdout_log TEXT NOT NULL,
    linkedin_stderr_log TEXT NOT NULL,
    rocketreach_stdout_log TEXT NOT NULL,
    rocketreach_stderr_log TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    stage_started_at TEXT,
    stage_finished_at TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0,
    email_total INTEGER NOT NULL DEFAULT 0,
    email_sent INTEGER NOT NULL DEFAULT 0,
    email_failed INTEGER NOT NULL DEFAULT 0,
    provider_success_count INTEGER NOT NULL DEFAULT 0,
    no_email_count INTEGER NOT NULL DEFAULT 0,
    provider_retry_count INTEGER NOT NULL DEFAULT 0,
    workflow_retry_count INTEGER NOT NULL DEFAULT 0,
    temporal_workflow_id TEXT NOT NULL DEFAULT '',
    temporal_task_queue TEXT NOT NULL DEFAULT '',
    orchestration_backend TEXT NOT NULL DEFAULT '',
    last_workflow_rerun_reason TEXT NOT NULL DEFAULT '',
    last_failed_stage TEXT NOT NULL DEFAULT '',
    note TEXT,
    last_error TEXT
)
"""

CREATE_ENRICHMENT_CACHE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS enrichment_cache (
    fingerprint TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    provider TEXT NOT NULL DEFAULT '',
    lookup_status TEXT NOT NULL DEFAULT '',
    email TEXT NOT NULL DEFAULT '',
    secondary_email TEXT NOT NULL DEFAULT '',
    contact TEXT NOT NULL DEFAULT '',
    provider_attempts INTEGER NOT NULL DEFAULT 0,
    provider_retry_count INTEGER NOT NULL DEFAULT 0,
    last_provider_error TEXT NOT NULL DEFAULT '',
    raw_payload TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL
)
"""


class PipelineStore:
    def __init__(self, root: str | Path | None = None) -> None:
        self.paths = PipelinePaths.create(root)
        self.paths.ensure_directories()
        self._initialize()
        self._migrate_existing_artifacts()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.paths.database)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.execute(CREATE_RUNS_TABLE_SQL)
            connection.execute(CREATE_ENRICHMENT_CACHE_TABLE_SQL)
            existing_columns = {
                row["name"]
                for row in connection.execute("PRAGMA table_info(runs)").fetchall()
            }
            if "external_jobs_csv_path" not in existing_columns:
                connection.execute(
                    "ALTER TABLE runs ADD COLUMN external_jobs_csv_path TEXT NOT NULL DEFAULT ''"
                )
            for column_name in (
                "email_total",
                "email_sent",
                "email_failed",
                "provider_success_count",
                "no_email_count",
                "provider_retry_count",
                "workflow_retry_count",
            ):
                if column_name not in existing_columns:
                    connection.execute(
                        f"ALTER TABLE runs ADD COLUMN {column_name} INTEGER NOT NULL DEFAULT 0"
                    )
            for column_name in (
                "temporal_workflow_id",
                "temporal_task_queue",
                "orchestration_backend",
                "last_workflow_rerun_reason",
                "last_failed_stage",
            ):
                if column_name not in existing_columns:
                    connection.execute(
                        f"ALTER TABLE runs ADD COLUMN {column_name} TEXT NOT NULL DEFAULT ''"
                    )
            connection.commit()

    def _copy_config(self, run_paths, config_path: str | None) -> str:
        if not config_path:
            return ""

        source_path = Path(config_path).expanduser().resolve()
        if not source_path.exists():
            raise FileNotFoundError(f"Config file not found: {source_path}")

        target_path = self.paths.for_run(
            run_paths.run_id,
            self._normalized_config_name(run_paths.run_id, source_path.name),
        ).config_copy_path
        target_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_path, target_path)
        return str(target_path)

    def _normalized_config_name(self, run_id: str, config_path: str | None) -> str:
        config_name = Path(config_path).name if config_path else "config.json"
        prefix = f"{run_id}-"
        while config_name.startswith(prefix):
            config_name = config_name[len(prefix):]
        return config_name or "config.json"

    def _reset_live_artifacts(self, run_paths) -> None:
        for artifact_path in (
            run_paths.external_jobs_csv,
            run_paths.recruiters_csv,
            run_paths.failed_jobs_csv,
        ):
            artifact_path.parent.mkdir(parents=True, exist_ok=True)
            artifact_path.unlink(missing_ok=True)

    def reset_live_artifacts_for_run(self, run_id: str) -> dict:
        record = self.get_run(run_id)
        config_name = Path(record['config_path']).name if record.get('config_path') else None
        run_paths = self.paths.for_run(run_id, config_name)
        run_paths.ensure_directories()
        self._reset_live_artifacts(run_paths)
        return self.get_run(run_id)

    def reset_fresh_artifacts_for_run(self, run_id: str) -> dict:
        record = self.get_run(run_id)
        config_name = Path(record['config_path']).name if record.get('config_path') else None
        run_paths = self.paths.for_run(run_id, config_name)
        run_paths.ensure_directories()
        for artifact_path in (
            run_paths.applied_csv,
            run_paths.external_jobs_csv,
            run_paths.recruiters_csv,
            run_paths.failed_jobs_csv,
            run_paths.send_report_csv,
            run_paths.linkedin_stdout_log,
            run_paths.linkedin_stderr_log,
            run_paths.rocketreach_stdout_log,
            run_paths.rocketreach_stderr_log,
        ):
            artifact_path.parent.mkdir(parents=True, exist_ok=True)
            artifact_path.unlink(missing_ok=True)
        return self.get_run(run_id)

    def get_active_live_run(self, exclude_run_id: str | None = None) -> dict | None:
        runs = self.list_active_live_runs(exclude_run_id=exclude_run_id)
        if not runs:
            return None
        shared_run_dir = self.paths.runs_dir.resolve()
        shared_runs = [
            record for record in runs
            if Path(record.get('run_dir') or '').resolve() == shared_run_dir
        ]
        if shared_runs:
            return shared_runs[-1]
        return runs[0]

    def list_active_live_runs(self, exclude_run_id: str | None = None) -> list[dict]:
        active_statuses = ('queued', 'waiting_login', 'linkedin_running', 'rocketreach_running', 'email_running', 'waiting_review', 'sending')
        placeholders = ', '.join('?' for _ in active_statuses)
        params: list[object] = list(active_statuses)
        query = f'SELECT * FROM runs WHERE status IN ({placeholders})'
        if exclude_run_id:
            query += ' AND id != ?'
            params.append(exclude_run_id)
        query += ' ORDER BY created_at ASC'

        with self._connect() as connection:
            rows = connection.execute(query, tuple(params)).fetchall()
        shared_run_dir = self.paths.runs_dir.resolve()
        results: list[dict] = []
        for row in rows:
            record = dict(row)
            run_dir = Path(record.get('run_dir') or '').resolve()
            if run_dir != shared_run_dir and run_dir.name == record['id']:
                continue
            if (
                record.get('status') == 'waiting_review'
                and recruiter_sendable_row_count(record.get('recruiters_csv_path') or '') == 0
            ):
                continue
            results.append(record)
        return results

    def _move_path(self, source: Path, destination: Path) -> bool:
        if not source.exists() or source.resolve() == destination.resolve():
            return False

        destination.parent.mkdir(parents=True, exist_ok=True)
        if source.is_dir():
            if destination.exists():
                for child in source.iterdir():
                    self._move_path(child, destination / child.name)
                shutil.rmtree(source, ignore_errors=True)
            else:
                shutil.move(str(source), str(destination))
            return True

        if destination.exists():
            source.unlink(missing_ok=True)
            return True
        try:
            shutil.move(str(source), str(destination))
        except FileNotFoundError:
            return False
        return True

    def _migrate_existing_artifacts(self) -> None:
        records = self.list_runs()
        if not records:
            return

        migrated_run_ids: list[str] = []
        with self._connect() as connection:
            try:
                for record in records:
                    config_name = self._normalized_config_name(record['id'], record.get('config_path'))
                    run_paths = self.paths.for_run(record['id'], config_name)
                    run_paths.ensure_directories()

                    old_run_dir = Path(record['run_dir'])
                    old_applied_csv = Path(record['applied_csv_path'])
                    old_external_jobs_csv = Path(record['external_jobs_csv_path'])
                    old_recruiters_csv = Path(record['recruiters_csv_path'])
                    old_manifest = Path(record['manifest_path'])
                    old_logs = Path(record['log_dir'])
                    old_send_report = Path(record['send_report_path'])
                    old_config = Path(record['config_path']) if record.get('config_path') else None
                    old_failed_jobs = Path(record['run_dir']) / 'failed_jobs.csv'

                    self._move_path(old_applied_csv, run_paths.applied_csv)
                    self._move_path(old_external_jobs_csv, run_paths.external_jobs_csv)
                    self._move_path(old_recruiters_csv, run_paths.recruiters_csv)
                    self._move_path(old_manifest, run_paths.manifest_json)
                    self._move_path(old_logs, run_paths.logs_dir)
                    self._move_path(old_send_report, run_paths.send_report_csv)
                    self._move_path(old_failed_jobs, run_paths.failed_jobs_csv)
                    if old_config is not None and old_config.exists():
                        self._move_path(old_config, run_paths.config_copy_path)
                    if (
                        old_run_dir.exists()
                        and old_run_dir.resolve() != run_paths.run_dir.resolve()
                        and old_run_dir.name == record['id']
                    ):
                        shutil.rmtree(old_run_dir, ignore_errors=True)

                    migrated_config_path = ''
                    if old_config is not None:
                        if run_paths.config_copy_path.exists():
                            migrated_config_path = str(run_paths.config_copy_path)
                        elif old_config.exists():
                            migrated_config_path = str(old_config)

                    updates = {
                        'config_path': migrated_config_path,
                        'run_dir': str(run_paths.run_dir),
                        'applied_csv_path': str(run_paths.applied_csv),
                        'external_jobs_csv_path': str(run_paths.external_jobs_csv),
                        'recruiters_csv_path': str(run_paths.recruiters_csv),
                        'send_report_path': str(run_paths.send_report_csv),
                        'manifest_path': str(run_paths.manifest_json),
                        'log_dir': str(run_paths.logs_dir),
                        'linkedin_stdout_log': str(run_paths.linkedin_stdout_log),
                        'linkedin_stderr_log': str(run_paths.linkedin_stderr_log),
                        'rocketreach_stdout_log': str(run_paths.rocketreach_stdout_log),
                        'rocketreach_stderr_log': str(run_paths.rocketreach_stderr_log),
                    }
                    last_error = (record.get('last_error') or '').strip()
                    if record.get('status') == 'failed' and (
                        'PIPELINE_LINKEDIN_PYTHON' in last_error
                        or 'LinkedIn runtime setup required.' in last_error
                    ):
                        updates['status'] = 'blocked_runtime'
                        updates['note'] = 'LinkedIn runtime setup required.'
                    if (
                        record.get('status') == 'failed'
                        and (
                            'LinkedIn login was not confirmed' in last_error
                            or 'Browser window closed or session became invalid.' in last_error
                            or 'Complete manual login in Chrome and keep the browser window open.' in last_error
                            or 'Automatic LinkedIn login did not complete successfully.' in last_error
                            or 'session was blocked by LinkedIn' in last_error
                        )
                    ):
                        updates['status'] = 'waiting_login'
                        updates['note'] = 'LinkedIn needs manual confirmation in Chrome. Complete login there and keep the browser window open.'
                    if (
                        record.get('status') == 'failed'
                        and record.get('note') == 'LinkedIn stage completed with zero saved applied rows.'
                        and record.get('last_error') == 'No confirmed Easy Apply submissions were written to applied_jobs.csv.'
                    ):
                        updates['status'] = 'completed'
                        updates['note'] = 'LinkedIn stage completed with no confirmed Easy Apply submissions. Nothing was queued for enrichment.'
                        updates['last_error'] = ''
                    if (
                        record.get('status') == 'waiting_review'
                        and recruiter_sendable_row_count(record.get('recruiters_csv_path') or '') == 0
                    ):
                        updates['status'] = 'completed'
                        updates['note'] = 'RocketReach enrichment completed with no sendable recruiter emails.'
                    changed_updates = {
                        column: value
                        for column, value in updates.items()
                        if record.get(column) != value
                    }
                    if not changed_updates:
                        continue

                    assignments = ', '.join(f"{column} = ?" for column in changed_updates)
                    connection.execute(
                        f"UPDATE runs SET {assignments} WHERE id = ?",
                        tuple(changed_updates.values()) + (record['id'],),
                    )
                    migrated_run_ids.append(record['id'])

                if migrated_run_ids:
                    connection.commit()
            except sqlite3.OperationalError as error:
                if "readonly" in str(error).lower():
                    return
                raise

        for run_id in migrated_run_ids:
            write_manifest(self.get_run(run_id))

    def create_run(self, run_id: str | None = None, config_path: str | None = None, *, allow_active_conflict: bool = False) -> dict:
        created_run_id = run_id or f"run-{uuid.uuid4().hex[:12]}"
        active_run = None if allow_active_conflict else self.get_active_live_run()
        if active_run is not None:
            raise RuntimeError(
                "A pipeline run is already active. "
                f"Finish or clear run {active_run['id']} before enqueueing another run."
            )

        config_name = self._normalized_config_name(created_run_id, config_path)
        run_paths = self.paths.for_run(created_run_id, config_name)
        run_paths.ensure_directories()
        self._reset_live_artifacts(run_paths)

        copied_config_path = self._copy_config(run_paths, config_path)

        now = utc_now_iso()
        payload = {
            'id': created_run_id,
            'status': 'queued',
            'config_path': copied_config_path,
            'run_dir': str(run_paths.run_dir),
            'applied_csv_path': str(run_paths.applied_csv),
            'external_jobs_csv_path': str(run_paths.external_jobs_csv),
            'recruiters_csv_path': str(run_paths.recruiters_csv),
            'send_report_path': str(run_paths.send_report_csv),
            'manifest_path': str(run_paths.manifest_json),
            'log_dir': str(run_paths.logs_dir),
            'linkedin_stdout_log': str(run_paths.linkedin_stdout_log),
            'linkedin_stderr_log': str(run_paths.linkedin_stderr_log),
            'rocketreach_stdout_log': str(run_paths.rocketreach_stdout_log),
            'rocketreach_stderr_log': str(run_paths.rocketreach_stderr_log),
            'created_at': now,
            'updated_at': now,
            'stage_started_at': '',
            'stage_finished_at': '',
            'retry_count': 0,
            'email_total': 0,
            'email_sent': 0,
            'email_failed': 0,
            'provider_success_count': 0,
            'no_email_count': 0,
            'provider_retry_count': 0,
            'workflow_retry_count': 0,
            'temporal_workflow_id': '',
            'temporal_task_queue': '',
            'orchestration_backend': '',
            'last_workflow_rerun_reason': '',
            'last_failed_stage': '',
            'note': 'Run enqueued.',
            'last_error': '',
        }

        columns = ', '.join(payload.keys())
        placeholders = ', '.join('?' for _ in payload)
        with self._connect() as connection:
            connection.execute(
                f"INSERT INTO runs ({columns}) VALUES ({placeholders})",
                tuple(payload.values()),
            )
            connection.commit()

        record = self.get_run(created_run_id)
        write_manifest(record)
        return record

    def get_run(self, run_id: str) -> dict:
        with self._connect() as connection:
            row = connection.execute(
                'SELECT * FROM runs WHERE id = ?',
                (run_id,),
            ).fetchone()
            result = dict(row) if row is not None else None
        if result is None:
            raise KeyError(f"Run not found: {run_id}")
        return result

    def list_runs(self, limit: int | None = None) -> list[dict]:
        query = 'SELECT * FROM runs ORDER BY created_at DESC'
        params: tuple[object, ...] = ()
        if limit is not None:
            query += ' LIMIT ?'
            params = (limit,)

        with self._connect() as connection:
            rows = connection.execute(query, params).fetchall()
            result = [dict(row) for row in rows]
        return result

    def get_next_queued_run(self) -> dict | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM runs WHERE status = 'queued' ORDER BY created_at ASC LIMIT 1"
            ).fetchone()
            result = dict(row) if row else None
        return result

    def update_run(self, run_id: str, **changes) -> dict:
        if not changes:
            return self.get_run(run_id)

        changes['updated_at'] = utc_now_iso()
        assignments = ', '.join(f"{column} = ?" for column in changes)
        values = list(changes.values())
        values.append(run_id)

        with self._connect() as connection:
            connection.execute(
                f"UPDATE runs SET {assignments} WHERE id = ?",
                tuple(values),
            )
            connection.commit()

        record = self.get_run(run_id)
        write_manifest(record)
        return record

    def get_enrichment_cache(self, fingerprint: str) -> dict | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM enrichment_cache WHERE fingerprint = ?",
                (fingerprint,),
            ).fetchone()
            return dict(row) if row is not None else None

    def upsert_enrichment_cache(self, fingerprint: str, **changes) -> dict:
        payload = {
            "fingerprint": fingerprint,
            "run_id": str(changes.get("run_id") or ""),
            "provider": str(changes.get("provider") or ""),
            "lookup_status": str(changes.get("lookup_status") or ""),
            "email": str(changes.get("email") or ""),
            "secondary_email": str(changes.get("secondary_email") or ""),
            "contact": str(changes.get("contact") or ""),
            "provider_attempts": int(changes.get("provider_attempts", 0) or 0),
            "provider_retry_count": int(changes.get("provider_retry_count", 0) or 0),
            "last_provider_error": str(changes.get("last_provider_error") or ""),
            "raw_payload": str(changes.get("raw_payload") or ""),
            "updated_at": utc_now_iso(),
        }
        columns = ", ".join(payload.keys())
        placeholders = ", ".join("?" for _ in payload)
        assignments = ", ".join(f"{column} = excluded.{column}" for column in payload.keys() if column != "fingerprint")
        with self._connect() as connection:
            connection.execute(
                f"""
                INSERT INTO enrichment_cache ({columns})
                VALUES ({placeholders})
                ON CONFLICT(fingerprint) DO UPDATE SET {assignments}
                """,
                tuple(payload.values()),
            )
            connection.commit()
            row = connection.execute(
                "SELECT * FROM enrichment_cache WHERE fingerprint = ?",
                (fingerprint,),
            ).fetchone()
        return dict(row) if row is not None else payload

    def recover_interrupted_runs(self) -> list[dict]:
        recovered: list[dict] = []
        for record in self.list_runs():
            status = record['status']
            if status in {'linkedin_running', 'rocketreach_running', 'email_running'}:
                recovered.append(
                    self.update_run(
                        record['id'],
                        status='queued',
                        note=f"Recovered interrupted {status} stage after restart.",
                    )
                )
            elif status == 'sending':
                recovered.append(
                    self.update_run(
                        record['id'],
                        status='waiting_review',
                        note='Recovered interrupted email send stage after restart.',
                    )
                )
            elif status == 'waiting_login':
                recovered.append(
                    self.update_run(
                        record['id'],
                        status='waiting_login',
                        note=record.get('note') or 'LinkedIn login is still required in the opened Chrome window.',
                    )
                )
        return recovered

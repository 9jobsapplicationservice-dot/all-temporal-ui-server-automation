from __future__ import annotations

import csv
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pipeline.adapters as adapters
from pipeline.constants import APPLIED_JOBS_HEADERS, ENRICHED_RECRUITER_HEADERS
from pipeline.storage import PipelineStore
from pipeline.worker import PipelineWorker
from pipeline.adapters import StageError, TransientStageError


def write_csv(path: Path, header: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', encoding='utf-8', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)


class RecordingConnection:
    def __init__(self, connection, statements: list[str]) -> None:
        self._connection = connection
        self._statements = statements

    def __enter__(self):
        self._connection.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb):
        return self._connection.__exit__(exc_type, exc, tb)

    def execute(self, sql: str, params=()):
        self._statements.append(sql)
        return self._connection.execute(sql, params)

    def __getattr__(self, name: str):
        return getattr(self._connection, name)


class PipelineStoreMigrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.root = Path(self.temp_dir.name)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_migration_skips_updates_and_manifest_writes_for_current_records(self) -> None:
        store = PipelineStore(self.root)
        store.create_run(run_id='run-current')

        statements: list[str] = []
        original_connect = store._connect

        def wrapped_connect():
            return RecordingConnection(original_connect(), statements)

        with patch.object(store, '_connect', side_effect=wrapped_connect), patch('pipeline.storage.write_manifest') as write_manifest:
            store._migrate_existing_artifacts()

        self.assertFalse(any(statement.startswith('UPDATE runs SET') for statement in statements))
        write_manifest.assert_not_called()

    def test_migration_updates_legacy_paths_and_rewrites_manifest(self) -> None:
        store = PipelineStore(self.root)
        record = store.create_run(run_id='run-legacy')

        manifest_path = Path(record['manifest_path'])
        legacy_manifest_dir = self.root / 'legacy-meta'
        legacy_manifest_dir.mkdir(parents=True, exist_ok=True)
        legacy_manifest_path = legacy_manifest_dir / manifest_path.name
        shutil.move(str(manifest_path), str(legacy_manifest_path))

        with store._connect() as connection:
            connection.execute(
                'UPDATE runs SET manifest_path = ? WHERE id = ?',
                (str(legacy_manifest_path), record['id']),
            )
            connection.commit()

        with patch('pipeline.storage.write_manifest') as write_manifest:
            store._migrate_existing_artifacts()

        updated_record = store.get_run(record['id'])
        self.assertEqual(updated_record['manifest_path'], str(manifest_path))
        self.assertFalse(legacy_manifest_path.exists())
        write_manifest.assert_called_once()
        self.assertEqual(write_manifest.call_args.args[0]['id'], record['id'])

    def test_create_run_rejects_second_active_run_in_shared_folder_mode(self) -> None:
        store = PipelineStore(self.root)
        first = store.create_run(run_id='run-active')
        store.update_run(first['id'], status='linkedin_running')

        with self.assertRaises(RuntimeError):
            store.create_run(run_id='run-blocked')

    def test_legacy_active_run_does_not_block_shared_live_enqueue(self) -> None:
        store = PipelineStore(self.root)
        record = store.create_run(run_id='run-legacy-active')
        legacy_run_dir = self.root / 'runs' / 'run-legacy-active'
        legacy_run_dir.mkdir(parents=True, exist_ok=True)
        legacy_applied = legacy_run_dir / 'applied_jobs.csv'
        legacy_recruiters = legacy_run_dir / 'recruiters_enriched.csv'

        with store._connect() as connection:
            connection.execute(
                'UPDATE runs SET status = ?, run_dir = ?, applied_csv_path = ?, recruiters_csv_path = ? WHERE id = ?',
                ('waiting_review', str(legacy_run_dir), str(legacy_applied), str(legacy_recruiters), record['id']),
            )
            connection.commit()

        created = store.create_run(run_id='run-shared')
        self.assertEqual(created['id'], 'run-shared')

    def test_waiting_review_without_sendable_emails_does_not_block_shared_live_enqueue(self) -> None:
        store = PipelineStore(self.root)
        record = store.create_run(run_id='run-no-sendable-review')
        write_csv(
            Path(record['recruiters_csv_path']),
            ENRICHED_RECRUITER_HEADERS,
            [{
                'Date': '31/03/2026',
                'Company Name': 'Acme',
                'Position': 'Backend Engineer',
                'Job Link': 'https://linkedin.com/jobs/view/1',
                'Submitted': 'Applied',
                'HR Name': 'Jane Doe',
                'HR Position': 'Recruiter',
                'HR Profile Link': 'https://linkedin.com/in/jane-doe',
                'HR Email': '',
                'HR Secondary Email': '',
                'HR Contact': '',
                'RocketReach Status': 'lookup_quota_reached',
            }],
        )
        store.update_run(record['id'], status='waiting_review')

        created = store.create_run(run_id='run-after-no-sendable')
        self.assertEqual(created['id'], 'run-after-no-sendable')
        self.assertFalse(Path(created['recruiters_csv_path']).exists())
        self.assertFalse(Path(created['applied_csv_path']).exists())


class LinkedInPythonResolutionTests(unittest.TestCase):
    def test_uses_configured_linkedin_python_when_set(self) -> None:
        executable = Path(__file__).resolve()
        env = {adapters.LINKEDIN_PYTHON_ENV_VAR: str(executable)}
        with patch.dict(os.environ, env, clear=False), patch('pipeline.adapters._probe_python_version', return_value=(3, 12)):
            resolved = adapters.resolve_linkedin_python_executable()

        self.assertEqual(resolved, str(executable.resolve()))

    def test_preflight_returns_blocked_runtime_for_missing_configured_path(self) -> None:
        env = {adapters.LINKEDIN_PYTHON_ENV_VAR: r'C:\missing\python312.exe'}
        with patch.dict(os.environ, env, clear=False):
            preflight = adapters.preflight_linkedin_runtime()

        self.assertFalse(preflight.is_available)
        self.assertIsNone(preflight.executable)
        self.assertIn(adapters.LINKEDIN_PYTHON_ENV_VAR, preflight.blocked_reason or '')

    def test_prefers_discovered_python312_over_current_python(self) -> None:
        discovered = r'C:\Python312\python.exe'
        current = r'C:\Python314\python.exe'
        with patch.dict(os.environ, {}, clear=False), patch('pipeline.adapters._discover_windows_supported_python_executables', return_value=[discovered]), patch('pipeline.adapters._probe_python_version', return_value=(3, 12)), patch('pipeline.adapters.shutil.which', return_value=current), patch('pipeline.adapters.sys.executable', current):
            resolved = adapters.resolve_linkedin_python_executable()

        self.assertEqual(resolved, discovered)

    def test_falls_back_to_current_python_when_no_python312_is_found(self) -> None:
        current = r'C:\Python312\python.exe'
        with patch.dict(os.environ, {}, clear=False), patch('pipeline.adapters._discover_windows_supported_python_executables', return_value=[]), patch('pipeline.adapters._probe_python_version', return_value=(3, 12)), patch('pipeline.adapters.shutil.which', return_value=current), patch('pipeline.adapters.sys.executable', current):
            resolved = adapters.resolve_linkedin_python_executable()

        self.assertEqual(resolved, current)

    def test_surfaces_clear_error_for_incompatible_selected_python(self) -> None:
        current = r'C:\Python314\python.exe'
        with patch.dict(os.environ, {}, clear=False), patch('pipeline.adapters._discover_windows_supported_python_executables', return_value=[]), patch('pipeline.adapters._probe_python_version', return_value=(3, 14)), patch('pipeline.adapters.shutil.which', return_value=current), patch('pipeline.adapters.sys.executable', current):
            with self.assertRaises(StageError) as error_context:
                adapters.resolve_linkedin_python_executable()

        message = str(error_context.exception)
        self.assertIn(current, message)
        self.assertIn(adapters.LINKEDIN_PYTHON_ENV_VAR, message)

    def test_preflight_returns_blocked_runtime_when_only_python314_is_available(self) -> None:
        current = r'C:\Python314\python.exe'
        with patch.dict(os.environ, {}, clear=False), patch('pipeline.adapters._discover_windows_supported_python_executables', return_value=[]), patch('pipeline.adapters._probe_python_version', return_value=(3, 14)), patch('pipeline.adapters.shutil.which', return_value=current), patch('pipeline.adapters.sys.executable', current):
            preflight = adapters.preflight_linkedin_runtime()

        self.assertFalse(preflight.is_available)
        self.assertEqual(preflight.executable, current)
        self.assertIn('Python 3.11', preflight.blocked_reason or '')


class PipelineWorkerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.root = Path(self.temp_dir.name)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_create_run_creates_artifacts_and_manifest(self) -> None:
        store = PipelineStore(self.root)
        record = store.create_run(run_id='run-create')

        run_dir = Path(record['run_dir'])
        self.assertEqual(record['status'], 'queued')
        self.assertTrue(run_dir.exists())
        self.assertEqual(run_dir.name, 'runs')
        self.assertTrue(Path(record['manifest_path']).exists())
        self.assertTrue(Path(record['log_dir']).exists())
        self.assertTrue(Path(record['manifest_path']).parent.name == 'meta')
        self.assertTrue(Path(record['log_dir']).parent.name == 'logs')
        self.assertTrue(Path(record['send_report_path']).parent.name == 'reports')
        self.assertEqual(sorted(path.name for path in run_dir.iterdir()), [])

    def test_create_run_reuses_single_live_folder_and_resets_artifacts(self) -> None:
        store = PipelineStore(self.root)
        first = store.create_run(run_id='run-first')
        write_csv(
            Path(first['applied_csv_path']),
            APPLIED_JOBS_HEADERS,
            [{
                'Date': '31/03/2026',
                'Company Name': 'Acme',
                'Position': 'Backend Engineer',
                'Job Link': 'https://linkedin.com/jobs/view/1',
                'Submitted': 'Applied',
                'HR Name': 'Jane Doe',
                'HR Position': 'Recruiter',
                'HR Profile Link': 'https://linkedin.com/in/jane-doe',
            }],
        )

        with self.assertRaises(RuntimeError):
            store.create_run(run_id='run-second')

        self.assertEqual(Path(first['run_dir']), self.root / 'runs')
        self.assertTrue(Path(first['applied_csv_path']).exists())

    def test_reset_live_artifacts_for_run_clears_shared_csvs(self) -> None:
        store = PipelineStore(self.root)
        record = store.create_run(run_id='run-reset')
        write_csv(
            Path(record['applied_csv_path']),
            APPLIED_JOBS_HEADERS,
            [{
                'Date': '31/03/2026',
                'Company Name': 'Acme',
                'Position': 'Backend Engineer',
                'Job Link': 'https://linkedin.com/jobs/view/1',
                'Submitted': 'Applied',
                'HR Name': 'Jane Doe',
                'HR Position': 'Recruiter',
                'HR Profile Link': 'https://linkedin.com/in/jane-doe',
            }],
        )

        store.reset_live_artifacts_for_run(record['id'])

        self.assertFalse(Path(record['applied_csv_path']).exists())
        self.assertFalse(Path(record['recruiters_csv_path']).exists())

    def test_worker_processes_linkedin_then_rocketreach(self) -> None:
        worker = PipelineWorker(root=self.root)
        record = worker.store.create_run(run_id='run-happy')

        def fake_linkedin(run_record: dict) -> dict:
            write_csv(
                Path(run_record['applied_csv_path']),
                APPLIED_JOBS_HEADERS,
                [{
                    'Date': '31/03/2026',
                    'Company Name': 'Acme',
                    'Position': 'Backend Engineer',
                    'Job Link': 'https://linkedin.com/jobs/view/1',
                    'Submitted': 'Applied',
                    'HR Name': 'Jane Doe',
                    'HR Position': 'Recruiter',
                    'HR Profile Link': 'https://linkedin.com/in/jane-doe',
                }],
            )
            return {
                'jobs_applied': 1,
                'external_links_logged': 0,
                'rows_written_to_applied_csv': 1,
                'rows_missing_hr_profile': 0,
                'unexpected_failure': False,
            }

        def fake_rocketreach(run_record: dict) -> dict:
            write_csv(
                Path(run_record['recruiters_csv_path']),
                ENRICHED_RECRUITER_HEADERS,
                [{
                    'Date': '31/03/2026',
                    'Company Name': 'Acme',
                    'Position': 'Backend Engineer',
                    'Job Link': 'https://linkedin.com/jobs/view/1',
                    'Submitted': 'Applied',
                    'HR Name': 'Jane Doe',
                    'HR Position': 'Recruiter',
                    'HR Profile Link': 'https://linkedin.com/in/jane-doe',
                    'HR Email': 'jane@acme.com',
                    'HR Secondary Email': '',
                    'HR Contact': '',
                    'RocketReach Status': 'matched',
                }],
            )
            return {
                'total': 1,
                'matched': 1,
                'failed': 0,
                'skipped': 0,
                'no_match': 0,
                'missing_hr_link': 0,
                'invalid_hr_link': 0,
                'profile_only': 0,
                'lookup_quota_reached': 0,
                'sendable_rows': 1,
            }

        with patch('pipeline.worker.preflight_linkedin_runtime', return_value=adapters.LinkedInRuntimePreflight(executable='C:\\Python312\\python.exe', source='test', blocked_reason=None)), patch('pipeline.worker.run_linkedin_stage', side_effect=fake_linkedin), patch('pipeline.worker.run_rocketreach_stage', side_effect=fake_rocketreach):
            final_record = worker.process_run(record['id'])

        self.assertEqual(final_record['status'], 'waiting_review')
        self.assertIn('sendable_rows=1', final_record['note'])
        self.assertTrue(Path(final_record['applied_csv_path']).exists())
        self.assertTrue(Path(final_record['recruiters_csv_path']).exists())
        self.assertEqual(
            sorted(path.name for path in Path(final_record['run_dir']).iterdir()),
            ['applied_jobs.csv', 'recruiters_enriched.csv'],
        )
        self.assertEqual(Path(final_record['run_dir']).name, 'runs')
        self.assertTrue(Path(final_record['manifest_path']).parent.name == 'meta')
        self.assertTrue(Path(final_record['log_dir']).parent.name == 'logs')

    def test_worker_updates_run_to_fallback_recruiter_csv_path(self) -> None:
        worker = PipelineWorker(root=self.root)
        record = worker.store.create_run(run_id='run-fallback-recruiter-path')
        write_csv(
            Path(record['applied_csv_path']),
            APPLIED_JOBS_HEADERS,
            [{
                'Date': '31/03/2026',
                'Company Name': 'Acme',
                'Position': 'Backend Engineer',
                'Job Link': 'https://linkedin.com/jobs/view/1',
                'Submitted': 'Applied',
                'HR Name': 'Jane Doe',
                'HR Position': 'Recruiter',
                'HR Profile Link': 'https://linkedin.com/in/jane-doe',
            }],
        )
        fallback_path = Path(record['run_dir']) / 'recruiters_enriched_latest.csv'
        write_csv(
            fallback_path,
            ENRICHED_RECRUITER_HEADERS,
            [{
                'Date': '31/03/2026',
                'Company Name': 'Acme',
                'Position': 'Backend Engineer',
                'Job Link': 'https://linkedin.com/jobs/view/1',
                'Submitted': 'Applied',
                'HR Name': 'Jane Doe',
                'HR Position': 'Recruiter',
                'HR Profile Link': 'https://linkedin.com/in/jane-doe',
                'HR Email': 'jane@acme.com',
                'HR Secondary Email': '',
                'HR Contact': '',
                'RocketReach Status': 'matched',
            }],
        )

        with patch(
            'pipeline.worker.run_rocketreach_stage',
            return_value={
                'total': 1,
                'matched': 1,
                'failed': 0,
                'skipped': 0,
                'no_match': 0,
                'missing_hr_link': 0,
                'invalid_hr_link': 0,
                'profile_only': 0,
                'lookup_quota_reached': 0,
                'sendable_rows': 1,
                'recruiters_csv_path': str(fallback_path),
                'output_note': "Main recruiter CSV 'recruiters_enriched.csv' was locked; wrote fallback file 'recruiters_enriched_latest.csv' instead.",
            },
        ):
            final_record = worker.process_run(record['id'])

        self.assertEqual(final_record['status'], 'waiting_review')
        self.assertEqual(final_record['recruiters_csv_path'], str(fallback_path))
        self.assertIn('locked', final_record['note'])

    def test_worker_marks_linkedin_zero_saved_rows_as_failed(self) -> None:
        worker = PipelineWorker(root=self.root)
        record = worker.store.create_run(run_id='run-zero-applied')

        def fake_linkedin(run_record: dict) -> dict:
            write_csv(Path(run_record['applied_csv_path']), APPLIED_JOBS_HEADERS, [])
            return {
                'jobs_applied': 3,
                'external_links_logged': 0,
                'rows_written_to_applied_csv': 0,
                'rows_missing_hr_profile': 0,
                'unexpected_failure': False,
            }

        with patch('pipeline.worker.preflight_linkedin_runtime', return_value=adapters.LinkedInRuntimePreflight(executable='C:\\Python312\\python.exe', source='test', blocked_reason=None)), patch('pipeline.worker.run_linkedin_stage', side_effect=fake_linkedin):
            final_record = worker.process_run(record['id'])

        self.assertEqual(final_record['status'], 'failed')
        self.assertIn('wrote zero rows', final_record['note'])

    def test_worker_skips_linkedin_when_applied_csv_exists(self) -> None:
        worker = PipelineWorker(root=self.root)
        record = worker.store.create_run(run_id='run-skip-linkedin')
        write_csv(
            Path(record['applied_csv_path']),
            APPLIED_JOBS_HEADERS,
            [{
                'Date': '31/03/2026',
                'Company Name': 'Acme',
                'Position': 'Backend Engineer',
                'Job Link': 'https://linkedin.com/jobs/view/1',
                'Submitted': 'Applied',
                'HR Name': 'Jane Doe',
                'HR Position': 'Recruiter',
                'HR Profile Link': 'https://linkedin.com/in/jane-doe',
            }],
        )

        def fake_rocketreach(run_record: dict) -> dict:
            write_csv(
                Path(run_record['recruiters_csv_path']),
                ENRICHED_RECRUITER_HEADERS,
                [{
                    'Date': '31/03/2026',
                    'Company Name': 'Acme',
                    'Position': 'Backend Engineer',
                    'Job Link': 'https://linkedin.com/jobs/view/1',
                    'Submitted': 'Applied',
                    'HR Name': 'Jane Doe',
                    'HR Position': 'Recruiter',
                    'HR Profile Link': 'https://linkedin.com/in/jane-doe',
                    'HR Email': 'jane@acme.com',
                    'HR Secondary Email': '',
                    'HR Contact': '',
                    'RocketReach Status': 'matched',
                }],
            )
            return {
                'total': 1,
                'matched': 1,
                'failed': 0,
                'skipped': 0,
                'no_match': 0,
                'missing_hr_link': 0,
                'invalid_hr_link': 0,
                'profile_only': 0,
                'lookup_quota_reached': 0,
                'sendable_rows': 1,
            }

        with patch('pipeline.worker.run_linkedin_stage') as linkedin_stage, patch('pipeline.worker.run_rocketreach_stage', side_effect=fake_rocketreach):
            final_record = worker.process_run(record['id'])

        linkedin_stage.assert_not_called()
        self.assertEqual(final_record['status'], 'waiting_review')
        self.assertIn('sendable_rows=1', final_record['note'])

    def test_worker_marks_linkedin_startup_failure_as_failed(self) -> None:
        worker = PipelineWorker(root=self.root)
        record = worker.store.create_run(run_id='run-linkedin-startup-failure')

        with patch('pipeline.worker.preflight_linkedin_runtime', return_value=adapters.LinkedInRuntimePreflight(executable='C:\\Python312\\python.exe', source='test', blocked_reason=None)), patch('pipeline.worker.run_linkedin_stage', side_effect=StageError('Chrome bootstrap failed.')):
            final_record = worker.process_run(record['id'])

        self.assertEqual(final_record['status'], 'failed')
        self.assertEqual(final_record['note'], 'LinkedIn stage failed.')
        self.assertIn('Chrome bootstrap failed.', final_record['last_error'])

    def test_worker_marks_missing_runtime_as_blocked(self) -> None:
        worker = PipelineWorker(root=self.root)
        record = worker.store.create_run(run_id='run-runtime-blocked')

        with patch('pipeline.worker.preflight_linkedin_runtime', return_value=adapters.LinkedInRuntimePreflight(executable=None, source='test', blocked_reason='Install Python 3.11 or set PIPELINE_LINKEDIN_PYTHON.')), patch('pipeline.worker.run_linkedin_stage') as run_stage:
            final_record = worker.process_run(record['id'])

        run_stage.assert_not_called()
        self.assertEqual(final_record['status'], 'blocked_runtime')
        self.assertEqual(final_record['note'], 'LinkedIn runtime setup required.')
        self.assertIn('PIPELINE_LINKEDIN_PYTHON', final_record['last_error'])

    def test_worker_reuses_zero_row_applied_csv_as_failed(self) -> None:
        worker = PipelineWorker(root=self.root)
        record = worker.store.create_run(run_id='run-empty-applied-artifact')
        write_csv(Path(record['applied_csv_path']), APPLIED_JOBS_HEADERS, [])

        final_record = worker.process_run(record['id'])

        self.assertEqual(final_record['status'], 'failed')
        self.assertIn('zero saved applied rows', final_record['note'])

    def test_worker_surfaces_no_sendable_contact_note(self) -> None:
        worker = PipelineWorker(root=self.root)
        record = worker.store.create_run(run_id='run-no-sendable')
        write_csv(
            Path(record['applied_csv_path']),
            APPLIED_JOBS_HEADERS,
            [{
                'Date': '31/03/2026',
                'Company Name': 'Acme',
                'Position': 'Backend Engineer',
                'Job Link': 'https://linkedin.com/jobs/view/1',
                'Submitted': 'Applied',
                'HR Name': 'Jane Doe',
                'HR Position': 'Recruiter',
                'HR Profile Link': 'https://linkedin.com/in/jane-doe',
            }],
        )

        def fake_rocketreach(run_record: dict) -> dict:
            write_csv(
                Path(run_record['recruiters_csv_path']),
                ENRICHED_RECRUITER_HEADERS,
                [{
                    'Date': '31/03/2026',
                    'Company Name': 'Acme',
                    'Position': 'Backend Engineer',
                    'Job Link': 'https://linkedin.com/jobs/view/1',
                    'Submitted': 'Applied',
                    'HR Name': 'Jane Doe',
                    'HR Position': 'Recruiter',
                    'HR Profile Link': 'https://linkedin.com/in/jane-doe',
                    'HR Email': '',
                    'HR Secondary Email': '',
                    'HR Contact': '',
                    'RocketReach Status': 'profile_only',
                }],
            )
            return {
                'total': 1,
                'matched': 0,
                'failed': 1,
                'skipped': 0,
                'no_match': 0,
                'missing_hr_link': 0,
                'invalid_hr_link': 0,
                'profile_only': 1,
                'lookup_quota_reached': 0,
                'sendable_rows': 0,
            }

        with patch('pipeline.worker.run_rocketreach_stage', side_effect=fake_rocketreach):
            final_record = worker.process_run(record['id'])

        self.assertEqual(final_record['status'], 'completed')
        self.assertIn('no sendable emails', final_record['note'])
        self.assertIn('profile_only=1', final_record['note'])

    def test_worker_recovers_interrupted_statuses(self) -> None:
        store = PipelineStore(self.root)
        linkedin_record = store.create_run(run_id='run-linkedin')
        store.update_run(linkedin_record['id'], status='linkedin_running')
        sending_record = {
            **linkedin_record,
            'id': 'run-sending',
            'status': 'sending',
            'note': 'Waiting for email send completion.',
            'last_error': '',
        }
        columns = ', '.join(sending_record.keys())
        placeholders = ', '.join('?' for _ in sending_record)
        with store._connect() as connection:
            connection.execute(
                f'INSERT INTO runs ({columns}) VALUES ({placeholders})',
                tuple(sending_record.values()),
            )
            connection.commit()

        worker = PipelineWorker(root=self.root)
        recovered = worker.recover()
        statuses = {record['id']: record['status'] for record in recovered}

        self.assertEqual(statuses['run-linkedin'], 'queued')
        self.assertEqual(statuses['run-sending'], 'waiting_review')

    def test_worker_resets_live_artifacts_when_linkedin_stage_starts(self) -> None:
        worker = PipelineWorker(root=self.root)
        record = worker.store.create_run(run_id='run-live-reset')
        write_csv(
            Path(record['applied_csv_path']),
            APPLIED_JOBS_HEADERS,
            [{
                'Date': '31/03/2026',
                'Company Name': 'OldCo',
                'Position': 'Old Role',
                'Job Link': 'https://linkedin.com/jobs/view/old',
                'Submitted': 'Applied',
                'HR Name': 'Old Recruiter',
                'HR Position': 'Recruiter',
                'HR Profile Link': 'https://linkedin.com/in/old-recruiter',
            }],
        )
        write_csv(
            Path(record['recruiters_csv_path']),
            ENRICHED_RECRUITER_HEADERS,
            [{
                'Date': '31/03/2026',
                'Company Name': 'OldCo',
                'Position': 'Old Role',
                'Job Link': 'https://linkedin.com/jobs/view/old',
                'Submitted': 'Applied',
                'HR Name': 'Old Recruiter',
                'HR Position': 'Recruiter',
                'HR Profile Link': 'https://linkedin.com/in/old-recruiter',
                'HR Email': 'old@oldco.com',
                'HR Secondary Email': '',
                'HR Contact': '',
                'RocketReach Status': 'matched',
            }],
        )

        def fake_linkedin(run_record: dict, python_executable: str | None = None) -> dict:
            self.assertFalse(Path(run_record['applied_csv_path']).exists())
            self.assertFalse(Path(run_record['recruiters_csv_path']).exists())
            write_csv(
                Path(run_record['applied_csv_path']),
                APPLIED_JOBS_HEADERS,
                [{
                    'Date': '31/03/2026',
                    'Company Name': 'NewCo',
                    'Position': 'New Role',
                    'Job Link': 'https://linkedin.com/jobs/view/new',
                    'Submitted': 'Applied',
                    'HR Name': 'New Recruiter',
                    'HR Position': 'Recruiter',
                    'HR Profile Link': 'https://linkedin.com/in/new-recruiter',
                }],
            )
            return {
                'jobs_applied': 1,
                'external_links_logged': 0,
                'rows_written_to_applied_csv': 1,
                'rows_missing_hr_profile': 0,
                'unexpected_failure': False,
            }

        def fake_rocketreach(run_record: dict) -> dict:
            write_csv(
                Path(run_record['recruiters_csv_path']),
                ENRICHED_RECRUITER_HEADERS,
                [{
                    'Date': '31/03/2026',
                    'Company Name': 'NewCo',
                    'Position': 'New Role',
                    'Job Link': 'https://linkedin.com/jobs/view/new',
                    'Submitted': 'Applied',
                    'HR Name': 'New Recruiter',
                    'HR Position': 'Recruiter',
                    'HR Profile Link': 'https://linkedin.com/in/new-recruiter',
                    'HR Email': 'new@newco.com',
                    'HR Secondary Email': '',
                    'HR Contact': '',
                    'RocketReach Status': 'matched',
                }],
            )
            return {
                'total': 1,
                'matched': 1,
                'failed': 0,
                'skipped': 0,
                'no_match': 0,
                'missing_hr_link': 0,
                'invalid_hr_link': 0,
                'profile_only': 0,
                'lookup_quota_reached': 0,
                'sendable_rows': 1,
            }

        with patch('pipeline.worker.preflight_linkedin_runtime', return_value=adapters.LinkedInRuntimePreflight(executable='C:\\Python311\\python.exe', source='test', blocked_reason=None)), patch('pipeline.worker.run_linkedin_stage', side_effect=fake_linkedin), patch('pipeline.worker.run_rocketreach_stage', side_effect=fake_rocketreach):
            final_record = worker.process_run(record['id'])

        self.assertEqual(final_record['status'], 'waiting_review')
        applied_rows = Path(final_record['applied_csv_path']).read_text(encoding='utf-8-sig')
        recruiters_rows = Path(final_record['recruiters_csv_path']).read_text(encoding='utf-8-sig')
        self.assertIn('NewCo', applied_rows)
        self.assertNotIn('OldCo', applied_rows)
        self.assertIn('new@newco.com', recruiters_rows)
        self.assertNotIn('old@oldco.com', recruiters_rows)

    def test_worker_retries_transient_rocketreach_failures(self) -> None:
        worker = PipelineWorker(root=self.root)
        record = worker.store.create_run(run_id='run-retry')
        write_csv(
            Path(record['applied_csv_path']),
            APPLIED_JOBS_HEADERS,
            [{
                'Date': '31/03/2026',
                'Company Name': 'Acme',
                'Position': 'Backend Engineer',
                'Job Link': 'https://linkedin.com/jobs/view/1',
                'Submitted': 'Applied',
                'HR Name': 'Jane Doe',
                'HR Position': 'Recruiter',
                'HR Profile Link': 'https://linkedin.com/in/jane-doe',
            }],
        )

        with patch('pipeline.worker.run_rocketreach_stage', side_effect=TransientStageError('rate limit reached')):
            updated_record = worker.process_run(record['id'])

        self.assertEqual(updated_record['status'], 'queued')
        self.assertEqual(updated_record['retry_count'], 1)

    def test_worker_requeues_blocked_runtime_when_runtime_becomes_available(self) -> None:
        worker = PipelineWorker(root=self.root)
        record = worker.store.create_run(run_id='run-recover-runtime')
        worker.store.update_run(
            record['id'],
            status='blocked_runtime',
            note='LinkedIn runtime setup required.',
            last_error='Install Python 3.11.',
        )

        with patch('pipeline.worker.preflight_linkedin_runtime', return_value=adapters.LinkedInRuntimePreflight(executable='C:\\Python312\\python.exe', source='test', blocked_reason=None)):
            recovered = worker.recover()

        recovered_record = worker.store.get_run(record['id'])
        self.assertTrue(any(item['id'] == record['id'] for item in recovered))
        self.assertEqual(recovered_record['status'], 'queued')
        self.assertIn('Requeued automatically', recovered_record['note'])

    def test_worker_converts_locked_csv_permission_error_into_failed_run(self) -> None:
        worker = PipelineWorker(root=self.root)
        record = worker.store.create_run(run_id='run-locked-csv')
        write_csv(
            Path(record['applied_csv_path']),
            APPLIED_JOBS_HEADERS,
            [{
                'Date': '31/03/2026',
                'Company Name': 'Acme',
                'Position': 'Backend Engineer',
                'Job Link': 'https://linkedin.com/jobs/view/1',
                'Submitted': 'Applied',
                'HR Name': 'Jane Doe',
                'HR Position': 'Recruiter',
                'HR Profile Link': 'https://linkedin.com/in/jane-doe',
            }],
        )

        locked_error = PermissionError(13, 'Permission denied', record['applied_csv_path'])

        with patch('pipeline.worker.run_rocketreach_stage', side_effect=locked_error):
            updated_record = worker.process_run(record['id'])

        self.assertEqual(updated_record['status'], 'failed')
        self.assertEqual(updated_record['note'], 'Pipeline file is locked.')
        self.assertIn('Close Excel', updated_record['last_error'])


if __name__ == '__main__':
    unittest.main()

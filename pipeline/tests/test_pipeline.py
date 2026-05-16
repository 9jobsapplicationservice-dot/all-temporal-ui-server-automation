from __future__ import annotations

import csv
import io
import os
import shutil
import sys
import tempfile
import unittest
import uuid
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pipeline.adapters as adapters
from pipeline.constants import APPLIED_JOBS_HEADERS, ENRICHED_RECRUITER_HEADERS
from pipeline.config import AutomationConfigError, load_automation_summary
from pipeline.storage import PipelineStore
from pipeline.stage_manager import PipelineStageManager
from pipeline.temporal_types import TemporalActivityResult, TemporalWorkflowInput
from pipeline.worker import PipelineWorker
from pipeline.adapters import StageError, TransientStageError
from pipeline.core.sentry_config import init_sentry
from pipeline.enrichment import RetryableProviderError
from pipeline.enrichment.errors import NonRetryableProviderError
from pipeline.temporal_interceptors import SentryActivityInboundInterceptor, SentryWorkflowInboundInterceptor
from pipeline.temporal_sdk import ExecuteActivityInput, ExecuteWorkflowInput
from pipeline.utils import csv_has_expected_header

EXTERNAL_JOBS_HEADERS = [
    'Date',
    'Company Name',
    'Position',
    'HR Name',
    'HR Profile Link',
]
TMP_ROOT = Path(tempfile.gettempdir()) / 'pipeline-tests-codex'


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
        TMP_ROOT.mkdir(parents=True, exist_ok=True)
        self.root = TMP_ROOT / f'pipeline-store-{uuid.uuid4().hex}'
        self.root.mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)

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

    def test_csv_header_validation_allows_additive_columns(self) -> None:
        csv_path = self.root / 'recruiters.csv'
        header = ENRICHED_RECRUITER_HEADERS + ['Extra Field']
        write_csv(csv_path, header, [{'Date': '01/01/2026'}])
        self.assertTrue(csv_has_expected_header(csv_path, ENRICHED_RECRUITER_HEADERS))

    def test_migration_normalizes_repeated_config_prefixes_and_ignores_missing_source(self) -> None:
        store = PipelineStore(self.root)
        record = store.create_run(run_id='run-repeat')
        repeated_name = 'run-repeat-run-repeat-automation.env'
        repeated_path = self.root / 'configs' / repeated_name
        repeated_path.write_text('PIPELINE_AUTO_SEND=false\n', encoding='utf-8')

        with store._connect() as connection:
            connection.execute(
                'UPDATE runs SET config_path = ? WHERE id = ?',
                (str(repeated_path), record['id']),
            )
            connection.commit()

        store._migrate_existing_artifacts()

        updated = store.get_run(record['id'])
        self.assertTrue(updated['config_path'].endswith('run-repeat-automation.env'))
        self.assertFalse(updated['config_path'].endswith(repeated_name))

    def test_create_run_rejects_second_active_run_in_shared_folder_mode(self) -> None:
        store = PipelineStore(self.root)
        first = store.create_run(run_id='run-active')
        store.update_run(first['id'], status='linkedin_running')

        with self.assertRaises(RuntimeError):
            store.create_run(run_id='run-blocked')

    def test_create_run_allows_explicit_conflict_override(self) -> None:
        store = PipelineStore(self.root)
        first = store.create_run(run_id='run-active-override')
        store.update_run(first['id'], status='linkedin_running')

        created = store.create_run(run_id='run-overridden', allow_active_conflict=True)
        self.assertEqual(created['id'], 'run-overridden')

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

    def test_stage_manager_finalizes_rocketreach_after_retry_exhaustion(self) -> None:
        store = PipelineStore(self.root)
        record = store.create_run(run_id='run-retry-finalize')
        manager = PipelineStageManager(store)
        store.update_run(
            record['id'],
            retry_count=5,
            status='queued',
        )

        final_stats = {
            'total': 1,
            'matched': 0,
            'preview_match': 0,
            'failed': 0,
            'skipped': 0,
            'no_match': 1,
            'missing_hr_link': 0,
            'invalid_hr_link': 0,
            'profile_only': 0,
            'lookup_quota_reached': 0,
            'authentication_failed': 0,
            'sendable_rows': 0,
            'provider_success_count': 0,
            'no_email_count': 1,
            'provider_retry_count': 5,
            'recruiters_csv_path': record['recruiters_csv_path'],
        }

        with patch('pipeline.stage_manager.run_rocketreach_stage', side_effect=[TransientStageError('quota hit'), final_stats]):
            result = manager.run_rocketreach(record['id'])

        self.assertEqual(result['status'], 'completed')
        self.assertIn('Automatic email sending was skipped', result['note'])
        self.assertEqual(int(result.get('no_email_count', 0) or 0), 1)


class SentryConfigTests(unittest.TestCase):
    def test_init_sentry_is_noop_when_dsn_missing(self) -> None:
        with patch.dict(os.environ, {}, clear=True), \
             patch('pipeline.core.sentry_config.load_runtime_env_values', return_value={}), \
             patch('pipeline.core.sentry_config.sentry_sdk') as sentry_sdk_mock:
            enabled = init_sentry()

        self.assertFalse(enabled)
        sentry_sdk_mock.init.assert_not_called()

    def test_init_sentry_enables_logging_integration_and_live_traces(self) -> None:
        logging_integration = unittest.mock.Mock(return_value="logging-integration")
        with patch.dict(os.environ, {"SENTRY_DSN": "test-dsn", "PIPELINE_ENVIRONMENT": "qa"}, clear=True), \
             patch('pipeline.core.sentry_config.load_runtime_env_values', return_value={}), \
             patch('pipeline.core.sentry_config.LoggingIntegration', logging_integration), \
             patch('pipeline.core.sentry_config.sentry_sdk') as sentry_sdk_mock:
            enabled = init_sentry()

        self.assertTrue(enabled)
        logging_integration.assert_called_once_with(level=unittest.mock.ANY, event_level=unittest.mock.ANY)
        sentry_sdk_mock.init.assert_called_once()
        kwargs = sentry_sdk_mock.init.call_args.kwargs
        self.assertEqual(kwargs["dsn"], "test-dsn")
        self.assertEqual(kwargs["environment"], "qa")
        self.assertEqual(kwargs["traces_sample_rate"], 1.0)
        self.assertEqual(kwargs["integrations"], ["logging-integration"])

    def test_log_and_capture_error_prints_captures_flushes_and_reraises(self) -> None:
        from pipeline.core.sentry_config import log_and_capture_error

        class ScopeContext:
            def __enter__(self):
                return SimpleNamespace(set_tag=lambda *args: None, set_extra=lambda *args: None)

            def __exit__(self, exc_type, exc, tb):
                return False

        output = io.StringIO()
        error = RuntimeError("live boom")
        with patch.dict(os.environ, {"SENTRY_DSN": "test-dsn"}, clear=True), \
             patch('pipeline.core.sentry_config.load_runtime_env_values', return_value={}), \
             patch('pipeline.core.sentry_config.sentry_sdk') as sentry_sdk_mock, \
             patch('sys.stdout', output):
            sentry_sdk_mock.push_scope.return_value = ScopeContext()
            with self.assertRaises(RuntimeError):
                try:
                    raise error
                except RuntimeError as caught:
                    log_and_capture_error(caught, message="captured live", tags={"stage": "test"})

        self.assertIn("RuntimeError: live boom", output.getvalue())
        sentry_sdk_mock.capture_exception.assert_called_once_with(error)
        sentry_sdk_mock.flush.assert_called_once()

    def test_load_runtime_env_values_exposes_sentry_keys(self) -> None:
        root = TMP_ROOT / f'sentry-env-{uuid.uuid4().hex}'
        root.mkdir(parents=True, exist_ok=True)
        config_path = root / 'automation.env'
        config_path.write_text('SENTRY_DSN=test-dsn\nSENTRY_ENVIRONMENT=qa\n', encoding='utf-8')

        from pipeline.config import load_runtime_env_values

        values = load_runtime_env_values(config_path)

        self.assertEqual(values['SENTRY_DSN'], 'test-dsn')
        self.assertEqual(values['SENTRY_ENVIRONMENT'], 'qa')
        shutil.rmtree(root, ignore_errors=True)

    def test_bootstrap_runtime_environment_populates_os_environ(self) -> None:
        root = TMP_ROOT / f'bootstrap-env-{uuid.uuid4().hex}'
        root.mkdir(parents=True, exist_ok=True)
        env_path = root / 'automation.env'
        env_path.write_text('APOLLO_API_KEY=apollo-test-key\n', encoding='utf-8')

        from pipeline.config import bootstrap_runtime_environment

        with patch('pipeline.config.PIPELINE_ENV_PATH', env_path), patch.dict(os.environ, {}, clear=True):
            info = bootstrap_runtime_environment()
            self.assertEqual(os.getenv('APOLLO_API_KEY'), 'apollo-test-key')

        self.assertIn(str(env_path), info['sources'])
        shutil.rmtree(root, ignore_errors=True)

    def test_bootstrap_runtime_environment_keeps_root_apollo_key_over_blank_run_snapshot(self) -> None:
        root = TMP_ROOT / f'bootstrap-stale-env-{uuid.uuid4().hex}'
        root.mkdir(parents=True, exist_ok=True)
        env_path = root / 'automation.env'
        run_env_path = root / 'run-copy.env'
        env_path.write_text('APOLLO_API_KEY=apollo-root-key\n', encoding='utf-8')
        run_env_path.write_text('APOLLO_API_KEY=\n', encoding='utf-8')

        from pipeline.config import bootstrap_runtime_environment

        with patch('pipeline.config.PIPELINE_ENV_PATH', env_path), patch.dict(os.environ, {}, clear=True):
            info = bootstrap_runtime_environment(run_env_path)
            self.assertEqual(os.getenv('APOLLO_API_KEY'), 'apollo-root-key')

        self.assertTrue(info['stale_apollo_snapshot'])
        shutil.rmtree(root, ignore_errors=True)


class TemporalInterceptorTests(unittest.IsolatedAsyncioTestCase):
    async def test_activity_interceptor_captures_expected_tags(self) -> None:
        captured: dict[str, object] = {}

        class NextInterceptor:
            async def execute_activity(self, input):
                raise RuntimeError('boom')

        payload = SimpleNamespace(run_id='run-123')
        interceptor = SentryActivityInboundInterceptor(NextInterceptor())
        input = ExecuteActivityInput(fn=lambda value: value, args=(payload,), executor=None, headers={})

        with patch('pipeline.temporal_interceptors.activity.info', return_value=SimpleNamespace(
            workflow_id='wf-1',
            workflow_type='WorkflowType',
            task_queue='queue-1',
            activity_type='rocketreach_activity',
            attempt=2,
        )), patch('pipeline.temporal_interceptors.capture_exception_with_context', side_effect=lambda error, **kwargs: captured.update(kwargs)):
            with self.assertRaises(RuntimeError):
                await interceptor.execute_activity(input)

        self.assertEqual(captured['tags']['workflow_id'], 'wf-1')
        self.assertEqual(captured['tags']['activity_name'], 'rocketreach_activity')
        self.assertEqual(captured['tags']['task_queue'], 'queue-1')
        self.assertEqual(captured['tags']['run_id'], 'run-123')

    async def test_workflow_interceptor_captures_expected_tags(self) -> None:
        captured: dict[str, object] = {}

        class Outbound:
            async def execute_workflow(self, input):
                raise RuntimeError('workflow boom')

        payload = SimpleNamespace(run_id='run-999')
        interceptor = SentryWorkflowInboundInterceptor(Outbound())
        input = ExecuteWorkflowInput(type=type('WorkflowType', (), {}), run_fn=lambda: None, args=(payload,), headers={})

        with patch('pipeline.temporal_interceptors.workflow.info', return_value=SimpleNamespace(
            workflow_id='wf-9',
            workflow_type='WorkflowType',
            task_queue='queue-9',
        )), patch('pipeline.temporal_interceptors.workflow_safe_capture_exception', side_effect=lambda error, **kwargs: captured.update(kwargs)):
            with self.assertRaises(RuntimeError):
                await interceptor.execute_workflow(input)

        self.assertEqual(captured['tags']['workflow_id'], 'wf-9')
        self.assertEqual(captured['tags']['workflow_type'], 'WorkflowType')
        self.assertEqual(captured['tags']['task_queue'], 'queue-9')
        self.assertEqual(captured['tags']['run_id'], 'run-999')


class SentryBoundaryTests(unittest.TestCase):
    def test_subprocess_nonzero_exit_raises_stage_error(self) -> None:
        run_root = TMP_ROOT / f'subprocess-error-{uuid.uuid4().hex}'
        run_root.mkdir(parents=True, exist_ok=True)
        try:
            with self.assertRaises(StageError) as context:
                adapters._run_subprocess(
                    [sys.executable, "-c", "import sys; sys.stderr.write('bad subprocess'); sys.exit(7)"],
                    run_root,
                    run_root / "stdout.log",
                    run_root / "stderr.log",
                    env={},
                )
            self.assertIn("exit code 7", str(context.exception))
        finally:
            shutil.rmtree(run_root, ignore_errors=True)

    def test_linkedin_subprocess_error_with_rows_still_raises(self) -> None:
        run_root = TMP_ROOT / f'linkedin-error-{uuid.uuid4().hex}'
        run_root.mkdir(parents=True, exist_ok=True)
        applied_csv = run_root / "applied_jobs.csv"
        write_csv(
            applied_csv,
            APPLIED_JOBS_HEADERS,
            [{
                'Date': '2026-04-25',
                'Company Name': 'Acme',
                'Position': 'Engineer',
                'Job Link': 'https://example.com/job',
                'Submitted': 'Applied',
            }],
        )
        record = {
            "id": "run-linkedin-error",
            "config_path": "",
            "run_dir": str(run_root),
            "log_dir": str(run_root),
            "applied_csv_path": str(applied_csv),
            "external_jobs_csv_path": str(run_root / "external_jobs.csv"),
            "recruiters_csv_path": str(run_root / "recruiters.csv"),
            "linkedin_stdout_log": str(run_root / "linkedin.stdout.log"),
            "linkedin_stderr_log": str(run_root / "linkedin.stderr.log"),
        }
        try:
            with patch('pipeline.adapters._run_subprocess', side_effect=StageError("subprocess failed")):
                with self.assertRaises(StageError):
                    adapters.run_linkedin_stage(record, python_executable=sys.executable)
        finally:
            shutil.rmtree(run_root, ignore_errors=True)

    def test_provider_unexpected_exception_is_captured_and_reraised(self) -> None:
        from pipeline.enrichment.models import EnrichmentContact
        from pipeline.enrichment.providers import RocketReachClient

        contact = EnrichmentContact(
            fingerprint='fingerprint',
            date='2026-04-18',
            company_name='Acme',
            position='Engineer',
            job_link='https://example.com/job',
            submitted='Applied',
            hr_name='Jane Recruiter',
            hr_position='Recruiter',
            hr_profile_link='https://linkedin.com/in/jane',
            company_domain='acme.com',
        )

        client = object.__new__(RocketReachClient)
        client._rr = SimpleNamespace(lookup_then_search=lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError('bad rr')), clean_output_row=lambda row, body: row)
        client._headers = {}
        captured: list[BaseException] = []

        with patch('pipeline.enrichment.providers.capture_exception_with_context', side_effect=lambda error, **kwargs: captured.append(error)):
            with self.assertRaises(NonRetryableProviderError):
                client.lookup(contact, limiter=SimpleNamespace(wait=lambda provider: 0))

        self.assertEqual(len(captured), 1)

    def test_smtp_send_exception_is_captured_and_preserves_retryable_error(self) -> None:
        from pipeline.config import AutomationConfig, SMTPConfig
        from pipeline.emailer import EmailContact, EmailSendError, _send_single_email

        config = AutomationConfig(
            auto_send=True,
            auto_send_reason=None,
            max_easy_apply=1,
            send_delay_seconds=0,
            linkedin_stage_timeout_seconds=1,
            linkedin_idle_timeout_seconds=1,
            temporal_auto_start=True,
            run_once_always_fresh=False,
            provider_rate_limit_per_minute=1,
            enrichment_sequential=True,
            workflow_max_reruns=1,
            email_subject='Hi',
            email_body='Body',
            sender_name='Sender',
            smtp=SMTPConfig(host='smtp.example.com', port=587, secure=False, user='user', password='pass', from_email='from@example.com'),
            source='test',
        )
        contact = EmailContact(
            email='jane@example.com',
            secondary_email='',
            name='Jane',
            company='Acme',
            position='Engineer',
            job_link='https://example.com/job',
        )
        captured: list[BaseException] = []

        with patch('pipeline.emailer.smtplib.SMTP', side_effect=TimeoutError('timed out')), \
             patch('pipeline.emailer.time.sleep', return_value=None), \
             patch('pipeline.emailer.capture_exception_with_context', side_effect=lambda error, **kwargs: captured.append(error)):
            with self.assertRaises(EmailSendError):
                _send_single_email(config, contact)

        self.assertEqual(len(captured), 3)


class TemporalWorkerSentryTests(unittest.IsolatedAsyncioTestCase):
    async def test_worker_registers_sentry_interceptors(self) -> None:
        from pipeline.temporal_worker import run_temporal_worker

        worker_instance = AsyncMock()
        worker_instance.run = AsyncMock(side_effect=RuntimeError('stop'))

        with patch('pipeline.temporal_worker.init_sentry') as init_sentry_mock, \
             patch('pipeline.temporal_worker.connect_temporal_client', AsyncMock(return_value=object())), \
             patch('pipeline.temporal_worker.Worker', return_value=worker_instance) as worker_cls:
            with self.assertRaises(RuntimeError):
                await run_temporal_worker()

        init_sentry_mock.assert_called_once_with()
        kwargs = worker_cls.call_args.kwargs
        self.assertIn('interceptors', kwargs)
        self.assertEqual(len(kwargs['interceptors']), 1)
        self.assertNotIn('workflow_runner', kwargs)


class RetryPolicyTests(unittest.TestCase):
    def test_retry_policies_use_expected_backoff(self) -> None:
        from pipeline.temporal_workflow import _stage_retry_policy

        linkedin = _stage_retry_policy('linkedin')
        rocketreach = _stage_retry_policy('rocketreach')
        email = _stage_retry_policy('email')

        self.assertEqual(linkedin.maximum_attempts, 1)
        self.assertEqual(rocketreach.backoff_coefficient, 3.0)
        self.assertEqual(email.backoff_coefficient, 3.0)
        self.assertEqual(int(rocketreach.initial_interval.total_seconds()), 10)
        self.assertEqual(int(email.maximum_interval.total_seconds()), 120)


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


class LinkedInStageEnvTests(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path(__file__).resolve().parents[2] / '.test-artifacts' / 'linkedin-stage-env'
        if self.root.exists():
            shutil.rmtree(self.root, ignore_errors=True)
        self.root.mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)

    def _make_record(self) -> dict[str, str]:
        run_dir = self.root / 'runs' / 'sample'
        log_dir = self.root / 'logs' / 'sample'
        return {
            'id': 'run-sample',
            'run_dir': str(run_dir),
            'log_dir': str(log_dir),
            'applied_csv_path': str(run_dir / 'csv' / 'applied_jobs.csv'),
            'external_jobs_csv_path': str(run_dir / 'csv' / 'external_jobs.csv'),
            'recruiters_csv_path': str(run_dir / 'csv' / 'recruiters_enriched.csv'),
            'linkedin_stdout_log': str(log_dir / 'linkedin.stdout.log'),
            'linkedin_stderr_log': str(log_dir / 'linkedin.stderr.log'),
        }

    def test_run_linkedin_stage_passes_popup_flag_when_configured(self) -> None:
        record = self._make_record()
        captured: dict[str, str] = {}

        def fake_run_subprocess(command, workdir, stdout_log, stderr_log, env=None):
            captured.update(env or {})
            write_csv(Path(record['applied_csv_path']), APPLIED_JOBS_HEADERS, [])
            Path(stdout_log).parent.mkdir(parents=True, exist_ok=True)
            Path(stdout_log).write_text('{"jobs_applied": 0, "rows_written_to_applied_csv": 0}', encoding='utf-8')
            return type('Completed', (), {'returncode': 0})()

        with patch.dict(os.environ, {adapters.LINKEDIN_POPUPS_ENV_VAR: '1'}, clear=False), \
             patch('pipeline.adapters.load_automation_config', side_effect=RuntimeError('skip config')), \
             patch('pipeline.adapters._run_subprocess', side_effect=fake_run_subprocess):
            payload = adapters.run_linkedin_stage(record, python_executable='python')

        self.assertEqual(captured['PIPELINE_MODE'], '1')
        self.assertEqual(captured[adapters.LINKEDIN_POPUPS_ENV_VAR], '1')
        self.assertEqual(captured['PIPELINE_SCREENSHOTS_DIR'], str(Path(record['log_dir']).parent / 'screenshots'))
        self.assertEqual(payload['jobs_applied'], 0)

    def test_run_linkedin_stage_omits_popup_flag_by_default(self) -> None:
        record = self._make_record()
        captured: dict[str, str] = {}

        def fake_run_subprocess(command, workdir, stdout_log, stderr_log, env=None):
            captured.update(env or {})
            write_csv(Path(record['applied_csv_path']), APPLIED_JOBS_HEADERS, [])
            Path(stdout_log).parent.mkdir(parents=True, exist_ok=True)
            Path(stdout_log).write_text('{"jobs_applied": 0, "rows_written_to_applied_csv": 0}', encoding='utf-8')
            return type('Completed', (), {'returncode': 0})()

        with patch.dict(os.environ, {}, clear=False), \
             patch('pipeline.adapters.load_automation_config', side_effect=RuntimeError('skip config')), \
             patch('pipeline.adapters._run_subprocess', side_effect=fake_run_subprocess):
            adapters.run_linkedin_stage(record, python_executable='python')

        self.assertEqual(captured['PIPELINE_MODE'], '1')
        self.assertNotIn(adapters.LINKEDIN_POPUPS_ENV_VAR, captured)
        self.assertEqual(captured['PIPELINE_SCREENSHOTS_DIR'], str(Path(record['log_dir']).parent / 'screenshots'))

    def test_run_linkedin_stage_passes_runtime_env_credentials(self) -> None:
        record = self._make_record()
        captured: dict[str, str] = {}

        def fake_run_subprocess(command, workdir, stdout_log, stderr_log, env=None):
            captured.update(env or {})
            write_csv(Path(record['applied_csv_path']), APPLIED_JOBS_HEADERS, [])
            Path(stdout_log).parent.mkdir(parents=True, exist_ok=True)
            Path(stdout_log).write_text('{"jobs_applied": 0, "rows_written_to_applied_csv": 0}', encoding='utf-8')
            return type('Completed', (), {'returncode': 0})()

        runtime_env = {
            'PIPELINE_LINKEDIN_USERNAME': 'sumedhakrishnarao@gmail.com',
            'PIPELINE_LINKEDIN_PASSWORD': 'Melbourne@1998',
            'PIPELINE_LINKEDIN_AUTO_LOGIN': 'true',
            'PIPELINE_LINKEDIN_SAFE_MODE': 'false',
        }

        with patch.dict(os.environ, {}, clear=False), \
             patch('pipeline.adapters.load_runtime_env_values', return_value=runtime_env), \
             patch('pipeline.adapters.load_automation_config', side_effect=RuntimeError('skip config')), \
             patch('pipeline.adapters._run_subprocess', side_effect=fake_run_subprocess):
            adapters.run_linkedin_stage(record, python_executable='python')

        self.assertEqual(captured['PIPELINE_LINKEDIN_USERNAME'], 'sumedhakrishnarao@gmail.com')
        self.assertEqual(captured['PIPELINE_LINKEDIN_PASSWORD'], 'Melbourne@1998')
        self.assertEqual(captured['PIPELINE_LINKEDIN_AUTO_LOGIN'], 'true')
        self.assertEqual(captured['PIPELINE_LINKEDIN_SAFE_MODE'], 'false')
        self.assertEqual(captured['PIPELINE_SCREENSHOTS_DIR'], str(Path(record['log_dir']).parent / 'screenshots'))


class LinkedInTimeoutRecoveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.root = TMP_ROOT / f'linkedin-timeout-{uuid.uuid4().hex}'
        self.root.mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)

    def _make_record(self) -> dict[str, str]:
        run_dir = self.root / 'runs' / 'sample'
        log_dir = self.root / 'logs' / 'sample'
        return {
            'id': 'run-timeout',
            'run_dir': str(run_dir),
            'log_dir': str(log_dir),
            'applied_csv_path': str(run_dir / 'csv' / 'applied_jobs.csv'),
            'external_jobs_csv_path': str(run_dir / 'csv' / 'external_jobs.csv'),
            'recruiters_csv_path': str(run_dir / 'csv' / 'recruiters_enriched.csv'),
            'linkedin_stdout_log': str(log_dir / 'linkedin.stdout.log'),
            'linkedin_stderr_log': str(log_dir / 'linkedin.stderr.log'),
        }

    def test_run_linkedin_stage_recovers_rows_after_timeout_without_final_summary(self) -> None:
        record = self._make_record()

        def fake_run_subprocess(command, workdir, stdout_log, stderr_log, env=None):
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
            Path(stdout_log).parent.mkdir(parents=True, exist_ok=True)
            Path(stdout_log).write_text('', encoding='utf-8')
            raise adapters.StageTimeoutError('LinkedIn stage timed out after 30 minutes.')

        with patch('pipeline.adapters.load_automation_config', side_effect=RuntimeError('skip config')), \
             patch('pipeline.adapters._run_subprocess', side_effect=fake_run_subprocess):
            payload = adapters.run_linkedin_stage(record, python_executable='python')

        self.assertEqual(payload['rows_written_to_applied_csv'], 1)
        self.assertEqual(payload['jobs_applied'], 1)
        self.assertTrue(payload['unexpected_failure'])
        self.assertIn('timed out', payload['session_end_reason'])

    def test_run_linkedin_stage_fails_when_timeout_produces_no_csv_rows(self) -> None:
        record = self._make_record()

        with patch('pipeline.adapters.load_automation_config', side_effect=RuntimeError('skip config')), \
             patch('pipeline.adapters._run_subprocess', side_effect=adapters.StageTimeoutError('LinkedIn stage timed out after 30 minutes.')):
            with self.assertRaises(adapters.StageTimeoutError):
                adapters.run_linkedin_stage(record, python_executable='python')


class AutomationSummaryTests(unittest.TestCase):
    def test_load_automation_summary_defaults_to_saved_session_mode(self) -> None:
        with patch.dict(os.environ, {}, clear=False):
            summary = load_automation_summary()

        linkedin = summary.get('linkedin')
        self.assertIsInstance(linkedin, dict)
        self.assertEqual(linkedin['mode'], 'saved_session')
        self.assertFalse(linkedin['auto_login'])
        self.assertFalse(linkedin['safe_mode'])

    def test_load_automation_summary_honors_linkedin_env_overrides(self) -> None:
        env = {
            'PIPELINE_LINKEDIN_AUTO_LOGIN': 'true',
            'PIPELINE_LINKEDIN_SAFE_MODE': 'true',
            'PIPELINE_LINKEDIN_USERNAME': 'user@example.com',
            'PIPELINE_MANUAL_LOGIN_TIMEOUT_SECONDS': '240',
        }
        with patch.dict(os.environ, env, clear=False):
            summary = load_automation_summary()

        linkedin = summary.get('linkedin')
        self.assertIsInstance(linkedin, dict)
        self.assertEqual(linkedin['mode'], 'auto_login')
        self.assertTrue(linkedin['auto_login'])
        self.assertTrue(linkedin['safe_mode'])
        self.assertTrue(linkedin['username_configured'])
        self.assertEqual(linkedin['manual_login_timeout_seconds'], 240)

    def test_load_automation_config_disables_auto_send_for_placeholder_smtp_values(self) -> None:
        from pipeline.config import load_automation_config

        config_path = TMP_ROOT / f'automation-config-{uuid.uuid4().hex}.env'
        try:
            config_path.write_text(
                '\n'.join([
                    'PIPELINE_AUTO_SEND=true',
                    'PIPELINE_RUN_ONCE_ALWAYS_FRESH=true',
                    'SMTP_HOST=smtp.gmail.com',
                    'SMTP_PORT=587',
                    'SMTP_SECURE=false',
                    'SMTP_USER=your-email@gmail.com',
                    'SMTP_PASS=your-app-password',
                    'SMTP_FROM=your-email@gmail.com',
                ]),
                encoding='utf-8',
            )

            config = load_automation_config(config_path)
        finally:
            config_path.unlink(missing_ok=True)

        self.assertFalse(config.auto_send)
        self.assertTrue(config.run_once_always_fresh)
        self.assertIn('placeholder values', config.auto_send_reason or '')

    def test_load_automation_summary_parses_python_config_file_safely(self) -> None:
        config_path = TMP_ROOT / f'automation-config-{uuid.uuid4().hex}.py'
        try:
            config_path.write_text(
                '\n'.join([
                    'switch_number = 7',
                    'PIPELINE_SEND_DELAY_SECONDS = 4',
                    'linkedin_auto_login = True',
                    'username = "user@example.com"',
                    'safe_mode = True',
                    'search_terms = ["Python Developer", "React Developer"]',
                    'easy_apply_only = True',
                    'target_job_link = "https://www.linkedin.com/jobs/view/123"',
                    'bad_value = unknown_call()',
                ]),
                encoding='utf-8',
            )

            summary = load_automation_summary(config_path)
        finally:
            config_path.unlink(missing_ok=True)

        self.assertEqual(summary['max_easy_apply'], 7)
        self.assertEqual(summary['send_delay_seconds'], 4)
        self.assertEqual(summary['linkedin']['mode'], 'auto_login')
        self.assertTrue(summary['linkedin']['safe_mode'])
        self.assertTrue(summary['linkedin']['username_configured'])
        self.assertEqual(summary['config_preview']['search_terms'], ['Python Developer', 'React Developer'])
        self.assertTrue(summary['config_preview']['easy_apply_only'])
        self.assertEqual(summary['config_preview']['target_job_link'], 'https://www.linkedin.com/jobs/view/123')
        self.assertNotIn('bad_value', summary['config_preview'])

    def test_load_and_update_editable_linkedin_config_files(self) -> None:
        from pipeline.config import load_editable_linkedin_config, update_editable_linkedin_config

        root = TMP_ROOT / f'linkedin-config-{uuid.uuid4().hex}'
        config_root = root / 'linkdin_automation' / 'config'
        config_root.mkdir(parents=True, exist_ok=True)
        try:
            (config_root / 'personals.py').write_text('first_name = "Old"\nphone_number = "111"\n', encoding='utf-8')
            (config_root / 'questions.py').write_text('years_of_experience = "1"\ndesired_salary = 100\n', encoding='utf-8')
            (config_root / 'search.py').write_text('search_terms = ["Python"]\nswitch_number = 5\n', encoding='utf-8')
            (config_root / 'secrets.py').write_text(
                'username = _read_str_env("PIPELINE_LINKEDIN_USERNAME", "")\n'
                'password = _read_str_env("PIPELINE_LINKEDIN_PASSWORD", "")\n'
                'linkedin_auto_login = _read_bool_env("PIPELINE_LINKEDIN_AUTO_LOGIN", False)\n',
                encoding='utf-8',
            )
            (config_root / 'settings.py').write_text('safe_mode = False\nrun_in_background = False\n', encoding='utf-8')

            initial = load_editable_linkedin_config(root)
            self.assertEqual(initial['files']['personals']['values']['first_name'], 'Old')
            self.assertEqual(initial['files']['secrets']['values']['username'], '')
            self.assertFalse(initial['files']['secrets']['values']['linkedin_auto_login'])

            updated = update_editable_linkedin_config(
                root,
                {
                    'personals': {'first_name': 'New', 'phone_number': '222'},
                    'questions': {'desired_salary': 250000},
                    'search': {'search_terms': ['React', 'Node'], 'switch_number': 12},
                    'secrets': {'username': 'shared@example.com', 'password': 'secret', 'linkedin_auto_login': True},
                    'settings': {'safe_mode': True},
                },
            )
        finally:
            shutil.rmtree(root, ignore_errors=True)

        self.assertEqual(updated['files']['personals']['values']['first_name'], 'New')
        self.assertEqual(updated['files']['questions']['values']['desired_salary'], 250000)
        self.assertEqual(updated['files']['search']['values']['search_terms'], ['React', 'Node'])
        self.assertEqual(updated['files']['secrets']['values']['username'], 'shared@example.com')
        self.assertTrue(updated['files']['secrets']['values']['linkedin_auto_login'])
        self.assertTrue(updated['files']['settings']['values']['safe_mode'])


class PipelineWorkerTests(unittest.TestCase):
    def setUp(self) -> None:
        TMP_ROOT.mkdir(parents=True, exist_ok=True)
        self.root = TMP_ROOT / f'pipeline-worker-{uuid.uuid4().hex}'
        self.root.mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)

    def test_create_run_creates_artifacts_and_manifest(self) -> None:
        store = PipelineStore(self.root)
        record = store.create_run(run_id='run-create')

        run_dir = Path(record['run_dir'])
        self.assertEqual(record['status'], 'queued')
        self.assertTrue(run_dir.exists())
        self.assertEqual(run_dir.name, 'runs')
        self.assertTrue(Path(record['manifest_path']).exists())
        self.assertTrue(Path(record['log_dir']).exists())
        self.assertTrue((self.root / 'logs' / 'screenshots').exists())
        self.assertTrue(Path(record['manifest_path']).parent.name == 'meta')
        self.assertTrue(Path(record['log_dir']).parent.name == 'logs')
        self.assertTrue(Path(record['send_report_path']).parent.name == 'reports')
        self.assertEqual(Path(record['external_jobs_csv_path']).name, 'external_jobs.csv')
        self.assertEqual(
            sorted(path.name for path in run_dir.iterdir()),
            ['csv', 'external', 'job_applied', 'rocket_enrich'],
        )

    def test_status_updates_rewrite_dashboard_manifest(self) -> None:
        store = PipelineStore(self.root)
        record = store.create_run(run_id='run-dashboard-sync')
        manifest_path = Path(record['manifest_path'])

        store.update_run(record['id'], status='linkedin_running', note='Running LinkedIn stage.')

        manifest_text = manifest_path.read_text(encoding='utf-8')
        self.assertIn('"status": "linkedin_running"', manifest_text)
        self.assertIn('"note": "Running LinkedIn stage."', manifest_text)
        self.assertFalse((self.root / record['id'] / 'manifest.json').exists())

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

    def test_reset_live_artifacts_for_run_preserves_applied_history_and_clears_other_shared_csvs(self) -> None:
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
        write_csv(
            Path(record['external_jobs_csv_path']),
            EXTERNAL_JOBS_HEADERS,
            [{
                'Date': '31/03/2026',
                'Company Name': 'Acme',
                'Position': 'Backend Engineer',
                'HR Name': 'Jane Doe',
                'HR Profile Link': 'https://linkedin.com/in/jane-doe',
            }],
        )

        store.reset_live_artifacts_for_run(record['id'])

        self.assertTrue(Path(record['applied_csv_path']).exists())
        self.assertFalse(Path(record['external_jobs_csv_path']).exists())
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
            write_csv(
                Path(run_record['external_jobs_csv_path']),
                EXTERNAL_JOBS_HEADERS,
                [{
                    'Date': '31/03/2026',
                    'Company Name': 'Beta',
                    'Position': 'External Engineer',
                    'HR Name': 'Rita Recruiter',
                    'HR Profile Link': 'https://linkedin.com/in/rita-recruiter',
                }],
            )
            return {
                'jobs_applied': 1,
                'external_links_logged': 1,
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
        self.assertTrue(Path(final_record['external_jobs_csv_path']).exists())
        self.assertTrue(Path(final_record['recruiters_csv_path']).exists())
        self.assertEqual(
            sorted(path.name for path in Path(final_record['run_dir']).iterdir()),
            ['csv', 'external', 'job_applied', 'rocket_enrich'],
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
        fallback_path = Path(record['run_dir']) / 'rocket_enrich' / 'recruiters_enriched_latest.csv'
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

        self.assertIn(final_record['status'], {'waiting_review', 'completed'})
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

    def test_worker_marks_manual_login_issue_as_waiting_login(self) -> None:
        worker = PipelineWorker(root=self.root)
        record = worker.store.create_run(run_id='run-waiting-login')

        with patch(
            'pipeline.worker.preflight_linkedin_runtime',
            return_value=adapters.LinkedInRuntimePreflight(executable='C:\\Python312\\python.exe', source='test', blocked_reason=None),
        ), patch(
            'pipeline.worker.run_linkedin_stage',
            side_effect=StageError('LinkedIn login was not confirmed. Complete manual login in Chrome and keep the browser window open.'),
        ):
            final_record = worker.process_run(record['id'])

        self.assertEqual(final_record['status'], 'waiting_login')
        self.assertIn('Chrome opened with your default profile', final_record['note'])
        self.assertIn('LinkedIn login was not confirmed', final_record['last_error'])

    def test_worker_marks_auto_login_block_as_waiting_login(self) -> None:
        worker = PipelineWorker(root=self.root)
        record = worker.store.create_run(run_id='run-auto-login-blocked')

        with patch(
            'pipeline.worker.preflight_linkedin_runtime',
            return_value=adapters.LinkedInRuntimePreflight(executable='C:\\Python312\\python.exe', source='test', blocked_reason=None),
        ), patch(
            'pipeline.worker.run_linkedin_stage',
            side_effect=StageError(
                'Automatic LinkedIn login did not complete successfully. '
                'The login page/form was unavailable or the session was blocked by LinkedIn. '
                'Complete manual login in Chrome and keep the browser window open.'
            ),
        ):
            final_record = worker.process_run(record['id'])

        self.assertEqual(final_record['status'], 'waiting_login')
        self.assertIn('manual verification in Chrome', final_record['note'])
        self.assertIn('Automatic LinkedIn login did not complete successfully', final_record['last_error'])

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
        write_csv(
            Path(record['external_jobs_csv_path']),
            EXTERNAL_JOBS_HEADERS,
            [{
                'Date': '31/03/2026',
                'Company Name': 'OldCo',
                'Position': 'External Old Role',
                'HR Name': 'Old Recruiter',
                'HR Profile Link': 'https://linkedin.com/in/old-recruiter',
            }],
        )

        def fake_linkedin(run_record: dict, python_executable: str | None = None) -> dict:
            applied_rows_before_run = Path(run_record['applied_csv_path']).read_text(encoding='utf-8-sig')
            self.assertIn('OldCo', applied_rows_before_run)
            self.assertFalse(Path(run_record['external_jobs_csv_path']).exists())
            self.assertFalse(Path(run_record['recruiters_csv_path']).exists())
            write_csv(
                Path(run_record['applied_csv_path']),
                APPLIED_JOBS_HEADERS,
                [
                    {
                        'Date': '31/03/2026',
                        'Company Name': 'OldCo',
                        'Position': 'Old Role',
                        'Job Link': 'https://linkedin.com/jobs/view/old',
                        'Submitted': 'Applied',
                        'HR Name': 'Old Recruiter',
                        'HR Position': 'Recruiter',
                        'HR Profile Link': 'https://linkedin.com/in/old-recruiter',
                    },
                    {
                        'Date': '31/03/2026',
                        'Company Name': 'NewCo',
                        'Position': 'New Role',
                        'Job Link': 'https://linkedin.com/jobs/view/new',
                        'Submitted': 'Applied',
                        'HR Name': 'New Recruiter',
                        'HR Position': 'Recruiter',
                        'HR Profile Link': 'https://linkedin.com/in/new-recruiter',
                    },
                ],
            )
            write_csv(
                Path(run_record['external_jobs_csv_path']),
                EXTERNAL_JOBS_HEADERS,
                [{
                    'Date': '31/03/2026',
                    'Company Name': 'ExternalCo',
                    'Position': 'External New Role',
                    'HR Name': 'Ext Recruiter',
                    'HR Profile Link': 'https://linkedin.com/in/ext-recruiter',
                }],
            )
            return {
                'jobs_applied': 1,
                'external_links_logged': 1,
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

        self.assertIn(final_record['status'], {'waiting_review', 'completed'})
        applied_rows = Path(final_record['applied_csv_path']).read_text(encoding='utf-8-sig')
        external_rows = Path(final_record['external_jobs_csv_path']).read_text(encoding='utf-8-sig')
        recruiters_rows = Path(final_record['recruiters_csv_path']).read_text(encoding='utf-8-sig')
        self.assertIn('NewCo', applied_rows)
        self.assertIn('OldCo', applied_rows)
        self.assertIn('ExternalCo', external_rows)
        self.assertNotIn('OldCo', external_rows)
        self.assertIn('new@newco.com', recruiters_rows)
        self.assertNotIn('old@oldco.com', recruiters_rows)

    def test_worker_runs_linkedin_for_new_queued_run_even_when_applied_history_exists(self) -> None:
        worker = PipelineWorker(root=self.root)
        record = worker.store.create_run(run_id='run-history-reuse')
        write_csv(
            Path(record['applied_csv_path']),
            APPLIED_JOBS_HEADERS,
            [{
                'Date': '31/03/2026',
                'Company Name': 'HistoryCo',
                'Position': 'Old Role',
                'Job Link': 'https://linkedin.com/jobs/view/history',
                'Submitted': 'Applied',
                'HR Name': 'History Recruiter',
                'HR Position': 'Recruiter',
                'HR Profile Link': 'https://linkedin.com/in/history-recruiter',
            }],
        )

        def fake_linkedin(run_record: dict, python_executable: str | None = None) -> dict:
            write_csv(
                Path(run_record['applied_csv_path']),
                APPLIED_JOBS_HEADERS,
                [
                    {
                        'Date': '31/03/2026',
                        'Company Name': 'HistoryCo',
                        'Position': 'Old Role',
                        'Job Link': 'https://linkedin.com/jobs/view/history',
                        'Submitted': 'Applied',
                        'HR Name': 'History Recruiter',
                        'HR Position': 'Recruiter',
                        'HR Profile Link': 'https://linkedin.com/in/history-recruiter',
                    },
                    {
                        'Date': '01/04/2026',
                        'Company Name': 'FreshCo',
                        'Position': 'Fresh Role',
                        'Job Link': 'https://linkedin.com/jobs/view/fresh',
                        'Submitted': 'Applied',
                        'HR Name': 'Fresh Recruiter',
                        'HR Position': 'Recruiter',
                        'HR Profile Link': 'https://linkedin.com/in/fresh-recruiter',
                    },
                ],
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
                    'Date': '01/04/2026',
                    'Company Name': 'FreshCo',
                    'Position': 'Fresh Role',
                    'Job Link': 'https://linkedin.com/jobs/view/fresh',
                    'Submitted': 'Applied',
                    'HR Name': 'Fresh Recruiter',
                    'HR Position': 'Recruiter',
                    'HR Profile Link': 'https://linkedin.com/in/fresh-recruiter',
                    'HR Email': 'fresh@freshco.com',
                    'HR Secondary Email': '',
                    'HR Contact': '',
                    'RocketReach Status': 'matched',
                }],
            )
            return {
                'total': 2,
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

        with patch('pipeline.worker.preflight_linkedin_runtime', return_value=adapters.LinkedInRuntimePreflight(executable='C:\\Python311\\python.exe', source='test', blocked_reason=None)), patch('pipeline.worker.run_linkedin_stage', side_effect=fake_linkedin) as linkedin_stage, patch('pipeline.worker.run_rocketreach_stage', side_effect=fake_rocketreach):
            final_record = worker.process_run(record['id'])

        linkedin_stage.assert_called_once()
        self.assertIn(final_record['status'], {'waiting_review', 'completed'})

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

    def test_worker_marks_linkedin_run_completed_when_no_jobs_applied_but_easy_apply_failed(self) -> None:
        worker = PipelineWorker(root=self.root)
        record = worker.store.create_run(run_id='run-linkedin-zero-applies')

        with patch('pipeline.worker.preflight_linkedin_runtime', return_value=adapters.LinkedInRuntimePreflight(executable='C:\\Python311\\python.exe', source='test', blocked_reason=None)), \
             patch('pipeline.worker.run_linkedin_stage', return_value={
                 'jobs_applied': 0,
                 'rows_written_to_applied_csv': 0,
                 'external_links_logged': 0,
                 'rows_missing_hr_profile': 0,
                 'failed_jobs': 2,
                 'unexpected_failure': False,
            }):
            updated_record = worker.process_run(record['id'])

        self.assertEqual(updated_record['status'], 'completed')
        self.assertIn('Skipped 2 failed Easy Apply attempt', updated_record['note'])
        self.assertIn('2 failed Easy Apply attempt', updated_record['last_error'])

    def test_run_rocketreach_stage_raises_transient_stage_error_for_retryable_provider_failures(self) -> None:
        record = PipelineStore(self.root).create_run(run_id='run-retryable-enrichment')

        with patch('pipeline.adapters.enrich_contacts', side_effect=RetryableProviderError('temporary quota issue')):
            with self.assertRaises(TransientStageError):
                adapters.run_rocketreach_stage(record)

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

    def test_active_waiting_login_run_blocks_new_enqueue(self) -> None:
        store = PipelineStore(self.root)
        record = store.create_run(run_id='run-login-blocked')
        store.update_run(record['id'], status='waiting_login', note='Login required.')

        with self.assertRaises(RuntimeError):
            store.create_run(run_id='run-should-block')

    def test_get_active_live_run_keeps_only_latest_shared_folder_run_authoritative(self) -> None:
        store = PipelineStore(self.root)
        older = store.create_run(run_id='run-shared-older', allow_active_conflict=True)
        newer = store.create_run(run_id='run-shared-newer', allow_active_conflict=True)
        store.update_run(older['id'], status='linkedin_running', note='Older shared run')
        store.update_run(newer['id'], status='waiting_login', note='Newer shared run')

        active_run = store.get_active_live_run()

        self.assertIsNotNone(active_run)
        self.assertEqual(active_run['id'], 'run-shared-newer')

    def test_worker_marks_chrome_startup_recovery_as_waiting_login(self) -> None:
        worker = PipelineWorker(root=self.root)
        record = worker.store.create_run(run_id='run-chrome-startup-waiting')

        with patch(
            'pipeline.worker.preflight_linkedin_runtime',
            return_value=adapters.LinkedInRuntimePreflight(executable='C:\\Python312\\python.exe', source='test', blocked_reason=None),
        ), patch(
            'pipeline.worker.run_linkedin_stage',
            side_effect=StageError(
                'Chrome startup needs manual recovery. '
                'Close extra Chrome windows, reopen LinkedIn in Chrome, and keep the browser window open.'
            ),
        ):
            final_record = worker.process_run(record['id'])

        self.assertEqual(final_record['status'], 'waiting_login')
        self.assertIn('Chrome opened with your default profile', final_record['note'])

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


class TemporalMetadataTests(unittest.TestCase):
    def setUp(self) -> None:
        TMP_ROOT.mkdir(parents=True, exist_ok=True)
        self.root = TMP_ROOT / f'temporal-metadata-{uuid.uuid4().hex}'
        self.root.mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)

    def test_store_persists_temporal_fields_and_manifest_block(self) -> None:
        store = PipelineStore(self.root)
        record = store.create_run(run_id='run-temporal-meta')
        updated = store.update_run(
            record['id'],
            temporal_workflow_id='pipeline-run-temporal-meta',
            temporal_task_queue='automation-pipeline',
            orchestration_backend='temporal',
        )

        self.assertEqual(updated['temporal_workflow_id'], 'pipeline-run-temporal-meta')
        manifest_text = Path(updated['manifest_path']).read_text(encoding='utf-8')
        self.assertIn('"workflow_id": "pipeline-run-temporal-meta"', manifest_text)
        self.assertIn('"task_queue": "automation-pipeline"', manifest_text)
        self.assertIn('"backend": "temporal"', manifest_text)


class EnrichmentServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        TMP_ROOT.mkdir(parents=True, exist_ok=True)
        self.root = TMP_ROOT / f'enrichment-service-{uuid.uuid4().hex}'
        self.root.mkdir(parents=True, exist_ok=True)
        self.store = PipelineStore(self.root)

    def tearDown(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)

    def _create_record_with_applied_row(self, run_id: str = 'run-enrichment') -> dict:
        record = self.store.create_run(run_id=run_id)
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
        return record

    def _fake_rr_module(self):
        class _FakeRocketReachModule:
            @staticmethod
            def clean_output_row(row, body):
                merged = dict(row)
                merged.setdefault('HR Email', '')
                merged.setdefault('HR Secondary Email', '')
                merged.setdefault('HR Email Preview', '')
                merged.setdefault('HR Contact', '')
                merged.setdefault('HR Contact Preview', '')
                merged.setdefault('RocketReach Status', '')
                return merged

            @staticmethod
            def write_output_csv(path, text):
                path = Path(path)
                path.write_text(text, encoding='utf-8')
                return path, ''

        return _FakeRocketReachModule()

    def test_rocketreach_client_maps_account_verification_error_to_non_retryable(self) -> None:
        from pipeline.enrichment.providers import RocketReachClient

        class FakeRocketReachModule:
            ENV_PATH = 'ignored'

            @staticmethod
            def build_rocketreach_headers(_env_path):
                return {}

            @staticmethod
            def lookup_then_search(_profile_link, _headers, row=None):
                raise RuntimeError('Verify your email in order to use the full API')

        with patch('pipeline.enrichment.providers.load_rocketreach_module', return_value=FakeRocketReachModule()):
            client = RocketReachClient()
            with self.assertRaises(NonRetryableProviderError) as context:
                client.lookup(
                    SimpleNamespace(
                        date='31/03/2026',
                        company_name='Acme',
                        position='Backend Engineer',
                        job_link='https://linkedin.com/jobs/view/1',
                        submitted='Applied',
                        hr_name='Jane Doe',
                        hr_position='Recruiter',
                        hr_profile_link='https://linkedin.com/in/jane-doe',
                    ),
                    limiter=SimpleNamespace(),
                )

        self.assertIn('authentication failed', str(context.exception).lower())

    def test_apollo_client_requires_api_key(self) -> None:
        from pipeline.enrichment.providers import ApolloClient

        with self.assertRaises(ValueError):
            ApolloClient(api_key='')

    def test_apollo_client_health_check_failure_is_non_retryable(self) -> None:
        from pipeline.enrichment.providers import ApolloClient

        response = SimpleNamespace(status_code=403, text='forbidden', json=lambda: {'message': 'forbidden'})
        client = ApolloClient(api_key='apollo-key')
        client.session = SimpleNamespace(
            get=lambda *args, **kwargs: response,
            post=lambda *args, **kwargs: response,
        )

        with self.assertRaises(NonRetryableProviderError) as context:
            client.lookup(
                SimpleNamespace(
                    date='31/03/2026',
                    company_name='Acme',
                    position='Backend Engineer',
                    job_link='https://linkedin.com/jobs/view/1',
                    submitted='Applied',
                    hr_name='Jane Doe',
                    hr_position='Recruiter',
                    hr_profile_link='https://linkedin.com/in/jane-doe',
                ),
                limiter=SimpleNamespace(),
            )

        self.assertIn('health-check failed', str(context.exception).lower())

    def test_enrich_contacts_returns_waiting_review_when_provider_auth_fails(self) -> None:
        from pipeline.enrichment.service import enrich_contacts

        class AuthFailureProvider:
            provider_name = 'rocketreach'
            is_usable = True

            def lookup(self, contact, limiter=None):
                raise NonRetryableProviderError(
                    'RocketReach authentication failed: Verify your email in order to use the full API',
                    provider=self.provider_name,
                )

        class UnconfiguredProvider:
            is_usable = False

            def __init__(self, provider_name: str) -> None:
                self.provider_name = provider_name

        record = self._create_record_with_applied_row('run-provider-auth')

        with patch('pipeline.enrichment.service.load_automation_config', return_value=SimpleNamespace(enrichment_sequential=True, provider_rate_limit_per_minute=2)), \
             patch('pipeline.enrichment.service._build_provider_clients', return_value=[AuthFailureProvider(), UnconfiguredProvider('hunter'), UnconfiguredProvider('apollo')]), \
             patch('pipeline.enrichment.service.load_rocketreach_module', return_value=self._fake_rr_module()):
            stats = enrich_contacts(record, self.store)

        self.assertEqual(stats['final_status'], 'waiting_review')
        self.assertEqual(stats['authentication_failed'], 1)
        self.assertEqual(stats['sendable_rows'], 0)
        self.assertIn('valid RocketReach/provider credentials', stats['final_reason'])

    def test_enrich_contacts_keeps_transient_provider_failures_retryable(self) -> None:
        from pipeline.enrichment.service import enrich_contacts

        class RetryableProvider:
            provider_name = 'rocketreach'
            is_usable = True

            def lookup(self, contact, limiter=None):
                raise RetryableProviderError('503 service unavailable', provider=self.provider_name)

        record = self._create_record_with_applied_row('run-provider-transient')

        with patch('pipeline.enrichment.service.load_automation_config', return_value=SimpleNamespace(enrichment_sequential=True, provider_rate_limit_per_minute=2)), \
             patch('pipeline.enrichment.service._build_provider_clients', return_value=[RetryableProvider()]), \
             patch('pipeline.enrichment.service.load_rocketreach_module', return_value=self._fake_rr_module()):
            stats = enrich_contacts(record, self.store)

        self.assertEqual(stats['final_status'], 'completed')
        self.assertEqual(stats['sendable_rows'], 0)
        self.assertEqual(stats['no_match'], 1)

    def test_enrich_contacts_returns_waiting_review_for_quota_without_fallback_credentials(self) -> None:
        from pipeline.enrichment.service import enrich_contacts
        from pipeline.enrichment.errors import QuotaExceededError

        class QuotaProvider:
            provider_name = 'rocketreach'
            is_usable = True

            def lookup(self, contact, limiter=None):
                raise QuotaExceededError('RocketReach quota exhausted', provider=self.provider_name)

        class UnconfiguredProvider:
            is_usable = False

            def __init__(self, provider_name: str) -> None:
                self.provider_name = provider_name

        record = self._create_record_with_applied_row('run-provider-quota-no-fallback')

        with patch('pipeline.enrichment.service.load_automation_config', return_value=SimpleNamespace(enrichment_sequential=True, provider_rate_limit_per_minute=2)), \
             patch('pipeline.enrichment.service._build_provider_clients', return_value=[QuotaProvider(), UnconfiguredProvider('hunter'), UnconfiguredProvider('apollo')]), \
             patch('pipeline.enrichment.service.load_rocketreach_module', return_value=self._fake_rr_module()):
            stats = enrich_contacts(record, self.store)

        self.assertEqual(stats['final_status'], 'waiting_review')
        self.assertEqual(stats['lookup_quota_reached'], 1)
        self.assertIn('Hunter/Apollo fallback credentials are missing', stats['final_reason'])

    def test_build_provider_clients_uses_env_and_orders_rocketreach_apollo_hunter(self) -> None:
        from pipeline.enrichment.service import _build_provider_clients

        rr_stub = SimpleNamespace(provider_name='rocketreach', is_usable=True)
        apollo_stub = SimpleNamespace(provider_name='apollo', is_usable=True)
        hunter_stub = SimpleNamespace(provider_name='hunter', is_usable=True)

        with patch('pipeline.enrichment.service.bootstrap_runtime_environment', return_value={'sources': ['pipeline/automation.env'], 'stale_apollo_snapshot': False}), \
             patch('pipeline.enrichment.service.load_automation_config', return_value=SimpleNamespace(linkedin_idle_timeout_seconds=30)), \
             patch('pipeline.enrichment.service.load_runtime_env_values', return_value={}), \
             patch.dict(os.environ, {'APOLLO_API_KEY': 'apollo-key', 'HUNTER_API_KEY': 'hunter-key', 'ROCKETREACH_API_KEY': 'rr-key'}, clear=True), \
             patch('pipeline.enrichment.service.RocketReachClient', return_value=rr_stub), \
             patch('pipeline.enrichment.service.ApolloClient', return_value=apollo_stub) as apollo_cls, \
             patch('pipeline.enrichment.service.HunterClient', return_value=hunter_stub):
            providers = _build_provider_clients(None)

        self.assertEqual([provider.provider_name for provider in providers], ['rocketreach', 'apollo', 'hunter'])
        self.assertEqual(apollo_cls.call_args.kwargs['api_key'], 'apollo-key')

    def test_enrich_contacts_continues_to_hunter_when_apollo_health_fails(self) -> None:
        from pipeline.enrichment.service import enrich_contacts

        class RocketReachRetryableProvider:
            provider_name = 'rocketreach'
            is_usable = True

            def lookup(self, contact, limiter=None):
                raise RetryableProviderError('RocketReach temporary failure', provider=self.provider_name)

        class ApolloHealthFailureProvider:
            provider_name = 'apollo'
            is_usable = True

            def lookup(self, contact, limiter=None):
                raise NonRetryableProviderError('Apollo health-check failed: invalid key', provider=self.provider_name)

        class HunterMatchedProvider:
            provider_name = 'hunter'
            is_usable = True

            def lookup(self, contact, limiter=None):
                return SimpleNamespace(
                    provider=self.provider_name,
                    status='matched',
                    email='jane@example.com',
                    secondary_email='',
                    email_preview='',
                    contact='',
                    contact_preview='',
                    normalized_profile_link=contact.hr_profile_link,
                    resolved_name=contact.hr_name,
                    resolved_position=contact.hr_position,
                    last_error='',
                    raw_payload={},
                )

        record = self._create_record_with_applied_row('run-provider-apollo-health-fail')

        with patch('pipeline.enrichment.service.load_automation_config', return_value=SimpleNamespace(enrichment_sequential=True, provider_rate_limit_per_minute=2)), \
             patch('pipeline.enrichment.service._build_provider_clients', return_value=[RocketReachRetryableProvider(), ApolloHealthFailureProvider(), HunterMatchedProvider()]), \
             patch('pipeline.enrichment.service.load_rocketreach_module', return_value=self._fake_rr_module()):
            stats = enrich_contacts(record, self.store)

        self.assertEqual(stats['matched'], 1)
        self.assertEqual(stats['sendable_rows'], 1)
        self.assertEqual(stats['provider_success_count'], 1)

    def test_enrich_contacts_does_not_crash_when_all_providers_fail(self) -> None:
        from pipeline.enrichment.service import enrich_contacts

        class RetryableProvider:
            def __init__(self, provider_name: str) -> None:
                self.provider_name = provider_name
                self.is_usable = True

            def lookup(self, contact, limiter=None):
                raise RetryableProviderError(f'{self.provider_name} temporary failure', provider=self.provider_name)

        record = self._create_record_with_applied_row('run-provider-all-fail')

        with patch('pipeline.enrichment.service.load_automation_config', return_value=SimpleNamespace(enrichment_sequential=True, provider_rate_limit_per_minute=2)), \
             patch('pipeline.enrichment.service._build_provider_clients', return_value=[RetryableProvider('rocketreach'), RetryableProvider('apollo'), RetryableProvider('hunter')]), \
             patch('pipeline.enrichment.service.load_rocketreach_module', return_value=self._fake_rr_module()):
            stats = enrich_contacts(record, self.store)

        self.assertEqual(stats['final_status'], 'completed')
        self.assertEqual(stats['sendable_rows'], 0)
        self.assertEqual(stats['no_match'], 1)


class RocketReachOutcomeTests(unittest.TestCase):
    def setUp(self) -> None:
        TMP_ROOT.mkdir(parents=True, exist_ok=True)
        self.root = TMP_ROOT / f'rocketreach-outcomes-{uuid.uuid4().hex}'
        self.root.mkdir(parents=True, exist_ok=True)
        self.store = PipelineStore(self.root)

    def tearDown(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)

    def test_stage_manager_marks_no_sendable_auth_failure_as_waiting_review(self) -> None:
        record = self.store.create_run(run_id='run-rr-auth-waiting')
        manager = PipelineStageManager(self.store)

        final_record = manager.handle_rocketreach_success(
            record['id'],
            {
                'sendable_rows': 0,
                'total': 1,
                'matched': 0,
                'preview_match': 0,
                'missing_hr_link': 0,
                'invalid_hr_link': 0,
                'profile_only': 0,
                'no_match': 0,
                'lookup_quota_reached': 0,
                'authentication_failed': 1,
                'provider_success_count': 0,
                'no_email_count': 1,
                'provider_retry_count': 0,
                'provider_configuration_blocked': 0,
                'final_status': 'waiting_review',
                'final_reason': 'Recruiter lookup needs valid RocketReach/provider credentials before recruiter emails can be enriched.',
            },
        )

        self.assertEqual(final_record['status'], 'waiting_review')
        self.assertIn('authentication failed', final_record['note'].lower())
        self.assertIn('valid RocketReach/provider credentials', final_record['note'])

    def test_stage_manager_marks_quota_without_fallback_as_waiting_review(self) -> None:
        record = self.store.create_run(run_id='run-rr-quota-waiting')
        manager = PipelineStageManager(self.store)

        final_record = manager.handle_rocketreach_success(
            record['id'],
            {
                'sendable_rows': 0,
                'total': 1,
                'matched': 0,
                'preview_match': 0,
                'missing_hr_link': 0,
                'invalid_hr_link': 0,
                'profile_only': 0,
                'no_match': 0,
                'lookup_quota_reached': 1,
                'authentication_failed': 0,
                'provider_success_count': 0,
                'no_email_count': 1,
                'provider_retry_count': 0,
                'provider_configuration_blocked': 1,
                'final_status': 'waiting_review',
                'final_reason': 'RocketReach quota/credits were exhausted and Hunter/Apollo fallback credentials are missing. Add RocketReach credits or configure HUNTER_API_KEY / APOLLO_API_KEY, then retry.',
            },
        )

        self.assertEqual(final_record['status'], 'waiting_review')
        self.assertIn('quota/credit/account verification blocked email lookup', final_record['note'])
        self.assertIn('Hunter/Apollo fallback credentials are missing', final_record['note'])


class TemporalWorkflowTests(unittest.IsolatedAsyncioTestCase):
    async def test_workflow_accepts_dict_activity_results(self) -> None:
        from pipeline.temporal_workflow import AutomationPipelineWorkflow

        execute_activity = AsyncMock(side_effect=[
            {'run_id': 'run-dict', 'status': 'queued', 'note': 'linkedin', 'outcome': 'success', 'current_stage': 'linkedin'},
            {'run_id': 'run-dict', 'status': 'completed', 'note': 'rocket', 'sendable_rows': 0, 'outcome': 'success', 'current_stage': 'rocketreach'},
        ])
        with patch('pipeline.temporal_workflow.workflow.execute_activity', execute_activity), \
             patch('pipeline.temporal_workflow.workflow.sleep', AsyncMock()), \
             patch('pipeline.temporal_workflow.workflow.info', return_value=type('Info', (), {'workflow_id': 'wf-dict', 'task_queue': 'automation-pipeline'})()):
            result = await AutomationPipelineWorkflow().run(TemporalWorkflowInput(run_id='run-dict'))

        self.assertEqual(result.status, 'completed')
        self.assertEqual(result.note, 'rocket')
        self.assertEqual(execute_activity.await_count, 2)

    async def test_workflow_runs_stages_in_sequence(self) -> None:
        from pipeline.temporal_workflow import AutomationPipelineWorkflow

        results = [
            TemporalActivityResult(run_id='run-1', status='queued', note='linkedin', outcome='success', current_stage='linkedin'),
            TemporalActivityResult(run_id='run-1', status='queued', note='rocketreach', sendable_rows=1, outcome='success', current_stage='rocketreach'),
            TemporalActivityResult(run_id='run-1', status='completed', note='email', outcome='success', current_stage='email'),
        ]

        execute_activity = AsyncMock(side_effect=results)
        with patch('pipeline.temporal_workflow.workflow.execute_activity', execute_activity), \
             patch('pipeline.temporal_workflow.workflow.sleep', AsyncMock()), \
             patch('pipeline.temporal_workflow.workflow.info', return_value=type('Info', (), {'workflow_id': 'wf-1', 'task_queue': 'automation-pipeline'})()):
            result = await AutomationPipelineWorkflow().run(TemporalWorkflowInput(run_id='run-1'))

        self.assertEqual(result.status, 'completed')
        self.assertEqual(result.workflow_id, 'wf-1')
        self.assertEqual(execute_activity.await_count, 3)

    async def test_workflow_skips_email_when_rocketreach_completes_without_sendable_contacts(self) -> None:
        from pipeline.temporal_workflow import AutomationPipelineWorkflow

        execute_activity = AsyncMock(side_effect=[
            TemporalActivityResult(run_id='run-2', status='queued', note='linkedin', outcome='success', current_stage='linkedin'),
            TemporalActivityResult(run_id='run-2', status='completed', note='no sendable rows', sendable_rows=0, outcome='success', current_stage='rocketreach'),
        ])
        with patch('pipeline.temporal_workflow.workflow.execute_activity', execute_activity), \
             patch('pipeline.temporal_workflow.workflow.sleep', AsyncMock()), \
             patch('pipeline.temporal_workflow.workflow.info', return_value=type('Info', (), {'workflow_id': 'wf-2', 'task_queue': 'automation-pipeline'})()):
            result = await AutomationPipelineWorkflow().run(TemporalWorkflowInput(run_id='run-2'))

        self.assertEqual(result.status, 'completed')
        self.assertEqual(execute_activity.await_count, 2)

    async def test_workflow_continues_to_email_after_rocketreach_activity_succeeds(self) -> None:
        from pipeline.temporal_workflow import AutomationPipelineWorkflow

        execute_activity = AsyncMock(side_effect=[
            TemporalActivityResult(run_id='run-3', status='queued', note='linkedin ok', outcome='success', current_stage='linkedin'),
            TemporalActivityResult(run_id='run-3', status='queued', note='rocket ok', sendable_rows=1, outcome='success', current_stage='rocketreach'),
            TemporalActivityResult(run_id='run-3', status='completed', note='email ok', outcome='success', current_stage='email'),
        ])

        with patch('pipeline.temporal_workflow.workflow.execute_activity', execute_activity), \
             patch('pipeline.temporal_workflow.workflow.sleep', AsyncMock()), \
             patch('pipeline.temporal_workflow.workflow.info', return_value=type('Info', (), {'workflow_id': 'wf-3', 'task_queue': 'automation-pipeline'})()):
            result = await AutomationPipelineWorkflow().run(TemporalWorkflowInput(run_id='run-3'))

        self.assertEqual(result.status, 'completed')
        self.assertEqual(execute_activity.await_count, 3)
        self.assertEqual(result.rocketreach_retry_count, 0)

    async def test_workflow_resumes_from_rocketreach_when_linkedin_recovered_applied_rows(self) -> None:
        from pipeline.temporal_workflow import AutomationPipelineWorkflow

        execute_activity = AsyncMock(side_effect=[
            TemporalActivityResult(run_id='run-4', status='queued', note='Recovered applied rows', outcome='success', current_stage='linkedin'),
            TemporalActivityResult(run_id='run-4', status='completed', note='no sendable rows', sendable_rows=0, outcome='success', current_stage='rocketreach'),
        ])

        with patch('pipeline.temporal_workflow.workflow.execute_activity', execute_activity), \
             patch('pipeline.temporal_workflow.workflow.sleep', AsyncMock()), \
             patch('pipeline.temporal_workflow.workflow.info', return_value=type('Info', (), {'workflow_id': 'wf-4', 'task_queue': 'automation-pipeline'})()):
            result = await AutomationPipelineWorkflow().run(TemporalWorkflowInput(run_id='run-4'))

        self.assertEqual(result.status, 'completed')
        self.assertEqual(execute_activity.await_count, 2)

    async def test_workflow_returns_failed_when_rocketreach_activity_returns_failed(self) -> None:
        from pipeline.temporal_workflow import AutomationPipelineWorkflow

        execute_activity = AsyncMock(side_effect=[
            TemporalActivityResult(run_id='run-5', status='queued', note='linkedin ok', outcome='success', current_stage='linkedin'),
            TemporalActivityResult(run_id='run-5', status='failed', note='RocketReach retries exhausted.', outcome='terminal_failure', current_stage='rocketreach', retry_count=5),
        ])

        with patch('pipeline.temporal_workflow.workflow.execute_activity', execute_activity), \
             patch('pipeline.temporal_workflow.workflow.sleep', AsyncMock()), \
             patch('pipeline.temporal_workflow.workflow.info', return_value=type('Info', (), {'workflow_id': 'wf-5', 'task_queue': 'automation-pipeline'})()):
            result = await AutomationPipelineWorkflow().run(TemporalWorkflowInput(run_id='run-5'))

        self.assertEqual(result.status, 'failed')
        self.assertEqual(execute_activity.await_count, 2)

    async def test_workflow_returns_completed_after_email_activity_succeeds(self) -> None:
        from pipeline.temporal_workflow import AutomationPipelineWorkflow

        execute_activity = AsyncMock(side_effect=[
            TemporalActivityResult(run_id='run-6', status='queued', note='linkedin ok', outcome='success', current_stage='linkedin'),
            TemporalActivityResult(run_id='run-6', status='queued', note='rocket ok', sendable_rows=1, outcome='success', current_stage='rocketreach'),
            TemporalActivityResult(run_id='run-6', status='completed', note='email ok', outcome='success', current_stage='email'),
        ])

        with patch('pipeline.temporal_workflow.workflow.execute_activity', execute_activity), \
             patch('pipeline.temporal_workflow.workflow.sleep', AsyncMock()), \
             patch('pipeline.temporal_workflow.workflow.info', return_value=type('Info', (), {'workflow_id': 'wf-6', 'task_queue': 'automation-pipeline'})()):
            result = await AutomationPipelineWorkflow().run(TemporalWorkflowInput(run_id='run-6'))

        self.assertEqual(result.status, 'completed')
        self.assertEqual(execute_activity.await_count, 3)

    async def test_workflow_stops_on_manual_linkedin_states(self) -> None:
        from pipeline.temporal_workflow import AutomationPipelineWorkflow

        for terminal_status in ('waiting_login', 'blocked_runtime'):
            execute_activity = AsyncMock(side_effect=[
                TemporalActivityResult(run_id='run-manual', status=terminal_status, note=terminal_status, outcome='terminal_failure', current_stage='linkedin'),
            ])
            with patch('pipeline.temporal_workflow.workflow.execute_activity', execute_activity), \
                 patch('pipeline.temporal_workflow.workflow.sleep', AsyncMock()), \
                 patch('pipeline.temporal_workflow.workflow.info', return_value=type('Info', (), {'workflow_id': f'wf-{terminal_status}', 'task_queue': 'automation-pipeline'})()):
                result = await AutomationPipelineWorkflow().run(TemporalWorkflowInput(run_id='run-manual'))

            self.assertEqual(result.status, terminal_status)
            self.assertEqual(execute_activity.await_count, 1)


class TemporalActivityResumeTests(unittest.TestCase):
    def setUp(self) -> None:
        TMP_ROOT.mkdir(parents=True, exist_ok=True)
        self.root = TMP_ROOT / f'temporal-activity-resume-{uuid.uuid4().hex}'
        self.root.mkdir(parents=True, exist_ok=True)
        self.store = PipelineStore(self.root)

    def tearDown(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)

    def test_linkedin_activity_skips_browser_when_applied_rows_already_exist(self) -> None:
        from pipeline.temporal_activities import linkedin_activity
        from pipeline.temporal_types import TemporalActivityInput

        record = self.store.create_run(run_id='run-linkedin-resume')
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

        with patch('pipeline.temporal_activities.PipelineStageManager.run_linkedin') as run_linkedin:
            result = linkedin_activity(TemporalActivityInput(run_id='run-linkedin-resume', root=str(self.root)))

        self.assertEqual(result.status, 'queued')
        self.assertEqual(result.outcome, 'success')
        self.assertIn('Chrome will not reopen', result.note)
        run_linkedin.assert_not_called()

    def test_linkedin_activity_fresh_mode_does_not_skip_browser(self) -> None:
        from pipeline.temporal_activities import linkedin_activity
        from pipeline.temporal_types import TemporalActivityInput

        record = self.store.create_run(run_id='run-linkedin-fresh')
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

        with patch('pipeline.temporal_activities.PipelineStageManager.run_linkedin', return_value={'id': 'run-linkedin-fresh', 'status': 'queued', 'note': 'fresh linkedin', 'last_error': ''}) as run_linkedin:
            result = linkedin_activity(TemporalActivityInput(run_id='run-linkedin-fresh', root=str(self.root), fresh=True))

        self.assertEqual(result.status, 'queued')
        self.assertEqual(result.outcome, 'success')
        run_linkedin.assert_called_once()

    def test_rocketreach_activity_skips_rerun_when_recruiter_csv_already_exists(self) -> None:
        from pipeline.temporal_activities import rocketreach_activity
        from pipeline.temporal_types import TemporalActivityInput

        record = self.store.create_run(run_id='run-rocket-resume')
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
                'HR Email': 'jane@example.com',
                'HR Secondary Email': '',
                'HR Contact': '',
                'RocketReach Status': 'matched',
            }],
        )

        with patch('pipeline.temporal_activities.run_rocketreach_stage') as run_rocketreach:
            result = rocketreach_activity(TemporalActivityInput(run_id='run-rocket-resume', root=str(self.root)))

        self.assertEqual(result.status, 'queued')
        self.assertEqual(result.outcome, 'success')
        self.assertEqual(result.sendable_rows, 1)
        self.assertIn('Resuming email/review stage', result.note)
        run_rocketreach.assert_not_called()

    def test_rocketreach_activity_raises_for_retryable_provider_failures(self) -> None:
        from pipeline.temporal_activities import rocketreach_activity
        from pipeline.temporal_types import TemporalActivityInput

        record = self.store.create_run(run_id='run-rocket-transient')
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

        with patch('pipeline.temporal_activities.run_rocketreach_stage', side_effect=TransientStageError('503 service unavailable')), \
             patch('pipeline.temporal_activities.logger') as logger_mock, \
             patch('pipeline.temporal_activities.log_and_capture_error', side_effect=lambda error, **kwargs: (_ for _ in ()).throw(error)) as capture_mock:
            with self.assertRaises(TransientStageError):
                rocketreach_activity(TemporalActivityInput(run_id='run-rocket-transient', root=str(self.root)))

        logger_mock.warning.assert_called()
        logger_mock.exception.assert_not_called()
        capture_mock.assert_called_once()

    def test_rocketreach_activity_settles_to_waiting_review_for_deterministic_provider_block(self) -> None:
        from pipeline.temporal_activities import rocketreach_activity
        from pipeline.temporal_types import TemporalActivityInput

        record = self.store.create_run(run_id='run-rocket-waiting-review')
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

        with patch(
            'pipeline.temporal_activities.run_rocketreach_stage',
            return_value={
                'sendable_rows': 0,
                'total': 1,
                'matched': 0,
                'preview_match': 0,
                'missing_hr_link': 0,
                'invalid_hr_link': 0,
                'profile_only': 0,
                'no_match': 0,
                'lookup_quota_reached': 0,
                'authentication_failed': 1,
                'provider_success_count': 0,
                'no_email_count': 1,
                'provider_retry_count': 0,
                'provider_configuration_blocked': 0,
                'final_status': 'waiting_review',
                'final_reason': 'Recruiter lookup needs valid RocketReach/provider credentials before recruiter emails can be enriched.',
            },
        ):
            result = rocketreach_activity(TemporalActivityInput(run_id='run-rocket-waiting-review', root=str(self.root)))

        self.assertEqual(result.status, 'waiting_review')
        self.assertEqual(result.outcome, 'success')
        self.assertEqual(result.sendable_rows, 0)

    def test_rocketreach_activity_settles_to_waiting_review_for_quota_without_fallback(self) -> None:
        from pipeline.temporal_activities import rocketreach_activity
        from pipeline.temporal_types import TemporalActivityInput

        record = self.store.create_run(run_id='run-rocket-quota-waiting-review')
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

        with patch(
            'pipeline.temporal_activities.run_rocketreach_stage',
            return_value={
                'sendable_rows': 0,
                'total': 1,
                'matched': 0,
                'preview_match': 0,
                'missing_hr_link': 0,
                'invalid_hr_link': 0,
                'profile_only': 0,
                'no_match': 0,
                'lookup_quota_reached': 1,
                'authentication_failed': 0,
                'provider_success_count': 0,
                'no_email_count': 1,
                'provider_retry_count': 0,
                'provider_configuration_blocked': 1,
                'final_status': 'waiting_review',
                'final_reason': 'RocketReach quota/credits were exhausted and Hunter/Apollo fallback credentials are missing. Add RocketReach credits or configure HUNTER_API_KEY / APOLLO_API_KEY, then retry.',
            },
        ):
            result = rocketreach_activity(TemporalActivityInput(run_id='run-rocket-quota-waiting-review', root=str(self.root)))

        self.assertEqual(result.status, 'waiting_review')
        self.assertEqual(result.outcome, 'success')
        self.assertIn('Hunter/Apollo fallback credentials are missing', result.note)

    def test_email_activity_raises_application_error_for_transient_send_errors(self) -> None:
        from pipeline.temporal_activities import email_activity
        from pipeline.temporal_types import TemporalActivityInput
        from pipeline.temporal_sdk import ApplicationError

        record = self.store.create_run(run_id='run-email-transient')
        self.store.update_run('run-email-transient', recruiters_csv_path=record['recruiters_csv_path'])

        config = SimpleNamespace(auto_send=True)
        email_result = {
            'email_total': 1,
            'email_sent': 0,
            'email_failed': 1,
            'transient_failure_count': 1,
            'permanent_failure_count': 0,
        }

        with patch('pipeline.temporal_activities.load_automation_config', return_value=config), \
             patch('pipeline.temporal_activities.send_run_emails', return_value=email_result):
            with self.assertRaises(ApplicationError):
                email_activity(TemporalActivityInput(run_id='run-email-transient', root=str(self.root)))

    def test_email_activity_moves_to_waiting_review_for_auth_failures(self) -> None:
        from pipeline.temporal_activities import email_activity
        from pipeline.temporal_types import TemporalActivityInput

        record = self.store.create_run(run_id='run-email-auth-failure')
        self.store.update_run('run-email-auth-failure', recruiters_csv_path=record['recruiters_csv_path'])

        config = SimpleNamespace(auto_send=True, auto_send_reason=None)
        email_result = {
            'email_total': 1,
            'email_sent': 0,
            'email_failed': 1,
            'transient_failure_count': 0,
            'permanent_failure_count': 1,
            'auth_failure_count': 1,
        }

        with patch('pipeline.temporal_activities.load_automation_config', return_value=config), \
             patch('pipeline.temporal_activities.send_run_emails', return_value=email_result):
            with self.assertRaises(StageError):
                email_activity(TemporalActivityInput(run_id='run-email-auth-failure', root=str(self.root)))

        updated = self.store.get_run('run-email-auth-failure')
        self.assertEqual(updated['status'], 'waiting_review')
        self.assertIn('SMTP authentication failed', updated['note'])


class TemporalEntrypointTests(unittest.IsolatedAsyncioTestCase):
    async def test_start_temporal_workflow_creates_run_and_starts_client_workflow(self) -> None:
        from pipeline.start_workflow import start_temporal_workflow

        TMP_ROOT.mkdir(parents=True, exist_ok=True)
        root = TMP_ROOT / f'temporal-start-{uuid.uuid4().hex}'
        root.mkdir(parents=True, exist_ok=True)
        client = type('ClientStub', (), {'start_workflow': AsyncMock(return_value=None)})()

        try:
            with patch('pipeline.start_workflow.connect_temporal_client', AsyncMock(return_value=client)):
                result = await start_temporal_workflow(run_id='run-start', root=str(root))

            store = PipelineStore(root)
            record = store.get_run('run-start')
            self.assertEqual(result.workflow_id, 'pipeline-run-start')
            self.assertEqual(record['temporal_workflow_id'], 'pipeline-run-start')
            self.assertEqual(record['orchestration_backend'], 'temporal')
            client.start_workflow.assert_awaited_once()
        finally:
            shutil.rmtree(root, ignore_errors=True)

    async def test_start_temporal_workflow_restarts_same_run_when_previous_workflow_is_closed(self) -> None:
        from pipeline.start_workflow import start_temporal_workflow

        TMP_ROOT.mkdir(parents=True, exist_ok=True)
        root = TMP_ROOT / f'temporal-restart-{uuid.uuid4().hex}'
        root.mkdir(parents=True, exist_ok=True)
        client = type('ClientStub', (), {'start_workflow': AsyncMock(return_value=None)})()

        try:
            store = PipelineStore(root)
            record = store.create_run(run_id='run-restart')
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
            store.update_run(
                'run-restart',
                temporal_workflow_id='pipeline-run-restart',
                temporal_task_queue='automation-pipeline',
                orchestration_backend='temporal',
                status='queued',
            )

            with patch('pipeline.start_workflow.connect_temporal_client', AsyncMock(return_value=client)), \
                 patch('pipeline.start_workflow.get_temporal_workflow_status', AsyncMock(return_value='failed')):
                result = await start_temporal_workflow(run_id='run-restart', root=str(root))

            updated = store.get_run('run-restart')
            self.assertTrue(result.workflow_id.startswith('pipeline-run-restart-restart-'))
            self.assertEqual(updated['temporal_workflow_id'], result.workflow_id)
            self.assertIn('without reopening Chrome', updated['note'])
            client.start_workflow.assert_awaited_once()
        finally:
            shutil.rmtree(root, ignore_errors=True)

    async def test_start_temporal_workflow_passes_fresh_flag(self) -> None:
        from pipeline.start_workflow import start_temporal_workflow

        TMP_ROOT.mkdir(parents=True, exist_ok=True)
        root = TMP_ROOT / f'temporal-fresh-{uuid.uuid4().hex}'
        root.mkdir(parents=True, exist_ok=True)
        client = type('ClientStub', (), {'start_workflow': AsyncMock(return_value=None)})()

        try:
            with patch('pipeline.start_workflow.connect_temporal_client', AsyncMock(return_value=client)):
                await start_temporal_workflow(run_id='run-fresh-flag', root=str(root), fresh=True)

            workflow_input = client.start_workflow.await_args.args[1]
            self.assertTrue(workflow_input.fresh)
        finally:
            shutil.rmtree(root, ignore_errors=True)

    async def test_start_temporal_workflow_reuses_existing_active_run_when_no_run_id_is_given(self) -> None:
        from pipeline.start_workflow import start_temporal_workflow

        TMP_ROOT.mkdir(parents=True, exist_ok=True)
        root = TMP_ROOT / f'temporal-attach-{uuid.uuid4().hex}'
        root.mkdir(parents=True, exist_ok=True)
        client = type('ClientStub', (), {'start_workflow': AsyncMock(return_value=None)})()

        try:
            store = PipelineStore(root)
            record = store.create_run(run_id='run-active-shared')
            store.update_run(
                record['id'],
                status='linkedin_running',
                temporal_workflow_id='pipeline-run-active-shared',
                temporal_task_queue='automation-pipeline',
                orchestration_backend='temporal',
                note='Running LinkedIn job application stage.',
            )

            with patch('pipeline.start_workflow.connect_temporal_client', AsyncMock(return_value=client)), \
                 patch('pipeline.start_workflow.get_temporal_workflow_status', AsyncMock(return_value='running')):
                result = await start_temporal_workflow(root=str(root))

            self.assertEqual(result.run_id, 'run-active-shared')
            self.assertEqual(result.workflow_id, 'pipeline-run-active-shared')
            client.start_workflow.assert_not_awaited()
        finally:
            shutil.rmtree(root, ignore_errors=True)

    async def test_recovery_worker_reruns_retry_eligible_failed_workflow_once(self) -> None:
        from pipeline.recovery_worker import recover_failed_workflows

        TMP_ROOT.mkdir(parents=True, exist_ok=True)
        root = TMP_ROOT / f'temporal-recovery-{uuid.uuid4().hex}'
        root.mkdir(parents=True, exist_ok=True)

        try:
            store = PipelineStore(root)
            record = store.create_run(run_id='run-recovery')
            store.update_run(
                record['id'],
                status='failed',
                temporal_workflow_id='pipeline-run-recovery',
                note='RocketReach retryable failure after timeout.',
                last_error='provider timeout 503',
            )

            with patch('pipeline.recovery_worker.get_temporal_workflow_status', AsyncMock(return_value='failed')), \
                 patch('pipeline.recovery_worker.start_temporal_workflow', AsyncMock()) as start_workflow:
                rerun_count = await recover_failed_workflows(str(root))

            updated = store.get_run(record['id'])
            self.assertEqual(rerun_count, 1)
            self.assertEqual(updated['workflow_retry_count'], 1)
            self.assertEqual(updated['status'], 'queued')
            start_workflow.assert_awaited_once()
        finally:
            shutil.rmtree(root, ignore_errors=True)

    async def test_recovery_worker_skips_when_workflow_reruns_are_exhausted(self) -> None:
        from pipeline.recovery_worker import recover_failed_workflows

        TMP_ROOT.mkdir(parents=True, exist_ok=True)
        root = TMP_ROOT / f'temporal-recovery-max-{uuid.uuid4().hex}'
        root.mkdir(parents=True, exist_ok=True)

        try:
            store = PipelineStore(root)
            record = store.create_run(run_id='run-recovery-max')
            config_path = root / 'automation.env'
            config_path.write_text('PIPELINE_WORKFLOW_MAX_RERUNS=3\n', encoding='utf-8')
            store.update_run(
                record['id'],
                status='failed',
                workflow_retry_count=3,
                config_path=str(config_path),
                temporal_workflow_id='pipeline-run-recovery-max',
                note='RocketReach retryable failure after timeout.',
                last_error='provider timeout 503',
            )

            with patch('pipeline.recovery_worker.get_temporal_workflow_status', AsyncMock(return_value='failed')), \
                 patch('pipeline.recovery_worker.start_temporal_workflow', AsyncMock()) as start_workflow:
                rerun_count = await recover_failed_workflows(str(root))

            self.assertEqual(rerun_count, 0)
            start_workflow.assert_not_awaited()
        finally:
            shutil.rmtree(root, ignore_errors=True)

    async def test_start_temporal_workflow_fresh_run_clears_old_shared_artifacts(self) -> None:
        from pipeline.start_workflow import start_temporal_workflow

        TMP_ROOT.mkdir(parents=True, exist_ok=True)
        root = TMP_ROOT / f'temporal-fresh-clean-{uuid.uuid4().hex}'
        root.mkdir(parents=True, exist_ok=True)
        client = type('ClientStub', (), {'start_workflow': AsyncMock(return_value=None)})()

        try:
            store = PipelineStore(root)
            record = store.create_run(run_id='run-fresh-clean')
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

            with patch('pipeline.start_workflow.connect_temporal_client', AsyncMock(return_value=client)):
                result = await start_temporal_workflow(run_id='run-fresh-clean', root=str(root), fresh=True)

            updated = store.get_run('run-fresh-clean')
            self.assertEqual(result.workflow_id, 'pipeline-run-fresh-clean')
            self.assertFalse(Path(updated['applied_csv_path']).exists())
            self.assertIn('Fresh run starting LinkedIn in Chrome', updated['note'])
        finally:
            shutil.rmtree(root, ignore_errors=True)

    async def test_start_temporal_workflow_fresh_restart_ignores_existing_artifacts(self) -> None:
        from pipeline.start_workflow import start_temporal_workflow

        TMP_ROOT.mkdir(parents=True, exist_ok=True)
        root = TMP_ROOT / f'temporal-fresh-restart-{uuid.uuid4().hex}'
        root.mkdir(parents=True, exist_ok=True)
        client = type('ClientStub', (), {'start_workflow': AsyncMock(return_value=None)})()

        try:
            store = PipelineStore(root)
            record = store.create_run(run_id='run-fresh-restart')
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
            store.update_run(
                'run-fresh-restart',
                temporal_workflow_id='pipeline-run-fresh-restart',
                temporal_task_queue='automation-pipeline',
                orchestration_backend='temporal',
                status='linkedin_running',
            )

            with patch('pipeline.start_workflow.connect_temporal_client', AsyncMock(return_value=client)), \
                 patch('pipeline.start_workflow.get_temporal_workflow_status', AsyncMock(return_value=None)):
                result = await start_temporal_workflow(run_id='run-fresh-restart', root=str(root), fresh=True)

            updated = store.get_run('run-fresh-restart')
            self.assertTrue(result.workflow_id.startswith('pipeline-run-fresh-restart-restart-'))
            self.assertIn('Fresh run starting LinkedIn in Chrome', updated['note'])
        finally:
            shutil.rmtree(root, ignore_errors=True)

    async def test_start_temporal_workflow_fresh_clears_multiple_conflicting_runs(self) -> None:
        from pipeline.start_workflow import start_temporal_workflow

        TMP_ROOT.mkdir(parents=True, exist_ok=True)
        root = TMP_ROOT / f'temporal-fresh-conflicts-{uuid.uuid4().hex}'
        root.mkdir(parents=True, exist_ok=True)
        client = type('ClientStub', (), {'start_workflow': AsyncMock(return_value=None), 'close': AsyncMock(return_value=None)})()

        try:
            store = PipelineStore(root)
            stale_one = store.create_run(run_id='run-stale-one')
            stale_two = store.create_run(run_id='run-stale-two', allow_active_conflict=True)
            store.update_run(
                stale_one['id'],
                status='linkedin_running',
                temporal_workflow_id='pipeline-run-stale-one',
                temporal_task_queue='automation-pipeline',
                orchestration_backend='temporal',
            )
            store.update_run(
                stale_two['id'],
                status='waiting_login',
                temporal_workflow_id='pipeline-run-stale-two',
                temporal_task_queue='automation-pipeline',
                orchestration_backend='temporal',
            )

            with patch('pipeline.start_workflow.connect_temporal_client', AsyncMock(return_value=client)), \
                 patch('pipeline.start_workflow.get_temporal_workflow_status', AsyncMock(side_effect=['running', 'completed'])), \
                 patch('pipeline.start_workflow.terminate_temporal_workflow', AsyncMock(return_value=True)):
                result = await start_temporal_workflow(root=str(root), fresh=True)

            terminated_one = store.get_run('run-stale-one')
            terminated_two = store.get_run('run-stale-two')
            self.assertEqual(terminated_one['status'], 'terminated')
            self.assertEqual(terminated_two['status'], 'terminated')
            self.assertTrue(result.run_id.startswith('run-'))
            self.assertNotIn(result.run_id, {'run-stale-one', 'run-stale-two'})
        finally:
            shutil.rmtree(root, ignore_errors=True)

    async def test_temporal_worker_registers_workflow_and_activities(self) -> None:
        from pipeline.temporal_worker import run_temporal_worker

        worker_run = AsyncMock(return_value=None)
        worker_instance = type('WorkerStub', (), {'run': worker_run})()

        with patch('pipeline.temporal_worker.connect_temporal_client', AsyncMock(return_value=object())), \
             patch('pipeline.temporal_worker.Worker', return_value=worker_instance) as worker_cls:
            await run_temporal_worker()

        worker_cls.assert_called_once()
        worker_run.assert_awaited_once()


class RunOnceTests(unittest.IsolatedAsyncioTestCase):
    async def test_run_once_fresh_mode_skips_attach_and_creates_new_run(self) -> None:
        from pipeline.run_once import run_once
        from pipeline.temporal_types import TemporalStartResult

        class FakeProcess:
            def __init__(self) -> None:
                self._poll = None

            def poll(self):
                return self._poll

            def wait(self, timeout=None):
                self._poll = 0
                return 0

            def kill(self):
                self._poll = 0

        worker_process = FakeProcess()
        completed_run = {
            'id': 'run-fresh',
            'status': 'completed',
            'note': 'Done.',
            'last_error': '',
            'manifest_path': 'meta/run-fresh.json',
            'log_dir': 'logs/run-fresh',
            'applied_csv_path': 'runs/applied_jobs.csv',
            'recruiters_csv_path': 'runs/recruiters.csv',
            'send_report_path': 'reports/run-fresh.csv',
        }

        with patch('pipeline.run_once.temporal_server_is_reachable', return_value=True), \
             patch('pipeline.run_once.load_automation_config', side_effect=AutomationConfigError('bad config')), \
             patch('pipeline.run_once._spawn_worker', return_value=worker_process), \
             patch('pipeline.run_once._wait_for_worker_start', return_value=None), \
             patch('pipeline.run_once.start_temporal_workflow', AsyncMock(return_value=TemporalStartResult(run_id='run-fresh', workflow_id='wf-fresh', task_queue='automation-pipeline'))) as start_workflow, \
             patch('pipeline.run_once.PipelineStore') as store_cls, \
             patch('pipeline.run_once._stop_process'):
            store_cls.return_value.get_active_live_run.return_value = None
            store_cls.return_value.get_run.return_value = completed_run
            exit_code = await run_once(config_path='pipeline/automation.env', fresh=True)

        self.assertEqual(exit_code, 0)
        start_workflow.assert_awaited_once()
        self.assertEqual(start_workflow.await_args.kwargs['run_id'], None)

    async def test_run_once_fresh_mode_supersedes_recent_running_fresh_run(self) -> None:
        from pipeline.run_once import run_once
        from pipeline.temporal_types import TemporalStartResult

        class FakeProcess:
            def __init__(self) -> None:
                self._poll = None

            def poll(self):
                return self._poll

            def wait(self, timeout=None):
                self._poll = 0
                return 0

            def kill(self):
                self._poll = 0

        worker_process = FakeProcess()
        active_run = {
            'id': 'run-existing-fresh',
            'status': 'linkedin_running',
            'note': 'Fresh run starting LinkedIn in Chrome.',
            'last_error': '',
            'temporal_workflow_id': 'pipeline-run-existing-fresh',
            'temporal_task_queue': 'automation-pipeline',
            'updated_at': '2026-04-16T10:00:00+00:00',
            'created_at': '2026-04-16T10:00:00+00:00',
            'manifest_path': 'meta/run-existing-fresh.json',
            'log_dir': 'logs/run-existing-fresh',
            'applied_csv_path': 'runs/applied_jobs.csv',
            'recruiters_csv_path': 'runs/recruiters.csv',
            'send_report_path': 'reports/run-existing-fresh.csv',
        }
        completed_run = dict(active_run, status='completed', note='Done.')

        with patch('pipeline.run_once.temporal_server_is_reachable', return_value=True), \
             patch('pipeline.run_once.load_automation_config', side_effect=AutomationConfigError('bad config')), \
             patch('pipeline.run_once._spawn_worker', return_value=worker_process), \
             patch('pipeline.run_once._wait_for_worker_start', return_value=None), \
             patch('pipeline.run_once.start_temporal_workflow', AsyncMock(return_value=TemporalStartResult(run_id='run-new-fresh', workflow_id='wf-new-fresh', task_queue='automation-pipeline'))) as start_workflow, \
             patch('pipeline.run_once.PipelineStore') as store_cls, \
             patch('pipeline.run_once._stop_process') as stop_process:
            store_cls.return_value.get_active_live_run.return_value = active_run
            store_cls.return_value.get_run.return_value = completed_run
            exit_code = await run_once(config_path='pipeline/automation.env', fresh=True)

        self.assertEqual(exit_code, 0)
        start_workflow.assert_awaited_once()
        self.assertTrue(start_workflow.await_args.kwargs['fresh'])
        self.assertIsNone(start_workflow.await_args.kwargs['run_id'])
        stop_process.assert_called_once_with(worker_process)

    async def test_run_once_attaches_to_existing_active_temporal_run(self) -> None:
        from pipeline.run_once import run_once

        class FakeProcess:
            def __init__(self) -> None:
                self._poll = None

            def poll(self):
                return self._poll

            def wait(self, timeout=None):
                self._poll = 0
                return 0

            def kill(self):
                self._poll = 0

        worker_process = FakeProcess()
        active_run = {
            'id': 'run-active',
            'status': 'linkedin_running',
            'note': 'Running LinkedIn job application stage.',
            'last_error': '',
            'temporal_workflow_id': 'pipeline-run-active',
            'temporal_task_queue': 'automation-pipeline',
            'manifest_path': 'meta/run-active.json',
            'log_dir': 'logs/run-active',
            'applied_csv_path': 'runs/applied_jobs.csv',
            'recruiters_csv_path': 'runs/recruiters.csv',
            'send_report_path': 'reports/run-active.csv',
        }
        completed_run = dict(active_run, status='completed', note='Done.')

        with patch('pipeline.run_once.temporal_server_is_reachable', return_value=True), \
             patch('pipeline.run_once.load_automation_config', side_effect=AutomationConfigError('bad config')), \
             patch('pipeline.run_once._spawn_worker', return_value=worker_process), \
             patch('pipeline.run_once._wait_for_worker_start', return_value=None), \
             patch('pipeline.run_once.get_temporal_workflow_status', AsyncMock(return_value='running')), \
             patch('pipeline.run_once.start_temporal_workflow', AsyncMock()) as start_workflow, \
             patch('pipeline.run_once.PipelineStore') as store_cls, \
             patch('pipeline.run_once._stop_process'):
            store_cls.return_value.get_active_live_run.return_value = active_run
            store_cls.return_value.get_run.side_effect = [completed_run]
            exit_code = await run_once(config_path='pipeline/automation.env', fresh=False)

        self.assertEqual(exit_code, 0)
        start_workflow.assert_not_awaited()

    async def test_run_once_restarts_same_run_when_stored_workflow_is_failed(self) -> None:
        from pipeline.run_once import run_once
        from pipeline.temporal_types import TemporalStartResult

        class FakeProcess:
            def __init__(self) -> None:
                self._poll = None

            def poll(self):
                return self._poll

            def wait(self, timeout=None):
                self._poll = 0
                return 0

            def kill(self):
                self._poll = 0

        worker_process = FakeProcess()
        active_run = {
            'id': 'run-stale',
            'status': 'queued',
            'note': 'LinkedIn stage completed.',
            'last_error': '',
            'temporal_workflow_id': 'pipeline-run-stale',
            'temporal_task_queue': 'automation-pipeline',
            'manifest_path': 'meta/run-stale.json',
            'log_dir': 'logs/run-stale',
            'applied_csv_path': 'runs/applied_jobs.csv',
            'recruiters_csv_path': 'runs/recruiters.csv',
            'send_report_path': 'reports/run-stale.csv',
        }
        completed_run = dict(active_run, status='completed', note='Done.')

        with patch('pipeline.run_once.temporal_server_is_reachable', return_value=True), \
             patch('pipeline.run_once.load_automation_config', side_effect=AutomationConfigError('bad config')), \
             patch('pipeline.run_once._spawn_worker', return_value=worker_process), \
             patch('pipeline.run_once._wait_for_worker_start', return_value=None), \
             patch('pipeline.run_once.get_temporal_workflow_status', AsyncMock(return_value='failed')), \
             patch('pipeline.run_once.start_temporal_workflow', AsyncMock(return_value=TemporalStartResult(run_id='run-stale', workflow_id='pipeline-run-stale-restart-1', task_queue='automation-pipeline'))) as start_workflow, \
             patch('pipeline.run_once.PipelineStore') as store_cls, \
             patch('pipeline.run_once._stop_process'):
            store_cls.return_value.get_active_live_run.return_value = active_run
            store_cls.return_value.get_run.side_effect = [completed_run]
            exit_code = await run_once(config_path='pipeline/automation.env', fresh=False)

        self.assertEqual(exit_code, 0)
        start_workflow.assert_awaited_once()

    async def test_run_once_force_restarts_recovered_run_even_when_workflow_looks_running(self) -> None:
        from pipeline.run_once import run_once
        from pipeline.temporal_types import TemporalStartResult

        class FakeProcess:
            def __init__(self) -> None:
                self._poll = None

            def poll(self):
                return self._poll

            def wait(self, timeout=None):
                self._poll = 0
                return 0

            def kill(self):
                self._poll = 0

        worker_process = FakeProcess()
        active_run = {
            'id': 'run-recovered',
            'status': 'queued',
            'note': 'Recovered interrupted rocketreach_running stage after restart.',
            'last_error': '',
            'temporal_workflow_id': 'pipeline-run-recovered',
            'temporal_task_queue': 'automation-pipeline',
            'manifest_path': 'meta/run-recovered.json',
            'log_dir': 'logs/run-recovered',
            'applied_csv_path': 'runs/applied_jobs.csv',
            'recruiters_csv_path': 'runs/recruiters.csv',
            'send_report_path': 'reports/run-recovered.csv',
        }
        completed_run = dict(active_run, status='completed', note='Done.')

        with patch('pipeline.run_once.temporal_server_is_reachable', return_value=True), \
             patch('pipeline.run_once.load_automation_config', side_effect=AutomationConfigError('bad config')), \
             patch('pipeline.run_once._spawn_worker', return_value=worker_process), \
             patch('pipeline.run_once._wait_for_worker_start', return_value=None), \
             patch('pipeline.run_once.get_temporal_workflow_status', AsyncMock(return_value='running')), \
             patch('pipeline.run_once.start_temporal_workflow', AsyncMock(return_value=TemporalStartResult(run_id='run-recovered', workflow_id='pipeline-run-recovered-restart-1', task_queue='automation-pipeline'))) as start_workflow, \
             patch('pipeline.run_once.PipelineStore') as store_cls, \
             patch('pipeline.run_once._stop_process'):
            store_cls.return_value.get_active_live_run.return_value = active_run
            store_cls.return_value.get_run.side_effect = [completed_run]
            exit_code = await run_once(config_path='pipeline/automation.env', fresh=False)

        self.assertEqual(exit_code, 0)
        start_workflow.assert_awaited_once()
        self.assertTrue(start_workflow.await_args.kwargs['force_restart'])

    async def test_run_once_defaults_to_attach_mode_and_prints_temporal_ui(self) -> None:
        from pipeline.run_once import run_once
        from pipeline.temporal_types import TemporalStartResult

        status_records = [
            {
                'id': 'run-once',
                'status': 'linkedin_running',
                'note': 'Running LinkedIn job application stage.',
                'last_error': '',
                'manifest_path': 'meta/run-once.json',
                'log_dir': 'logs/run-once',
                'applied_csv_path': 'runs/applied_jobs.csv',
                'recruiters_csv_path': 'runs/recruiters.csv',
                'send_report_path': 'reports/run-once.csv',
            },
            {
                'id': 'run-once',
                'status': 'completed',
                'note': 'Automated email stage completed successfully.',
                'last_error': '',
                'manifest_path': 'meta/run-once.json',
                'log_dir': 'logs/run-once',
                'applied_csv_path': 'runs/applied_jobs.csv',
                'recruiters_csv_path': 'runs/recruiters.csv',
                'send_report_path': 'reports/run-once.csv',
            },
        ]

        class FakeProcess:
            def __init__(self) -> None:
                self._poll = None

            def poll(self):
                return self._poll

            def wait(self, timeout=None):
                self._poll = 0
                return 0

            def kill(self):
                self._poll = 0

        worker_process = FakeProcess()

        with patch('pipeline.run_once.temporal_server_is_reachable', return_value=True), \
             patch('pipeline.run_once._spawn_worker', return_value=worker_process), \
             patch('pipeline.run_once._wait_for_worker_start', return_value=None), \
             patch('pipeline.run_once.start_temporal_workflow', AsyncMock(return_value=TemporalStartResult(run_id='run-once', workflow_id='wf-1', task_queue='automation-pipeline'))), \
             patch('pipeline.run_once.PipelineStore') as store_cls, \
             patch('pipeline.run_once._stop_process') as stop_process, \
             patch('pipeline.run_once.load_automation_config', side_effect=AutomationConfigError('bad config')), \
             patch('builtins.print') as print_mock:
            store_cls.return_value.get_active_live_run.return_value = None
            store_cls.return_value.get_run.side_effect = status_records
            exit_code = await run_once(config_path='pipeline/automation.env')

        self.assertEqual(exit_code, 0)
        stop_process.assert_called_once_with(worker_process)
        printed_lines = [' '.join(str(arg) for arg in call.args) for call in print_mock.call_args_list]
        self.assertTrue(any('Temporal UI: http://localhost:8233' in line for line in printed_lines))
        self.assertTrue(any('run_id=run-once' in line for line in printed_lines))
        self.assertTrue(any('temporal_ui=http://localhost:8233' in line for line in printed_lines))
        self.assertTrue(any('final_status=completed' in line for line in printed_lines))

    async def test_run_once_honors_config_flag_to_force_linkedin_restart(self) -> None:
        from pipeline.run_once import run_once
        from pipeline.temporal_types import TemporalStartResult

        completed_run = {
            'id': 'run-forced-fresh',
            'status': 'completed',
            'note': 'Done.',
            'last_error': '',
            'manifest_path': 'meta/run-forced-fresh.json',
            'log_dir': 'logs/run-forced-fresh',
            'applied_csv_path': 'runs/applied_jobs.csv',
            'recruiters_csv_path': 'runs/recruiters.csv',
            'send_report_path': 'reports/run-forced-fresh.csv',
        }

        class FakeProcess:
            def __init__(self) -> None:
                self._poll = None

            def poll(self):
                return self._poll

            def wait(self, timeout=None):
                self._poll = 0
                return 0

            def kill(self):
                self._poll = 0

        worker_process = FakeProcess()
        config = SimpleNamespace(temporal_auto_start=True, run_once_always_fresh=True)

        with patch('pipeline.run_once.temporal_server_is_reachable', return_value=True), \
             patch('pipeline.run_once.load_automation_config', return_value=config), \
             patch('pipeline.run_once._spawn_worker', return_value=worker_process), \
             patch('pipeline.run_once._wait_for_worker_start', return_value=None), \
             patch('pipeline.run_once.start_temporal_workflow', AsyncMock(return_value=TemporalStartResult(run_id='run-forced-fresh', workflow_id='wf-forced-fresh', task_queue='automation-pipeline'))) as start_workflow, \
             patch('pipeline.run_once.PipelineStore') as store_cls, \
             patch('pipeline.run_once._stop_process'):
            store_cls.return_value.get_active_live_run.return_value = None
            store_cls.return_value.get_run.return_value = completed_run
            exit_code = await run_once(config_path='pipeline/automation.env', fresh=False)

        self.assertEqual(exit_code, 0)
        self.assertTrue(start_workflow.await_args.kwargs['fresh'])

    async def test_run_once_default_attach_mode_reuses_stale_active_run_id_for_restart(self) -> None:
        from pipeline.run_once import run_once
        from pipeline.temporal_types import TemporalStartResult

        class FakeProcess:
            def __init__(self) -> None:
                self._poll = None

            def poll(self):
                return self._poll

            def wait(self, timeout=None):
                self._poll = 0
                return 0

            def kill(self):
                self._poll = 0

        worker_process = FakeProcess()
        stale_run = {
            'id': 'run-stale-fresh',
            'status': 'queued',
            'note': 'Run enqueued.',
            'last_error': '',
            'temporal_workflow_id': 'pipeline-run-stale-fresh',
            'temporal_task_queue': 'automation-pipeline',
            'manifest_path': 'meta/run-stale-fresh.json',
            'log_dir': 'logs/run-stale-fresh',
            'applied_csv_path': 'runs/applied_jobs.csv',
            'recruiters_csv_path': 'runs/recruiters.csv',
            'send_report_path': 'reports/run-stale-fresh.csv',
        }
        completed_run = {
            'id': 'run-brand-new',
            'status': 'completed',
            'note': 'Done.',
            'last_error': '',
            'manifest_path': 'meta/run-brand-new.json',
            'log_dir': 'logs/run-brand-new',
            'applied_csv_path': 'runs/applied_jobs.csv',
            'recruiters_csv_path': 'runs/recruiters.csv',
            'send_report_path': 'reports/run-brand-new.csv',
        }

        with patch('pipeline.run_once.temporal_server_is_reachable', return_value=True), \
             patch('pipeline.run_once.load_automation_config', side_effect=AutomationConfigError('bad config')), \
             patch('pipeline.run_once._spawn_worker', return_value=worker_process), \
             patch('pipeline.run_once._wait_for_worker_start', return_value=None), \
             patch('pipeline.run_once.get_temporal_workflow_status', AsyncMock(return_value='failed')), \
             patch('pipeline.run_once.start_temporal_workflow', AsyncMock(return_value=TemporalStartResult(run_id='run-brand-new', workflow_id='wf-new', task_queue='automation-pipeline'))) as start_workflow, \
             patch('pipeline.run_once.PipelineStore') as store_cls, \
             patch('pipeline.run_once._stop_process'):
            store_cls.return_value.get_active_live_run.return_value = stale_run
            store_cls.return_value.get_run.return_value = completed_run
            exit_code = await run_once(config_path='pipeline/automation.env')

        self.assertEqual(exit_code, 0)
        self.assertEqual(start_workflow.await_args.kwargs['run_id'], 'run-stale-fresh')
        self.assertFalse(start_workflow.await_args.kwargs.get('fresh', False))

    async def test_run_once_autostarts_server_when_not_running(self) -> None:
        from pipeline.run_once import run_once
        from pipeline.temporal_types import TemporalStartResult

        class FakeProcess:
            def __init__(self) -> None:
                self._poll = None

            def poll(self):
                return self._poll

            def wait(self, timeout=None):
                self._poll = 0
                return 0

            def kill(self):
                self._poll = 0

        server_process = FakeProcess()
        worker_process = FakeProcess()
        config = SimpleNamespace(temporal_auto_start=True)
        completed_record = {
            'id': 'run-auto',
            'status': 'completed',
            'note': 'Done.',
            'last_error': '',
            'manifest_path': 'meta/run-auto.json',
            'log_dir': 'logs/run-auto',
            'applied_csv_path': 'runs/applied_jobs.csv',
            'recruiters_csv_path': 'runs/recruiters.csv',
            'send_report_path': 'reports/run-auto.csv',
        }

        with patch('pipeline.run_once.temporal_server_is_reachable', side_effect=[False]), \
             patch('pipeline.run_once.load_automation_config', return_value=config), \
             patch('pipeline.run_once._spawn_temporal_server', return_value=server_process), \
             patch('pipeline.run_once._wait_for_temporal_server', return_value=None), \
             patch('pipeline.run_once._spawn_worker', return_value=worker_process), \
             patch('pipeline.run_once._wait_for_worker_start', return_value=None), \
             patch('pipeline.run_once.start_temporal_workflow', AsyncMock(return_value=TemporalStartResult(run_id='run-auto', workflow_id='wf-auto', task_queue='automation-pipeline'))), \
             patch('pipeline.run_once.PipelineStore') as store_cls, \
             patch('pipeline.run_once._stop_process') as stop_process, \
             patch('pipeline.run_once._close_process_logs') as close_process_logs:
            store_cls.return_value.get_active_live_run.return_value = None
            store_cls.return_value.get_run.return_value = completed_record
            exit_code = await run_once(config_path='pipeline/automation.env')

        self.assertEqual(exit_code, 0)
        stop_process.assert_called_once_with(worker_process)
        close_process_logs.assert_called_once_with(server_process)


if __name__ == '__main__':
    unittest.main()

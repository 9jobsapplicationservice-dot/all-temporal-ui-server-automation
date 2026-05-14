# Temporal orchestration

Install the Temporal Python SDK before using the Temporal worker or workflow starter:

```bash
pip install temporalio sentry-sdk python-dotenv
```

Recommended local command:

```bash
python -m pipeline.run_once --config pipeline/automation.env
```

Optional environment variables:

```bash
TEMPORAL_ADDRESS=localhost:7233
TEMPORAL_NAMESPACE=default
TEMPORAL_TASK_QUEUE=automation-pipeline
PIPELINE_LINKEDIN_STAGE_TIMEOUT_SECONDS=1800
PIPELINE_LINKEDIN_IDLE_TIMEOUT_SECONDS=300
PIPELINE_TEMPORAL_AUTO_START=true
SENTRY_DSN=https://your-key@o0.ingest.sentry.io/0
SENTRY_ENVIRONMENT=local
SENTRY_RELEASE=automation-pipeline-dev
SENTRY_TRACES_SAMPLE_RATE=0.0
```

Sentry notes:

- Put the `SENTRY_*` variables in `pipeline/automation.env` for local runs.
- Start the worker normally with `python -m pipeline.temporal_worker`; it will initialize Sentry during startup.
- Workflow and activity failures are tagged with Temporal metadata such as `workflow_id`, `activity_name`, and `task_queue`.

Advanced/manual mode is still available if you want to manage the processes yourself:

```bash
temporal server start-dev --db-filename temporal.db
python -m pipeline.temporal_worker
python -m pipeline.start_workflow --config pipeline/automation.env
```

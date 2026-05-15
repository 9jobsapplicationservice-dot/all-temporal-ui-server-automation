import 'server-only';

import fs from 'node:fs/promises';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import { spawn, spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import Papa from 'papaparse';

import type {
  AppliedJobPreviewRow,
  ContactRow,
  DeliveryLogPreviewRow,
  EmailLog,
  EditableLinkedInConfig,
  EditableLinkedInConfigUpdates,
  PipelineArtifactSummary,
  PipelineFailureSummary,
  PipelineRunSummary,
  RecruiterEnrichmentSummary,
  RecruiterPreviewRow,
  WorkflowAutomationSummary,
  WorkflowCounts,
  WorkflowDashboardPayload,
  WorkflowConfigPayload,
  WorkflowRunSummary,
  WorkflowStageId,
  WorkflowStageState,
} from '@/types';


type PipelineManifest = {
  run_id: string;
  status: string;
  created_at?: string;
  updated_at: string;
  note?: string;
  last_error?: string;
  email_stats?: {
    total?: number;
    sent?: number;
    failed?: number;
  };
  automation?: WorkflowAutomationSummary;
  paths: {
    run_dir: string;
    applied_csv?: string;
    recruiters_csv: string;
    send_report_csv: string;
    manifest_json?: string;
    logs_dir?: string;
    linkedin_stdout_log?: string;
    linkedin_stderr_log?: string;
    rocketreach_stdout_log?: string;
    rocketreach_stderr_log?: string;
  };
  live_status?: {
    current_url?: string;
    page_title?: string;
    login_required?: boolean;
    checkpoint_required?: boolean;
    job_cards_count?: string | number;
    job_details_count?: string | number;
    easy_apply_count?: string | number;
    last_screenshot?: string;
    latestLog?: string;
    latestError?: string;
  };
  artifacts?: {
    applied_csv_exists?: boolean;
    recruiters_csv_exists?: boolean;
    send_report_exists?: boolean;
  };
};

const LIB_ROOT = path.dirname(fileURLToPath(import.meta.url));
const APP_ROOT = path.resolve(LIB_ROOT, '..', '..');
const DEFAULT_WORKSPACE_ROOT = path.resolve(APP_ROOT, '..', '..');
const WORKSPACE_ROOT = process.env.PIPELINE_WORKSPACE_ROOT?.trim()
  ? path.resolve(process.env.PIPELINE_WORKSPACE_ROOT)
  : DEFAULT_WORKSPACE_ROOT;
const PIPELINE_ROOT = process.env.PIPELINE_ROOT?.trim()
  ? path.resolve(process.env.PIPELINE_ROOT)
  : path.join(WORKSPACE_ROOT, 'pipeline');
const RUNS_ROOT = path.join(PIPELINE_ROOT, 'runs');
const META_ROOT = path.join(PIPELINE_ROOT, 'meta');
const LOGS_ROOT = path.join(PIPELINE_ROOT, 'logs');
const REPORTS_ROOT = path.join(PIPELINE_ROOT, 'reports');
const ROCKETREACH_ROOT = path.join(WORKSPACE_ROOT, 'rocket_reach - testing');
const LINKEDIN_LEGACY_CONFIG_PATH = path.join(WORKSPACE_ROOT, 'linkdin_automation', 'modules', '__deprecated__', '__setup__', 'config.py');
const PIPELINE_ENV_PATH = path.join(PIPELINE_ROOT, 'automation.env');
const PYTHON_BIN = process.env.PIPELINE_PYTHON?.trim() || 'python';
const IS_VERCEL_RUNTIME = Boolean(process.env.VERCEL);

type LatestPipelinePayload = {
  run: PipelineRunSummary | null;
  contacts: ContactRow[];
  latestFailure: PipelineFailureSummary | null;
};

type ManualEnrichmentResult = {
  runDir: string;
  recruitersCsvPath: string;
  contacts: ContactRow[];
  stats: RecruiterEnrichmentSummary;
};

function parseCsvText(csvText: string): Record<string, string>[] {
  const result = Papa.parse<Record<string, string>>(csvText, {
    header: true,
    skipEmptyLines: true,
  });

  if (result.errors.length > 0) {
    throw new Error(result.errors[0]?.message ?? 'Failed to parse pipeline CSV.');
  }

  return result.data;
}

function cleanString(value: unknown): string {
  return typeof value === 'string' ? value.trim() : '';
}

function summarizeLastError(value: unknown): string {
  const raw = cleanString(value);
  if (!raw) {
    return '';
  }

  // Common user-friendly mappings
  const lowered = raw.toLowerCase();
  if (lowered.includes('linkedin login was not confirmed') || lowered.includes('session was blocked') || lowered.includes('checkpoint')) {
    return 'LinkedIn login session missing or verification required.';
  }
  if (lowered.includes('chrome startup needs manual recovery') || lowered.includes('chrome default profile crashed')) {
    return 'Chrome failed to initialize properly. Profile may be locked or corrupted.';
  }
  if (lowered.includes('browser window closed') || lowered.includes('session became invalid')) {
    return 'Automation stopped because the browser was closed or the session timed out.';
  }
  if (lowered.includes('no easy apply jobs found for this search')) {
    return 'No Easy Apply jobs found for the specified search terms.';
  }
  if (lowered.includes('no linkedin job cards found') || lowered.includes('job details not found')) {
    return 'LinkedIn search returned no results or failed to load job listings.';
  }
  if (lowered.includes('no confirmed easy apply submissions')) {
    return 'No jobs were successfully applied to (LinkedIn Easy Apply failed or no matches found).';
  }

  const lines = raw
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .filter((line) => !line.startsWith('Traceback '))
    .filter((line) => !line.startsWith('File "'))
    .filter((line) => !line.includes('FutureWarning'))
    .filter((line) => !line.includes('google.generativeai'))
    .filter((line) => !line.includes('deprecated-generative-ai-python'))
    .filter((line) => !line.includes('Exception ignored in:'))
    .filter((line) => !line.includes('Chrome.__del__'))
    .filter((line) => !line.includes('undetected_chromedriver'))
    .filter((line) => !line.includes('[WinError 6]'));

  const useful = lines.find((line) => line.startsWith('Command failed with exit code'))
    ?? lines.find((line) => line.toLowerCase().includes('startup failure:'))
    ?? lines.find((line) => line.toLowerCase().includes('error'))
    ?? lines[0]
    ?? raw.split(/\r?\n/)[0]?.trim()
    ?? '';

  return useful.replace(/\s+/g, ' ').trim();
}

function firstUsableEmail(...candidates: unknown[]): string {
  for (const candidate of candidates) {
    const value = cleanString(candidate);
    if (value.includes('@')) {
      return value;
    }
  }
  return '';
}

async function safeReadManifest(manifestPath: string): Promise<PipelineManifest | null> {
  try {
    const raw = await fs.readFile(manifestPath, 'utf8');
    return JSON.parse(raw) as PipelineManifest;
  } catch {
    return null;
  }
}

function mapContacts(runId: string, rows: Record<string, string>[]) {
  const contacts: ContactRow[] = [];
  const seenEmails = new Set<string>();

  for (const row of rows) {
    const email = firstUsableEmail(row['HR Email'], row['HR Secondary Email']);
    if (!email) {
      continue;
    }

    const normalizedEmail = email.toLowerCase();
    if (seenEmails.has(normalizedEmail)) {
      continue;
    }
    seenEmails.add(normalizedEmail);

    const secondaryEmail = cleanString(row['HR Secondary Email']);
    contacts.push({
      id: `${runId}-${contacts.length}`,
      runId,
      email,
      secondaryEmail: secondaryEmail && secondaryEmail.toLowerCase() !== normalizedEmail ? secondaryEmail : undefined,
      name: cleanString(row['HR Name']) || 'Hiring Team',
      company: cleanString(row['Company Name']),
      position: cleanString(row['Position']) || cleanString(row['HR Position']),
      jobLink: cleanString(row['Job Link']),
      sourceStatus: cleanString(row['RocketReach Status']),
    });
  }

  return contacts;
}

function summarizeRun(manifest: PipelineManifest, rows: Record<string, string>[], contacts: ContactRow[]): PipelineRunSummary {
  const sendableRows = rows.filter((row) => Boolean(firstUsableEmail(row['HR Email'], row['HR Secondary Email']))).length;
  const skippedRows = Math.max(rows.length - sendableRows, 0);
  const blockedReason = manifest.status === 'blocked_runtime'
    ? cleanString(manifest.last_error) || cleanString(manifest.note)
    : '';
  const noSendableReason = manifest.status === 'blocked_runtime'
    ? blockedReason
    : sendableRows > 0
      ? ''
      : cleanString(manifest.note) || 'RocketReach completed, but no recruiter rows contained a usable email address.';

  return {
    runId: manifest.run_id,
    status: manifest.status,
    updatedAt: manifest.updated_at,
    runDir: manifest.paths.run_dir,
    recruitersCsvPath: manifest.paths.recruiters_csv,
    totalRows: rows.length,
    sendableRows,
    dedupedRows: contacts.length,
    skippedRows,
    readyToSend: manifest.status !== 'blocked_runtime' && contacts.length > 0,
    setupRequired: manifest.status === 'blocked_runtime',
    blockedReason,
    noSendableReason,
    note: manifest.note || '',
  };
}

function summarizeFailure(manifest: PipelineManifest): PipelineFailureSummary {
  return {
    runId: manifest.run_id,
    status: manifest.status,
    updatedAt: manifest.updated_at,
    note: cleanString(manifest.note),
    lastError: summarizeLastError(manifest.last_error),
  };
}

function isActionableFailure(manifest: PipelineManifest): boolean {
  if (manifest.status !== 'failed') {
    return false;
  }

  const note = cleanString(manifest.note);
  const lastError = cleanString(manifest.last_error);
  if (
    lastError.includes('LinkedIn login was not confirmed')
    || lastError.includes('Browser window closed or session became invalid.')
  ) {
    return false;
  }
  if (
    note === 'LinkedIn stage completed with zero saved applied rows.'
    && lastError === 'No confirmed Easy Apply submissions were written to applied_jobs.csv.'
  ) {
    return false;
  }

  return true;
}

async function readRecruiterCsv(csvPath: string, runId: string): Promise<{ rows: Record<string, string>[]; contacts: ContactRow[] }> {
  const csvText = await fs.readFile(csvPath, 'utf8');
  const rows = parseCsvText(csvText);
  return {
    rows,
    contacts: mapContacts(runId, rows),
  };
}

async function listManifestPaths(): Promise<string[]> {
  try {
    const entries = await fs.readdir(META_ROOT, { withFileTypes: true });
    return entries
      .filter((entry) => entry.isFile() && entry.name.endsWith('.json'))
      .map((entry) => path.join(META_ROOT, entry.name));
  } catch {
    return [];
  }
}

function parseStatsPayload(stdout: string): RecruiterEnrichmentSummary {
  const trimmed = stdout.trim();
  if (!trimmed) {
    throw new Error('RocketReach enrichment did not report summary stats.');
  }

  const start = trimmed.lastIndexOf('{');
  if (start < 0) {
    throw new Error('RocketReach enrichment returned unreadable stats output.');
  }

  return JSON.parse(trimmed.slice(start)) as RecruiterEnrichmentSummary;
}

function buildFailureMessage(prefix: string, stderr: string, stdout: string): string {
  const detail = stderr.trim() || stdout.trim();
  return detail ? `${prefix} ${detail}` : prefix;
}

function buildManualRunId(): string {
  return `run-manual-${randomUUID().replace(/-/g, '').slice(0, 12)}`;
}

function buildManualNote(stats: RecruiterEnrichmentSummary): string {
  return [
    'Manual RocketReach enrichment completed.',
    `total=${stats.total}`,
    `matched=${stats.matched}`,
    `sendable_rows=${stats.sendable_rows}`,
    `profile_only=${stats.profile_only}`,
    `no_match=${stats.no_match}`,
    `lookup_quota_reached=${stats.lookup_quota_reached}`,
  ].join(' ');
}

function csvEscape(value: string): string {
  if (value.includes(',') || value.includes('"') || value.includes('\n')) {
    return `"${value.replace(/"/g, '""')}"`;
  }
  return value;
}

async function fileExists(targetPath: string | undefined): Promise<boolean> {
  if (!targetPath) {
    return false;
  }

  try {
    await fs.access(targetPath);
    return true;
  } catch {
    return false;
  }
}

async function readCsvIfExists(csvPath: string | undefined): Promise<Record<string, string>[]> {
  if (!csvPath || !(await fileExists(csvPath))) {
    return [];
  }

  try {
    const csvText = await fs.readFile(csvPath, 'utf8');
    return parseCsvText(csvText);
  } catch {
    return [];
  }
}

function toInt(value: unknown): number {
  const num = Number(value);
  return Number.isFinite(num) ? num : 0;
}

function pickCurrentStage(status: string): WorkflowStageId {
  switch (status) {
    case 'queued':
    case 'blocked_runtime':
    case 'waiting_login':
    case 'linkedin_running':
      return 'linkedin';
    case 'rocketreach_running':
      return 'rocketreach';
    case 'email_running':
    case 'waiting_review':
    case 'sending':
    case 'completed':
    case 'failed':
    case 'manual_review':
      return 'email';
    default:
      return 'linkedin';
  }
}

function buildCounts(
  manifest: PipelineManifest,
  appliedRows: Record<string, string>[],
  recruiterRows: Record<string, string>[],
  contacts: ContactRow[],
): WorkflowCounts {
  const sendableRows = recruiterRows.filter((row) => Boolean(firstUsableEmail(row['HR Email'], row['HR Secondary Email']))).length;
  return {
    appliedRows: appliedRows.length,
    recruiterRows: recruiterRows.length,
    sendableRows,
    dedupedSendableRows: contacts.length,
    skippedRows: Math.max(recruiterRows.length - sendableRows, 0),
    emailTotal: toInt(manifest.email_stats?.total),
    emailSent: toInt(manifest.email_stats?.sent),
    emailFailed: toInt(manifest.email_stats?.failed),
  };
}

function buildArtifacts(manifest: PipelineManifest): PipelineArtifactSummary[] {
  return [
    {
      key: 'appliedCsv',
      label: 'Applied Jobs CSV',
      available: Boolean(manifest.artifacts?.applied_csv_exists),
    },
    {
      key: 'recruitersCsv',
      label: 'Recruiter CSV',
      available: Boolean(manifest.artifacts?.recruiters_csv_exists),
    },
    {
      key: 'sendReportCsv',
      label: 'Send Report CSV',
      available: Boolean(manifest.artifacts?.send_report_exists),
    },
    {
      key: 'linkedinStdout',
      label: 'LinkedIn Stdout',
      available: Boolean(cleanString(manifest.paths.linkedin_stdout_log)),
    },
    {
      key: 'linkedinStderr',
      label: 'LinkedIn Stderr',
      available: Boolean(cleanString(manifest.paths.linkedin_stderr_log)),
    },
    {
      key: 'rocketreachStdout',
      label: 'RocketReach Stdout',
      available: Boolean(cleanString(manifest.paths.rocketreach_stdout_log)),
    },
    {
      key: 'rocketreachStderr',
      label: 'RocketReach Stderr',
      available: Boolean(cleanString(manifest.paths.rocketreach_stderr_log)),
    },
  ];
}

function buildStageStates(manifest: PipelineManifest, counts: WorkflowCounts): WorkflowStageState[] {
  const status = manifest.status;
  const note = cleanString(manifest.note);
  const error = summarizeLastError(manifest.last_error);
  const linkedinMode = manifest.automation?.linkedin?.mode || 'saved_session';

  const linkedinDetail = counts.appliedRows > 0
    ? `${counts.appliedRows} applied row(s) captured.`
    : status === 'blocked_runtime'
      ? (cleanString(manifest.last_error) || 'Compatible LinkedIn runtime is required.')
      : status === 'waiting_login'
        ? note || (linkedinMode === 'auto_login'
          ? 'Auto-login needs attention. Check credentials or complete LinkedIn verification in Chrome.'
          : 'Chrome opened with your default profile. Log into LinkedIn there and keep the browser window open.')
      : note || 'LinkedIn application stage is ready to start.';

  const rocketreachDetail = counts.recruiterRows > 0
    ? `${counts.sendableRows} sendable recruiter row(s) found from ${counts.recruiterRows} enriched row(s).`
    : status === 'rocketreach_running'
      ? 'Recruiter enrichment is currently running.'
      : 'Recruiter enrichment will run after LinkedIn applications are saved.';

  const emailDetail = counts.emailTotal > 0 || counts.emailSent > 0 || counts.emailFailed > 0
    ? `total=${counts.emailTotal} sent=${counts.emailSent} failed=${counts.emailFailed}`
    : status === 'waiting_review' || status === 'manual_review'
      ? 'Manual review is waiting before emails are sent.'
      : counts.dedupedSendableRows > 0
        ? `${counts.dedupedSendableRows} deduped contact(s) are ready for email review.`
        : note || 'Email stage will activate after enrichment.';

  const states: WorkflowStageState[] = [
    {
      id: 'linkedin',
      label: 'Apply LinkedIn Jobs',
      status:
        status === 'blocked_runtime' ? 'blocked'
          : status === 'waiting_login' ? 'waiting'
          : status === 'failed' && counts.appliedRows === 0 ? 'failed'
            : ['linkedin_running'].includes(status) ? 'running'
              : counts.appliedRows > 0 || ['rocketreach_running', 'email_running', 'waiting_review', 'sending', 'completed', 'manual_review'].includes(status) ? 'completed'
                : status === 'queued' ? 'queued'
                  : 'idle',
      description: 'Run the LinkedIn automation and save applied jobs to CSV.',
      detail: linkedinDetail,
    },
    {
      id: 'rocketreach',
      label: 'RocketReach Data',
      status:
        status === 'failed' && counts.appliedRows > 0 && counts.recruiterRows === 0 ? 'failed'
          : status === 'rocketreach_running' ? 'running'
            : counts.recruiterRows > 0 || ['email_running', 'waiting_review', 'sending', 'completed', 'manual_review'].includes(status) ? 'completed'
              : counts.appliedRows > 0 || status === 'queued' ? 'queued'
                : 'idle',
      description: 'Fetch recruiter emails and save the enrichment output.',
      detail: rocketreachDetail,
    },
    {
      id: 'email',
      label: 'Auto Email Campaign',
      status:
        status === 'failed' && (counts.recruiterRows > 0 || counts.emailTotal > 0) ? 'failed'
          : ['email_running', 'sending'].includes(status) ? 'running'
            : ['waiting_review', 'manual_review'].includes(status) ? 'waiting'
              : status === 'completed' ? 'completed'
                : counts.recruiterRows > 0 ? 'queued'
                  : 'idle',
      description: 'Review sendable contacts, then send and track outcomes.',
      detail: error && status === 'failed' ? error : emailDetail,
    },
  ];

  return states;
}

function mapAppliedPreview(rows: Record<string, string>[]): AppliedJobPreviewRow[] {
  return rows.slice(0, 8).map((row) => ({
    company: cleanString(row['Company Name']),
    position: cleanString(row['Position']),
    submitted: cleanString(row['Submitted']) || 'Pending',
    date: cleanString(row['Date']),
    hrName: cleanString(row['HR Name']),
    jobLink: cleanString(row['Job Link']),
  }));
}

function mapRecruiterPreview(rows: Record<string, string>[]): RecruiterPreviewRow[] {
  return rows
    .filter((row) => Boolean(firstUsableEmail(row['HR Email'], row['HR Secondary Email']) || cleanString(row['HR Name']) || cleanString(row['Company Name'])))
    .slice(0, 8)
    .map((row) => ({
      name: cleanString(row['HR Name']) || 'Hiring Team',
      company: cleanString(row['Company Name']),
      position: cleanString(row['Position']) || cleanString(row['HR Position']),
      email: firstUsableEmail(row['HR Email']),
      secondaryEmail: cleanString(row['HR Secondary Email']),
      status: cleanString(row['RocketReach Status']) || 'pending',
    }));
}

function mapSendReportPreview(rows: Record<string, string>[]): DeliveryLogPreviewRow[] {
  return rows.slice(0, 8).map((row) => ({
    timestamp: cleanString(row.Timestamp),
    email: cleanString(row.Email),
    success: cleanString(row.Success).toLowerCase() === 'true',
    error: cleanString(row.Error),
    company: cleanString(row.Company),
    position: cleanString(row.Position),
  }));
}

function buildActiveRunPriority(status: string): number {
  switch (status) {
    case 'waiting_login':
      return 8;
    case 'sending':
    case 'email_running':
      return 7;
    case 'waiting_review':
    case 'manual_review':
      return 6;
    case 'rocketreach_running':
      return 5;
    case 'linkedin_running':
      return 4;
    case 'queued':
      return 3;
    case 'blocked_runtime':
      return 2;
    case 'failed':
      return 1;
    case 'completed':
      return 0;
    default:
      return -1;
  }
}

async function waitForWorkflowRun(runId: string, timeoutMs = 60000): Promise<WorkflowRunSummary> {
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown = null;

  while (Date.now() < deadline) {
    try {
      return await getWorkflowRun(runId);
    } catch (error: unknown) {
      lastError = error;
      await new Promise((resolve) => setTimeout(resolve, 500));
    }
  }

  throw lastError instanceof Error ? lastError : new Error(`Pipeline run ${runId} did not become visible in time.`);
}

async function hydrateWorkflowRun(manifest: PipelineManifest): Promise<WorkflowRunSummary> {
  const appliedRows = await readCsvIfExists(manifest.paths.applied_csv);
  const recruiterRows = await readCsvIfExists(manifest.paths.recruiters_csv);
  const sendReportRows = await readCsvIfExists(manifest.paths.send_report_csv);
  const contacts = mapContacts(manifest.run_id, recruiterRows);
  const counts = buildCounts(manifest, appliedRows, recruiterRows, contacts);
  const blockedReason = manifest.status === 'blocked_runtime'
    ? cleanString(manifest.last_error) || cleanString(manifest.note)
    : '';
  const noSendableReason = manifest.status === 'waiting_login'
    ? cleanString(manifest.last_error) || cleanString(manifest.note)
    : blockedReason || (counts.sendableRows > 0 ? '' : cleanString(manifest.note));

  return {
    runId: manifest.run_id,
    status: manifest.status,
    currentStage: pickCurrentStage(manifest.status),
    updatedAt: manifest.updated_at,
    createdAt: manifest.created_at,
    note: cleanString(manifest.note),
    lastError: summarizeLastError(manifest.last_error),
    retryEligible: manifest.status === 'waiting_login',
    readyToSend: manifest.status !== 'blocked_runtime' && counts.dedupedSendableRows > 0,
    setupRequired: manifest.status === 'blocked_runtime',
    blockedReason,
    noSendableReason,
    runDir: manifest.paths.run_dir,
    recruitersCsvPath: manifest.paths.recruiters_csv,
    stageStates: buildStageStates(manifest, counts),
    counts,
    artifacts: buildArtifacts(manifest),
    automation: manifest.automation ?? null,
    liveStatus: manifest.live_status ? {
      currentUrl: manifest.live_status.current_url,
      pageTitle: manifest.live_status.page_title,
      loginRequired: manifest.live_status.login_required,
      checkpointRequired: manifest.live_status.checkpoint_required,
      jobCardsCount: manifest.live_status.job_cards_count,
      jobDetailsCount: manifest.live_status.job_details_count,
      easyApplyCount: manifest.live_status.easy_apply_count,
      lastScreenshot: manifest.live_status.last_screenshot,
      latestLog: manifest.live_status.latestLog,
      latestError: manifest.live_status.latestError,
    } : undefined,
    logs: await getLatestLogs(manifest.run_id, 100),
    contacts,
    preview: {
      appliedJobs: mapAppliedPreview(appliedRows),
      recruiters: mapRecruiterPreview(recruiterRows),
      emailLogs: mapSendReportPreview(sendReportRows),
    },
  };
}

async function readManifestByRunId(runId: string): Promise<PipelineManifest> {
  const manifestPath = path.join(META_ROOT, `${runId}.json`);
  const manifest = await safeReadManifest(manifestPath);
  if (!manifest) {
    throw new Error(`Pipeline run not found: ${runId}`);
  }
  return manifest;
}

function assertWithinPipelineRoot(targetPath: string): string {
  const resolved = path.resolve(targetPath);
  const pipelineRootWithSep = `${path.resolve(PIPELINE_ROOT)}${path.sep}`;
  if (!resolved.startsWith(pipelineRootWithSep) && resolved !== path.resolve(PIPELINE_ROOT)) {
    throw new Error('Requested artifact is outside the pipeline workspace.');
  }
  return resolved;
}

function resolveArtifactPath(manifest: PipelineManifest, artifactKey: PipelineArtifactSummary['key']): string {
  const mapping: Record<PipelineArtifactSummary['key'], string | undefined> = {
    appliedCsv: manifest.paths.applied_csv,
    recruitersCsv: manifest.paths.recruiters_csv,
    sendReportCsv: manifest.paths.send_report_csv,
    linkedinStdout: manifest.paths.linkedin_stdout_log,
    linkedinStderr: manifest.paths.linkedin_stderr_log,
    rocketreachStdout: manifest.paths.rocketreach_stdout_log,
    rocketreachStderr: manifest.paths.rocketreach_stderr_log,
  };

  const targetPath = mapping[artifactKey];
  if (!targetPath) {
    throw new Error(`Artifact is unavailable for run ${manifest.run_id}.`);
  }
  return assertWithinPipelineRoot(targetPath);
}

export async function findWorkflowDashboard(limit = 8): Promise<WorkflowDashboardPayload> {
  try {
    const manifestPaths = await listManifestPaths();
    // Optimization: Only read and process the most recent 25 manifests to avoid 502 timeouts on Render
    const manifests = (await Promise.all(manifestPaths.slice(0, 25).map((manifestPath) => safeReadManifest(manifestPath))))
      .filter((manifest): manifest is PipelineManifest => Boolean(manifest))
      .sort((left, right) => right.updated_at.localeCompare(left.updated_at));

    const latestFailure = manifests.find((manifest) => isActionableFailure(manifest)) ?? null;
    const hydratedRuns = await Promise.all(manifests.slice(0, Math.min(limit, manifests.length)).map((manifest) => hydrateWorkflowRun(manifest)));
    const activeRun = hydratedRuns
      .slice()
      .sort((left, right) => {
        const priorityDiff = buildActiveRunPriority(right.status) - buildActiveRunPriority(left.status);
        if (priorityDiff !== 0) {
          return priorityDiff;
        }
        return right.updatedAt.localeCompare(left.updatedAt);
      })[0] ?? null;

    return {
      activeRun,
      recentRuns: hydratedRuns,
      latestFailure: latestFailure ? summarizeFailure(latestFailure) : null,
    };
  } catch {
    return {
      activeRun: null,
      recentRuns: [],
      latestFailure: null,
    };
  }
}

export async function findLatestPipelineRun(): Promise<LatestPipelinePayload> {
  try {
    const manifestPaths = await listManifestPaths();
    // Optimization: Only scan the most recent 25 runs for the 'latest' actionable run
    const manifests = await Promise.all(manifestPaths.slice(0, 25).map((manifestPath) => safeReadManifest(manifestPath)));
    const resolvedManifests = manifests.filter((manifest): manifest is PipelineManifest => Boolean(manifest));
    const latestFailure = resolvedManifests
      .filter((manifest) => isActionableFailure(manifest))
      .sort((left, right) => right.updated_at.localeCompare(left.updated_at))[0] ?? null;

    const eligible = resolvedManifests
      .filter((manifest) => ['waiting_review', 'waiting_login', 'sending', 'blocked_runtime'].includes(manifest.status))
      .sort((left, right) => right.updated_at.localeCompare(left.updated_at));

    for (const manifest of eligible) {
      if (manifest.status === 'blocked_runtime') {
        return {
          run: summarizeRun(manifest, [], []),
          contacts: [],
          latestFailure: latestFailure ? summarizeFailure(latestFailure) : null,
        };
      }

      try {
        const { rows, contacts } = await readRecruiterCsv(manifest.paths.recruiters_csv, manifest.run_id);
        return {
          run: summarizeRun(manifest, rows, contacts),
          contacts,
          latestFailure: latestFailure ? summarizeFailure(latestFailure) : null,
        };
      } catch {
        continue;
      }
    }

    return {
      run: null,
      contacts: [],
      latestFailure: latestFailure ? summarizeFailure(latestFailure) : null,
    };
  } catch {
    return { run: null, contacts: [], latestFailure: null };
  }
}

async function readRunData(runId: string): Promise<{ manifest: PipelineManifest; rows: Record<string, string>[]; contacts: ContactRow[] }> {
  const manifest = await readManifestByRunId(runId);
  const { rows, contacts } = await readRecruiterCsv(manifest.paths.recruiters_csv, runId);
  return {
    manifest,
    rows,
    contacts,
  };
}

export async function writeSendReport(runId: string, logs: EmailLog[]): Promise<string> {
  const { manifest, contacts } = await readRunData(runId);
  const contactByEmail = new Map(contacts.map((contact) => [contact.email.toLowerCase(), contact]));
  const lines = [
    'Timestamp,Email,Success,Error,Message ID,Name,Company,Position,Job Link',
  ];

  for (const log of logs) {
    const contact = contactByEmail.get(log.email.toLowerCase());
    const row = [
      log.timestamp || '',
      log.email || '',
      log.success ? 'true' : 'false',
      log.error || '',
      log.messageId || '',
      contact?.name || '',
      contact?.company || '',
      contact?.position || '',
      contact?.jobLink || '',
    ].map((value) => csvEscape(String(value)));
    lines.push(row.join(','));
  }

  await fs.mkdir(REPORTS_ROOT, { recursive: true });
  await fs.writeFile(manifest.paths.send_report_csv, `${lines.join('\n')}\n`, 'utf8');
  return manifest.paths.send_report_csv;
}

export function updatePipelineRunStatus(runId: string, status: 'waiting_review' | 'sending' | 'completed' | 'failed', note: string): void {
  const result = spawnSync(
    PYTHON_BIN,
    ['-m', 'pipeline.mark_status', '--run-id', runId, '--status', status, '--note', note],
    {
      cwd: WORKSPACE_ROOT,
      encoding: 'utf8',
    },
  );

  if (result.status !== 0) {
    const stderr = result.stderr?.trim();
    const stdout = result.stdout?.trim();
    throw new Error(stderr || stdout || 'Failed to update pipeline run status.');
  }
}

export async function getWorkflowRun(runId: string): Promise<WorkflowRunSummary> {
  const manifest = await readManifestByRunId(runId);
  return hydrateWorkflowRun(manifest);
}

export async function getArtifactForRun(runId: string, artifactKey: PipelineArtifactSummary['key']) {
  const manifest = await readManifestByRunId(runId);
  const artifactPath = resolveArtifactPath(manifest, artifactKey);
  if (!(await fileExists(artifactPath))) {
    throw new Error(`Artifact file does not exist for run ${runId}.`);
  }

  return {
    filePath: artifactPath,
    fileName: path.basename(artifactPath),
  };
}

async function launchAutomationProcess(runId: string, args: string[]): Promise<void> {
  const logFile = path.join(PIPELINE_ROOT, runId, 'automation.log');
  await fs.mkdir(path.dirname(logFile), { recursive: true });

  console.log(`[Pipeline] Launching automation process for ${runId}...`);
  console.log(`[Pipeline] Command: ${PYTHON_BIN} -u ${args.join(' ')}`);

  // Force unbuffered Python output
  const child = spawn(
    PYTHON_BIN,
    ['-u', ...args],
    {
      cwd: WORKSPACE_ROOT,
      detached: false,
      stdio: ['ignore', 'pipe', 'pipe'],
    }
  );

  const pid = child.pid;
  console.log(`[Pipeline] AUTOMATION_PROCESS_SPAWNED pid=${pid}`);

  let started = false;
  const startTimeout = setTimeout(() => {
    if (!started) {
      console.error(`[Pipeline] ERROR: Automation process started but no stdout logs were received within 15 seconds.`);
      try {
        updatePipelineRunStatus(runId, 'failed', 'Automation process started but no stdout logs were received.');
      } catch (e) {
        console.error('[Pipeline] Failed to update status to failed:', e);
      }
      child.kill();
    }
  }, 15000);

  child.stdout.on('data', async (data) => {
    const output = data.toString();
    // Pipe to console for Render logs
    process.stdout.write(`[automation stdout] ${output}`);
    
    // Append to log file
    try {
      await fs.appendFile(logFile, output);
    } catch (e) {
      console.error(`[Pipeline] Failed to write to log file: ${e}`);
    }

    if (output.includes('SCRIPT_STARTED: pipeline.run_once')) {
      started = true;
      clearTimeout(startTimeout);
      try {
        updatePipelineRunStatus(runId, 'running', 'Automation script confirmed running.');
      } catch (e) {
        console.error('[Pipeline] Failed to update status to running:', e);
      }
    }

    // Update manifest live_status with latest log line
    try {
      const manifestPath = path.join(META_ROOT, `${runId}.json`);
      if (await fileExists(manifestPath)) {
        const manifestStr = await fs.readFile(manifestPath, 'utf8');
        const manifest = JSON.parse(manifestStr) as PipelineManifest;
        const lastLine = output.trim().split('\n').pop();
        if (lastLine) {
          manifest.live_status = {
            ...(manifest.live_status || {}),
            latestLog: lastLine,
          };
          manifest.updated_at = new Date().toISOString();
          await fs.writeFile(manifestPath, JSON.stringify(manifest, null, 2), 'utf8');
        }
      }
    } catch (e) {
      // Ignore update errors during high-frequency logs
    }
  });

  child.stderr.on('data', async (data) => {
    const output = data.toString();
    process.stderr.write(`[automation stderr] ${output}`);
    
    try {
      await fs.appendFile(logFile, output);
    } catch (e) {
      console.error(`[Pipeline] Failed to write to log file: ${e}`);
    }

    // Update manifest live_status with latest error line
    try {
      const manifestPath = path.join(META_ROOT, `${runId}.json`);
      if (await fileExists(manifestPath)) {
        const manifestStr = await fs.readFile(manifestPath, 'utf8');
        const manifest = JSON.parse(manifestStr) as PipelineManifest;
        const lastLine = output.trim().split('\n').pop();
        if (lastLine) {
          manifest.live_status = {
            ...(manifest.live_status || {}),
            latestError: lastLine,
          };
          manifest.updated_at = new Date().toISOString();
          await fs.writeFile(manifestPath, JSON.stringify(manifest, null, 2), 'utf8');
        }
      }
    } catch (e) {
      // Ignore
    }
  });

  child.on('error', (err) => {
    console.error(`[Pipeline] AUTOMATION_PROCESS_ERROR ${err.stack}`);
    try {
      updatePipelineRunStatus(runId, 'failed', `Automation process error: ${err.message}`);
    } catch (e) {
      console.error('[Pipeline] Failed to update status on process error:', e);
    }
  });

  child.on('exit', (code, signal) => {
    console.log(`[Pipeline] AUTOMATION_PROCESS_EXIT code=${code} signal=${signal}`);
  });
}

async function pickDefaultAutomationConfigPath(): Promise<string> {
  const configuredPath = process.env.PIPELINE_AUTOMATION_CONFIG_PATH?.trim();
  if (configuredPath) {
    return path.resolve(configuredPath);
  }
  if (await fileExists(PIPELINE_ENV_PATH)) {
    return PIPELINE_ENV_PATH;
  }
  if (await fileExists(LINKEDIN_LEGACY_CONFIG_PATH)) {
    return LINKEDIN_LEGACY_CONFIG_PATH;
  }
  return '';
}

function assertConfigPathAllowed(configPath: string): string {
  if (!configPath.trim()) {
    return '';
  }
  const resolved = path.resolve(configPath);
  const workspaceRootWithSep = `${path.resolve(WORKSPACE_ROOT)}${path.sep}`;
  if (!resolved.startsWith(workspaceRootWithSep) && resolved !== path.resolve(WORKSPACE_ROOT)) {
    throw new Error('Automation config must be inside the workspace.');
  }
  return resolved;
}

function runPythonJson<T>(script: string, input?: unknown): T {
  const result = spawnSync(PYTHON_BIN, ['-c', script], {
    cwd: WORKSPACE_ROOT,
    encoding: 'utf8',
    input: input === undefined ? undefined : JSON.stringify(input),
  });

  if (result.status !== 0) {
    const detail = result.stderr?.trim() || result.stdout?.trim();
    throw new Error(detail || 'Python helper failed.');
  }

  return JSON.parse(result.stdout.trim() || '{}') as T;
}

function loadEditableConfig(): EditableLinkedInConfig {
  return runPythonJson<EditableLinkedInConfig>([
    'import json',
    'from pipeline.config import load_editable_linkedin_config',
    'print(json.dumps(load_editable_linkedin_config()))',
  ].join('; '));
}

export async function getWorkflowConfig(): Promise<WorkflowConfigPayload> {
  if (IS_VERCEL_RUNTIME) {
    return {
      configPath: '',
      summary: {
        error: 'Connect your local bridge URL to load and save Python automation settings from this SaaS dashboard.',
        source: 'vercel-saas-mode',
        linkedin: {
          mode: 'saved_session',
          auto_login: false,
          safe_mode: true,
          username_configured: false,
        },
      },
      editable: { files: {} },
    };
  }

  const configPath = await pickDefaultAutomationConfigPath();
  const summary = runPythonJson<WorkflowAutomationSummary>([
    'import json',
    'from pipeline.config import load_automation_summary',
    'print(json.dumps(load_automation_summary(r"""' + configPath.replaceAll('\\', '\\\\').replaceAll('"""', '\\"\\"\\"') + '""")))',
  ].join('; '));

  return {
    configPath,
    summary,
    editable: loadEditableConfig(),
  };
}

export async function saveWorkflowConfig(updates: EditableLinkedInConfigUpdates): Promise<WorkflowConfigPayload> {
  if (IS_VERCEL_RUNTIME) {
    throw new Error('Connect a local bridge URL before saving Python automation settings from Vercel.');
  }

  runPythonJson<EditableLinkedInConfig>([
    'import json, sys',
    'from pipeline.config import update_editable_linkedin_config',
    'payload=json.load(sys.stdin)',
    'print(json.dumps(update_editable_linkedin_config(None, payload)))',
  ].join('; '), updates);
  return getWorkflowConfig();
}

export async function getLatestLogs(runId: string, maxLines = 100): Promise<string[]> {
  const logFile = path.join(PIPELINE_ROOT, runId, 'automation.log');
  if (!(await fileExists(logFile))) {
    return [];
  }
  try {
    const content = await fs.readFile(logFile, 'utf8');
    const lines = content.split(/\r?\n/).filter(Boolean);
    return lines.slice(-maxLines);
  } catch (e) {
    console.error(`[Pipeline] Failed to read logs for ${runId}:`, e);
    return [];
  }
}

export async function startPipelineRun(configPath?: string): Promise<{ runId: string; run: WorkflowRunSummary }> {
  if (IS_VERCEL_RUNTIME) {
    throw new Error('Connect a local bridge URL before starting browser automation from Vercel.');
  }
  const runId = `run-ui-${randomUUID().replace(/-/g, '').slice(0, 12)}`;
  const resolvedConfigPath = assertConfigPathAllowed(configPath ?? await pickDefaultAutomationConfigPath());
  
  console.log(`[Pipeline] Initializing fresh run: ${runId}`);
  const args = ['-m', 'pipeline.run_once', '--fresh', '--run-id', runId];
  if (resolvedConfigPath) {
    console.log(`[Pipeline] Using config: ${resolvedConfigPath}`);
    args.push('--config', resolvedConfigPath);
  }

  // Pre-create the directory so mark_status doesn't fail if manifest isn't there yet
  await fs.mkdir(path.join(RUNS_ROOT, runId), { recursive: true });
  
  // Set initial status to starting
  try {
    updatePipelineRunStatus(runId, 'failed', 'Preparing automation environment...');
    // We mark it as failed first just so it exists in DB, then run_once will fix it or we mark it running
    // Actually, mark_status probably creates it. 
    // Let's just launch it and let launchAutomationProcess handle status flow.
  } catch (e) {
    // Ignore if not exists yet
  }

  launchAutomationProcess(runId, args);
  
  console.log(`[Pipeline] Waiting for manifest visibility for ${runId}...`);
  const run = await waitForWorkflowRun(runId);
  console.log(`[Pipeline] Run visible. Current status: ${run.status}`);
  return { runId, run };
}

export async function retryPipelineRun(runId: string): Promise<{ runId: string; run: WorkflowRunSummary }> {
  if (IS_VERCEL_RUNTIME) {
    throw new Error('Connect a local bridge URL before retrying browser automation from Vercel.');
  }
  
  launchAutomationProcess(runId, ['-m', 'pipeline.run_once', '--resume', '--run-id', runId]);
  
  const run = await waitForWorkflowRun(runId);
  return { runId, run };
}

export async function createManualRecruiterEnrichment(fileName: string, csvBuffer: Buffer): Promise<ManualEnrichmentResult> {
  if (IS_VERCEL_RUNTIME) {
    throw new Error('Connect a local bridge URL before running manual Python enrichment from Vercel.');
  }
  if (!csvBuffer.length) {
    throw new Error('Uploaded CSV file is empty.');
  }

  const runId = buildManualRunId();
  const runDir = path.join(RUNS_ROOT, runId);
  const logsDir = path.join(LOGS_ROOT, runId);
  const inputCsvPath = path.join(runDir, 'applied_jobs.csv');
  const recruitersCsvPath = path.join(runDir, 'recruiters_enriched.csv');
  const stdoutLogPath = path.join(logsDir, 'rocketreach.stdout.log');
  const stderrLogPath = path.join(logsDir, 'rocketreach.stderr.log');
  const manifestPath = path.join(META_ROOT, `${runId}.json`);
  const reportPath = path.join(REPORTS_ROOT, `${runId}.csv`);

  await fs.mkdir(runDir, { recursive: true });
  await fs.mkdir(logsDir, { recursive: true });
  await fs.mkdir(META_ROOT, { recursive: true });
  await fs.mkdir(REPORTS_ROOT, { recursive: true });
  await fs.writeFile(inputCsvPath, csvBuffer);

  const result = spawnSync(
    PYTHON_BIN,
    ['bulk_enrich.py', '--input', inputCsvPath, '--output', recruitersCsvPath],
    {
      cwd: ROCKETREACH_ROOT,
      encoding: 'utf8',
    },
  );

  await fs.writeFile(stdoutLogPath, result.stdout ?? '', 'utf8');
  await fs.writeFile(stderrLogPath, result.stderr ?? '', 'utf8');

  if (result.status !== 0) {
    throw new Error(buildFailureMessage(`RocketReach enrichment failed for ${path.basename(fileName || 'uploaded.csv')}.`, result.stderr ?? '', result.stdout ?? ''));
  }

  const stats = parseStatsPayload(result.stdout ?? '');
  const { contacts } = await readRecruiterCsv(recruitersCsvPath, runId);

  const manifest = {
    run_id: runId,
    status: 'manual_review',
    created_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
    stage_started_at: '',
    stage_finished_at: '',
    retry_count: 0,
    note: buildManualNote(stats),
    last_error: '',
    paths: {
      run_dir: runDir,
      applied_csv: inputCsvPath,
      recruiters_csv: recruitersCsvPath,
      send_report_csv: reportPath,
      manifest_json: manifestPath,
      logs_dir: logsDir,
      linkedin_stdout_log: path.join(logsDir, 'linkedin.stdout.log'),
      linkedin_stderr_log: path.join(logsDir, 'linkedin.stderr.log'),
      rocketreach_stdout_log: stdoutLogPath,
      rocketreach_stderr_log: stderrLogPath,
    },
    artifacts: {
      applied_csv_exists: true,
      recruiters_csv_exists: true,
      send_report_exists: false,
    },
  };
  await fs.writeFile(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`, 'utf8');

  return {
    runDir,
    recruitersCsvPath,
    contacts,
    stats,
  };
}

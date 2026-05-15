export interface ContactRow {
  id?: string;
  email: string;
  name?: string;
  company?: string;
  position?: string;
  secondaryEmail?: string;
  jobLink?: string;
  runId?: string;
  sourceStatus?: string;
  [key: string]: unknown;
}

export interface PipelineRunSummary {
  runId: string;
  status: string;
  updatedAt: string;
  runDir: string;
  recruitersCsvPath: string;
  totalRows: number;
  sendableRows: number;
  dedupedRows: number;
  skippedRows: number;
  readyToSend: boolean;
  setupRequired?: boolean;
  blockedReason?: string;
  noSendableReason?: string;
  note?: string;
}

export interface PipelineFailureSummary {
  runId: string;
  status: string;
  updatedAt: string;
  note?: string;
  lastError?: string;
}

export interface RecruiterEnrichmentSummary {
  total: number;
  matched: number;
  failed: number;
  skipped: number;
  no_match: number;
  missing_hr_link: number;
  invalid_hr_link: number;
  profile_only: number;
  lookup_quota_reached: number;
  sendable_rows: number;
}

export interface ManualRecruiterEnrichmentResponse {
  runDir: string;
  recruitersCsvPath: string;
  contacts: ContactRow[];
  stats: RecruiterEnrichmentSummary;
}

export type WorkflowStageId = 'linkedin' | 'rocketreach' | 'email';

export type WorkflowStageStatus =
  | 'idle'
  | 'queued'
  | 'running'
  | 'waiting'
  | 'completed'
  | 'failed'
  | 'blocked';

export interface WorkflowStageState {
  id: WorkflowStageId;
  label: string;
  status: WorkflowStageStatus;
  description: string;
  detail: string;
}

export interface WorkflowCounts {
  appliedRows: number;
  recruiterRows: number;
  sendableRows: number;
  dedupedSendableRows: number;
  skippedRows: number;
  emailTotal: number;
  emailSent: number;
  emailFailed: number;
}

export interface PipelineArtifactSummary {
  key: 'appliedCsv' | 'recruitersCsv' | 'sendReportCsv' | 'linkedinStdout' | 'linkedinStderr' | 'rocketreachStdout' | 'rocketreachStderr';
  label: string;
  available: boolean;
}

export interface AppliedJobPreviewRow {
  company: string;
  position: string;
  submitted: string;
  date: string;
  hrName: string;
  jobLink: string;
}

export interface RecruiterPreviewRow {
  name: string;
  company: string;
  position: string;
  email: string;
  secondaryEmail: string;
  status: string;
}

export interface DeliveryLogPreviewRow {
  timestamp: string;
  email: string;
  success: boolean;
  error: string;
  company: string;
  position: string;
}

export interface WorkflowPreviewData {
  appliedJobs: AppliedJobPreviewRow[];
  recruiters: RecruiterPreviewRow[];
  emailLogs: DeliveryLogPreviewRow[];
}

export interface WorkflowAutomationSummary {
  auto_send?: boolean;
  max_easy_apply?: number;
  send_delay_seconds?: number;
  sender_name?: string;
  email_subject?: string;
  source?: string;
  error?: string;
  smtp?: {
    host?: string;
    port?: number;
    secure?: boolean;
    user?: string;
    from?: string;
  } | null;
  linkedin?: {
    mode?: 'saved_session' | 'auto_login';
    auto_login?: boolean;
    safe_mode?: boolean;
    username_configured?: boolean;
    manual_login_timeout_seconds?: number;
  } | null;
  config_preview?: Record<string, unknown>;
}

export interface WorkflowConfigPayload {
  configPath: string;
  summary: WorkflowAutomationSummary;
  editable: EditableLinkedInConfig;
}

export type EditableConfigFieldType = 'text' | 'password' | 'textarea' | 'number' | 'boolean' | 'list';

export interface EditableConfigField {
  name: string;
  type: EditableConfigFieldType;
  value: string | number | boolean | string[];
}

export interface EditableConfigFile {
  label: string;
  path: string;
  fields: EditableConfigField[];
  values: Record<string, string | number | boolean | string[]>;
}

export interface EditableLinkedInConfig {
  files: Record<string, EditableConfigFile>;
}

export type EditableLinkedInConfigUpdates = Record<string, Record<string, string | number | boolean | string[]>>;

export interface WorkflowRunSummary {
  runId: string;
  status: string;
  currentStage: WorkflowStageId;
  updatedAt: string;
  createdAt?: string;
  note: string;
  lastError: string;
  readyToSend: boolean;
  setupRequired: boolean;
  retryEligible?: boolean;
  blockedReason: string;
  noSendableReason: string;
  runDir: string;
  recruitersCsvPath: string;
  stageStates: WorkflowStageState[];
  counts: WorkflowCounts;
  artifacts: PipelineArtifactSummary[];
  automation: WorkflowAutomationSummary | null;
  contacts: ContactRow[];
  liveStatus?: {
    currentUrl?: string;
    pageTitle?: string;
    loginRequired?: boolean;
    checkpointRequired?: boolean;
    jobCardsCount?: string | number;
    jobDetailsCount?: string | number;
    easyApplyCount?: string | number;
    lastScreenshot?: string;
  };
  preview: WorkflowPreviewData;
}

export interface WorkflowDashboardPayload {
  activeRun: WorkflowRunSummary | null;
  recentRuns: WorkflowRunSummary[];
  latestFailure: PipelineFailureSummary | null;
}

export type SendStatus = 'idle' | 'sending' | 'paused' | 'completed' | 'stopped';

export interface EmailLog {
  id: string;
  email: string;
  success: boolean;
  error?: string;
  timestamp: string;
  messageId?: string;
}

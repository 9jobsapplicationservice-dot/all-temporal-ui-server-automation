'use client';

import React, { useCallback, useEffect, useEffectEvent, useMemo, useRef, useState } from 'react';
import {
  Bell,
  BriefcaseBusiness,
  Mail,
  Menu,
  Play,
  RefreshCcw,
  Rocket,
  Save,
  Search,
  Settings,
  Sparkles,
  Users,
  X,
} from 'lucide-react';
import RecruiterEnricher from '@/components/RecruiterEnricher';
import SMTPSettings from '@/components/SMTPSettings';
import SendDashboard from '@/components/SendDashboard';
import TemplateEditor from '@/components/TemplateEditor';
import WorkflowPreviewPanel from '@/components/WorkflowPreviewPanel';
import WorkflowProgressPanel from '@/components/WorkflowProgressPanel';
import WorkflowRunsTable from '@/components/WorkflowRunsTable';
import WorkflowStageCard from '@/components/WorkflowStageCard';
import LiveLogs from '@/components/LiveLogs';
import { readApiJson } from '@/lib/api-client';
import type {
  EditableConfigField,
  EditableLinkedInConfigUpdates,
  EmailLog,
  ManualRecruiterEnrichmentResponse,
  PipelineArtifactSummary,
  PipelineRunStatus,
  SendStatus,
  WorkflowDashboardPayload,
  WorkflowConfigPayload,
  WorkflowRunSummary,
} from '@/types';

const DEFAULT_SUBJECT = 'Quick note re: {{position}} at {{company}}';

const DEFAULT_BODY = `Hi {{name}},

I recently applied for the {{position}} role at {{company}} and wanted to introduce myself directly.

I would love the opportunity to connect and share how I can contribute.

Best regards,
{{sendername}}`;

function normalizeBridgeUrl(value: string) {
  return value.trim().replace(/\/$/, '');
}

function buildBridgeHeaders(baseUrl: string, headers: Record<string, string> = {}) {
  return baseUrl ? { ...headers, 'ngrok-skip-browser-warning': 'any' } : headers;
}

function getStoredValue(key: string, fallback: string) {
  if (typeof window === 'undefined') {
    return fallback;
  }

  const stored = window.localStorage.getItem(key);
  return stored && stored.trim() ? stored : fallback;
}

type PreviewTab = 'applied' | 'recruiters' | 'email';

interface DashboardHomeProps {
  initialDashboard: WorkflowDashboardPayload;
}

export default function DashboardHome({ initialDashboard }: DashboardHomeProps) {
  const [dashboard, setDashboard] = useState<WorkflowDashboardPayload>(initialDashboard);
  const [isLoading, setIsLoading] = useState(false);
  const [isStartingRun, setIsStartingRun] = useState(false);
  const [pageMessage, setPageMessage] = useState<string | null>(null);
  const [pageError, setPageError] = useState<string | null>(null);
  const [configPayload, setConfigPayload] = useState<WorkflowConfigPayload | null>(null);
  const [isSettingsOpen, setIsSettingsOpen] = useState(false);
  const [isConfigLoading, setIsConfigLoading] = useState(false);
  const [isConfigSaving, setIsConfigSaving] = useState(false);
  const [configError, setConfigError] = useState<string | null>(null);
  const [configDraft, setConfigDraft] = useState<EditableLinkedInConfigUpdates>({});
  const [activeConfigSection, setActiveConfigSection] = useState('personals');
  const [selectedTab, setSelectedTab] = useState<PreviewTab>('applied');
  const [templateBody, setTemplateBody] = useState(() => getStoredValue('rr_template_body_v3', DEFAULT_BODY));
  const [templateSubject, setTemplateSubject] = useState(() => getStoredValue('rr_template_subject_v3', DEFAULT_SUBJECT));
  const [senderName, setSenderName] = useState(() => getStoredValue('rr_sender_name_v3', ''));
  const [delayStr, setDelayStr] = useState(() => getStoredValue('rr_send_delay_v3', '10'));
  const [status, setStatus] = useState<SendStatus>('idle');
  const [logs, setLogs] = useState<EmailLog[]>([]);
  const [activeEmail, setActiveEmail] = useState<string | null>(null);
  const [localBridgeUrl] = useState(() => getStoredValue('rr_local_bridge_url', ''));
  const localBridgeUrlRef = useRef(localBridgeUrl);

  const statusRef = useRef(status);
  const logsRef = useRef(logs);
  const activeRunRef = useRef<WorkflowRunSummary | null>(null);
  const syncedPipelineStatusRef = useRef<PipelineRunStatus | null>(null);
  const previousRunIdRef = useRef<string | null>(null);

  const enrichmentRef = useRef<HTMLDivElement | null>(null);
  const templateRef = useRef<HTMLDivElement | null>(null);
  const sendRef = useRef<HTMLDivElement | null>(null);

  const activeRun = dashboard.activeRun;
  const contacts = activeRun?.contacts ?? [];
  const activeRunLabel = !activeRun
    ? 'No Active Workflow'
    : activeRun.status === 'queued' && activeRun.note.toLowerCase().includes('fresh run')
      ? 'Fresh Starting'
      : activeRun.status === 'waiting_login'
        ? 'Waiting For LinkedIn Login'
        : activeRun.status === 'linkedin_running'
          ? 'Applying Jobs'
          : activeRun.status === 'rocketreach_running'
            ? 'Enriching Recruiters'
            : (activeRun.status === 'waiting_review' || activeRun.status === 'manual_review') && activeRun.counts.dedupedSendableRows > 0
              ? 'Ready To Send'
              : activeRun.status.replaceAll('_', ' ');

  useEffect(() => {
    statusRef.current = status;
  }, [status]);

  useEffect(() => {
    logsRef.current = logs;
  }, [logs]);

  useEffect(() => {
    activeRunRef.current = activeRun;
  }, [activeRun]);

  useEffect(() => {
    window.localStorage.setItem('rr_template_body_v3', templateBody);
    window.localStorage.setItem('rr_template_subject_v3', templateSubject);
    window.localStorage.setItem('rr_sender_name_v3', senderName);
    window.localStorage.setItem('rr_send_delay_v3', delayStr);
    window.localStorage.setItem('rr_local_bridge_url', localBridgeUrl);
  }, [delayStr, localBridgeUrl, senderName, templateBody, templateSubject]);

  useEffect(() => {
    const currentRunId = activeRun?.runId ?? null;
    if (previousRunIdRef.current === currentRunId) {
      return;
    }

    previousRunIdRef.current = currentRunId;
    syncedPipelineStatusRef.current = null;
    setLogs([]);
    setStatus('idle');
    setActiveEmail(null);
  }, [activeRun?.runId]);

  const loadDashboard = useCallback(async (opts?: { quiet?: boolean }) => {
    if (!opts?.quiet) {
      setIsLoading(true);
    }

    try {
      const baseUrl = normalizeBridgeUrl(localBridgeUrlRef.current);
      const url = baseUrl ? `${baseUrl}/api/dashboard/overview` : '/api/dashboard/overview';

      const response = await fetch(url, { cache: 'no-store', headers: buildBridgeHeaders(baseUrl) });
      const payload = await readApiJson<WorkflowDashboardPayload>(response, 'Failed to load workflow dashboard.');

      setDashboard(payload);
      setPageError(null);
    } catch (error: unknown) {
      setPageError(error instanceof Error ? error.message : 'Failed to load workflow dashboard.');
    } finally {
      if (!opts?.quiet) {
        setIsLoading(false);
      }
    }
  }, []);

  const loadConfig = useCallback(async () => {
    setIsConfigLoading(true);
    setConfigError(null);
    try {
      const baseUrl = normalizeBridgeUrl(localBridgeUrlRef.current);
      const url = baseUrl ? `${baseUrl}/api/pipeline/config` : '/api/pipeline/config';

      const response = await fetch(url, { cache: 'no-store', headers: buildBridgeHeaders(baseUrl) });
      const payload = await readApiJson<WorkflowConfigPayload>(response, 'Failed to load automation config.');
      setConfigPayload(payload);
    } catch (error: unknown) {
      setConfigError(error instanceof Error ? error.message : 'Failed to load automation config.');
    } finally {
      setIsConfigLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadDashboard();
    void loadConfig();
  }, [loadConfig, loadDashboard]);

  useEffect(() => {
    const files = configPayload?.editable.files;
    if (!files) {
      return;
    }

    const nextDraft: EditableLinkedInConfigUpdates = {};
    for (const [section, file] of Object.entries(files)) {
      nextDraft[section] = { ...file.values };
    }
    setConfigDraft(nextDraft);
    setActiveConfigSection((current) => files[current] ? current : Object.keys(files)[0] ?? 'personals');
  }, [configPayload]);

  useEffect(() => {
    if (!activeRun || !['queued', 'linkedin_running', 'rocketreach_running', 'email_running', 'sending'].includes(activeRun.status)) {
      return;
    }

    const timer = window.setInterval(() => {
      void loadDashboard({ quiet: true });
    }, 5000);

    return () => window.clearInterval(timer);
  }, [activeRun, loadDashboard]);

  const sendBlockedReason = useMemo(() => {
    if (!activeRun) {
      return 'Start or load a workflow run first.';
    }
    if (activeRun.setupRequired) {
      return activeRun.blockedReason || 'Pipeline setup is required before this run can continue.';
    }
    if (!activeRun.readyToSend) {
      return activeRun.noSendableReason || 'No sendable contacts are available for this run.';
    }
    return null;
  }, [activeRun]);

  const buildPipelineNote = (nextStatus: PipelineRunStatus) => {
    const sent = logsRef.current.filter((log) => log.success).length;
    const failed = logsRef.current.filter((log) => !log.success).length;
    if (nextStatus === 'sending') {
      return `Manual review finished. Started sending to ${contacts.length} contact(s).`;
    }
    if (nextStatus === 'completed') {
      return `Email sending completed. sent=${sent} failed=${failed}`;
    }
    if (nextStatus === 'failed') {
      return `Email sending failed. sent=${sent} failed=${failed}`;
    }
    return `Returned to review state. sent=${sent} failed=${failed}`;
  };

  const syncPipelineStatus = useEffectEvent(async (nextStatus: PipelineRunStatus) => {
    if (!activeRunRef.current) {
      return;
    }

    const baseUrl = normalizeBridgeUrl(localBridgeUrlRef.current);
    const url = baseUrl ? `${baseUrl}/api/pipeline/run-status` : '/api/pipeline/run-status';
    const headers = buildBridgeHeaders(baseUrl, { 'Content-Type': 'application/json' });

    const response = await fetch(url, {
      method: 'POST',
      headers,
      body: JSON.stringify({
        runId: activeRunRef.current.runId,
        status: nextStatus,
        note: buildPipelineNote(nextStatus),
        logs: logsRef.current,
      }),
    });

    await readApiJson<{ success: boolean; reportPath?: string }>(response, 'Failed to sync pipeline status.');
  });

  useEffect(() => {
    if (!activeRun) {
      syncedPipelineStatusRef.current = null;
      return;
    }

    let nextPipelineStatus: PipelineRunStatus | null = null;
    if (status === 'sending') {
      nextPipelineStatus = 'sending';
    } else if (status === 'completed') {
      nextPipelineStatus = 'completed';
    } else if (status === 'paused' || status === 'stopped') {
      nextPipelineStatus = 'waiting_review';
    }

    if (!nextPipelineStatus || syncedPipelineStatusRef.current === nextPipelineStatus) {
      return;
    }

    syncedPipelineStatusRef.current = nextPipelineStatus;
    void syncPipelineStatus(nextPipelineStatus)
      .then(async () => {
        if (nextPipelineStatus !== 'sending') {
          await loadDashboard({ quiet: true });
        }
      })
      .catch((error: unknown) => {
        setPageError(error instanceof Error ? error.message : 'Failed to sync pipeline status.');
        syncedPipelineStatusRef.current = null;
      });
  }, [activeRun, loadDashboard, status]);

  const sendNextEmail = async (currentIndex: number) => {
    if (statusRef.current !== 'sending') {
      return;
    }
    if (currentIndex >= contacts.length) {
      setActiveEmail(null);
      setStatus('completed');
      return;
    }

    const contact = contacts[currentIndex];
    setActiveEmail(contact.email);

    const replaceVars = (value: string) => value.replace(/\{\{(\w+)\}\}/g, (match, variableName) => {
      if (variableName.toLowerCase() === 'sendername') {
        return senderName || match;
      }
      const replacement = contact[variableName.toLowerCase()];
      return typeof replacement === 'string' && replacement.trim() ? replacement : match;
    });

    try {
      const response = await fetch('/api/send', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          to: contact.email,
          subject: replaceVars(templateSubject),
          text: replaceVars(templateBody),
        }),
      });
      const data = await readApiJson<{ error?: string; messageId?: string }>(response, 'Failed to send email.');

      const newLog: EmailLog = {
        id: `${contact.email}-${currentIndex}-${response.ok ? 'success' : 'failure'}`,
        email: contact.email,
        success: true,
        error: undefined,
        timestamp: new Date().toLocaleTimeString(),
        messageId: data.messageId,
      };
      setLogs((prev) => [newLog, ...prev]);
    } catch (error: unknown) {
      const newLog: EmailLog = {
        id: `${contact.email}-${currentIndex}-failure`,
        email: contact.email,
        success: false,
        error: error instanceof Error ? error.message : 'Failed to send email',
        timestamp: new Date().toLocaleTimeString(),
      };
      setLogs((prev) => [newLog, ...prev]);
    }

    if (statusRef.current === 'sending') {
      const ms = parseInt(delayStr, 10) * 1000 || 10000;
      window.setTimeout(() => {
        void sendNextEmail(currentIndex + 1);
      }, ms);
    } else {
      setActiveEmail(null);
    }
  };

  const sentCount = logs.filter((log) => log.success).length;
  const failedCount = logs.filter((log) => !log.success).length;

  const handleEmailStart = () => {
    if (!templateBody || !templateSubject) {
      window.alert('Please enter a subject and body template.');
      return;
    }
    if (sendBlockedReason) {
      window.alert(sendBlockedReason);
      return;
    }
    if (contacts.length === 0) {
      window.alert('No contacts are available to send.');
      return;
    }

    syncedPipelineStatusRef.current = null;
    statusRef.current = 'sending';
    setStatus('sending');
    const resumeIndex = logs.length;
    void sendNextEmail(resumeIndex);
  };

  const handlePause = () => {
    syncedPipelineStatusRef.current = null;
    statusRef.current = 'paused';
    setActiveEmail(null);
    setStatus('paused');
  };

  const handleStop = () => {
    syncedPipelineStatusRef.current = null;
    statusRef.current = 'stopped';
    setActiveEmail(null);
    setStatus('stopped');
  };

  const handleStartPipeline = async (configPathOverride?: string) => {
    setIsStartingRun(true);
    setPageMessage(null);
    setPageError(null);
    try {
      const baseUrl = normalizeBridgeUrl(localBridgeUrlRef.current);
      const url = baseUrl ? `${baseUrl}/api/pipeline/start` : '/api/pipeline/start';
      const headers = buildBridgeHeaders(baseUrl, { 'Content-Type': 'application/json' });

      const response = await fetch(url, {
        method: 'POST',
        headers,
        body: JSON.stringify({ configPath: configPathOverride ?? configPayload?.configPath }),
      });
      const payload = await readApiJson<{ run: WorkflowRunSummary }>(response, 'Failed to start workflow run.');

      const run = payload.run;
      setPageMessage(`Fresh workflow run ${run.runId} started. Any stale shared-folder runs were cleared automatically.`);
      await loadDashboard({ quiet: true });
    } catch (error: unknown) {
      setPageError(error instanceof Error ? error.message : 'Failed to start workflow run.');
    } finally {
      setIsStartingRun(false);
    }
  };

  const handleOpenSettings = () => {
    setIsSettingsOpen(true);
    void loadConfig();
  };

  const handleStartFromSettings = async () => {
    const savedPayload = await handleSaveConfig();
    if (!savedPayload) {
      return;
    }
    setIsSettingsOpen(false);
    await handleStartPipeline(savedPayload.configPath);
  };

  const handleConfigFieldChange = (section: string, field: EditableConfigField, rawValue: string | boolean) => {
    const value = field.type === 'boolean'
      ? Boolean(rawValue)
      : field.type === 'number'
        ? Number(String(rawValue).trim() || 0)
        : field.type === 'list'
          ? String(rawValue).split(/\r?\n|,/).map((item) => item.trim()).filter(Boolean)
          : String(rawValue);

    setConfigDraft((prev) => ({
      ...prev,
      [section]: {
        ...(prev[section] ?? {}),
        [field.name]: value,
      },
    }));
  };

  const handleSaveConfig = async () => {
    setIsConfigSaving(true);
    setConfigError(null);
    try {
      const baseUrl = normalizeBridgeUrl(localBridgeUrlRef.current);
      const url = baseUrl ? `${baseUrl}/api/pipeline/config` : '/api/pipeline/config';
      const headers = buildBridgeHeaders(baseUrl, { 'Content-Type': 'application/json' });

      const response = await fetch(url, {
        method: 'POST',
        headers,
        body: JSON.stringify({ updates: configDraft }),
      });
      const payload = await readApiJson<WorkflowConfigPayload>(response, 'Failed to save automation config.');
      setConfigPayload(payload);
      setPageMessage('Settings saved. New workflow runs will use the updated LinkedIn config files.');
      return payload;
    } catch (error: unknown) {
      setConfigError(error instanceof Error ? error.message : 'Failed to save automation config.');
      return null;
    } finally {
      setIsConfigSaving(false);
    }
  };

  const renderConfigField = (section: string, field: EditableConfigField) => {
    const value = configDraft[section]?.[field.name] ?? field.value;
    const label = field.name.replaceAll('_', ' ');
    if (field.type === 'boolean') {
      return (
        <label key={field.name} className="config-toggle-row">
          <span>{label}</span>
          <input
            type="checkbox"
            checked={Boolean(value)}
            onChange={(event) => handleConfigFieldChange(section, field, event.target.checked)}
          />
        </label>
      );
    }

    const inputValue = Array.isArray(value) ? value.join('\n') : String(value ?? '');
    if (field.type === 'textarea' || field.type === 'list') {
      return (
        <label key={field.name} className="config-field config-field-wide">
          <span>{label}</span>
          <textarea
            value={inputValue}
            rows={field.type === 'list' ? 4 : 7}
            onChange={(event) => handleConfigFieldChange(section, field, event.target.value)}
          />
        </label>
      );
    }

    return (
      <label key={field.name} className="config-field">
        <span>{label}</span>
        <input
          type={field.type === 'password' ? 'password' : field.type === 'number' ? 'number' : 'text'}
          value={inputValue}
          onChange={(event) => handleConfigFieldChange(section, field, event.target.value)}
        />
      </label>
    );
  };

  const linkedInLoginFields = {
    username: {
      name: 'username',
      type: 'text',
      value: configDraft.secrets?.username ?? '',
    } as EditableConfigField,
    password: {
      name: 'password',
      type: 'password',
      value: configDraft.secrets?.password ?? '',
    } as EditableConfigField,
    autoLogin: {
      name: 'linkedin_auto_login',
      type: 'boolean',
      value: configDraft.secrets?.linkedin_auto_login ?? false,
    } as EditableConfigField,
  };

  const handleRetryPipeline = async () => {
    if (!activeRun?.runId) {
      return;
    }
    setIsStartingRun(true);
    setPageError(null);
    setPageMessage(null);
    try {
      const baseUrl = normalizeBridgeUrl(localBridgeUrlRef.current);
      const url = baseUrl ? `${baseUrl}/api/pipeline/retry` : '/api/pipeline/retry';
      const headers = buildBridgeHeaders(baseUrl, { 'Content-Type': 'application/json' });

      const response = await fetch(url, {
        method: 'POST',
        headers,
        body: JSON.stringify({ runId: activeRun.runId }),
      });
      await readApiJson<{ run: WorkflowRunSummary }>(response, 'Failed to retry LinkedIn workflow run.');
      setPageMessage(`Retry started for ${activeRun.runId}. LinkedIn will reopen and the dashboard will keep tracking the same run.`);
      await loadDashboard({ quiet: true });
    } catch (error: unknown) {
      setPageError(error instanceof Error ? error.message : 'Failed to retry LinkedIn workflow run.');
    } finally {
      setIsStartingRun(false);
    }
  };

  const handleContinueRocketReach = async () => {
    if (!activeRun?.runId) {
      return;
    }
    setIsStartingRun(true);
    setPageError(null);
    setPageMessage(null);
    try {
      const baseUrl = normalizeBridgeUrl(localBridgeUrlRef.current);
      const url = baseUrl ? `${baseUrl}/api/pipeline/continue` : '/api/pipeline/continue';
      const headers = buildBridgeHeaders(baseUrl, { 'Content-Type': 'application/json' });

      const response = await fetch(url, {
        method: 'POST',
        headers,
        body: JSON.stringify({ runId: activeRun.runId }),
      });
      await readApiJson<{ run: WorkflowRunSummary }>(response, 'Failed to resume RocketReach enrichment.');
      setPageMessage(`Resuming enrichment for ${activeRun.runId}. The pipeline will check for the ROCKETREACH_API_KEY and continue if found.`);
      await loadDashboard({ quiet: true });
    } catch (error: unknown) {
      setPageError(error instanceof Error ? error.message : 'Failed to resume RocketReach enrichment.');
    } finally {
      setIsStartingRun(false);
    }
  };

  const handleRecruiterEnriched = async (payload: ManualRecruiterEnrichmentResponse) => {
    setPageMessage(`Manual RocketReach enrichment completed. Sendable contacts: ${payload.stats.sendable_rows}/${payload.stats.total}.`);
    await loadDashboard({ quiet: true });
  };

  const scrollToRef = (ref: React.RefObject<HTMLDivElement | null>) => {
    ref.current?.scrollIntoView({ behavior: 'smooth', block: 'start' });
  };

  const handleArtifactDownload = (runId: string, artifact: PipelineArtifactSummary['key']) => {
    const baseUrl = normalizeBridgeUrl(localBridgeUrlRef.current);
    const prefix = baseUrl ? baseUrl : '';
    const url = `${prefix}/api/pipeline/artifact?runId=${encodeURIComponent(runId)}&artifact=${encodeURIComponent(artifact)}`;
    window.open(url, '_blank', 'noopener,noreferrer');
  };

  const stageCards = activeRun?.stageStates ?? [
    {
      id: 'linkedin',
      label: 'Apply LinkedIn Jobs',
      status: 'idle',
      description: 'Run the LinkedIn automation and save applied jobs to CSV.',
      detail: 'Create a new workflow run to begin.',
    },
    {
      id: 'rocketreach',
      label: 'RocketReach Data',
      status: 'idle',
      description: 'Fetch recruiter emails and save the enrichment output.',
      detail: 'Use the enrichment panel for manual CSV enrichment.',
    },
    {
      id: 'email',
      label: 'Auto Email Campaign',
      status: 'idle',
      description: 'Review sendable contacts, then send and track outcomes.',
      detail: 'Email review unlocks after recruiter data is ready.',
    },
  ];

  return (
    <div className="workflow-app-shell">
      <aside className="workflow-sidebar">
        <div>
          <div className="brand-lockup">
            <div className="brand-badge">
              <Rocket className="h-5 w-5" />
            </div>
            <div>
              <h1 className="text-2xl font-semibold tracking-tight text-slate-950">RocketFlow</h1>
              <p className="text-sm text-slate-500">Automation workflows</p>
            </div>
          </div>

          <nav className="mt-10 space-y-2">
            <div className="sidebar-link sidebar-link-active">Dashboard</div>
            <div className="sidebar-link">Workflows</div>
            <div className="sidebar-link">CSV Manager</div>
            <div className="sidebar-link">Logs</div>
            <div className="sidebar-link">Settings</div>
          </nav>
        </div>

        <div className="rounded-[24px] border border-slate-200 bg-white/80 p-4 text-sm text-slate-600">
          <p className="font-medium text-slate-900">Need attention?</p>
          <p className="mt-2">SMTP stays server-side and the dashboard always reflects the latest pipeline manifest.</p>
        </div>
      </aside>

      <div className="workflow-main">
        <header className="workflow-topbar">
          <div className="flex items-center gap-3">
            <button type="button" className="icon-surface">
              <Menu className="h-5 w-5" />
            </button>
            <div className="search-shell">
              <Search className="h-4 w-4 text-slate-400" />
              <input type="text" placeholder="Search anything..." className="search-input" />
            </div>
          </div>
          <div className="flex items-center gap-3">
            <button type="button" onClick={() => void loadDashboard()} className="icon-surface">
              <RefreshCcw className={`h-4 w-4 ${isLoading ? 'animate-spin' : ''}`} />
            </button>
            <button type="button" onClick={handleOpenSettings} className="icon-surface" aria-label="Open settings">
              <Settings className="h-4 w-4" />
            </button>
            <button type="button" className="icon-surface">
              <Bell className="h-4 w-4" />
            </button>
            <div className="avatar-pill">A</div>
          </div>
        </header>

        <main className="workflow-content">
          <section className="mb-8 flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
            <div>
              <p className="eyebrow">Automation Workflows</p>
              <h2 className="text-4xl font-semibold tracking-tight text-slate-950">Fresh-run command center</h2>
              <p className="mt-2 max-w-3xl text-base text-slate-600">
                Start a brand-new LinkedIn-first run, clear stale conflicts automatically, track login/apply progress, then continue to recruiter enrichment and email review from one live dashboard.
              </p>
              <p className="mt-3 text-sm font-medium text-slate-500">
                Current focus: {activeRunLabel}
              </p>
            </div>
            <button
              type="button"
              onClick={() => void handleStartPipeline()}
              disabled={isStartingRun}
              className="hero-action-btn disabled:cursor-not-allowed disabled:opacity-60"
            >
              <Sparkles className="h-4 w-4" />
              {isStartingRun ? 'Starting Automation...' : 'Start Automation'}
            </button>
          </section>

          {isSettingsOpen ? (
            <div className="settings-overlay" role="dialog" aria-modal="true" aria-label="Workflow settings">
              <div className="settings-panel animate-scale-up">
                <div className="settings-panel-head">
                  <div>
                    <p className="eyebrow">Workflow Settings</p>
                    <h3 className="mt-2 text-2xl font-semibold text-slate-950">LinkedIn Apply Config</h3>
                    <p className="mt-2 max-w-2xl text-sm text-slate-500">
                      Shared login and profile data are saved into the Python config files used by every new workflow run.
                    </p>
                  </div>
                  <button type="button" className="icon-surface" onClick={() => setIsSettingsOpen(false)} aria-label="Close settings">
                    <X className="h-4 w-4" />
                  </button>
                </div>

                {configError ? <div className="message-banner message-banner-error mt-5">{configError}</div> : null}
                {isConfigLoading ? <div className="message-banner message-banner-info mt-5">Loading config...</div> : null}

                <div className="linkedin-login-card">
                  <div>
                    <p className="eyebrow">LinkedIn Login</p>
                    <h4>Shared browser credentials</h4>
                  </div>
                  <div className="linkedin-login-grid">
                    {renderConfigField('secrets', linkedInLoginFields.username)}
                    {renderConfigField('secrets', linkedInLoginFields.password)}
                    {renderConfigField('secrets', linkedInLoginFields.autoLogin)}
                  </div>
                </div>

                <div className="linkedin-login-card mt-5">
                  <div>
                    <p className="eyebrow">Hosted Worker</p>
                    <h4>Runs on the SaaS server</h4>
                  </div>
                  <div className="message-banner message-banner-info mt-4">
                    Automation starts from this deployed server. No ngrok URL or local laptop process is required.
                  </div>
                </div>

                <div className="settings-editor-shell">
                  <aside className="settings-tabs" aria-label="Config files">
                    {Object.entries(configPayload?.editable.files ?? {}).map(([section, file]) => (
                      <button
                        key={section}
                        type="button"
                        className={`settings-tab ${activeConfigSection === section ? 'settings-tab-active' : ''}`}
                        onClick={() => setActiveConfigSection(section)}
                      >
                        <span>{file.label}</span>
                        <small>{section}.py</small>
                      </button>
                    ))}
                  </aside>

                  <section className="settings-form-panel">
                    {configPayload?.editable.files[activeConfigSection] ? (
                      <>
                        <div className="settings-file-path">
                          {configPayload.editable.files[activeConfigSection].path}
                        </div>
                        <div className="config-form-grid">
                          {configPayload.editable.files[activeConfigSection].fields.map((field) => renderConfigField(activeConfigSection, field))}
                        </div>
                      </>
                    ) : (
                      <div className="message-banner message-banner-info">No config file loaded.</div>
                    )}
                  </section>
                </div>

                <div className="settings-footer">
                  <button type="button" className="stage-action-btn stage-action-secondary" onClick={() => void loadConfig()} disabled={isConfigLoading || isConfigSaving}>
                    <RefreshCcw className={`h-4 w-4 ${isConfigLoading ? 'animate-spin' : ''}`} />
                    Reload Config
                  </button>
                  <button type="button" className="stage-action-btn stage-action-secondary" onClick={() => void handleSaveConfig()} disabled={isConfigLoading || isConfigSaving}>
                    <Save className="h-4 w-4" />
                    {isConfigSaving ? 'Saving...' : 'Save Settings'}
                  </button>
                  <button
                    type="button"
                    className="hero-action-btn disabled:cursor-not-allowed disabled:opacity-60"
                    disabled={isStartingRun || isConfigLoading || isConfigSaving}
                    onClick={() => void handleStartFromSettings()}
                  >
                    <Play className="h-4 w-4" />
                    {isStartingRun ? 'Starting...' : 'Start Automation'}
                  </button>
                </div>
              </div>
            </div>
          ) : null}

          <section className="mb-6 grid gap-4 md:grid-cols-4">
            <div className="metric-card">
              <span className="metric-label">Applied Rows</span>
              <strong>{activeRun?.counts.appliedRows ?? 0}</strong>
            </div>
            <div className="metric-card">
              <span className="metric-label">Recruiter Rows</span>
              <strong>{activeRun?.counts.recruiterRows ?? 0}</strong>
            </div>
            <div className="metric-card">
              <span className="metric-label">Ready To Send</span>
              <strong>{activeRun?.counts.dedupedSendableRows ?? 0}</strong>
            </div>
            <div className="metric-card">
              <span className="metric-label">Email Sent</span>
              <strong>{activeRun?.counts.emailSent ?? 0}</strong>
            </div>
          </section>

          {pageMessage ? <div className="message-banner message-banner-info">{pageMessage}</div> : null}
          {pageError ? <div className="message-banner message-banner-error">{pageError}</div> : null}
          {activeRun?.status === 'waiting_login' ? (
            <div className="message-banner message-banner-info">
              {activeRun.note || 'Chrome opened with your default profile. Log into LinkedIn there and keep the browser window open.'}
            </div>
          ) : null}
          {dashboard.latestFailure ? (
            <div className="message-banner message-banner-warning">
              Latest failed run {dashboard.latestFailure.runId}: {dashboard.latestFailure.note || dashboard.latestFailure.lastError}
            </div>
          ) : null}

          <section className="dashboard-grid">
            <div className="space-y-6">
              <WorkflowStageCard
                index={1}
                stage={stageCards[0]}
                accentClass="accent-indigo"
                icon={<BriefcaseBusiness className="h-5 w-5" />}
                primaryLabel={activeRun?.retryEligible ? (isStartingRun ? 'Retrying...' : 'Retry Login') : (isStartingRun ? 'Starting...' : 'Start Automation')}
                onPrimary={activeRun?.retryEligible ? handleRetryPipeline : () => void handleStartPipeline()}
                disabledPrimary={isStartingRun}
                secondaryLabel="Reload"
                onSecondary={() => void loadDashboard()}
                downloadArtifact={activeRun?.artifacts.find((artifact) => artifact.key === 'appliedCsv') ?? null}
                onDownload={activeRun ? () => handleArtifactDownload(activeRun.runId, 'appliedCsv') : undefined}
              />

              <WorkflowStageCard
                index={2}
                stage={stageCards[1]}
                accentClass="accent-sky"
                icon={<Users className="h-5 w-5" />}
                primaryLabel={activeRun?.status === 'linkedin_completed_rocketreach_on_hold' ? (isStartingRun ? 'Resuming...' : 'Continue RocketReach') : 'Open Upload'}
                onPrimary={activeRun?.status === 'linkedin_completed_rocketreach_on_hold' ? handleContinueRocketReach : () => scrollToRef(enrichmentRef)}
                disabledPrimary={!activeRun || isStartingRun || ['rocketreach_running', 'email_running', 'waiting_review', 'sending', 'completed', 'manual_review'].includes(activeRun.status)}
                secondaryLabel="Download CSV"
                onSecondary={activeRun ? () => handleArtifactDownload(activeRun.runId, 'recruitersCsv') : undefined}
                disabledSecondary={!activeRun?.artifacts.find((artifact) => artifact.key === 'recruitersCsv' && artifact.available)}
                downloadArtifact={null}
              />

              <WorkflowStageCard
                index={3}
                stage={stageCards[2]}
                accentClass="accent-violet"
                icon={<Mail className="h-5 w-5" />}
                primaryLabel={status === 'paused' || status === 'stopped' ? 'Resume' : 'Start'}
                onPrimary={() => {
                  scrollToRef(sendRef);
                  handleEmailStart();
                }}
                disabledPrimary={Boolean(sendBlockedReason) || contacts.length === 0}
                secondaryLabel="Edit Template"
                onSecondary={() => scrollToRef(templateRef)}
                downloadArtifact={activeRun?.artifacts.find((artifact) => artifact.key === 'sendReportCsv') ?? null}
                onDownload={activeRun ? () => handleArtifactDownload(activeRun.runId, 'sendReportCsv') : undefined}
              />

              <div ref={enrichmentRef}>
                <RecruiterEnricher 
                  onEnriched={(payload) => void handleRecruiterEnriched(payload)} 
                  bridgeUrl={localBridgeUrl}
                />
              </div>
            </div>

            <div className="space-y-6">
              <WorkflowProgressPanel run={activeRun} />

              <div className="workflow-card">
                <div className="flex items-center justify-between gap-3">
                  <div>
                    <p className="eyebrow">Sender Identity</p>
                    <h3 className="text-2xl font-semibold text-slate-950">Campaign Settings</h3>
                  </div>
                </div>
                <label className="mt-5 block text-sm font-medium text-slate-700">Sender Name</label>
                <input
                  type="text"
                  value={senderName}
                  onChange={(e) => setSenderName(e.target.value)}
                  placeholder="e.g. Om Patel"
                  className="mt-2 w-full rounded-2xl border border-slate-200 bg-white px-4 py-3 text-sm text-slate-900 outline-none transition focus:border-sky-400 focus:ring-4 focus:ring-sky-100"
                />
                <p className="mt-3 text-xs text-slate-500">
                  Used as <code className="rounded bg-slate-100 px-1 py-0.5">{'{{sendername}}'}</code> in your email template.
                </p>
              </div>

              <div ref={templateRef}>
                <TemplateEditor
                  templateBody={templateBody}
                  setTemplateBody={setTemplateBody}
                  templateSubject={templateSubject}
                  setTemplateSubject={setTemplateSubject}
                  previewContact={contacts[0] ?? null}
                  senderName={senderName}
                />
              </div>

              <div ref={sendRef}>
                <SendDashboard
                  status={status}
                  total={contacts.length}
                  sent={sentCount}
                  failed={failedCount}
                  activeEmail={activeEmail}
                  delayStr={delayStr}
                  setDelayStr={setDelayStr}
                  onStart={handleEmailStart}
                  onPause={handlePause}
                  onStop={handleStop}
                  logs={logs}
                  canStart={!sendBlockedReason}
                  blockedReason={sendBlockedReason}
                />
              </div>
            </div>

            <div className="space-y-6">
              <WorkflowPreviewPanel run={activeRun} selectedTab={selectedTab} onTabChange={setSelectedTab} />
              <SMTPSettings />
            </div>
          </section>

          <section className="mt-8">
            <LiveLogs runId={activeRun?.runId ?? null} bridgeUrl={localBridgeUrl} />
          </section>

          <section className="mt-8">
            <WorkflowRunsTable runs={dashboard.recentRuns} onDownload={handleArtifactDownload} />
          </section>
        </main>
      </div>
    </div>
  );
}

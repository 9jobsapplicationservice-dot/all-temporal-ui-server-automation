import React from 'react';
import { AlertTriangle, CheckCircle2, Clock3, LoaderCircle, ShieldAlert } from 'lucide-react';
import type { WorkflowRunSummary, WorkflowStageState } from '@/types';

function stageIcon(status: WorkflowStageState['status']) {
  switch (status) {
    case 'completed':
      return <CheckCircle2 className="h-5 w-5 text-emerald-600" />;
    case 'running':
      return <LoaderCircle className="h-5 w-5 animate-spin text-sky-600" />;
    case 'waiting':
      return <Clock3 className="h-5 w-5 text-amber-600" />;
    case 'failed':
      return <AlertTriangle className="h-5 w-5 text-rose-600" />;
    case 'blocked':
      return <ShieldAlert className="h-5 w-5 text-rose-700" />;
    case 'on_hold':
      return <Clock3 className="h-5 w-5 text-amber-600" />;
    default:
      return <div className="h-3 w-3 rounded-full bg-slate-300" />;
  }
}

type WorkflowProgressPanelProps = {
  run: WorkflowRunSummary | null;
};

export default function WorkflowProgressPanel({ run }: WorkflowProgressPanelProps) {
  const statusLabel = !run
    ? ''
    : run.status === 'queued' && run.note.toLowerCase().includes('fresh run')
      ? 'Fresh Starting'
      : run.status === 'waiting_login'
        ? 'Waiting For LinkedIn Login'
        : run.status === 'linkedin_running'
          ? 'Applying Jobs'
          : run.status === 'rocketreach_running'
            ? 'Enriching Recruiters'
            : (run.status === 'waiting_review' || run.status === 'manual_review') && run.counts.dedupedSendableRows > 0
              ? 'Ready To Send'
              : run.status.replaceAll('_', ' ');
  return (
    <section className="workflow-card">
      <div className="flex items-center justify-between gap-3">
        <div>
          <p className="eyebrow">Workflow Settings</p>
          <h2 className="text-2xl font-semibold text-slate-950">Stage Progress</h2>
        </div>
        {run ? <span className="status-chip">{statusLabel}</span> : null}
      </div>

      {!run ? (
        <p className="mt-5 text-sm text-slate-500">No pipeline run is active yet. Start the LinkedIn stage to begin a new workflow.</p>
      ) : (
        <div className="mt-6 space-y-4">
          <div className="grid grid-cols-2 gap-3 rounded-[28px] border border-slate-200 bg-slate-50/80 p-4 text-sm text-slate-600">
            <div>
              <span className="text-slate-400">Run ID</span>
              <p className="mt-1 font-medium text-slate-900">{run.runId}</p>
            </div>
            <div>
              <span className="text-slate-400">Updated</span>
              <p className="mt-1 font-medium text-slate-900">{run.updatedAt.replace('T', ' ').slice(0, 16)}</p>
            </div>
            <div>
              <span className="text-slate-400">Sendable</span>
              <p className="mt-1 font-medium text-slate-900">{run.counts.dedupedSendableRows}</p>
            </div>
            <div>
              <span className="text-slate-400">Email Sent</span>
              <p className="mt-1 font-medium text-slate-900">{run.counts.emailSent}</p>
            </div>
            <div>
              <span className="text-slate-400">LinkedIn Login</span>
              <p className="mt-1 font-medium capitalize text-slate-900">{run.automation?.linkedin?.mode?.replace('_', ' ') || 'saved session'}</p>
            </div>
            <div>
              <span className="text-slate-400">Chrome Profile</span>
              <p className="mt-1 font-medium text-slate-900">{run.automation?.linkedin?.safe_mode ? 'Guest / Safe Mode' : 'Default Profile'}</p>
            </div>
          </div>

          <div className="space-y-4">
            {run.stageStates.map((stage, index) => (
              <div key={stage.id} className="relative rounded-[26px] border border-slate-200 bg-white/80 p-5">
                {index < run.stageStates.length - 1 ? <div className="stage-connector" aria-hidden="true" /> : null}
                <div className="flex items-start gap-4">
                  <div className="mt-1 shrink-0">{stageIcon(stage.status)}</div>
                  <div className="min-w-0 flex-1">
                    <div className="flex items-center justify-between gap-3">
                      <h3 className="text-lg font-semibold text-slate-950">{stage.label}</h3>
                      <span className="stage-inline-pill">{stage.status}</span>
                    </div>
                    <p className="mt-2 text-sm text-slate-600">{stage.description}</p>
                    <p className="mt-2 text-sm text-slate-500">{stage.detail}</p>
                  </div>
                </div>
              </div>
            ))}
          </div>

          {run.note ? (
            <div className="rounded-[24px] border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm text-emerald-900">
              {run.note}
            </div>
          ) : null}

          {run.lastError ? (
            <div className="rounded-[24px] border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-800">
              {run.lastError}
            </div>
          ) : null}
        </div>
      )}
    </section>
  );
}

'use client';

import React from 'react';
import { ArrowDownToLine, ArrowUpFromLine, CircleDot, Play, Settings2 } from 'lucide-react';
import type { PipelineArtifactSummary, WorkflowStageState } from '@/types';

type WorkflowStageCardProps = {
  index: number;
  stage: WorkflowStageState;
  accentClass: string;
  icon: React.ReactNode;
  onPrimary?: () => void;
  onSecondary?: () => void;
  onDownload?: () => void;
  primaryLabel?: string;
  secondaryLabel?: string;
  downloadArtifact?: PipelineArtifactSummary | null;
  disabledPrimary?: boolean;
  disabledSecondary?: boolean;
};

function statusTone(status: WorkflowStageState['status']) {
  switch (status) {
    case 'completed':
      return 'bg-emerald-100 text-emerald-700';
    case 'running':
      return 'bg-sky-100 text-sky-700';
    case 'waiting':
      return 'bg-amber-100 text-amber-800';
    case 'blocked':
    case 'failed':
      return 'bg-rose-100 text-rose-700';
    case 'queued':
      return 'bg-violet-100 text-violet-700';
    case 'on_hold':
      return 'bg-amber-100 text-amber-800';
    default:
      return 'bg-slate-100 text-slate-600';
  }
}

export default function WorkflowStageCard({
  index,
  stage,
  accentClass,
  icon,
  onPrimary,
  onSecondary,
  onDownload,
  primaryLabel = 'Start',
  secondaryLabel = 'Open Settings',
  downloadArtifact,
  disabledPrimary = false,
  disabledSecondary = false,
}: WorkflowStageCardProps) {
  return (
    <article className="workflow-card stage-card">
      <div className="flex items-start justify-between gap-3">
        <div className="flex items-center gap-3">
          <div className={`stage-number ${accentClass}`}>{index}</div>
          <div className={`stage-icon ${accentClass}`}>{icon}</div>
        </div>
        <span className={`stage-pill ${statusTone(stage.status)}`}>{stage.status.replace('_', ' ')}</span>
      </div>

      <div className="mt-5 space-y-2">
        <h3 className="text-xl font-semibold text-slate-950">{stage.label}</h3>
        <p className="text-sm text-slate-600">{stage.description}</p>
        <p className="text-sm text-slate-500">{stage.detail}</p>
      </div>

      <div className="mt-5 flex flex-wrap gap-3">
        {onPrimary ? (
          <button
            type="button"
            onClick={onPrimary}
            disabled={disabledPrimary}
            className="stage-action-btn stage-action-primary disabled:opacity-50 disabled:cursor-not-allowed"
          >
            <Play className="h-4 w-4" />
            {primaryLabel}
          </button>
        ) : null}

        {onSecondary ? (
          <button
            type="button"
            onClick={onSecondary}
            disabled={disabledSecondary}
            className="stage-action-btn stage-action-secondary disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {secondaryLabel.toLowerCase().includes('upload') ? <ArrowUpFromLine className="h-4 w-4" /> : <Settings2 className="h-4 w-4" />}
            {secondaryLabel}
          </button>
        ) : null}

        {downloadArtifact && downloadArtifact.available && onDownload ? (
          <button type="button" onClick={onDownload} className="stage-action-btn stage-action-secondary">
            <ArrowDownToLine className="h-4 w-4" />
            Download
          </button>
        ) : null}
      </div>

      <div className="mt-5 flex items-center gap-2 text-xs uppercase tracking-[0.24em] text-slate-400">
        <CircleDot className="h-3.5 w-3.5" />
        workflow stage
      </div>
    </article>
  );
}

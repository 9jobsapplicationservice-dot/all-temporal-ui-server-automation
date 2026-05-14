'use client';

import React from 'react';
import { BriefcaseBusiness, MailCheck, Users } from 'lucide-react';
import type { WorkflowRunSummary } from '@/types';

type PreviewTab = 'applied' | 'recruiters' | 'email';

type WorkflowPreviewPanelProps = {
  run: WorkflowRunSummary | null;
  selectedTab: PreviewTab;
  onTabChange: (tab: PreviewTab) => void;
};

export default function WorkflowPreviewPanel({ run, selectedTab, onTabChange }: WorkflowPreviewPanelProps) {
  const tabs: Array<{ key: PreviewTab; label: string; icon: React.ReactNode }> = [
    { key: 'applied', label: 'Applied Jobs', icon: <BriefcaseBusiness className="h-4 w-4" /> },
    { key: 'recruiters', label: 'RocketReach Data', icon: <Users className="h-4 w-4" /> },
    { key: 'email', label: 'Email Logs', icon: <MailCheck className="h-4 w-4" /> },
  ];

  const content = !run ? [] : selectedTab === 'applied'
    ? run.preview.appliedJobs
    : selectedTab === 'recruiters'
      ? run.preview.recruiters
      : run.preview.emailLogs;

  return (
    <section className="workflow-card h-full">
      <div className="flex items-start justify-between gap-3">
        <div>
          <p className="eyebrow">Live Preview</p>
          <h2 className="text-2xl font-semibold text-slate-950">Run Snapshot</h2>
        </div>
        <span className="status-chip status-chip-live">Live</span>
      </div>

      <div className="mt-5 flex flex-wrap gap-2 border-b border-slate-200 pb-3">
        {tabs.map((tab) => (
          <button
            key={tab.key}
            type="button"
            onClick={() => onTabChange(tab.key)}
            className={`preview-tab ${selectedTab === tab.key ? 'preview-tab-active' : ''}`}
          >
            {tab.icon}
            {tab.label}
          </button>
        ))}
      </div>

      {!run ? (
        <p className="mt-6 text-sm text-slate-500">A workflow preview will appear here once a run is available.</p>
      ) : content.length === 0 ? (
        <p className="mt-6 text-sm text-slate-500">No preview rows available for this stage yet.</p>
      ) : (
        <div className="mt-5 space-y-3">
          {selectedTab === 'applied' && run.preview.appliedJobs.map((row, index) => (
            <div key={`${row.jobLink}-${index}`} className="preview-row">
              <div>
                <p className="preview-title">{row.position || 'Untitled role'}</p>
                <p className="preview-meta">{row.company || 'Unknown company'}{row.hrName ? ` • ${row.hrName}` : ''}</p>
              </div>
              <div className="text-right">
                <p className="preview-time">{row.date || 'Pending'}</p>
                <span className="preview-pill">{row.submitted || 'Pending'}</span>
              </div>
            </div>
          ))}

          {selectedTab === 'recruiters' && run.preview.recruiters.map((row, index) => (
            <div key={`${row.email}-${index}`} className="preview-row">
              <div>
                <p className="preview-title">{row.name}</p>
                <p className="preview-meta">{row.company || 'Unknown company'}{row.position ? ` • ${row.position}` : ''}</p>
              </div>
              <div className="text-right">
                <p className="preview-time">{row.email || row.secondaryEmail || 'Email pending'}</p>
                <span className="preview-pill">{row.status || 'pending'}</span>
              </div>
            </div>
          ))}

          {selectedTab === 'email' && run.preview.emailLogs.map((row, index) => (
            <div key={`${row.email}-${row.timestamp}-${index}`} className="preview-row">
              <div>
                <p className="preview-title">{row.email}</p>
                <p className="preview-meta">{row.company || 'Unknown company'}{row.position ? ` • ${row.position}` : ''}</p>
              </div>
              <div className="text-right">
                <p className="preview-time">{row.timestamp || 'Recently'}</p>
                <span className={`preview-pill ${row.success ? 'preview-pill-success' : 'preview-pill-danger'}`}>
                  {row.success ? 'Sent' : 'Failed'}
                </span>
              </div>
            </div>
          ))}
        </div>
      )}
    </section>
  );
}

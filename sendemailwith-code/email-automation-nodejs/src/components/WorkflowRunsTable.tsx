import React from 'react';
import { ArrowDownToLine } from 'lucide-react';
import type { PipelineArtifactSummary, WorkflowRunSummary } from '@/types';

type WorkflowRunsTableProps = {
  runs: WorkflowRunSummary[];
  onDownload: (runId: string, artifact: PipelineArtifactSummary['key']) => void;
};

function preferredArtifact(run: WorkflowRunSummary): PipelineArtifactSummary['key'] | null {
  if (run.artifacts.find((artifact) => artifact.key === 'sendReportCsv' && artifact.available)) {
    return 'sendReportCsv';
  }
  if (run.artifacts.find((artifact) => artifact.key === 'recruitersCsv' && artifact.available)) {
    return 'recruitersCsv';
  }
  if (run.artifacts.find((artifact) => artifact.key === 'appliedCsv' && artifact.available)) {
    return 'appliedCsv';
  }
  return null;
}

export default function WorkflowRunsTable({ runs, onDownload }: WorkflowRunsTableProps) {
  return (
    <section className="workflow-card">
      <div className="flex items-center justify-between gap-3">
        <div>
          <p className="eyebrow">Workflow Logs</p>
          <h2 className="text-2xl font-semibold text-slate-950">Recent Runs</h2>
        </div>
      </div>

      {runs.length === 0 ? (
        <p className="mt-5 text-sm text-slate-500">No workflow runs found yet.</p>
      ) : (
        <div className="mt-5 overflow-x-auto">
          <table className="w-full min-w-[760px] text-left text-sm">
            <thead>
              <tr className="border-b border-slate-200 text-xs uppercase tracking-[0.24em] text-slate-400">
                <th className="pb-3">Run</th>
                <th className="pb-3">Status</th>
                <th className="pb-3">Counts</th>
                <th className="pb-3">Updated</th>
                <th className="pb-3">Note</th>
                <th className="pb-3 text-right">Action</th>
              </tr>
            </thead>
            <tbody>
              {runs.map((run) => {
                const artifact = preferredArtifact(run);
                return (
                  <tr key={run.runId} className="border-b border-slate-100 align-top last:border-0">
                    <td className="py-4 font-medium text-slate-900">{run.runId}</td>
                    <td className="py-4">
                      <span className="stage-inline-pill">{run.status.replace('_', ' ')}</span>
                    </td>
                    <td className="py-4 text-slate-600">
                      applied={run.counts.appliedRows} recruiters={run.counts.recruiterRows} sent={run.counts.emailSent}
                    </td>
                    <td className="py-4 text-slate-500">{run.updatedAt.replace('T', ' ').slice(0, 16)}</td>
                    <td className="py-4 text-slate-500">{run.note || run.lastError || '-'}</td>
                    <td className="py-4 text-right">
                      {artifact ? (
                        <button type="button" onClick={() => onDownload(run.runId, artifact)} className="table-action-btn">
                          <ArrowDownToLine className="h-4 w-4" />
                          Download
                        </button>
                      ) : (
                        <span className="text-slate-300">No artifact</span>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}

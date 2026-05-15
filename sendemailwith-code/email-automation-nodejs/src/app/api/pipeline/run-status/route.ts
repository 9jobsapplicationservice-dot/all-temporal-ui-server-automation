import { NextResponse } from 'next/server';
import { writeSendReport, updatePipelineRunStatus } from '@/lib/pipeline';
import type { EmailLog, PipelineRunStatus } from '@/types';

type RunStatusPayload = {
  runId?: string;
  status?: PipelineRunStatus;
  note?: string;
  logs?: EmailLog[];
};

function buildSummaryNote(baseNote: string, logs: EmailLog[]): string {
  const sent = logs.filter((log) => log.success).length;
  const failed = logs.filter((log) => !log.success).length;
  const parts = [baseNote.trim(), `sent=${sent}`, `failed=${failed}`].filter(Boolean);
  return parts.join(' | ');
}

export async function POST(req: Request) {
  try {
    const body = (await req.json()) as RunStatusPayload;
    const runId = body.runId?.trim();
    const status = body.status;
    const logs = Array.isArray(body.logs) ? body.logs : [];

    if (!runId || !status) {
      return NextResponse.json({ error: 'runId and status are required' }, { status: 400 });
    }

    const note = buildSummaryNote(body.note ?? '', logs);
    let reportPath = '';
    if (logs.length > 0 && status !== 'sending') {
      reportPath = await writeSendReport(runId, logs);
    }

    updatePipelineRunStatus(runId, status, note);
    return NextResponse.json({ success: true, reportPath });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : 'Failed to update pipeline run status';
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

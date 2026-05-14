import { NextResponse } from 'next/server';
import { retryPipelineRun } from '@/lib/pipeline';

export async function POST(req: Request) {
  try {
    const body = await req.json().catch(() => ({}));
    const runId = typeof body.runId === 'string' ? body.runId.trim() : '';
    if (!runId) {
      return NextResponse.json({ error: 'runId is required.' }, { status: 400 });
    }

    const { run } = await retryPipelineRun(runId);
    return NextResponse.json({ run });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : 'Failed to retry pipeline run.';
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

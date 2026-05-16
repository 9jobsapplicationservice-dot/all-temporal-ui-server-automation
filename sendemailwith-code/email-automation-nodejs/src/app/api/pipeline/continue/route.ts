import { NextResponse } from 'next/server';
import { retryPipelineRun } from '@/lib/pipeline';

export async function POST(req: Request) {
  console.log('[API] /api/pipeline/continue - Received request to resume RocketReach enrichment');
  try {
    const body = await req.json();
    const runId = body.runId;
    if (!runId) {
      return NextResponse.json({ error: 'runId is required' }, { status: 400 });
    }
    console.log(`[API] Resuming enrichment for runId: ${runId}`);
    const { run } = await retryPipelineRun(runId);
    return NextResponse.json({ run });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : 'Failed to resume RocketReach enrichment.';
    console.error(`[API] Error resuming enrichment: ${message}`);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

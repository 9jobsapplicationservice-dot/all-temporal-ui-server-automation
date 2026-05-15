import { NextResponse } from 'next/server';
import { startPipelineRun } from '@/lib/pipeline';

export async function POST(req: Request) {
  console.log('[API] /api/pipeline/start - Received request to start LinkedIn automation');
  try {
    const body = await req.json().catch(() => ({}));
    const configPath = typeof body.configPath === 'string' ? body.configPath.trim() : undefined;
    console.log(`[API] Starting pipeline with configPath: ${configPath || 'default'}`);
    const { run } = await startPipelineRun(configPath);
    console.log(`[API] Pipeline started successfully. runId: ${run.runId}`);
    return NextResponse.json({ run });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : 'Failed to start pipeline run.';
    console.error(`[API] Error starting pipeline: ${message}`);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

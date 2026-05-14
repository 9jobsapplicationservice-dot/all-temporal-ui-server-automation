import { NextResponse } from 'next/server';
import { startPipelineRun } from '@/lib/pipeline';

export async function POST(req: Request) {
  try {
    const body = await req.json().catch(() => ({}));
    const configPath = typeof body.configPath === 'string' ? body.configPath.trim() : undefined;
    const { run } = await startPipelineRun(configPath);
    return NextResponse.json({ run });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : 'Failed to start pipeline run.';
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

import { NextResponse } from 'next/server';
import { findLatestPipelineRun } from '@/lib/pipeline';

export async function GET() {
  try {
    const payload = await findLatestPipelineRun();
    return NextResponse.json(payload);
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : 'Failed to load pipeline run';
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

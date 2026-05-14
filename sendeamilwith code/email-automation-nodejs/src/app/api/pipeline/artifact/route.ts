import fs from 'node:fs/promises';
import { NextResponse } from 'next/server';
import { getArtifactForRun } from '@/lib/pipeline';
import type { PipelineArtifactSummary } from '@/types';

const ALLOWED_KEYS = new Set<PipelineArtifactSummary['key']>([
  'appliedCsv',
  'recruitersCsv',
  'sendReportCsv',
  'linkedinStdout',
  'linkedinStderr',
  'rocketreachStdout',
  'rocketreachStderr',
]);

export async function GET(req: Request) {
  try {
    const { searchParams } = new URL(req.url);
    const runId = searchParams.get('runId')?.trim() || '';
    const artifactKey = (searchParams.get('artifact')?.trim() || '') as PipelineArtifactSummary['key'];

    if (!runId || !ALLOWED_KEYS.has(artifactKey)) {
      return NextResponse.json({ error: 'runId and a valid artifact key are required.' }, { status: 400 });
    }

    const { filePath, fileName } = await getArtifactForRun(runId, artifactKey);
    const fileBuffer = await fs.readFile(filePath);

    return new NextResponse(fileBuffer, {
      status: 200,
      headers: {
        'Content-Disposition': `attachment; filename="${fileName}"`,
        'Content-Type': 'text/plain; charset=utf-8',
      },
    });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : 'Failed to download artifact.';
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

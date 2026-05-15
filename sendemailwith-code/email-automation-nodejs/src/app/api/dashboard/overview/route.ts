import { NextResponse } from 'next/server';
import { findWorkflowDashboard } from '@/lib/pipeline';

export async function GET() {
  const start = Date.now();
  console.log('[API] /api/dashboard/overview - Start');
  try {
    const payload = await findWorkflowDashboard();
    const duration = Date.now() - start;
    console.log(`[API] /api/dashboard/overview - Success (${duration}ms)`);
    return NextResponse.json(payload);
  } catch (error: unknown) {
    const duration = Date.now() - start;
    const message = error instanceof Error ? error.message : 'Failed to load workflow dashboard.';
    console.error(`[API] /api/dashboard/overview - Error after ${duration}ms: ${message}`);
    // Return a 200 with error info so the UI doesn't show a hard 502/error
    return NextResponse.json({ 
      activeRun: null, 
      recentRuns: [], 
      latestFailure: null,
      error: message 
    });
  }
}

import { NextResponse } from 'next/server';
import { findWorkflowDashboard } from '@/lib/pipeline';

export async function GET() {
  try {
    const payload = await findWorkflowDashboard();
    return NextResponse.json(payload);
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : 'Failed to load workflow dashboard.';
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

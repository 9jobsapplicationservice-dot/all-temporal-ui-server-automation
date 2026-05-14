import { NextResponse } from 'next/server';
import { getWorkflowConfig, saveWorkflowConfig } from '@/lib/pipeline';
import type { EditableLinkedInConfigUpdates } from '@/types';

export async function GET() {
  try {
    const payload = await getWorkflowConfig();
    return NextResponse.json(payload);
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : 'Failed to load automation config.';
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

export async function POST(req: Request) {
  try {
    const body = await req.json().catch(() => ({}));
    const updates = body && typeof body === 'object' && 'updates' in body
      ? (body.updates as EditableLinkedInConfigUpdates)
      : {};
    const payload = await saveWorkflowConfig(updates);
    return NextResponse.json(payload);
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : 'Failed to save automation config.';
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

import { NextResponse } from 'next/server';
import { resetPipelineRun } from '@/lib/pipeline';

export async function POST() {
  try {
    await resetPipelineRun();
    return NextResponse.json({ success: true, message: 'Pipeline environment reset successfully.' });
  } catch (error) {
    console.error('[API/Pipeline/Reset] Error:', error);
    return NextResponse.json({ 
      success: false, 
      error: error instanceof Error ? error.message : 'Unknown error during reset' 
    }, { status: 500 });
  }
}

import { NextResponse } from 'next/server';
import { createManualRecruiterEnrichment } from '@/lib/pipeline';

export async function POST(req: Request) {
  try {
    const formData = await req.formData();
    const file = formData.get('file');
    if (!(file instanceof File)) {
      return NextResponse.json({ error: 'CSV file is required.' }, { status: 400 });
    }

    const arrayBuffer = await file.arrayBuffer();
    const payload = await createManualRecruiterEnrichment(file.name, Buffer.from(arrayBuffer));
    return NextResponse.json(payload);
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : 'Failed to enrich recruiter CSV.';
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

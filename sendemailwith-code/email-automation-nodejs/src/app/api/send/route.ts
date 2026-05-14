import { NextResponse } from 'next/server';
import { getSMTPConfig, getSMTPTransporter } from '@/lib/smtp';

const TRANSIENT_SMTP_MARKERS = [
  'etimedout',
  'econnreset',
  'econnrefused',
  'connection closed',
  'connection reset',
  'timed out',
  'greeting never received',
  '421',
  '450',
  '451',
  '452',
];

function delay(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isTransientSMTPError(error: unknown) {
  const message = error instanceof Error ? error.message.toLowerCase() : String(error).toLowerCase();
  return TRANSIENT_SMTP_MARKERS.some((marker) => message.includes(marker));
}

export async function POST(req: Request) {
  try {
    const body = await req.json();
    const { to, subject, text } = body;

    if (!to || !subject || !text) {
      return NextResponse.json({ error: 'Missing required fields' }, { status: 400 });
    }

    const smtp = getSMTPConfig();
    const transporter = getSMTPTransporter();

    let lastError: unknown = null;
    for (let attempt = 1; attempt <= 3; attempt += 1) {
      try {
        const info = await transporter.sendMail({
          from: smtp.from,
          to,
          subject,
          text,
        });

        return NextResponse.json({ success: true, messageId: info.messageId, attempt });
      } catch (error: unknown) {
        lastError = error;
        if (attempt === 3 || !isTransientSMTPError(error)) {
          throw error;
        }
        await delay(attempt * 1000);
      }
    }

    throw lastError;
  } catch (error: unknown) {
    console.error('Email sending error:', error);
    const message = error instanceof Error ? error.message : 'Failed to send email';
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

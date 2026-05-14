import fs from 'node:fs';
import path from 'node:path';
import Papa from 'papaparse';
import nodemailer from 'nodemailer';

type ContactRow = {
  id?: string;
  email: string;
  name?: string;
  company?: string;
  position?: string;
  [key: string]: unknown;
};

type SMTPConfig = {
  host: string;
  port: number;
  secure: boolean;
  user: string;
  pass: string;
  from: string;
};

function loadEnvFile(envPath: string) {
  const raw = fs.readFileSync(envPath, 'utf8');
  for (const line of raw.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;

    const separatorIndex = trimmed.indexOf('=');
    if (separatorIndex === -1) continue;

    const key = trimmed.slice(0, separatorIndex).trim();
    const value = trimmed.slice(separatorIndex + 1).trim();
    if (key && !(key in process.env)) {
      process.env[key] = value;
    }
  }
}

function readRequiredEnv(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) {
    throw new Error(`Missing required SMTP environment variable: ${name}`);
  }
  return value;
}

function readSMTPConfig(): SMTPConfig {
  const host = readRequiredEnv('SMTP_HOST');
  const port = Number(readRequiredEnv('SMTP_PORT'));
  const secure = (process.env.SMTP_SECURE ?? 'false').trim().toLowerCase() === 'true';
  const user = readRequiredEnv('SMTP_USER');
  const pass = readRequiredEnv('SMTP_PASS');
  const from = process.env.SMTP_FROM?.trim() || user;

  if (Number.isNaN(port) || port <= 0) {
    throw new Error('SMTP_PORT must be a valid positive number');
  }

  return { host, port, secure, user, pass, from };
}

function parseCSV(filePath: string): Record<string, string>[] {
  const raw = fs.readFileSync(filePath, 'utf8');
  const result = Papa.parse<Record<string, string>>(raw, {
    header: true,
    skipEmptyLines: true,
  });

  if (result.errors.length > 0) {
    throw new Error(`CSV parse failed: ${result.errors[0]?.message ?? 'Unknown error'}`);
  }

  return result.data;
}

function normalizeRows(rows: Record<string, string>[]): ContactRow[] {
  const hasEmail = rows.length > 0 && Object.keys(rows[0]).some((key) => key.toLowerCase().includes('email'));
  if (!hasEmail) {
    throw new Error('CSV must contain at least one email column');
  }

  return rows.map((row, index) => {
    const normalized: ContactRow = { id: index.toString(), email: '' };

    for (const key in row) {
      const value = row[key];
      const lowerKey = key.toLowerCase().trim();

      if (lowerKey.includes('email') && value && value.includes('@')) {
        const currentEmails = normalized.email
          ? normalized.email.split(',').map((email) => email.trim()).filter(Boolean)
          : [];
        const newEmails = value.split(',').map((email) => email.trim()).filter((email) => email.includes('@'));

        for (const email of newEmails) {
          if (!currentEmails.includes(email)) currentEmails.push(email);
        }

        if (currentEmails.length > 0) {
          normalized.email = currentEmails.join(', ');
        }
      }

      if (lowerKey.includes('name') && !lowerKey.includes('company') && !lowerKey.includes('file') && !normalized.name && value) {
        normalized.name = value;
      }

      const isCompanyHeader = lowerKey === 'company' || lowerKey.includes('company') || lowerKey.includes('employer') || lowerKey.includes('organization') || lowerKey.includes('work');
      if (isCompanyHeader && !normalized.company && value) {
        normalized.company = value;
      }

      const isPositionHeader = lowerKey === 'position' || lowerKey.includes('position') || lowerKey.includes('title') || lowerKey.includes('role') || lowerKey.includes('job');
      if (isPositionHeader && !normalized.position && value) {
        normalized.position = value;
      }

      normalized[lowerKey] = value;
    }

    if (!normalized.company && normalized.email) {
      const firstEmail = normalized.email.split(',')[0]?.trim();
      const domainMatch = firstEmail?.match(/@([^.]+)\./);
      if (domainMatch?.[1]) {
        const domain = domainMatch[1];
        const genericProviders = ['gmail', 'outlook', 'hotmail', 'yahoo', 'icloud', 'me', 'msn', 'live'];
        if (!genericProviders.includes(domain.toLowerCase())) {
          normalized.company = domain
            .split(/[-_]/)
            .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
            .join('-');
        }
      }
    }

    return normalized;
  }).filter((row) => row.email);
}

function buildSubject(contact: ContactRow): string {
  const company = contact.company || 'your team';
  const position = contact.position || 'the role';
  return `Application follow-up for ${position} at ${company}`;
}

function buildBody(contact: ContactRow): string {
  const name = contact.name || 'Hiring Team';
  const company = contact.company || 'your company';
  const position = contact.position || 'the role';

  return [
    `Hi ${name},`,
    '',
    `I recently applied for the ${position} opportunity at ${company} and wanted to follow up directly.`,
    'I am very interested in the role and would appreciate the chance to speak with your team if there is a fit.',
    '',
    'Please let me know if I can share any additional information.',
    '',
    'Best regards,',
    'Candidate Follow-Up Test',
  ].join('\n');
}

async function main() {
  const repoRoot = path.resolve(__dirname, '..');
  const envPath = path.join(repoRoot, '.env.local');
  const csvArg = process.argv[2];

  if (!csvArg) {
    throw new Error('Usage: node send-csv.test.js <path-to-csv>');
  }

  loadEnvFile(envPath);
  const smtp = readSMTPConfig();
  const csvPath = path.resolve(csvArg);
  const rows = parseCSV(csvPath);
  const contacts = normalizeRows(rows);

  if (contacts.length === 0) {
    throw new Error('No sendable contacts found in CSV after normalization');
  }

  const transporter = nodemailer.createTransport({
    host: smtp.host,
    port: smtp.port,
    secure: smtp.secure,
    auth: {
      user: smtp.user,
      pass: smtp.pass,
    },
  });

  await transporter.verify();
  console.log(`SMTP verified for ${smtp.user}`);
  console.log(`Found ${contacts.length} sendable contact rows in ${path.basename(csvPath)}`);

  let sent = 0;
  for (const [index, contact] of contacts.entries()) {
    const info = await transporter.sendMail({
      from: smtp.from,
      to: contact.email,
      subject: buildSubject(contact),
      text: buildBody(contact),
    });

    sent += 1;
    console.log(`[${index + 1}/${contacts.length}] Sent to ${contact.email} | messageId=${info.messageId}`);
  }

  console.log(`Completed send run. Emails sent: ${sent}`);
}

main().catch((error: unknown) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`Send test failed: ${message}`);
  process.exit(1);
});

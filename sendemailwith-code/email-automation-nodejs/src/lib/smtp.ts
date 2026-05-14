import 'server-only';
import nodemailer from 'nodemailer';

type SMTPEnv = {
  host: string;
  port: number;
  secure: boolean;
  user: string;
  pass: string;
  from: string;
};

let cachedTransporter: nodemailer.Transporter | null = null;

function readRequiredEnv(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) {
    throw new Error(`Missing required SMTP environment variable: ${name}`);
  }
  return value;
}

function parseBoolean(value: string): boolean {
  return value.trim().toLowerCase() === 'true';
}

function getSMTPEnv(): SMTPEnv {
  const host = readRequiredEnv('SMTP_HOST');
  const port = Number(readRequiredEnv('SMTP_PORT'));
  const secure = parseBoolean(process.env.SMTP_SECURE ?? 'false');
  const user = readRequiredEnv('SMTP_USER');
  const pass = readRequiredEnv('SMTP_PASS');
  const from = process.env.SMTP_FROM?.trim() || user;

  if (Number.isNaN(port) || port <= 0) {
    throw new Error('SMTP_PORT must be a valid positive number');
  }

  return { host, port, secure, user, pass, from };
}

export function getSMTPConfig() {
  const config = getSMTPEnv();
  return {
    host: config.host,
    port: config.port,
    secure: config.secure,
    user: config.user,
    from: config.from,
  };
}

export function getSMTPTransporter() {
  if (cachedTransporter) {
    return cachedTransporter;
  }

  const config = getSMTPEnv();
  cachedTransporter = nodemailer.createTransport({
    host: config.host,
    port: config.port,
    secure: config.secure,
    auth: {
      user: config.user,
      pass: config.pass,
    },
  });

  return cachedTransporter;
}

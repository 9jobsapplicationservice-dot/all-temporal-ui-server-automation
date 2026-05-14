This is a Next.js cold outreach tool for uploading contact CSVs, drafting email templates, and sending messages through a server-side SMTP connection.

## Getting Started

1. Create a local environment file:

```bash
cp .env.example .env.local
```

2. Update `.env.local` with your Gmail SMTP settings:

```env
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_SECURE=false
SMTP_USER=9jobsapplicationservice@gmail.com
SMTP_PASS=your-new-16-character-app-password
SMTP_FROM=9jobsapplicationservice@gmail.com
```

Important:

- Revoke the Gmail app password that was previously shared and generate a new one before using this app.
- Do not use your normal Gmail password.
- Keep `.env.local` private; it is ignored by Git.

3. Run the development server:

```bash
npm run dev
```

Open [http://localhost:3000](http://localhost:3000) with your browser to see the result.

If SMTP variables are missing, the send API will return a clear configuration error.

## Notes

- SMTP credentials are never collected in the browser.
- The UI now expects server-side configuration in `.env.local`.
- Gmail should use STARTTLS on port `587` with an app password.

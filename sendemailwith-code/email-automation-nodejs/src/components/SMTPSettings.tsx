'use client';
import React from 'react';

export default function SMTPSettings() {
  return (
    <div className="bg-white p-6 rounded-lg shadow-sm border border-gray-200 hover-lift active:bg-gray-50 transition-colors">
      <h2 className="text-xl font-semibold mb-4 text-gray-800">SMTP Settings</h2>
      <div className="space-y-3 text-sm text-gray-700">
        <p>
          This app now reads SMTP credentials from server-side environment variables only.
        </p>
        <div className="rounded-md border border-amber-200 bg-amber-50 p-3 text-amber-900">
          Add your Gmail SMTP values to <code className="bg-white/70 px-1 rounded">.env.local</code> before sending emails.
        </div>
        <div className="rounded-md border border-gray-200 bg-gray-50 p-3">
          <p><strong>Required:</strong> <code>SMTP_HOST</code>, <code>SMTP_PORT</code>, <code>SMTP_SECURE</code>, <code>SMTP_USER</code>, <code>SMTP_PASS</code></p>
          <p className="mt-1"><strong>Optional:</strong> <code>SMTP_FROM</code> to override the sender address shown in outgoing emails.</p>
        </div>
        <div className="rounded-md border border-gray-200 bg-white p-3 font-mono text-xs leading-6 text-gray-800 overflow-x-auto">
          <div>SMTP_HOST=smtp.gmail.com</div>
          <div>SMTP_PORT=587</div>
          <div>SMTP_SECURE=false</div>
          <div>SMTP_USER=9jobsapplicationservice@gmail.com</div>
          <div>SMTP_PASS=your-new-16-character-app-password</div>
          <div>SMTP_FROM=9jobsapplicationservice@gmail.com</div>
        </div>
        <p className="text-xs text-gray-500">
          Revoke the app password shared earlier and create a fresh one before adding it to your local environment file.
        </p>
      </div>
    </div>
  );
}

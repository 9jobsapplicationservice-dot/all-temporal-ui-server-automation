'use client';

import React, { useState } from 'react';
import { FileSearch, LoaderCircle } from 'lucide-react';
import { readApiJson } from '@/lib/api-client';
import type { ManualRecruiterEnrichmentResponse } from '@/types';

interface RecruiterEnricherProps {
  onEnriched: (payload: ManualRecruiterEnrichmentResponse) => void;
  bridgeUrl?: string;
}

export default function RecruiterEnricher({ onEnriched, bridgeUrl }: RecruiterEnricherProps) {
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [summary, setSummary] = useState<ManualRecruiterEnrichmentResponse | null>(null);

  const handleUpload = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file) {
      return;
    }

    setIsSubmitting(true);
    setError(null);

    try {
      const formData = new FormData();
      formData.append('file', file);

      const baseUrl = bridgeUrl?.trim() || '';
      const url = baseUrl ? `${baseUrl.replace(/\/$/, '')}/api/pipeline/enrich` : '/api/pipeline/enrich';

      const headers: Record<string, string> = {};
      if (baseUrl) {
        headers['ngrok-skip-browser-warning'] = 'any';
      }

      const response = await fetch(url, {
        method: 'POST',
        headers,
        body: formData,
      });
      const payload = await readApiJson<ManualRecruiterEnrichmentResponse>(response, 'Failed to enrich recruiter CSV.');

      setSummary(payload);
      onEnriched(payload);
      event.target.value = '';
    } catch (uploadError: unknown) {
      setError(uploadError instanceof Error ? uploadError.message : 'Failed to enrich recruiter CSV.');
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className="bg-white p-6 rounded-lg shadow-sm border border-gray-200 hover-lift">
      <div className="flex items-center gap-3 mb-3">
        <div className="w-10 h-10 rounded-full bg-emerald-50 text-emerald-700 flex items-center justify-center">
          <FileSearch className="w-5 h-5" />
        </div>
        <div>
          <h2 className="text-xl font-semibold text-gray-800">Recruiter Enrichment</h2>
          <p className="text-sm text-gray-500">Upload a CSV with HR profile links and generate a fresh recruiter CSV.</p>
        </div>
      </div>

      <label className="block text-sm text-gray-600 mb-3">
        Expected input: CSV containing <code className="bg-gray-100 px-1 rounded">HR Profile Link</code> and optional job columns.
      </label>

      <input
        type="file"
        accept=".csv"
        disabled={isSubmitting}
        onChange={handleUpload}
        className="w-full text-sm file:mr-4 file:py-2 file:px-4 file:rounded-full file:border-0 file:text-sm file:font-semibold file:bg-emerald-50 file:text-emerald-700 hover:file:bg-emerald-100 cursor-pointer disabled:cursor-not-allowed"
      />

      {isSubmitting ? (
        <div className="mt-4 flex items-center gap-2 text-sm text-emerald-700">
          <LoaderCircle className="w-4 h-4 animate-spin" />
          Running RocketReach enrichment and building recruiter CSV...
        </div>
      ) : null}

      {error ? <p className="mt-4 text-sm text-red-600">{error}</p> : null}

      {summary ? (
        <div className="mt-4 rounded-md border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm text-emerald-900 space-y-1">
          <p><strong>Run folder:</strong> {summary.runDir}</p>
          <p><strong>Recruiter CSV:</strong> {summary.recruitersCsvPath}</p>
          <p><strong>Total rows:</strong> {summary.stats.total} | <strong>Matched:</strong> {summary.stats.matched} | <strong>Sendable:</strong> {summary.stats.sendable_rows}</p>
          <p><strong>Profile only:</strong> {summary.stats.profile_only} | <strong>No match:</strong> {summary.stats.no_match} | <strong>Quota reached:</strong> {summary.stats.lookup_quota_reached}</p>
        </div>
      ) : null}
    </div>
  );
}

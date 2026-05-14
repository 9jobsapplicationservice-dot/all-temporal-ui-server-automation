'use client';
import React, { useState } from 'react';
import Papa from 'papaparse';
import { UploadCloud } from 'lucide-react';
import { ContactRow } from '@/types';

interface CSVUploaderProps {
  onDataLoaded: (data: ContactRow[]) => void;
}

type ParsedCsvRow = Record<string, string>;

export default function CSVUploader({ onDataLoaded }: CSVUploaderProps) {
  const [error, setError] = useState<string | null>(null);
  const [preview, setPreview] = useState<ContactRow[]>([]);
  const [isUploading, setIsUploading] = useState(false);
  const [isSuccess, setIsSuccess] = useState(false);

  const handleFileUpload = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;

    setIsUploading(true);
    setIsSuccess(false);

    // Simulate a brief "processing" delay for UX
      setTimeout(() => {
    Papa.parse(file, {
      header: true,
      skipEmptyLines: true,
      complete: (results) => {
        const rows = results.data as ParsedCsvRow[];
        
        // Basic validation checking for 'email' column
        const hasEmail = rows.length > 0 && Object.keys(rows[0]).some(k => k.toLowerCase().includes('email'));
        if (!hasEmail) {
          setError('CSV must contain an "email" column.');
          return;
        }

        // Normalize email column internally (always lowercase key)
        const normalizedRows = rows.map((row, index) => {
          const newRow: ContactRow = { id: index.toString(), email: '' };
          for (const key in row) {
             const lowerKey = key.toLowerCase().trim();
             
             // Smart column mapping for RocketReach / LinkedIn exports
             if (lowerKey.includes('email') && row[key] && String(row[key]).includes('@')) {
               const currentEmails = newRow.email ? newRow.email.split(',').map((e: string) => e.trim()) : [];
               const newEmails = String(row[key]).split(',').map(e => e.trim()).filter(e => e.includes('@'));
               newEmails.forEach(e => {
                 if (!currentEmails.includes(e)) currentEmails.push(e);
               });
               if (currentEmails.length > 0) newRow.email = currentEmails.join(', ');
             }

             // Map Name
             if ((lowerKey.includes('name') && !lowerKey.includes('company') && !lowerKey.includes('file')) && !newRow.name && row[key]) {
               newRow.name = row[key];
             }

             // Map Company - added more variations like 'employer', 'organization', 'work', 'current'
             const isCompanyHeader = lowerKey === 'company' || lowerKey.includes('company') || lowerKey.includes('employer') || lowerKey.includes('organization') || lowerKey.includes('work');
             if (isCompanyHeader && !newRow.company && row[key]) {
               newRow.company = row[key];
             }

             // Map Position - added 'title', 'role', 'hr position', 'job'
             const isPositionHeader = lowerKey === 'position' || lowerKey.includes('position') || lowerKey.includes('title') || lowerKey.includes('role') || lowerKey.includes('job');
             if (isPositionHeader && !newRow.position && row[key]) {
               newRow.position = row[key];
             }

             newRow[lowerKey] = row[key];
          }

          // Smart Fallback: If no company was found, extract it from the email domain
          if (!newRow.company && newRow.email) {
            const firstEmail = newRow.email.split(',')[0].trim();
            const domainMatch = firstEmail.match(/@([^.]+)\./);
            if (domainMatch && domainMatch[1]) {
              const domain = domainMatch[1];
              // Skip generic providers
              const genericProviders = ['gmail', 'outlook', 'hotmail', 'yahoo', 'icloud', 'me', 'msn', 'live'];
              if (!genericProviders.includes(domain.toLowerCase())) {
                // Capitalize and clean (e.g., "shop-grok" -> "Shop-Grok")
                const formattedDomain = domain
                  .split(/[-_]/)
                  .map((word: string) => word.charAt(0).toUpperCase() + word.slice(1))
                  .join('-');
                newRow.company = formattedDomain;
              }
            }
          }

          return newRow as ContactRow;
        }).filter(r => r.email); // filter out rows without email

        setError(null);
        setPreview(normalizedRows.slice(0, 5));
        onDataLoaded(normalizedRows);
        setIsUploading(false);
        setIsSuccess(true);
        // Clear success state after 3 seconds
        setTimeout(() => setIsSuccess(false), 3000);
      },
      error: (err: Error) => {
        setError(err.message);
        setIsUploading(false);
      }
    });
    }, 800);
  };

  return (
    <div className={`border-2 border-dashed rounded-lg p-6 flex flex-col items-center justify-center transition-all duration-300 ${
      isSuccess ? 'border-green-400 bg-green-50' : 'border-gray-300 bg-gray-50'
    } ${isUploading ? 'opacity-70 animate-pulse' : 'hover:bg-gray-100'}`}>
      
      {isSuccess ? (
        <div className="flex flex-col items-center animate-scale-up">
          <div className="w-10 h-10 mb-3 bg-green-100 rounded-full flex items-center justify-center">
             <svg className="w-6 h-6 text-green-600" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M5 13l4 4L19 7"></path></svg>
          </div>
          <p className="mb-2 text-sm font-semibold text-green-700">File Processed Successfully!</p>
        </div>
      ) : (
        <>
          <UploadCloud className={`w-10 h-10 mb-3 ${isUploading ? 'text-blue-500 animate-bounce' : 'text-gray-400'}`} />
          <p className="mb-2 text-sm text-gray-500">{isUploading ? 'Parsing CSV...' : 'Upload CSV containing recipients data'}</p>
        </>
      )}

      <input 
        type="file" 
        accept=".csv" 
        disabled={isUploading}
        onChange={handleFileUpload} 
        className="text-sm file:mr-4 file:py-2 file:px-4 file:rounded-full file:border-0 file:text-sm file:font-semibold file:bg-blue-50 file:text-blue-700 hover:file:bg-blue-100 cursor-pointer disabled:cursor-not-allowed" 
      />
      {error && <p className="text-red-500 text-sm mt-3 animate-bounce">{error}</p>}
      
      {preview.length > 0 && (
        <div className="w-full mt-6 text-left">
          <h4 className="text-sm font-semibold text-gray-700 mb-2 border-b pb-1">Preview (First 5 Rows)</h4>
          <div className="overflow-x-auto">
            <table className="w-full text-xs text-gray-600">
              <thead>
                <tr className="border-b bg-gray-50">
                  <th className="py-2 text-left font-medium">Email</th>
                  <th className="py-2 text-left font-medium">Name</th>
                  <th className="py-2 text-left font-medium">Company</th>
                  <th className="py-2 text-left font-medium">Position</th>
                </tr>
              </thead>
              <tbody>
                {preview.map((row, idx) => (
                  <tr key={idx} className="border-b border-gray-100 last:border-0 h-8">
                    <td className="pr-2">{row.email}</td>
                    <td className="pr-2">{row.name || '-'}</td>
                    <td className="pr-2">{row.company || '-'}</td>
                    <td className="pr-2">{row.position || '-'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}

'use client';
import React from 'react';
import { ContactRow } from '@/types';

interface TemplateEditorProps {
  templateBody: string;
  setTemplateBody: (val: string) => void;
  templateSubject: string;
  setTemplateSubject: (val: string) => void;
  previewContact: ContactRow | null;
  senderName: string;
}

export default function TemplateEditor({
  templateBody,
  setTemplateBody,
  templateSubject,
  setTemplateSubject,
  previewContact,
  senderName
}: TemplateEditorProps) {
  
  const insertVariable = (variable: string) => {
    setTemplateBody(templateBody + `{{${variable}}}`);
  };

  const getPreviewText = (text: string) => {
    if (!previewContact) return text;
    let preview = text;
    const regex = /\{\{(\w+)\}\}/g;
    preview = preview.replace(regex, (match, p1) => {
      if (p1.toLowerCase() === 'sendername') return senderName || match;
      const replacement = previewContact[p1.toLowerCase()];
      return typeof replacement === 'string' && replacement.trim() ? replacement : match;
    });
    return preview;
  };

  return (
    <div className="bg-white p-6 rounded-lg shadow-sm border border-gray-200 hover-lift active:bg-gray-50 transition-colors">
      <h2 className="text-xl font-semibold mb-4 text-gray-800">Email Template</h2>
      
      <div className="mb-4">
        <label className="block text-sm font-medium text-gray-700 mb-1">Subject</label>
        <input 
          type="text" 
          value={templateSubject}
          onChange={(e) => setTemplateSubject(e.target.value)}
          className="w-full border border-gray-300 rounded-md p-2 focus-glow" 
          placeholder="e.g. Application for {{position}} at {{company}}"
        />
      </div>

      <div className="mb-4">
        <label className="block text-sm font-medium text-gray-700 mb-1">Body</label>
        <div className="flex gap-2 mb-2">
          {['name', 'company', 'position', 'sendername'].map(v => (
            <button 
              key={v} 
              type="button"
              onClick={() => insertVariable(v)}
              className="text-xs bg-gray-100 px-2 py-1 rounded text-gray-600 hover:bg-gray-200 transition-colors btn-press"
            >
              +{`{{${v}}}`}
            </button>
          ))}
        </div>
        <textarea 
          rows={6}
          value={templateBody}
          onChange={(e) => setTemplateBody(e.target.value)}
          className="w-full border border-gray-300 rounded-md p-2 focus-glow"
          placeholder="Hi {{name}},&#10;&#10;I am writing to..."
        />
      </div>

      {previewContact && (
        <div className="mt-6 bg-gray-50 p-4 rounded-md border border-gray-200">
          <h3 className="text-sm font-medium text-gray-500 uppercase mb-3">Live Preview (First Row)</h3>
          <div className="text-sm">
            <p className="mb-2"><strong>To:</strong> {previewContact.email}</p>
            <p className="mb-4"><strong>Subject:</strong> {getPreviewText(templateSubject)}</p>
            <div className="whitespace-pre-wrap text-gray-800 border-t pt-4 border-gray-200">
              {getPreviewText(templateBody)}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}



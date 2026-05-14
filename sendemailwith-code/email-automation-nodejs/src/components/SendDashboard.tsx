'use client';
import React from 'react';
import { SendStatus, EmailLog } from '@/types';
import { Play, Pause, Square, AlertCircle, CheckCircle, Send } from 'lucide-react';

interface SendDashboardProps {
  status: SendStatus;
  total: number;
  sent: number;
  failed: number;
  activeEmail: string | null;
  delayStr: string;
  setDelayStr: (val: string) => void;
  onStart: () => void;
  onPause: () => void;
  onStop: () => void;
  logs: EmailLog[];
  canStart?: boolean;
  blockedReason?: string | null;
}

export default function SendDashboard({
  status, total, sent, failed, activeEmail, delayStr, setDelayStr, onStart, onPause, onStop, logs, canStart = true, blockedReason = null,
}: SendDashboardProps) {
  const pending = total - sent - failed;
  const progressPercent = total > 0 ? ((sent + failed) / total) * 100 : 0;

  return (
    <div className="bg-white p-6 rounded-lg shadow-sm border border-gray-200 hover-lift active:bg-gray-50 transition-colors">
      <h2 className="text-xl font-semibold mb-4 text-gray-800">Send Delivery Engine</h2>

      <div className="flex items-center gap-6 mb-6">
        <div className="flex flex-col">
          <span className="text-sm text-gray-500 uppercase tracking-widest font-semibold">Total</span>
          <span className="text-2xl font-bold">{total}</span>
        </div>
        <div className="h-10 w-px bg-gray-200" />
        <div className="flex flex-col">
          <span className={`text-sm text-gray-500 uppercase tracking-widest font-semibold text-blue-600 ${status === 'sending' ? 'animate-pulse' : ''}`}>Pending</span>
          <span className="text-2xl font-bold text-blue-800">{pending}</span>
        </div>
        <div className="h-10 w-px bg-gray-200" />
        <div className="flex flex-col">
          <span className="text-sm text-gray-500 uppercase tracking-widest font-semibold text-green-600">Sent</span>
          <span className="text-2xl font-bold text-green-800">{sent}</span>
        </div>
        <div className="h-10 w-px bg-gray-200" />
        <div className="flex flex-col">
          <span className="text-sm text-gray-500 uppercase tracking-widest font-semibold text-red-600">Failed</span>
          <span className="text-2xl font-bold text-red-800">{failed}</span>
        </div>
        {status === 'sending' && (
          <div className="ml-auto flex items-center gap-2 bg-blue-50 px-3 py-1 rounded-full border border-blue-100 animate-pulse">
            <Send className="w-4 h-4 text-blue-600 animate-bounce" />
            <span className="text-xs font-bold text-blue-600 uppercase tracking-tighter">Sending...</span>
          </div>
        )}
      </div>

      <div className="mb-6">
        <label className="text-sm font-medium text-gray-700 mr-2">Delay between emails (seconds):</label>
        <input
          type="number"
          min="1"
          value={delayStr}
          onChange={(e) => setDelayStr(e.target.value)}
          className="w-20 border border-gray-300 rounded px-2 py-1 text-sm focus-glow"
          disabled={status === 'sending'}
        />
      </div>

      <div className="w-full bg-gray-200 rounded-full h-2.5 mb-6 overflow-hidden shadow-inner">
        <div className={`${status === 'completed' ? 'bg-green-500' : 'bg-blue-600'} h-2.5 rounded-full transition-all duration-500 ${status === 'sending' ? 'animate-shimmer' : ''}`} style={{ width: `${progressPercent}%` }}></div>
      </div>

      <div className="flex gap-3 mb-6">
        {(status === 'idle' || status === 'paused' || status === 'stopped') && total > 0 && pending > 0 && canStart && (
          <button onClick={onStart} className="flex items-center gap-1 bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded-md font-medium transition btn-press">
            <Play className="w-4 h-4" /> {status === 'idle' ? 'Start Sending' : 'Resume'}
          </button>
        )}
        {status === 'sending' && (
          <button onClick={onPause} className="flex items-center gap-1 bg-yellow-500 hover:bg-yellow-600 text-white px-4 py-2 rounded-md font-medium transition btn-press">
            <Pause className="w-4 h-4" /> Pause
          </button>
        )}
        {(status === 'sending' || status === 'paused') && (
          <button onClick={onStop} className="flex items-center gap-1 bg-gray-600 hover:bg-gray-700 text-white px-4 py-2 rounded-md font-medium transition btn-press">
            <Square className="w-4 h-4" /> Stop
          </button>
        )}
      </div>

      {!canStart && blockedReason ? (
        <div className="mb-6 rounded-md border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800">
          {blockedReason}
        </div>
      ) : null}

      {(status === 'sending' && activeEmail) || logs.length > 0 ? (
        <div className="mt-8 border-t border-gray-200 pt-6">
          <h3 className="text-sm font-semibold uppercase text-gray-500 tracking-wider mb-4">Delivery Logs</h3>
          <div className="max-h-60 overflow-y-auto space-y-2 border border-gray-100 rounded-md p-2 bg-gray-50 custom-scrollbar">
            {status === 'sending' && activeEmail && (
              <div className="text-sm flex py-1 border-b border-gray-200 items-start animate-fade-in">
                <span className="w-20 text-gray-400 shrink-0 text-xs mt-0.5">Now</span>
                <Send className="w-4 h-4 text-blue-500 mr-2 shrink-0 mt-0.5" />
                <div className="flex-1 min-w-0">
                  <p className="font-medium text-blue-700 truncate">{activeEmail}</p>
                  <p className="text-xs text-blue-500 break-words">Request started...</p>
                </div>
              </div>
            )}
            {logs.map((log) => (
              <div key={log.id} className="text-sm flex py-1 border-b border-gray-200 last:border-0 items-start animate-fade-in">
                <span className="w-20 text-gray-400 shrink-0 text-xs mt-0.5">{log.timestamp}</span>
                {log.success ? (
                  <CheckCircle className="w-4 h-4 text-green-500 mr-2 shrink-0 mt-0.5" />
                ) : (
                  <AlertCircle className="w-4 h-4 text-red-500 mr-2 shrink-0 mt-0.5" />
                )}
                <div className="flex-1 min-w-0">
                  <p className={`font-medium ${log.success ? 'text-green-700' : 'text-red-700'} truncate`}>{log.email}</p>
                  {!log.success && <p className="text-xs text-red-500 break-words">{log.error}</p>}
                </div>
              </div>
            ))}
          </div>
        </div>
      ) : null}
    </div>
  );
}

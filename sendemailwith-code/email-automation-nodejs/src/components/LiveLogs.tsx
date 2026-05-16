import React, { useEffect, useState, useRef } from 'react';
import { Terminal, RefreshCcw, Maximize2, Minimize2 } from 'lucide-react';

interface LiveLogsProps {
  runId: string | null;
  bridgeUrl?: string;
}

export default function LiveLogs({ runId, bridgeUrl }: LiveLogsProps) {
  const [logs, setLogs] = useState<string>('Select a run to view live logs.');
  const [isExpanded, setIsExpanded] = useState(false);
  const [isAutoScroll, setIsAutoScroll] = useState(true);
  const preRef = useRef<HTMLPreElement>(null);

  useEffect(() => {
    if (!runId) return;

    const fetchLogs = async () => {
      try {
        const prefix = bridgeUrl ? bridgeUrl.replace(/\/$/, '') : '';
        const url = `${prefix}/api/pipeline/logs?runId=${encodeURIComponent(runId)}`;
        const response = await fetch(url, { cache: 'no-store' });
        const data = await response.json();
        if (data.logs) {
          setLogs(data.logs);
        }
      } catch (err) {
        console.error('Failed to fetch logs:', err);
      }
    };

    fetchLogs();
    const interval = setInterval(fetchLogs, 3000);
    return () => clearInterval(interval);
  }, [runId, bridgeUrl]);

  useEffect(() => {
    if (isAutoScroll && preRef.current) {
      preRef.current.scrollTop = preRef.current.scrollHeight;
    }
  }, [logs, isAutoScroll]);

  if (!runId) return null;

  return (
    <div className={`workflow-card flex flex-col transition-all duration-300 ${isExpanded ? 'fixed inset-4 z-50 h-auto' : 'h-[400px]'}`}>
      <div className="mb-4 flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Terminal className="h-5 w-5 text-slate-600" />
          <h3 className="text-xl font-semibold text-slate-950">Automation Logs</h3>
        </div>
        <div className="flex items-center gap-2">
          <button 
            onClick={() => setIsAutoScroll(!isAutoScroll)}
            className={`text-xs px-2 py-1 rounded-full transition-colors ${isAutoScroll ? 'bg-sky-100 text-sky-700' : 'bg-slate-100 text-slate-600'}`}
          >
            Auto-scroll: {isAutoScroll ? 'ON' : 'OFF'}
          </button>
          <button 
            onClick={() => setIsExpanded(!isExpanded)}
            className="icon-surface"
          >
            {isExpanded ? <Minimize2 className="h-4 w-4" /> : <Maximize2 className="h-4 w-4" />}
          </button>
        </div>
      </div>

      <div className="relative flex-1 overflow-hidden rounded-2xl bg-slate-950 p-4">
        <pre 
          ref={preRef}
          className="h-full overflow-y-auto font-mono text-xs leading-relaxed text-slate-300 scrollbar-thin scrollbar-thumb-slate-800"
        >
          {logs || 'Waiting for automation output...'}
        </pre>
      </div>
      
      <p className="mt-3 text-[10px] text-slate-400">
        Showing last 100 lines of <code className="bg-slate-100 px-1 rounded">automation.log</code>
      </p>
    </div>
  );
}

import DashboardHome from '@/components/DashboardHome';
import type { WorkflowDashboardPayload } from '@/types';

const EMPTY_DASHBOARD: WorkflowDashboardPayload = {
  activeRun: null,
  recentRuns: [],
  latestFailure: null,
  preview: {
    applied_csv: [],
    recruiter_csv: [],
    email_logs: []
  },
  artifacts: {
    applied_csv: null,
    recruiter_csv: null,
    email_log_csv: null
  }
};

export default function Home() {
  return <DashboardHome initialDashboard={EMPTY_DASHBOARD} />;
}

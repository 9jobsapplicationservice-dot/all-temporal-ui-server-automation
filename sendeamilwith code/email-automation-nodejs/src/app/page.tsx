import DashboardHome from '@/components/DashboardHome';
import type { WorkflowDashboardPayload } from '@/types';

const EMPTY_DASHBOARD: WorkflowDashboardPayload = {
  activeRun: null,
  recentRuns: [],
  latestFailure: null,
};

export default function Home() {
  return <DashboardHome initialDashboard={EMPTY_DASHBOARD} />;
}

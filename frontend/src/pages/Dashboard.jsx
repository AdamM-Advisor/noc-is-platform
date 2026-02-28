import { useEffect } from 'react';
import { BarChart3, RefreshCw } from 'lucide-react';
import useDashboardStore from '../stores/dashboardStore';
import DashboardSelector from '../components/dashboard/DashboardSelector';
import OverallStatusBanner from '../components/dashboard/OverallStatusBanner';
import KpiSnapshotRow from '../components/dashboard/KpiSnapshotRow';
import EntityStatusTable from '../components/dashboard/EntityStatusTable';
import RecommendationPanel from '../components/dashboard/RecommendationPanel';
import QuickChartGrid from '../components/dashboard/QuickChartGrid';

function Dashboard() {
  const {
    period, periods, viewLevel, parentFilter, parentOptions,
    data, loading, error,
    setPeriod, setViewLevel, setParentFilter,
    fetchPeriods, fetchParentOptions, fetchDashboard,
  } = useDashboardStore();

  useEffect(() => {
    fetchPeriods();
  }, []);

  useEffect(() => {
    if (period) fetchDashboard();
  }, [period, viewLevel, parentFilter]);

  useEffect(() => {
    fetchParentOptions();
  }, [viewLevel]);

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <div className="flex items-center gap-2">
            <BarChart3 size={22} className="text-blue-600" />
            <h2 className="text-xl font-bold text-gray-800">Dashboard NOC-IS</h2>
          </div>
          <p className="text-xs text-gray-400 mt-0.5">Author: Dr. Adam M.</p>
        </div>
      </div>

      <DashboardSelector
        period={period}
        periods={periods}
        viewLevel={viewLevel}
        parentFilter={parentFilter}
        parentOptions={parentOptions}
        onPeriodChange={(p) => setPeriod(p)}
        onLevelChange={(l) => { setViewLevel(l); }}
        onParentChange={(f) => setParentFilter(f)}
        onRefresh={fetchDashboard}
      />

      {loading && (
        <div className="flex items-center justify-center py-12 text-gray-400">
          <RefreshCw size={24} className="animate-spin mr-2" /> Memuat dashboard...
        </div>
      )}

      {error && (
        <div className="bg-red-50 border border-red-200 rounded-lg p-4 text-sm text-red-700">
          {error}
        </div>
      )}

      {data && !loading && (
        <>
          <div>
            <p className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-2">Status Keseluruhan</p>
            <OverallStatusBanner status={data.overall_status} />
          </div>

          <div>
            <p className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-2">KPI Snapshot</p>
            <KpiSnapshotRow kpis={data.kpi_snapshot} />
          </div>

          <div className="bg-white rounded-xl border border-gray-200 p-5">
            <p className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-3">
              Child Entities Status Map
            </p>
            <EntityStatusTable entities={data.entities} viewLevel={viewLevel} />
          </div>

          {data.recommendations?.length > 0 && (
            <div>
              <p className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-2">
                Top {data.recommendations.length} Rekomendasi
              </p>
              <RecommendationPanel recommendations={data.recommendations} />
            </div>
          )}

          <div>
            <p className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-2">Quick Charts</p>
            <QuickChartGrid charts={data.charts} />
          </div>
        </>
      )}
    </div>
  );
}

export default Dashboard;

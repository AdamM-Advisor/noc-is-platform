import { useState } from 'react';
import { Activity, Grid3x3, GitBranch } from 'lucide-react';
import TrendChart from './TrendChart';
import HeatmapAdaptive from './HeatmapAdaptive';
import ChildTrendBar from './ChildTrendBar';

const TABS = [
  { id: 'trend', label: 'Tren & Anomali', icon: Activity },
  { id: 'heatmap', label: 'Heatmap', icon: Grid3x3 },
  { id: 'child-trend', label: 'Dekomposisi Tren', icon: GitBranch },
];

export default function TemporalPanel({
  trendData,
  trendMultiData,
  trendKpis,
  trendLoading,
  heatmapData,
  heatmapLoading,
  childTrendData,
  childTrendLoading,
  annotations,
  entityLevel,
  onAddKpi,
  onRemoveKpi,
  onChildTrendKpiChange,
  onHeatmapCellClick,
}) {
  const [activeTab, setActiveTab] = useState('trend');

  const visibleTabs = TABS.filter(t => {
    if (t.id === 'child-trend' && entityLevel === 'site') return false;
    return true;
  });

  return (
    <div className="bg-white rounded-lg border p-5 space-y-4">
      <div className="flex items-center justify-between flex-wrap gap-2">
        <h3 className="text-sm font-semibold text-gray-700 uppercase tracking-wide">
          Panel 2: Analisis Temporal
        </h3>
        <div className="flex border rounded-lg overflow-hidden">
          {visibleTabs.map(tab => {
            const Icon = tab.icon;
            return (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id)}
                className={`flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium transition-colors ${
                  activeTab === tab.id
                    ? 'bg-blue-600 text-white'
                    : 'bg-white text-gray-600 hover:bg-gray-50'
                }`}
              >
                <Icon size={14} />
                {tab.label}
              </button>
            );
          })}
        </div>
      </div>

      {activeTab === 'trend' && (
        <TrendChart
          trendData={trendData}
          trendMultiData={trendMultiData}
          trendKpis={trendKpis}
          annotations={annotations}
          onAddKpi={onAddKpi}
          onRemoveKpi={onRemoveKpi}
        />
      )}

      {activeTab === 'heatmap' && (
        <HeatmapAdaptive
          data={heatmapData}
          loading={heatmapLoading}
          onCellClick={onHeatmapCellClick}
        />
      )}

      {activeTab === 'child-trend' && entityLevel !== 'site' && (
        <ChildTrendBar
          data={childTrendData}
          loading={childTrendLoading}
          kpi={trendKpis[0] || 'sla_pct'}
          onKpiChange={onChildTrendKpiChange}
          entityLevel={entityLevel}
        />
      )}

      {trendLoading && activeTab === 'trend' && (
        <div className="flex items-center justify-center py-12 text-gray-400">
          <Activity size={24} className="animate-spin mr-2" /> Memuat data tren...
        </div>
      )}
    </div>
  );
}

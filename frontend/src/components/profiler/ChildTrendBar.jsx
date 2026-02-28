import { RefreshCw } from 'lucide-react';

const KPI_LABELS = {
  sla_pct: 'SLA %',
  avg_mttr_min: 'MTTR',
  total_tickets: 'Volume',
  escalation_pct: 'Eskalasi %',
  auto_resolve_pct: 'Auto-resolve %',
  repeat_pct: 'Repeat %',
};

export default function ChildTrendBar({ data, loading, kpi, onKpiChange, entityLevel }) {
  if (entityLevel === 'site') return null;

  if (loading) {
    return (
      <div className="flex items-center justify-center py-8 text-gray-400">
        <RefreshCw size={20} className="animate-spin mr-2" /> Memuat tren child...
      </div>
    );
  }

  if (!data || !data.children?.length) {
    return (
      <div className="text-center py-6 text-gray-400 text-sm">
        Tidak ada data child trend.
      </div>
    );
  }

  const maxAbsSlope = Math.max(
    ...data.children.map(c => Math.abs(c.slope)),
    0.1
  );

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between flex-wrap gap-2">
        <div className="flex items-center gap-3">
          <h4 className="text-xs font-semibold text-gray-500 uppercase tracking-wide">
            Dekomposisi Tren Child
          </h4>
          <div className="flex gap-2 text-[10px]">
            <span className="inline-flex items-center gap-1">
              <span className="w-3 h-2 rounded bg-red-400" /> Memburuk
            </span>
            <span className="inline-flex items-center gap-1">
              <span className="w-3 h-2 rounded bg-gray-300" /> Stabil
            </span>
            <span className="inline-flex items-center gap-1">
              <span className="w-3 h-2 rounded bg-green-400" /> Membaik
            </span>
          </div>
        </div>
        <select
          className="border rounded px-2 py-1 text-xs"
          value={kpi}
          onChange={(e) => onKpiChange(e.target.value)}
        >
          {Object.entries(KPI_LABELS).map(([k, v]) => (
            <option key={k} value={k}>{v}</option>
          ))}
        </select>
      </div>

      {data.summary && (
        <div className="flex gap-4 text-xs">
          <span className="inline-flex items-center gap-1 px-2 py-1 rounded bg-gray-50 text-gray-600 font-medium border border-gray-200">
            <span className="inline-block w-2 h-2 rounded-full" style={{ backgroundColor: 'var(--status-good-dot)' }} />
            {data.summary.n_improving} membaik
          </span>
          <span className="inline-flex items-center gap-1 px-2 py-1 rounded bg-gray-50 text-gray-600 font-medium border border-gray-200">
            <span className="inline-block w-2 h-2 rounded-full" style={{ backgroundColor: 'var(--status-neutral-dot)' }} />
            {data.summary.n_stable} stabil
          </span>
          <span className="inline-flex items-center gap-1 px-2 py-1 rounded bg-gray-50 text-gray-600 font-medium border border-gray-200">
            <span className="inline-block w-2 h-2 rounded-full" style={{ backgroundColor: 'var(--status-critical-dot)' }} />
            {data.summary.n_worsening} memburuk
          </span>
        </div>
      )}

      <div className="space-y-1">
        {data.children.map((child) => {
          const pct = maxAbsSlope > 0 ? (child.slope / maxAbsSlope) * 100 : 0;
          const isWorsening = child.quality === 'worsening';
          const isImproving = child.quality === 'improving';

          let barColor = '#9CA3AF';
          if (isWorsening) barColor = '#EF4444';
          if (isImproving) barColor = '#10B981';

          return (
            <div key={child.entity_id} className="flex items-center gap-2 py-1">
              <span className="w-28 text-xs text-gray-700 truncate text-right" title={child.entity_name}>
                {child.entity_name}
              </span>

              <div className="flex-1 flex items-center h-5 relative">
                <div className="absolute left-1/2 top-0 bottom-0 w-px bg-gray-300" />
                {pct < 0 ? (
                  <div
                    className="absolute h-4 rounded-l transition-all"
                    style={{
                      right: '50%',
                      width: `${Math.min(Math.abs(pct) / 2, 50)}%`,
                      backgroundColor: barColor,
                    }}
                  />
                ) : (
                  <div
                    className="absolute h-4 rounded-r transition-all"
                    style={{
                      left: '50%',
                      width: `${Math.min(Math.abs(pct) / 2, 50)}%`,
                      backgroundColor: barColor,
                    }}
                  />
                )}
              </div>

              <span className="w-16 text-xs text-right font-mono" style={{ color: barColor }}>
                {child.slope > 0 ? '+' : ''}{child.slope.toFixed(1)}
              </span>
              <span className="w-14 text-xs text-gray-500 text-right">
                {child.current_value.toFixed(1)}
              </span>
              <span className="inline-block w-2 h-2 rounded-full flex-shrink-0" style={{ backgroundColor: isWorsening ? '#DC2626' : isImproving ? '#16A34A' : '#94A3B8' }} />
            </div>
          );
        })}
      </div>

      {data.narrative && (
        <div className="text-sm px-3 py-2 rounded border bg-gray-50 border-gray-200 text-gray-600">
          {data.narrative.replace(/[⚠️🔴🟡🟢📉📈🟠]/g, '').trim()}
        </div>
      )}
    </div>
  );
}

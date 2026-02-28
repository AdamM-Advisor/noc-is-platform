import StatusDot from '../ui/StatusDot';

const KPI_CONFIG = {
  sla_pct: { label: 'SLA', unit: '%', format: v => v?.toFixed(1) },
  avg_mttr_min: { label: 'MTTR', unit: 'min', format: v => Math.round(v || 0) },
  total_tickets: { label: 'Volume', unit: '', format: v => (v >= 1000 ? `${(v / 1000).toFixed(1)}K` : Math.round(v || 0)) },
  escalation_pct: { label: 'Eskalasi', unit: '%', format: v => v?.toFixed(1) },
  auto_resolve_pct: { label: 'Auto-resolve', unit: '%', format: v => v?.toFixed(1) },
  repeat_pct: { label: 'Repeat', unit: '%', format: v => v?.toFixed(1) },
};

function mapColorToStatus(color) {
  if (color === 'red') return 'critical';
  if (color === 'orange' || color === 'yellow') return 'warning';
  if (color === 'green') return 'good';
  return 'neutral';
}

export default function SideBySideKpi({ kpisA, kpisB, statusA, statusB, profileA, profileB }) {
  if (!kpisA || !kpisB) return null;

  const kpiKeys = Object.keys(KPI_CONFIG);

  return (
    <div className="bg-white rounded-lg border p-5 space-y-4">
      <h3 className="text-sm font-semibold text-gray-700 uppercase tracking-wide">KPI Side-by-Side</h3>
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b">
              <th className="text-left py-2 px-3 font-medium" style={{ color: 'var(--text-muted)' }}>KPI</th>
              <th className="text-center py-2 px-3 font-medium" style={{ color: 'var(--text-secondary)' }}>
                <div className="flex items-center justify-center gap-1.5">
                  {statusA && <StatusDot status={mapColorToStatus(statusA?.color)} />}
                  {profileA?.entity_name || 'A'}
                </div>
              </th>
              <th className="text-center py-2 px-3 font-medium" style={{ color: 'var(--text-secondary)' }}>
                <div className="flex items-center justify-center gap-1.5">
                  {statusB && <StatusDot status={mapColorToStatus(statusB?.color)} />}
                  {profileB?.entity_name || 'B'}
                </div>
              </th>
              <th className="text-center py-2 px-3 font-medium" style={{ color: 'var(--text-muted)' }}>Delta</th>
            </tr>
          </thead>
          <tbody>
            {kpiKeys.map(key => {
              const cfg = KPI_CONFIG[key];
              const valA = kpisA[key];
              const valB = kpisB[key];
              const delta = (valB ?? 0) - (valA ?? 0);

              return (
                <tr key={key} className="border-b last:border-0 hover:bg-gray-50">
                  <td className="py-2 px-3 font-medium" style={{ color: 'var(--text-secondary)' }}>{cfg.label}</td>
                  <td className="py-2 px-3 text-center">
                    <span className="font-semibold" style={{ color: 'var(--text-primary)' }}>
                      {cfg.format(valA)}{cfg.unit && <span className="text-xs ml-0.5" style={{ color: 'var(--text-muted)' }}>{cfg.unit}</span>}
                    </span>
                  </td>
                  <td className="py-2 px-3 text-center">
                    <span className="font-semibold" style={{ color: 'var(--text-primary)' }}>
                      {cfg.format(valB)}{cfg.unit && <span className="text-xs ml-0.5" style={{ color: 'var(--text-muted)' }}>{cfg.unit}</span>}
                    </span>
                  </td>
                  <td className="py-2 px-3 text-center">
                    <span className="font-medium" style={{ color: 'var(--text-secondary)' }}>
                      {delta > 0 ? '+' : ''}{delta.toFixed(1)}
                    </span>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

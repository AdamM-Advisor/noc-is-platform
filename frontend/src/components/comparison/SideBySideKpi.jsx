const KPI_CONFIG = {
  sla_pct: { label: 'SLA', unit: '%', format: v => v?.toFixed(1) },
  avg_mttr_min: { label: 'MTTR', unit: 'min', format: v => Math.round(v || 0) },
  total_tickets: { label: 'Volume', unit: '', format: v => (v >= 1000 ? `${(v / 1000).toFixed(1)}K` : Math.round(v || 0)) },
  escalation_pct: { label: 'Eskalasi', unit: '%', format: v => v?.toFixed(1) },
  auto_resolve_pct: { label: 'Auto-resolve', unit: '%', format: v => v?.toFixed(1) },
  repeat_pct: { label: 'Repeat', unit: '%', format: v => v?.toFixed(1) },
};

function StatusDot({ status }) {
  const colors = {
    green: 'bg-green-500',
    yellow: 'bg-yellow-500',
    orange: 'bg-orange-500',
    red: 'bg-red-500',
    gray: 'bg-gray-400',
  };
  return <span className={`inline-block w-3 h-3 rounded-full ${colors[status?.color] || colors.gray}`} />;
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
              <th className="text-left py-2 px-3 text-gray-500 font-medium">KPI</th>
              <th className="text-center py-2 px-3 text-blue-600 font-medium">
                <div className="flex items-center justify-center gap-1">
                  {statusA && <StatusDot status={statusA} />}
                  {profileA?.entity_name || 'A'}
                </div>
              </th>
              <th className="text-center py-2 px-3 text-emerald-600 font-medium">
                <div className="flex items-center justify-center gap-1">
                  {statusB && <StatusDot status={statusB} />}
                  {profileB?.entity_name || 'B'}
                </div>
              </th>
              <th className="text-center py-2 px-3 text-gray-500 font-medium">Delta</th>
            </tr>
          </thead>
          <tbody>
            {kpiKeys.map(key => {
              const cfg = KPI_CONFIG[key];
              const valA = kpisA[key];
              const valB = kpisB[key];
              const delta = (valB ?? 0) - (valA ?? 0);
              const isPositive = key === 'sla_pct' || key === 'auto_resolve_pct';
              const deltaGood = isPositive ? delta > 0 : delta < 0;
              const deltaBad = isPositive ? delta < 0 : delta > 0;

              return (
                <tr key={key} className="border-b last:border-0 hover:bg-gray-50">
                  <td className="py-2 px-3 font-medium text-gray-700">{cfg.label}</td>
                  <td className="py-2 px-3 text-center">
                    <span className="text-blue-700 font-semibold">
                      {cfg.format(valA)}{cfg.unit && <span className="text-xs text-gray-400 ml-0.5">{cfg.unit}</span>}
                    </span>
                  </td>
                  <td className="py-2 px-3 text-center">
                    <span className="text-emerald-700 font-semibold">
                      {cfg.format(valB)}{cfg.unit && <span className="text-xs text-gray-400 ml-0.5">{cfg.unit}</span>}
                    </span>
                  </td>
                  <td className="py-2 px-3 text-center">
                    <span className={`font-medium ${deltaGood ? 'text-green-600' : deltaBad ? 'text-red-600' : 'text-gray-500'}`}>
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

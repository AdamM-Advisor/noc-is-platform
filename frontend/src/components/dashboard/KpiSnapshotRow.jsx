const KPI_CONFIGS = [
  { key: 'total_volume', label: 'Volume', format: (v) => v >= 1000 ? `${(v/1000).toFixed(1)}K` : v, delta: 'volume_mom_pct', deltaUnit: '%', thresholds: null },
  { key: 'sla_pct', label: 'SLA', format: (v) => `${v}%`, delta: 'sla_mom_pp', deltaUnit: 'pp', thresholds: { warn: 90, crit: 85 } },
  { key: 'avg_mttr_min', label: 'MTTR', format: (v) => `${Math.round(v)}m`, delta: 'mttr_mom_pct', deltaUnit: '%', thresholds: { warn: 720, crit: 1440 }, invert: true },
  { key: 'escalation_pct', label: 'Eskalasi', format: (v) => `${v}%`, delta: 'esc_mom_pp', deltaUnit: 'pp', thresholds: { warn: 7, crit: 10 }, invert: true },
  { key: 'auto_resolve_pct', label: 'Auto-resolve', format: (v) => `${v}%`, delta: 'auto_mom_pp', deltaUnit: 'pp', thresholds: { warn: 40, crit: 30 } },
];

function getStatusColor(value, config) {
  if (!config.thresholds) return 'text-gray-600';
  const { warn, crit } = config.thresholds;
  if (config.invert) {
    if (value > crit) return 'text-red-600';
    if (value > warn) return 'text-amber-600';
    return 'text-green-600';
  }
  if (value < crit) return 'text-red-600';
  if (value < warn) return 'text-amber-600';
  return 'text-green-600';
}

function getDeltaDisplay(delta, unit, config) {
  if (delta == null) return null;
  const isGood = config?.invert ? delta < 0 : delta > 0;
  const isBad = config?.invert ? delta > 0 : delta < 0;
  const arrow = isGood ? '▲' : (isBad ? '▼' : '─');
  const color = isGood ? 'text-green-600' : (isBad ? 'text-red-600' : 'text-gray-400');
  return <span className={`text-[10px] font-medium ${color}`}>{arrow} {delta > 0 ? '+' : ''}{delta}{unit}</span>;
}

export default function KpiSnapshotRow({ kpis }) {
  if (!kpis) return null;

  return (
    <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-3">
      {KPI_CONFIGS.map((cfg) => {
        const value = kpis[cfg.key];
        const delta = kpis[cfg.delta];
        return (
          <div key={cfg.key} className="bg-white rounded-xl border border-gray-200 p-4 text-center">
            <p className="text-xs text-gray-500 mb-1">{cfg.label}</p>
            <p className={`text-xl font-bold ${getStatusColor(value, cfg)}`}>
              {cfg.format(value)}
            </p>
            <div className="mt-1">
              {getDeltaDisplay(delta, cfg.deltaUnit, cfg)}
            </div>
          </div>
        );
      })}
    </div>
  );
}

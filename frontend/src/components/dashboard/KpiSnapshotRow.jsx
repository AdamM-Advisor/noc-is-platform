import StatusDot from '../ui/StatusDot';

const KPI_CONFIGS = [
  { key: 'total_volume', label: 'Volume', format: (v) => v >= 1000 ? `${(v/1000).toFixed(1)}K` : v, delta: 'volume_mom_pct', deltaUnit: '%', thresholds: null },
  { key: 'sla_pct', label: 'SLA', format: (v) => `${v}%`, delta: 'sla_mom_pp', deltaUnit: 'pp', thresholds: { warn: 90, crit: 85 } },
  { key: 'avg_mttr_min', label: 'MTTR', format: (v) => `${Math.round(v)}m`, delta: 'mttr_mom_pct', deltaUnit: '%', thresholds: { warn: 720, crit: 1440 }, invert: true },
  { key: 'escalation_pct', label: 'Eskalasi', format: (v) => `${v}%`, delta: 'esc_mom_pp', deltaUnit: 'pp', thresholds: { warn: 7, crit: 10 }, invert: true },
  { key: 'auto_resolve_pct', label: 'Auto-resolve', format: (v) => `${v}%`, delta: 'auto_mom_pp', deltaUnit: 'pp', thresholds: { warn: 40, crit: 30 } },
];

function isAlert(value, config) {
  if (!config.thresholds) return false;
  const { warn, crit } = config.thresholds;
  if (config.invert) {
    return value > warn;
  }
  return value < warn;
}

function formatDelta(delta, unit) {
  if (delta == null) return null;
  const sign = delta > 0 ? '+' : (delta < 0 ? '\u2212' : '');
  const abs = Math.abs(delta);
  return `${sign}${abs}${unit}`;
}

export default function KpiSnapshotRow({ kpis }) {
  if (!kpis) return null;

  return (
    <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-3">
      {KPI_CONFIGS.map((cfg) => {
        const value = kpis[cfg.key];
        const delta = kpis[cfg.delta];
        const alert = isAlert(value, cfg);
        return (
          <div key={cfg.key} className="bg-white rounded-xl border border-gray-200 p-4 text-center">
            <p className="text-xs mb-1" style={{ color: 'var(--text-muted)' }}>{cfg.label}</p>
            <div className="flex items-center justify-center gap-1.5">
              {alert && <StatusDot status="critical" size={8} />}
              <p className="text-xl font-bold" style={{ color: 'var(--text-primary)' }}>
                {cfg.format(value)}
              </p>
            </div>
            {delta != null && (
              <div className="mt-1">
                <span className="text-[10px] font-medium" style={{ color: 'var(--text-muted)' }}>
                  {formatDelta(delta, cfg.deltaUnit)}
                </span>
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

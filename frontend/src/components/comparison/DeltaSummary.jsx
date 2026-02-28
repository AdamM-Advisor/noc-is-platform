import StatusDot from '../ui/StatusDot';

function mapQualityToStatus(quality) {
  if (quality === 'worsening') return 'critical';
  if (quality === 'improving') return 'good';
  return 'neutral';
}

function DeltaCard({ kpi, data }) {
  const statusLevel = mapQualityToStatus(data.quality);
  const borderStyle = {
    improving: 'border-l-4',
    worsening: 'border-l-4',
    stable: 'border-l-4',
  };

  const borderColor = {
    improving: 'var(--status-good-dot)',
    worsening: 'var(--status-critical-dot)',
    stable: 'var(--status-neutral-dot)',
  };

  return (
    <div
      className="rounded-lg border p-3"
      style={{
        borderLeftWidth: '4px',
        borderLeftColor: borderColor[data.quality] || borderColor.stable,
        backgroundColor: 'var(--bg-secondary)',
      }}
    >
      <div className="flex items-center justify-between mb-1">
        <span className="text-xs font-medium uppercase" style={{ color: 'var(--text-muted)' }}>{data.label}</span>
        <StatusDot status={statusLevel} />
      </div>
      <div className="flex items-baseline gap-2 mb-1">
        <span className="text-lg font-bold" style={{ color: 'var(--text-primary)' }}>
          {data.delta > 0 ? '+' : ''}{data.delta}
        </span>
        <span className="text-xs" style={{ color: 'var(--text-muted)' }}>
          ({data.pct_change > 0 ? '+' : ''}{data.pct_change}%)
        </span>
      </div>
      <div className="flex items-center gap-2 text-xs" style={{ color: 'var(--text-secondary)' }}>
        <span className="px-1.5 py-0.5 rounded" style={{ backgroundColor: 'var(--bg-hover)' }}>{data.value_a}</span>
        <span>→</span>
        <span className="px-1.5 py-0.5 rounded" style={{ backgroundColor: 'var(--bg-hover)' }}>{data.value_b}</span>
      </div>
    </div>
  );
}

export default function DeltaSummary({ deltas }) {
  if (!deltas || Object.keys(deltas).length === 0) return null;

  const improving = Object.values(deltas).filter(d => d.quality === 'improving').length;
  const worsening = Object.values(deltas).filter(d => d.quality === 'worsening').length;
  const stable = Object.values(deltas).filter(d => d.quality === 'stable').length;

  return (
    <div className="bg-white rounded-lg border p-5 space-y-4">
      <div className="flex items-center justify-between">
        <h3 className="text-sm font-semibold text-gray-700 uppercase tracking-wide">Delta KPI</h3>
        <div className="flex items-center gap-3 text-xs">
          {improving > 0 && (
            <span className="inline-flex items-center gap-1.5" style={{ color: 'var(--text-secondary)' }}>
              <StatusDot status="good" size={6} /> {improving} membaik
            </span>
          )}
          {worsening > 0 && (
            <span className="inline-flex items-center gap-1.5" style={{ color: 'var(--text-secondary)' }}>
              <StatusDot status="critical" size={6} /> {worsening} memburuk
            </span>
          )}
          {stable > 0 && (
            <span className="inline-flex items-center gap-1.5" style={{ color: 'var(--text-secondary)' }}>
              <StatusDot status="neutral" size={6} /> {stable} stabil
            </span>
          )}
        </div>
      </div>
      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3">
        {Object.entries(deltas).map(([kpi, data]) => (
          <DeltaCard key={kpi} kpi={kpi} data={data} />
        ))}
      </div>
    </div>
  );
}

import StatusDot from '../ui/StatusDot';

const PRIORITY_CONFIG = {
  SEGERA: { borderColor: 'var(--status-critical-dot)', labelColor: 'var(--status-critical-text)', dotStatus: 'critical', label: 'SEGERA' },
  MINGGU_INI: { borderColor: 'var(--status-warning-dot)', labelColor: 'var(--status-warning-text)', dotStatus: 'warning', label: 'MINGGU INI' },
  BULAN_INI: { borderColor: 'var(--accent-brand)', labelColor: 'var(--accent-brand)', dotStatus: 'neutral', label: 'BULAN INI' },
  RUTIN: { borderColor: 'var(--status-neutral-dot)', labelColor: 'var(--text-secondary)', dotStatus: 'neutral', label: 'RUTIN' },
};

export default function RecommendationPanel({ recommendations }) {
  if (!recommendations?.length) return null;

  const grouped = {};
  recommendations.forEach((r) => {
    const p = r.priority || 'RUTIN';
    if (!grouped[p]) grouped[p] = [];
    grouped[p].push(r);
  });

  const order = ['SEGERA', 'MINGGU_INI', 'BULAN_INI', 'RUTIN'];

  return (
    <div className="rounded-xl border overflow-hidden" style={{ borderColor: 'var(--border)' }}>
      {order.map((priority) => {
        const items = grouped[priority];
        if (!items?.length) return null;
        const config = PRIORITY_CONFIG[priority] || PRIORITY_CONFIG.RUTIN;

        return (
          <div
            key={priority}
            style={{
              borderLeft: `3px solid ${config.borderColor}`,
              backgroundColor: 'var(--bg-primary)',
              borderBottom: '1px solid var(--border)',
            }}
            className="last:border-b-0"
          >
            <div
              className="px-4 py-2 flex items-center gap-2"
              style={{
                borderBottom: '1px solid var(--border)',
                backgroundColor: 'var(--bg-secondary)',
              }}
            >
              <StatusDot status={config.dotStatus} size={7} />
              <span className="text-xs font-bold" style={{ color: config.labelColor }}>
                {config.label}
              </span>
            </div>
            <div className="px-4 py-3 space-y-3">
              {items.map((r, i) => (
                <div key={i} className="text-sm">
                  <p className="font-medium" style={{ color: 'var(--text-primary)' }}>
                    {recommendations.indexOf(r) + 1}. {r.message}
                  </p>
                  {r.action && (
                    <p className="ml-4 mt-0.5" style={{ color: 'var(--text-secondary)' }}>
                      AKSI: {r.action}
                    </p>
                  )}
                  {r.impact && (
                    <p className="ml-4 mt-0.5 text-xs" style={{ color: 'var(--text-muted)' }}>
                      Impact: {r.impact}
                    </p>
                  )}
                </div>
              ))}
            </div>
          </div>
        );
      })}
    </div>
  );
}

import StatusDot from '../ui/StatusDot';

function mapQualityToStatus(quality) {
  if (quality === 'worsening') return 'critical';
  if (quality === 'improving') return 'neutral';
  return 'neutral';
}

export default function DeltaBadge({ delta, quality, unit = '' }) {
  if (delta === undefined || delta === null) return null;
  const statusLevel = mapQualityToStatus(quality);
  const sign = delta > 0 ? '+' : '';

  return (
    <span
      className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium"
      style={{
        backgroundColor: 'var(--bg-hover)',
        color: 'var(--text-secondary)',
      }}
    >
      <StatusDot status={statusLevel} size={6} />
      {sign}{typeof delta === 'number' ? delta.toFixed(1) : delta}{unit}
    </span>
  );
}

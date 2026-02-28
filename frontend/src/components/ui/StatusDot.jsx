const STATUS_COLORS = {
  critical: 'var(--status-critical-dot)',
  warning: 'var(--status-warning-dot)',
  good: 'var(--status-good-dot)',
  neutral: 'var(--status-neutral-dot)',
};

function StatusDot({ status, size = 8 }) {
  return (
    <span
      style={{
        display: 'inline-block',
        width: size,
        height: size,
        borderRadius: '50%',
        backgroundColor: STATUS_COLORS[status] || STATUS_COLORS.neutral,
        flexShrink: 0,
      }}
    />
  );
}

export function getStatusLevel(statusText) {
  if (!statusText) return 'neutral';
  const s = statusText.toUpperCase();
  if (s.includes('KRITIS')) return 'critical';
  if (s.includes('PERHATIAN') || s.includes('PERLU')) return 'warning';
  if (s.includes('SANGAT BAIK')) return 'good';
  if (s.includes('BAIK')) return 'good';
  return 'neutral';
}

export default StatusDot;

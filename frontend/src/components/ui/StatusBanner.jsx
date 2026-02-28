import StatusDot from './StatusDot';

const BORDER_COLORS = {
  critical: 'var(--status-critical-dot)',
  warning: 'var(--status-warning-dot)',
  good: 'var(--status-good-dot)',
  neutral: 'var(--status-neutral-dot)',
};

function StatusBanner({ status, title, narrative }) {
  const borderColor = BORDER_COLORS[status] || BORDER_COLORS.neutral;

  return (
    <div
      className="rounded-r-md mb-6"
      style={{
        borderLeft: `3px solid ${borderColor}`,
        padding: '16px 20px',
        backgroundColor: 'var(--bg-secondary)',
      }}
    >
      <div className="flex items-center gap-2 mb-2">
        <StatusDot status={status} />
        <span className="text-[15px] font-semibold" style={{ color: 'var(--text-primary)' }}>
          {title}
        </span>
      </div>
      {narrative && (
        <p className="text-[13px] leading-relaxed m-0" style={{ color: 'var(--text-secondary)' }}>
          {narrative}
        </p>
      )}
    </div>
  );
}

export default StatusBanner;

const DOT_COLORS = {
  overload: 'var(--status-critical-dot)',
  near_capacity: 'var(--status-critical-dot)',
  tight: 'var(--status-warning-dot)',
  ok: 'var(--status-neutral-dot)',
};

const BORDER_COLORS = {
  overload: 'border-l-red-500',
  near_capacity: 'border-l-red-500',
  tight: 'border-l-amber-500',
  ok: 'border-l-gray-300',
};

export default function CapacityAlert({ data }) {
  if (!data) return null;

  const dotColor = DOT_COLORS[data.status] || DOT_COLORS.ok;
  const borderCls = BORDER_COLORS[data.status] || BORDER_COLORS.ok;

  return (
    <div className={`flex items-start gap-2 px-4 py-3 rounded bg-gray-50 border border-gray-200 border-l-4 ${borderCls} text-sm text-gray-700`}>
      <span className="inline-block w-2 h-2 rounded-full mt-1 flex-shrink-0" style={{ backgroundColor: dotColor }} />
      <span>{data.narrative}</span>
    </div>
  );
}

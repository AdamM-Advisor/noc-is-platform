export default function CapacityAlert({ data }) {
  if (!data) return null;

  const colorMap = {
    overload: 'bg-red-50 border-red-200 text-red-700',
    near_capacity: 'bg-red-50 border-red-200 text-red-700',
    tight: 'bg-amber-50 border-amber-200 text-amber-700',
    ok: 'bg-green-50 border-green-200 text-green-700',
  };

  const cls = colorMap[data.status] || colorMap.ok;

  return (
    <div className={`flex items-start gap-2 px-4 py-3 rounded-lg border text-sm ${cls}`}>
      <span className="text-base">{data.icon || (data.status === 'ok' ? '🟢' : data.status === 'overload' ? '🔴🔴' : '🔴')}</span>
      <span>{data.narrative}</span>
    </div>
  );
}

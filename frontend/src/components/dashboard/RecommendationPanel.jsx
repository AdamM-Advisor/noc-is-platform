const PRIORITY_STYLES = {
  SEGERA: { bg: 'bg-red-50', border: 'border-red-200', text: 'text-red-700', label: '🔴 SEGERA' },
  MINGGU_INI: { bg: 'bg-amber-50', border: 'border-amber-200', text: 'text-amber-700', label: '🟡 MINGGU INI' },
  BULAN_INI: { bg: 'bg-blue-50', border: 'border-blue-200', text: 'text-blue-700', label: '🔵 BULAN INI' },
  RUTIN: { bg: 'bg-green-50', border: 'border-green-200', text: 'text-green-700', label: '🟢 RUTIN' },
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
    <div className="rounded-xl border border-gray-200 overflow-hidden">
      {order.map((priority) => {
        const items = grouped[priority];
        if (!items?.length) return null;
        const style = PRIORITY_STYLES[priority] || PRIORITY_STYLES.RUTIN;

        return (
          <div key={priority} className={`${style.bg} border-b ${style.border} last:border-b-0`}>
            <div className={`px-4 py-2 text-xs font-bold ${style.text} border-b ${style.border}`}>
              {style.label}
            </div>
            <div className="px-4 py-3 space-y-3">
              {items.map((r, i) => (
                <div key={i} className="text-sm">
                  <p className={`font-medium ${style.text}`}>
                    {recommendations.indexOf(r) + 1}. {r.message}
                  </p>
                  {r.action && (
                    <p className="text-gray-600 ml-4 mt-0.5">
                      ▶ AKSI: {r.action}
                    </p>
                  )}
                  {r.impact && (
                    <p className="text-gray-500 ml-4 mt-0.5 text-xs">
                      📊 Impact: {r.impact}
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

const RISK_BADGE = {
  HIGH: 'border border-gray-200 bg-gray-50 text-gray-700',
  MEDIUM: 'border border-gray-200 bg-gray-50 text-gray-700',
  LOW: 'border border-gray-200 bg-gray-50 text-gray-600',
};

const RISK_DOT = {
  HIGH: 'bg-red-500',
  MEDIUM: 'bg-amber-500',
  LOW: 'bg-gray-400',
};

export default function RiskSiteTable({ sites }) {
  if (!sites?.length) return null;

  return (
    <div className="space-y-2">
      <p className="text-xs font-semibold text-gray-500 uppercase">Top High-Risk Sites</p>
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b">
              <th className="text-left px-2 py-1.5 text-gray-500">#</th>
              <th className="text-left px-2 py-1.5 text-gray-500">Site</th>
              <th className="text-right px-2 py-1.5 text-gray-500">Risk</th>
              <th className="text-left px-2 py-1.5 text-gray-500">Top Component</th>
              <th className="text-right px-2 py-1.5 text-gray-500">Last Incident</th>
            </tr>
          </thead>
          <tbody>
            {sites.map((s, i) => {
              const level = s.risk_score >= 70 ? 'HIGH' : s.risk_score >= 40 ? 'MEDIUM' : 'LOW';
              return (
                <tr key={s.id || i} className="border-b hover:bg-gray-50">
                  <td className="px-2 py-1.5 text-gray-400">{i + 1}</td>
                  <td className="px-2 py-1.5 font-medium text-gray-700 max-w-[10rem] truncate" title={s.name}>
                    {s.name || s.id}
                  </td>
                  <td className="px-2 py-1.5 text-right">
                    <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded text-[10px] font-bold ${RISK_BADGE[level]}`}>
                      <span className={`inline-block w-1.5 h-1.5 rounded-full ${RISK_DOT[level]}`} />
                      {s.risk_score?.toFixed(0)}
                    </span>
                  </td>
                  <td className="px-2 py-1.5 text-gray-600">{s.top_component || '—'}</td>
                  <td className="px-2 py-1.5 text-right text-gray-600">
                    {s.days_since != null ? `${s.days_since} hari lalu` : '—'}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

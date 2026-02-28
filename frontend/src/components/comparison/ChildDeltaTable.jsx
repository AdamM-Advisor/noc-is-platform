import StatusDot from '../ui/StatusDot';

function mapQualityToStatus(quality) {
  if (quality === 'worsening') return 'critical';
  if (quality === 'improving') return 'neutral';
  return 'neutral';
}

export default function ChildDeltaTable({ childrenDelta, profileA, profileB }) {
  if (!childrenDelta || childrenDelta.length === 0) return null;

  const improving = childrenDelta.filter(c => c.quality === 'improving').length;
  const worsening = childrenDelta.filter(c => c.quality === 'worsening').length;

  return (
    <div className="bg-white rounded-lg border p-5 space-y-4">
      <div className="flex items-center justify-between">
        <h3 className="text-sm font-semibold text-gray-700 uppercase tracking-wide">Perbandingan Child Entity</h3>
        <div className="flex items-center gap-3 text-xs">
          {improving > 0 && (
            <span className="inline-flex items-center gap-1.5 text-gray-600">
              <StatusDot status="good" size={6} /> {improving} membaik
            </span>
          )}
          {worsening > 0 && (
            <span className="inline-flex items-center gap-1.5 text-gray-600">
              <StatusDot status="critical" size={6} /> {worsening} memburuk
            </span>
          )}
        </div>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b bg-gray-50">
              <th className="text-left py-2 px-3 text-gray-500 font-medium">Entity</th>
              <th className="text-center py-2 px-3 text-gray-500 font-medium">SLA A</th>
              <th className="text-center py-2 px-3 text-gray-500 font-medium">SLA B</th>
              <th className="text-center py-2 px-3 text-gray-500 font-medium">Delta</th>
              <th className="text-center py-2 px-3 text-gray-500 font-medium">MTTR A</th>
              <th className="text-center py-2 px-3 text-gray-500 font-medium">MTTR B</th>
              <th className="text-center py-2 px-3 text-gray-500 font-medium">Status</th>
            </tr>
          </thead>
          <tbody>
            {childrenDelta.map(child => {
              const statusLevel = mapQualityToStatus(child.quality);
              const deltaText = `${child.delta > 0 ? '+' : ''}${child.delta?.toFixed(1)}pp`;

              return (
                <tr key={child.id} className="border-b last:border-0 hover:bg-gray-50">
                  <td className="py-2 px-3 font-medium text-gray-700">{child.name}</td>
                  <td className="py-2 px-3 text-center" style={{ color: 'var(--text-primary)' }}>{child.sla_a?.toFixed(1)}%</td>
                  <td className="py-2 px-3 text-center" style={{ color: 'var(--text-primary)' }}>{child.sla_b?.toFixed(1)}%</td>
                  <td className="py-2 px-3 text-center font-medium" style={{ color: 'var(--text-secondary)' }}>
                    {deltaText}
                  </td>
                  <td className="py-2 px-3 text-center" style={{ color: 'var(--text-primary)' }}>{Math.round(child.mttr_a || 0)}</td>
                  <td className="py-2 px-3 text-center" style={{ color: 'var(--text-primary)' }}>{Math.round(child.mttr_b || 0)}</td>
                  <td className="py-2 px-3 text-center">
                    <StatusDot status={statusLevel} />
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

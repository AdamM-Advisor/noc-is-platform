import { TrendingUp, TrendingDown, Minus } from 'lucide-react';

export default function ChildDeltaTable({ childrenDelta, profileA, profileB }) {
  if (!childrenDelta || childrenDelta.length === 0) return null;

  const improving = childrenDelta.filter(c => c.quality === 'improving').length;
  const worsening = childrenDelta.filter(c => c.quality === 'worsening').length;

  return (
    <div className="bg-white rounded-lg border p-5 space-y-4">
      <div className="flex items-center justify-between">
        <h3 className="text-sm font-semibold text-gray-700 uppercase tracking-wide">Perbandingan Child Entity</h3>
        <div className="flex items-center gap-3 text-xs">
          {improving > 0 && <span className="text-green-600">✅ {improving} membaik</span>}
          {worsening > 0 && <span className="text-red-600">❌ {worsening} memburuk</span>}
        </div>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b bg-gray-50">
              <th className="text-left py-2 px-3 text-gray-500 font-medium">Entity</th>
              <th className="text-center py-2 px-3 text-blue-600 font-medium">SLA A</th>
              <th className="text-center py-2 px-3 text-emerald-600 font-medium">SLA B</th>
              <th className="text-center py-2 px-3 text-gray-500 font-medium">Delta</th>
              <th className="text-center py-2 px-3 text-blue-600 font-medium">MTTR A</th>
              <th className="text-center py-2 px-3 text-emerald-600 font-medium">MTTR B</th>
              <th className="text-center py-2 px-3 text-gray-500 font-medium">Status</th>
            </tr>
          </thead>
          <tbody>
            {childrenDelta.map(child => {
              const Icon = child.quality === 'improving' ? TrendingUp : child.quality === 'worsening' ? TrendingDown : Minus;
              const iconColor = child.quality === 'improving' ? 'text-green-500' : child.quality === 'worsening' ? 'text-red-500' : 'text-gray-400';
              const deltaColor = child.quality === 'improving' ? 'text-green-600' : child.quality === 'worsening' ? 'text-red-600' : 'text-gray-500';
              const rowBg = child.quality === 'worsening' ? 'bg-red-50/50' : child.quality === 'improving' ? 'bg-green-50/50' : '';

              return (
                <tr key={child.id} className={`border-b last:border-0 hover:bg-gray-50 ${rowBg}`}>
                  <td className="py-2 px-3 font-medium text-gray-700">{child.name}</td>
                  <td className="py-2 px-3 text-center text-blue-700">{child.sla_a?.toFixed(1)}%</td>
                  <td className="py-2 px-3 text-center text-emerald-700">{child.sla_b?.toFixed(1)}%</td>
                  <td className={`py-2 px-3 text-center font-semibold ${deltaColor}`}>
                    {child.delta > 0 ? '+' : ''}{child.delta?.toFixed(1)}pp
                  </td>
                  <td className="py-2 px-3 text-center text-blue-700">{Math.round(child.mttr_a || 0)}</td>
                  <td className="py-2 px-3 text-center text-emerald-700">{Math.round(child.mttr_b || 0)}</td>
                  <td className="py-2 px-3 text-center">
                    <Icon size={16} className={`inline ${iconColor}`} />
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

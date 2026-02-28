import { TrendingUp, TrendingDown, Minus } from 'lucide-react';

function DeltaCard({ kpi, data }) {
  const qualityColors = {
    improving: 'border-green-300 bg-green-50',
    worsening: 'border-red-300 bg-red-50',
    stable: 'border-gray-200 bg-gray-50',
  };
  const qualityText = {
    improving: 'text-green-700',
    worsening: 'text-red-700',
    stable: 'text-gray-600',
  };

  const Icon = data.quality === 'improving' ? TrendingUp : data.quality === 'worsening' ? TrendingDown : Minus;
  const iconColor = data.quality === 'improving' ? 'text-green-500' : data.quality === 'worsening' ? 'text-red-500' : 'text-gray-400';

  return (
    <div className={`rounded-lg border-2 p-3 ${qualityColors[data.quality]}`}>
      <div className="flex items-center justify-between mb-1">
        <span className="text-xs font-medium text-gray-500 uppercase">{data.label}</span>
        <Icon size={16} className={iconColor} />
      </div>
      <div className="flex items-baseline gap-2 mb-1">
        <span className="text-lg font-bold text-gray-900">
          {data.delta > 0 ? '+' : ''}{data.delta}
        </span>
        <span className="text-xs text-gray-500">
          ({data.pct_change > 0 ? '+' : ''}{data.pct_change}%)
        </span>
      </div>
      <div className="flex items-center gap-2 text-xs text-gray-500">
        <span className="px-1.5 py-0.5 bg-blue-100 rounded text-blue-700">{data.value_a}</span>
        <span>→</span>
        <span className="px-1.5 py-0.5 bg-emerald-100 rounded text-emerald-700">{data.value_b}</span>
      </div>
    </div>
  );
}

export default function DeltaSummary({ deltas }) {
  if (!deltas || Object.keys(deltas).length === 0) return null;

  const improving = Object.values(deltas).filter(d => d.quality === 'improving').length;
  const worsening = Object.values(deltas).filter(d => d.quality === 'worsening').length;
  const stable = Object.values(deltas).filter(d => d.quality === 'stable').length;

  return (
    <div className="bg-white rounded-lg border p-5 space-y-4">
      <div className="flex items-center justify-between">
        <h3 className="text-sm font-semibold text-gray-700 uppercase tracking-wide">Delta KPI</h3>
        <div className="flex items-center gap-3 text-xs">
          {improving > 0 && <span className="text-green-600">✅ {improving} membaik</span>}
          {worsening > 0 && <span className="text-red-600">❌ {worsening} memburuk</span>}
          {stable > 0 && <span className="text-gray-500">─ {stable} stabil</span>}
        </div>
      </div>
      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3">
        {Object.entries(deltas).map(([kpi, data]) => (
          <DeltaCard key={kpi} kpi={kpi} data={data} />
        ))}
      </div>
    </div>
  );
}

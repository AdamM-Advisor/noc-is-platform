import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, ReferenceLine, BarChart, Bar, Cell,
} from 'recharts';

const RISK_COLORS = { high: '#DC2626', medium: '#D97706', low: '#16A34A' };
const BEHAVIOR_COLORS = {
  chronic: '#DC2626', deteriorating: '#D97706', sporadic: '#F59E0B',
  seasonal: '#8B5CF6', improving: '#3B82F6', healthy: '#16A34A',
};

function MiniTrend({ data, dataKey, title, color, target }) {
  if (!data?.length) return <div className="text-xs text-gray-400">No data</div>;
  return (
    <div className="bg-white rounded-xl border border-gray-200 p-4">
      <p className="text-xs font-semibold text-gray-500 mb-2">{title}</p>
      <ResponsiveContainer width="100%" height={140}>
        <LineChart data={data} margin={{ top: 5, right: 10, left: -10, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#F3F4F6" />
          <XAxis dataKey="period" tick={{ fontSize: 10 }} tickFormatter={(v) => v?.slice(5)} />
          <YAxis tick={{ fontSize: 10 }} domain={['auto', 'auto']} />
          <Tooltip
            contentStyle={{ fontSize: 11, border: '1px solid #E5E7EB', borderRadius: 8 }}
            formatter={(v) => [typeof v === 'number' ? v.toFixed(1) : v, title]}
          />
          {target != null && (
            <ReferenceLine y={target} stroke="#DC2626" strokeDasharray="5 5" label={{ value: `Target ${target}%`, position: 'right', fontSize: 9, fill: '#DC2626' }} />
          )}
          <Line type="monotone" dataKey={dataKey} stroke={color} strokeWidth={2} dot={{ r: 3 }} />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}

function DistributionCard({ title, data, colorMap, labelMap }) {
  if (!data) return null;
  const total = data.total || Object.values(data).reduce((s, v) => s + (typeof v === 'number' ? v : 0), 0);
  const items = Object.entries(data)
    .filter(([k]) => k !== 'total')
    .map(([k, v]) => ({ key: k, count: v, pct: total > 0 ? (v / total * 100).toFixed(1) : 0 }));

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-4">
      <p className="text-xs font-semibold text-gray-500 mb-3">{title}</p>
      <div className="space-y-2">
        {items.map(({ key, count, pct }) => (
          <div key={key} className="flex items-center gap-2">
            <span
              className="w-3 h-3 rounded-full shrink-0"
              style={{ backgroundColor: colorMap[key] || '#9CA3AF' }}
            />
            <span className="text-xs text-gray-600 flex-1 capitalize">
              {labelMap?.[key] || key}
            </span>
            <span className="text-xs font-medium text-gray-700">
              {count >= 1000 ? `${(count / 1000).toFixed(1)}K` : count}
            </span>
            <span className="text-[10px] text-gray-400 w-12 text-right">({pct}%)</span>
          </div>
        ))}
      </div>
      {total > 0 && (
        <div className="mt-2 h-2 rounded-full bg-gray-100 flex overflow-hidden">
          {items.map(({ key, count }) => (
            <div
              key={key}
              style={{ width: `${(count / total * 100)}%`, backgroundColor: colorMap[key] || '#9CA3AF' }}
              className="h-full"
            />
          ))}
        </div>
      )}
    </div>
  );
}

const RISK_LABELS = { high: 'HIGH', medium: 'MEDIUM', low: 'LOW' };
const BEHAVIOR_LABELS = {
  chronic: 'Chronic', deteriorating: 'Deteriorating', sporadic: 'Sporadic',
  seasonal: 'Seasonal', improving: 'Improving', healthy: 'Healthy',
};

export default function QuickChartGrid({ charts }) {
  if (!charts) return null;

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
      <MiniTrend
        data={charts.sla_trend}
        dataKey="value"
        title="SLA Trend"
        color="#2563EB"
        target={charts.sla_target}
      />
      <MiniTrend
        data={charts.volume_trend}
        dataKey="value"
        title="Volume Trend"
        color="#7C3AED"
      />
      <DistributionCard
        title="Risk Distribution"
        data={charts.risk_distribution}
        colorMap={RISK_COLORS}
        labelMap={RISK_LABELS}
      />
      <DistributionCard
        title="Behavior Distribution"
        data={charts.behavior_distribution}
        colorMap={BEHAVIOR_COLORS}
        labelMap={BEHAVIOR_LABELS}
      />
    </div>
  );
}

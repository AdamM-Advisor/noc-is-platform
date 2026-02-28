import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Cell } from 'recharts';

const RISK_COLORS = { HIGH: '#DC2626', MEDIUM: '#D97706', LOW: '#16A34A' };

export default function RiskAggregation({ data }) {
  if (!data?.summary) return null;

  const { summary, narrative, worst_site } = data;

  const distData = [
    { name: 'HIGH', value: summary.n_high || 0, color: RISK_COLORS.HIGH },
    { name: 'MEDIUM', value: summary.n_medium || 0, color: RISK_COLORS.MEDIUM },
    { name: 'LOW', value: summary.n_low || 0, color: RISK_COLORS.LOW },
  ];

  return (
    <div className="space-y-4">
      <div className="flex flex-col md:flex-row gap-6">
        <div className="w-full md:w-1/2 h-52">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={distData} layout="vertical" margin={{ left: 10, right: 20 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#E5E7EB" horizontal={false} />
              <XAxis type="number" tick={{ fontSize: 11 }} />
              <YAxis type="category" dataKey="name" tick={{ fontSize: 12, fontWeight: 600 }} width={70} />
              <Tooltip formatter={(v) => v.toLocaleString()} />
              <Bar dataKey="value" radius={[0, 4, 4, 0]} barSize={28}>
                {distData.map((d, i) => (
                  <Cell key={i} fill={d.color} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>

        <div className="w-full md:w-1/2 space-y-3">
          <div className="grid grid-cols-2 gap-2">
            <div className="bg-gray-50 rounded-lg p-3 border">
              <div className="text-xl font-bold text-gray-900">{(summary.avg_risk ?? 0).toFixed(0)}</div>
              <div className="text-xs text-gray-500">Avg Risk Score</div>
            </div>
            <div className="bg-gray-50 rounded-lg p-3 border">
              <div className="text-xl font-bold text-gray-900">{summary.total ?? 0}</div>
              <div className="text-xs text-gray-500">Total Sites</div>
            </div>
            <div className="bg-red-50 rounded-lg p-3 border border-red-100">
              <div className="text-xl font-bold text-red-600">{summary.n_high ?? 0} <span className="text-sm font-normal">({(summary.pct_high ?? 0).toFixed(0)}%)</span></div>
              <div className="text-xs text-gray-500">High Risk</div>
            </div>
            {worst_site && (
              <div className="bg-gray-50 rounded-lg p-3 border">
                <div className="text-sm font-bold text-gray-900">{worst_site.name}</div>
                <div className="text-xs text-gray-500">Worst: {worst_site.score}</div>
              </div>
            )}
          </div>

          {narrative && (
            <div className={`text-sm px-3 py-2 rounded border ${
              (summary.pct_high ?? 0) > 20 ? 'bg-red-50 border-red-200 text-red-700' :
              (summary.pct_high ?? 0) > 10 ? 'bg-amber-50 border-amber-200 text-amber-700' :
              'bg-green-50 border-green-200 text-green-700'
            }`}>
              {narrative}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

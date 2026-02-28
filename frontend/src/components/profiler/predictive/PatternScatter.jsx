import {
  ScatterChart, Scatter, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, ZAxis, Cell,
} from 'recharts';

const RISK_COLORS = { HIGH: '#DC2626', MEDIUM: '#D97706', LOW: '#16A34A' };

function CustomTooltip({ active, payload }) {
  if (!active || !payload?.length) return null;
  const d = payload[0]?.payload;
  if (!d) return null;

  return (
    <div className="bg-white border border-gray-200 shadow-lg rounded-lg p-3 text-xs max-w-xs">
      <p className="font-semibold text-gray-700">{d.site_name || d.site_id}</p>
      <p className="text-gray-500">Avg Interval: {d.avg_gap_days?.toFixed(1)} hari</p>
      <p className="text-gray-500">Risk Score: {d.risk_score?.toFixed(0)}</p>
      <p className="text-gray-500">Tiket: {d.ticket_count}</p>
      {d.pattern && <p className="text-gray-500">Pola: {d.pattern}</p>}
      {d.predicted_next && <p className="text-gray-500">Next: {d.predicted_next}</p>}
    </div>
  );
}

export default function PatternScatter({ data, consistentSites }) {
  if (!data?.length) return <p className="text-sm text-gray-400">Tidak ada data pattern.</p>;

  return (
    <div className="space-y-4">
      <ResponsiveContainer width="100%" height={300}>
        <ScatterChart margin={{ top: 10, right: 20, left: 10, bottom: 5 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#E5E7EB" />
          <XAxis
            dataKey="avg_gap_days"
            name="Avg Interval (hari)"
            tick={{ fontSize: 11 }}
            label={{ value: 'Avg Interval Gap (hari)', position: 'insideBottom', offset: -3, fontSize: 11 }}
          />
          <YAxis
            dataKey="risk_score"
            name="Risk Score"
            tick={{ fontSize: 11 }}
            domain={[0, 100]}
            label={{ value: 'Risk Score', angle: -90, position: 'insideLeft', fontSize: 11 }}
          />
          <ZAxis dataKey="ticket_count" range={[40, 400]} name="Volume" />
          <Tooltip content={<CustomTooltip />} />
          <Scatter data={data} name="Sites">
            {data.map((d, i) => {
              const level = d.risk_score >= 70 ? 'HIGH' : d.risk_score >= 40 ? 'MEDIUM' : 'LOW';
              return <Cell key={i} fill={RISK_COLORS[level]} fillOpacity={0.7} />;
            })}
          </Scatter>
        </ScatterChart>
      </ResponsiveContainer>

      <div className="flex items-center gap-4 text-xs text-gray-500">
        <span className="flex items-center gap-1"><span className="w-3 h-3 rounded-full bg-red-500 inline-block" /> HIGH</span>
        <span className="flex items-center gap-1"><span className="w-3 h-3 rounded-full bg-yellow-500 inline-block" /> MEDIUM</span>
        <span className="flex items-center gap-1"><span className="w-3 h-3 rounded-full bg-green-500 inline-block" /> LOW</span>
        <span className="text-gray-400">| Size = ticket count</span>
      </div>

      {consistentSites?.length > 0 && (
        <div className="space-y-1">
          <p className="text-xs font-semibold text-gray-500 uppercase">Sites dengan pola konsisten:</p>
          {consistentSites.map((s, i) => (
            <div key={i} className="text-xs text-gray-600">
              • <span className="font-medium">{s.site_name || s.site_id}</span>: setiap ~{s.avg_gap_days?.toFixed(0)} hari
              <span className="text-gray-400"> (CV {s.cv?.toFixed(2)})</span>
              {s.predicted_next && <span className="text-gray-500 ml-1">next: {s.predicted_next}</span>}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

import { Radar, RadarChart, PolarGrid, PolarAngleAxis, PolarRadiusAxis, ResponsiveContainer, Legend } from 'recharts';

export default function RadarOverlay({ radar, profileA, profileB }) {
  if (!radar || !radar.axes) return null;

  const data = radar.axes.map((axis, i) => ({
    axis,
    A: radar.values_a[i],
    B: radar.values_b[i],
  }));

  return (
    <div className="bg-white rounded-lg border p-5 space-y-4">
      <h3 className="text-sm font-semibold text-gray-700 uppercase tracking-wide">Radar Overlay</h3>
      <div className="flex items-center justify-center gap-6 text-xs text-gray-500">
        <span>
          Komposit A: <span className="font-semibold text-blue-600">{radar.composite_a}</span>
        </span>
        <span>
          Komposit B: <span className="font-semibold text-emerald-600">{radar.composite_b}</span>
        </span>
      </div>
      <div className="h-80">
        <ResponsiveContainer width="100%" height="100%">
          <RadarChart data={data} cx="50%" cy="50%" outerRadius="70%">
            <PolarGrid stroke="#e5e7eb" />
            <PolarAngleAxis dataKey="axis" tick={{ fontSize: 11, fill: '#6b7280' }} />
            <PolarRadiusAxis angle={30} domain={[0, 100]} tick={{ fontSize: 10 }} />
            <Radar
              name={profileA?.entity_name || 'A'}
              dataKey="A"
              stroke="#3b82f6"
              fill="#3b82f6"
              fillOpacity={0.2}
              strokeWidth={2}
            />
            <Radar
              name={profileB?.entity_name || 'B'}
              dataKey="B"
              stroke="#10b981"
              fill="#10b981"
              fillOpacity={0.2}
              strokeWidth={2}
            />
            <Legend wrapperStyle={{ fontSize: 12 }} />
          </RadarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

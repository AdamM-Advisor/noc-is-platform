import {
  RadarChart, Radar, PolarGrid, PolarAngleAxis, PolarRadiusAxis,
  ResponsiveContainer, Tooltip,
} from 'recharts';
import FormulaBox from './FormulaBox';

const COMP_LABELS = {
  frequency: 'Frekuensi',
  recency: 'Recency',
  severity: 'Severity Mix',
  mttr_trend: 'MTTR Trend',
  repeat: 'Repeat Rate',
  device: 'Device Aging',
  escalation: 'Eskalasi',
};

const COMP_ORDER = ['frequency', 'recency', 'severity', 'mttr_trend', 'repeat', 'device', 'escalation'];

export default function RiskRadarChart({ data }) {
  if (!data) return null;

  const radarData = COMP_ORDER.map(key => ({
    name: COMP_LABELS[key] || key,
    value: data.components?.[key] ?? 0,
    weight: data.weights?.[key] ?? 0,
    threshold: 70,
  }));

  const status = data.status || {};
  const score = data.risk_score ?? 0;

  return (
    <div className="space-y-4">
      <div className="flex flex-col md:flex-row gap-6">
        <div className="w-full md:w-1/2 h-72">
          <ResponsiveContainer width="100%" height="100%">
            <RadarChart data={radarData} cx="50%" cy="50%" outerRadius="70%">
              <PolarGrid stroke="#E5E7EB" />
              <PolarAngleAxis dataKey="name" tick={{ fontSize: 11, fill: '#6B7280' }} />
              <PolarRadiusAxis angle={90} domain={[0, 100]} tick={{ fontSize: 9 }} />
              <Radar
                name="Threshold"
                dataKey="threshold"
                stroke="#DC2626"
                fill="none"
                strokeDasharray="4 3"
                strokeWidth={1.5}
              />
              <Radar
                name="Score"
                dataKey="value"
                stroke="#3B82F6"
                fill="#3B82F6"
                fillOpacity={0.25}
                strokeWidth={2}
              />
              <Tooltip
                formatter={(val, name, props) => {
                  const w = props.payload?.weight;
                  return [`${val.toFixed(0)} (bobot: ${((w || 0) * 100).toFixed(0)}%)`, name];
                }}
              />
            </RadarChart>
          </ResponsiveContainer>
        </div>

        <div className="w-full md:w-1/2 space-y-3">
          <div className="flex items-center gap-3">
            <span className="text-3xl font-bold" style={{ color: status.color || '#374151' }}>
              {status.icon} {score.toFixed(0)}
            </span>
            <span className="text-lg text-gray-500">/ 100</span>
          </div>
          <div className="text-sm font-semibold" style={{ color: status.color }}>
            Status: {status.level || 'N/A'}
          </div>

          <div className="space-y-1.5 mt-2">
            <p className="text-xs font-semibold text-gray-500 uppercase">Komponen tertinggi:</p>
            {COMP_ORDER
              .sort((a, b) => (data.components?.[b] ?? 0) - (data.components?.[a] ?? 0))
              .slice(0, 3)
              .map(key => (
                <div key={key} className="flex items-center gap-2 text-sm">
                  <span className="text-gray-500">•</span>
                  <span className="text-gray-700 font-medium">{COMP_LABELS[key]}:</span>
                  <span className="text-gray-600">{(data.components?.[key] ?? 0).toFixed(0)}</span>
                  <span className="text-gray-400 text-xs">({((data.weights?.[key] ?? 0) * 100).toFixed(0)}%)</span>
                </div>
              ))}
          </div>

          {data.narrative && (
            <div className={`text-sm px-3 py-2 rounded border mt-3 ${
              score >= 70 ? 'bg-red-50 border-red-200 text-red-700' :
              score >= 40 ? 'bg-amber-50 border-amber-200 text-amber-700' :
              'bg-green-50 border-green-200 text-green-700'
            }`}>
              {data.narrative}
            </div>
          )}
        </div>
      </div>

      <FormulaBox title="Risk Score Formula" formulaText={data.formula_text} />
    </div>
  );
}

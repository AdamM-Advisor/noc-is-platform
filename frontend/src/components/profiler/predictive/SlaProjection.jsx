import { useMemo } from 'react';
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, ReferenceLine, ReferenceArea,
} from 'recharts';
import FormulaBox from './FormulaBox';

export default function SlaProjection({ data }) {
  if (!data) return null;
  if (data.error) return <p className="text-sm text-red-500">{data.error}</p>;

  const chartData = useMemo(() => {
    const rows = [];

    (data.historical || []).forEach(h => {
      rows.push({
        label: h.period,
        actual: h.sla_pct ?? h.value,
        projected: null,
        type: 'historical',
      });
    });

    const lastActual = rows.length > 0 ? rows[rows.length - 1].actual : null;

    (data.projections || []).forEach((p, i) => {
      if (i === 0 && rows.length > 0) {
        rows[rows.length - 1].projected = lastActual;
      }
      rows.push({
        label: `+${p.week_offset}w`,
        actual: null,
        projected: p.projected_sla,
        type: 'projection',
      });
    });

    return rows;
  }, [data]);

  const statusColor = data.status === 'already_breached' ? 'bg-red-50 border-red-200 text-red-700' :
    data.status === 'breach_predicted' ? 'bg-amber-50 border-amber-200 text-amber-700' :
    'bg-green-50 border-green-200 text-green-700';

  return (
    <div className="space-y-4">
      <ResponsiveContainer width="100%" height={280}>
        <LineChart data={chartData} margin={{ top: 10, right: 20, left: 10, bottom: 5 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#E5E7EB" />
          <XAxis dataKey="label" tick={{ fontSize: 11 }} />
          <YAxis tick={{ fontSize: 11 }} domain={['auto', 'auto']} />
          <Tooltip formatter={(val) => val != null ? `${val.toFixed(1)}%` : null} />

          {data.target && (
            <ReferenceLine
              y={data.target}
              stroke="#DC2626"
              strokeDasharray="6 3"
              label={{ value: `Target: ${data.target}%`, position: 'right', fontSize: 10, fill: '#DC2626' }}
            />
          )}

          {data.breach_week && (
            <ReferenceLine
              x={`+${data.breach_week}w`}
              stroke="#DC2626"
              strokeDasharray="3 3"
              label={{ value: '⊗ Breach', position: 'top', fontSize: 11, fill: '#DC2626' }}
            />
          )}

          <Line
            type="monotone"
            dataKey="actual"
            stroke="#2563EB"
            strokeWidth={2}
            dot={{ r: 3 }}
            connectNulls={false}
            name="SLA Aktual"
          />

          <Line
            type="monotone"
            dataKey="projected"
            stroke="#D97706"
            strokeWidth={2}
            strokeDasharray="8 4"
            dot={{ r: 3, strokeDasharray: '' }}
            connectNulls={false}
            name="SLA Projected"
          />
        </LineChart>
      </ResponsiveContainer>

      {data.narrative && (
        <div className={`text-sm px-3 py-2 rounded border ${statusColor}`}>
          {data.narrative}
        </div>
      )}

      <FormulaBox title="SLA Projection Formula" formulaText={data.formula_text} />
    </div>
  );
}

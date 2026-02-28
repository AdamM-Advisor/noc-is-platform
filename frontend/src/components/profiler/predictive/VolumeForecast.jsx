import { useMemo } from 'react';
import {
  ComposedChart, Line, Area, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer,
} from 'recharts';
import FormulaBox from './FormulaBox';
import CapacityAlert from './CapacityAlert';

export default function VolumeForecast({ data }) {
  if (!data) return null;
  if (data.error) return <p className="text-sm text-red-500">{data.error}</p>;

  const chartData = useMemo(() => {
    const rows = [];

    (data.historical || []).forEach((h, i) => {
      rows.push({
        label: h.period,
        actual: h.value,
        forecast: null,
        ci_lower: null,
        ci_upper: null,
        ciRange: null,
        type: 'historical',
      });
    });

    const lastHistorical = data.historical?.[data.historical.length - 1];

    (data.forecasts || []).forEach((f, i) => {
      const periodLabel = `+${f.period_offset}`;
      if (i === 0 && lastHistorical) {
        const lastRow = rows[rows.length - 1];
        if (lastRow) {
          lastRow.forecast = lastRow.actual;
          lastRow.ci_lower = lastRow.actual;
          lastRow.ci_upper = lastRow.actual;
          lastRow.ciRange = [lastRow.actual, lastRow.actual];
        }
      }
      rows.push({
        label: f.period_label || periodLabel,
        actual: null,
        forecast: f.forecast,
        ci_lower: f.ci_lower,
        ci_upper: f.ci_upper,
        ciRange: [f.ci_lower, f.ci_upper],
        type: 'forecast',
      });
    });

    return rows;
  }, [data]);

  const trendColor = data.pct_change > 5 ? '#DC2626' : data.pct_change < -5 ? '#475569' : '#6B7280';
  const trendLabel = data.pct_change > 5 ? 'Naik' : data.pct_change < -5 ? 'Turun' : 'Stabil';

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-4 flex-wrap">
        <span className="text-sm text-gray-600">
          Trend: <span className="font-semibold" style={{ color: trendColor }}>{trendLabel} — {data.trend_word}</span>
          <span className="text-gray-400 ml-1">({data.pct_change > 0 ? '+' : ''}{data.pct_change?.toFixed(1)}%)</span>
        </span>
      </div>

      <ResponsiveContainer width="100%" height={280}>
        <ComposedChart data={chartData} margin={{ top: 10, right: 20, left: 10, bottom: 5 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#E5E7EB" />
          <XAxis dataKey="label" tick={{ fontSize: 11 }} />
          <YAxis tick={{ fontSize: 11 }} tickFormatter={(v) => v >= 1000 ? `${(v / 1000).toFixed(0)}K` : v} />
          <Tooltip
            formatter={(val, name) => {
              if (val == null) return [null, null];
              const label = name === 'actual' ? 'Aktual' : name === 'forecast' ? 'Forecast' : name;
              return [typeof val === 'number' ? val.toLocaleString() : val, label];
            }}
          />

          <Area
            dataKey="ciRange"
            fill="#3B82F6"
            fillOpacity={0.1}
            stroke="none"
            name="CI Band"
          />

          <Line
            type="monotone"
            dataKey="actual"
            stroke="#2563EB"
            strokeWidth={2}
            dot={{ r: 3 }}
            connectNulls={false}
            name="actual"
          />

          <Line
            type="monotone"
            dataKey="forecast"
            stroke="#2563EB"
            strokeWidth={2}
            strokeDasharray="8 4"
            dot={{ r: 3, strokeDasharray: '' }}
            connectNulls={false}
            name="forecast"
          />
        </ComposedChart>
      </ResponsiveContainer>

      {data.forecasts?.length > 0 && (
        <div className="space-y-1">
          <p className="text-xs font-semibold text-gray-500 uppercase">Forecast Detail</p>
          {data.forecasts.map((f, i) => (
            <div key={i} className="text-xs text-gray-600 flex gap-2">
              <span className="font-medium">{f.period_label || `+${f.period_offset}`}:</span>
              <span>{f.forecast?.toLocaleString()}</span>
              <span className="text-gray-400">(CI: {f.ci_lower?.toLocaleString()} – {f.ci_upper?.toLocaleString()})</span>
            </div>
          ))}
        </div>
      )}

      {data.narrative && (
        <div className="text-sm px-3 py-2 rounded border bg-gray-50 border-gray-200 text-gray-600">
          {data.narrative}
        </div>
      )}

      <CapacityAlert data={data.capacity_check} />
      <FormulaBox title="Forecast Formula" formulaText={data.formula_text} />
    </div>
  );
}

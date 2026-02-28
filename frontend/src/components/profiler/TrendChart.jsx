import { useMemo, useState } from 'react';
import {
  ComposedChart, Line, Area, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, ReferenceLine, Scatter,
} from 'recharts';
import { Eye, EyeOff } from 'lucide-react';

const KPI_COLORS = {
  sla_pct: '#2563EB',
  avg_mttr_min: '#DC2626',
  total_tickets: '#059669',
  escalation_pct: '#D97706',
  auto_resolve_pct: '#7C3AED',
  repeat_pct: '#EC4899',
};

const KPI_LABELS = {
  sla_pct: 'SLA %',
  avg_mttr_min: 'MTTR (min)',
  total_tickets: 'Volume',
  escalation_pct: 'Eskalasi %',
  auto_resolve_pct: 'Auto-resolve %',
  repeat_pct: 'Repeat %',
};

function AnomalyDot({ cx, cy, payload }) {
  if (!payload || !payload._isAnomaly) return null;
  return (
    <circle cx={cx} cy={cy} r={6} fill="#DC2626" stroke="#fff" strokeWidth={2} />
  );
}

function CustomTooltip({ active, payload, label, anomalyMap, annotationMap }) {
  if (!active || !payload?.length) return null;

  const period = payload[0]?.payload?.period || label;
  const anomaly = anomalyMap?.[period];
  const annots = annotationMap?.[period] || [];

  return (
    <div className="bg-white border border-gray-200 shadow-lg rounded-lg p-3 max-w-xs text-xs">
      <p className="font-semibold text-gray-700 mb-1">{label}</p>
      {payload.map((p, i) => (
        <div key={i} className="flex items-center gap-2">
          <span className="w-3 h-3 rounded-full" style={{ backgroundColor: p.color }} />
          <span className="text-gray-600">{p.name}:</span>
          <span className="font-medium">{typeof p.value === 'number' ? p.value.toFixed(1) : p.value}</span>
        </div>
      ))}
      {anomaly && (
        <div className="mt-2 pt-2 border-t border-red-100 text-red-600">
          {anomaly.narrative}
        </div>
      )}
      {annots.map((a, i) => (
        <div key={i} className="mt-1 pt-1 border-t border-blue-100 text-blue-600">
          <span className="font-medium">{a.title}</span>
          {a.description && <span className="text-blue-500"> — {a.description}</span>}
          <span className="text-blue-400 ml-1">({a.source})</span>
        </div>
      ))}
    </div>
  );
}

export default function TrendChart({
  trendData,
  trendMultiData,
  trendKpis,
  annotations,
  onAddKpi,
  onRemoveKpi,
}) {
  const [showTrend, setShowTrend] = useState(true);
  const [showTarget, setShowTarget] = useState(true);
  const [showBand, setShowBand] = useState(true);
  const [showAnomaly, setShowAnomaly] = useState(true);
  const [showAnnotations, setShowAnnotations] = useState(true);

  const primaryKpi = trendKpis[0] || 'sla_pct';
  const primaryData = trendMultiData[primaryKpi] || trendData;

  const anomalyMap = useMemo(() => {
    if (!primaryData?.anomalies) return {};
    const m = {};
    primaryData.anomalies.forEach(a => { m[a.period] = a; });
    return m;
  }, [primaryData]);

  const annotationMap = useMemo(() => {
    if (!annotations?.length) return {};
    const m = {};
    annotations.forEach(a => {
      const ym = a.date?.substring(0, 7);
      if (ym) {
        if (!m[ym]) m[ym] = [];
        m[ym].push(a);
      }
    });
    return m;
  }, [annotations]);

  const chartData = useMemo(() => {
    if (!primaryData?.data_points?.length) return [];
    return primaryData.data_points.map((dp, idx) => {
      const row = {
        label: dp.label,
        period: dp.period,
        [primaryKpi]: dp.value,
        _isAnomaly: showAnomaly && !!anomalyMap[dp.period],
      };

      if (showTrend && primaryData.trend_line) {
        row._trendLine = primaryData.trend_line[idx];
      }
      if (showBand && primaryData.band?.upper?.length) {
        row._upper = primaryData.band.upper[idx];
        row._lower = primaryData.band.lower[idx];
        row._bandRange = [primaryData.band.lower[idx], primaryData.band.upper[idx]];
      }

      trendKpis.slice(1).forEach(kpi => {
        const kd = trendMultiData[kpi];
        if (kd?.data_points?.[idx]) {
          row[kpi] = kd.data_points[idx].value;
        }
      });

      return row;
    });
  }, [primaryData, trendMultiData, trendKpis, anomalyMap, showTrend, showBand, showAnomaly]);

  const periodToLabel = useMemo(() => {
    if (!primaryData?.data_points) return {};
    const m = {};
    primaryData.data_points.forEach(d => { m[d.period] = d.label; });
    return m;
  }, [primaryData]);

  const annotationLines = useMemo(() => {
    if (!showAnnotations || !annotations?.length || !primaryData?.data_points?.length) return [];
    const periods = new Set(primaryData.data_points.map(d => d.period));
    return annotations
      .filter(a => a.show_on_chart && periods.has(a.date?.substring(0, 7)))
      .map(a => ({
        xLabel: periodToLabel[a.date?.substring(0, 7)] || a.date?.substring(0, 7),
        icon: '|',
        title: a.title,
        color: a.color || '#6366F1',
      }));
  }, [annotations, primaryData, showAnnotations, periodToLabel]);

  const availableKpis = Object.keys(KPI_LABELS).filter(k => !trendKpis.includes(k));

  if (!primaryData) return null;

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between flex-wrap gap-2">
        <div className="flex items-center gap-2 flex-wrap">
          {trendKpis.map(kpi => (
            <span
              key={kpi}
              className="inline-flex items-center gap-1 px-2.5 py-1 rounded-full text-xs font-medium border"
              style={{ borderColor: KPI_COLORS[kpi], color: KPI_COLORS[kpi], backgroundColor: KPI_COLORS[kpi] + '10' }}
            >
              <span className="w-2 h-2 rounded-full" style={{ backgroundColor: KPI_COLORS[kpi] }} />
              {KPI_LABELS[kpi]}
              {trendKpis.length > 1 && (
                <button onClick={() => onRemoveKpi(kpi)} className="ml-1 hover:opacity-70">×</button>
              )}
            </span>
          ))}
          {trendKpis.length < 4 && availableKpis.length > 0 && (
            <select
              className="border rounded px-2 py-1 text-xs text-gray-500"
              value=""
              onChange={(e) => { if (e.target.value) onAddKpi(e.target.value); }}
            >
              <option value="">+ Tambah KPI</option>
              {availableKpis.map(k => (
                <option key={k} value={k}>{KPI_LABELS[k]}</option>
              ))}
            </select>
          )}
        </div>

        <div className="flex items-center gap-3 text-xs">
          {[
            { key: 'trend', label: 'Tren', state: showTrend, set: setShowTrend },
            { key: 'target', label: 'Target', state: showTarget, set: setShowTarget },
            { key: 'band', label: '±2σ Band', state: showBand, set: setShowBand },
            { key: 'anomaly', label: 'Anomali', state: showAnomaly, set: setShowAnomaly },
            { key: 'annot', label: 'Anotasi', state: showAnnotations, set: setShowAnnotations },
          ].map(tog => (
            <button
              key={tog.key}
              onClick={() => tog.set(!tog.state)}
              className={`flex items-center gap-1 px-2 py-1 rounded border transition-colors ${tog.state ? 'bg-blue-50 border-blue-200 text-blue-700' : 'bg-gray-50 border-gray-200 text-gray-400'}`}
            >
              {tog.state ? <Eye size={12} /> : <EyeOff size={12} />}
              {tog.label}
            </button>
          ))}
        </div>
      </div>

      {primaryData.trend?.narrative && (
        <div
          className="text-sm px-3 py-2 rounded-r border"
          style={{
            borderLeft: `3px solid ${primaryData.trend.quality === 'worsening' ? 'var(--status-critical-dot)' : primaryData.trend.quality === 'improving' ? 'var(--status-good-dot)' : 'var(--border)'}`,
            backgroundColor: 'var(--bg-secondary)',
            color: 'var(--text-secondary)',
          }}
        >
          {primaryData.trend.narrative}
        </div>
      )}

      <ResponsiveContainer width="100%" height={320}>
        <ComposedChart data={chartData} margin={{ top: 10, right: 20, left: 10, bottom: 5 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#E5E7EB" />
          <XAxis dataKey="label" tick={{ fontSize: 11 }} />
          <YAxis tick={{ fontSize: 11 }} />
          <Tooltip content={<CustomTooltip anomalyMap={anomalyMap} annotationMap={annotationMap} />} />

          {showBand && primaryData.band?.upper?.length > 0 && (
            <Area
              dataKey="_bandRange"
              fill="#2563EB"
              fillOpacity={0.08}
              stroke="none"
              name="±2σ Band"
            />
          )}

          {showTarget && primaryData.target && (
            <ReferenceLine
              y={primaryData.target}
              stroke="#DC2626"
              strokeDasharray="6 3"
              label={{ value: `Target: ${primaryData.target}`, position: 'right', fontSize: 10, fill: '#DC2626' }}
            />
          )}

          {annotationLines.map((al, i) => (
            <ReferenceLine
              key={i}
              x={al.xLabel}
              stroke={al.color}
              strokeDasharray="3 3"
              label={{ value: al.icon, position: 'top', fontSize: 14 }}
            />
          ))}

          {trendKpis.map(kpi => (
            <Line
              key={kpi}
              type="monotone"
              dataKey={kpi}
              stroke={KPI_COLORS[kpi]}
              strokeWidth={2}
              dot={{ r: 3 }}
              name={KPI_LABELS[kpi]}
              yAxisId={0}
            />
          ))}

          {showTrend && primaryData.trend_line && (
            <Line
              type="monotone"
              dataKey="_trendLine"
              stroke={KPI_COLORS[primaryKpi]}
              strokeWidth={1.5}
              strokeDasharray="8 4"
              dot={false}
              name="Tren"
              yAxisId={0}
            />
          )}

          {showAnomaly && (
            <Scatter
              dataKey={primaryKpi}
              fill="#DC2626"
              shape={<AnomalyDot />}
              name="Anomali"
              yAxisId={0}
            />
          )}
        </ComposedChart>
      </ResponsiveContainer>

      {primaryData.anomalies?.length > 0 && showAnomaly && (
        <div className="space-y-1">
          <p className="text-xs font-semibold text-gray-500 uppercase">Anomali Terdeteksi</p>
          {primaryData.anomalies.map((a, i) => (
            <div key={i} className={`text-xs px-3 py-1.5 rounded border ${a.severity === 'significant' ? 'bg-red-50 border-red-200 text-red-700' : 'bg-amber-50 border-amber-200 text-amber-700'}`}>
              {a.narrative}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

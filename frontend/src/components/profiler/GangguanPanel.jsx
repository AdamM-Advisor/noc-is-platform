import { useState, useEffect, useMemo } from 'react';
import { RefreshCw, AlertTriangle, BarChart3, PieChart as PieChartIcon, Layers, BookOpen } from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import {
  PieChart, Pie, Cell, ResponsiveContainer, Tooltip,
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Line, ComposedChart,
} from 'recharts';

const SEVERITY_COLORS = {
  Critical: '#DC2626',
  Major: '#F59E0B',
  Minor: '#3B82F6',
  Low: '#6B7280',
};

const PARETO_COLOR = '#2563EB';
const CUMULATIVE_COLOR = '#DC2626';

const RC_COLORS = ['#2563EB', '#059669', '#D97706', '#7C3AED', '#EC4899', '#14B8A6', '#F97316', '#6366F1'];

function interpolateHeatColor(value, min, max) {
  if (value === 0) return '#F3F4F6';
  const ratio = max > min ? (value - min) / (max - min) : 0;
  const r = Math.round(247 + (220 - 247) * ratio);
  const g = Math.round(247 + (38 - 247) * ratio);
  const b = Math.round(247 + (38 - 247) * ratio);
  return `rgb(${r}, ${g}, ${b})`;
}

function SeverityDonut({ counts, total }) {
  const data = Object.entries(counts)
    .map(([name, value]) => ({ name, value }))
    .sort((a, b) => b.value - a.value);

  return (
    <div className="flex items-center gap-6 flex-wrap">
      <div className="w-48 h-48">
        <ResponsiveContainer width="100%" height="100%">
          <PieChart>
            <Pie
              data={data}
              cx="50%"
              cy="50%"
              innerRadius={45}
              outerRadius={75}
              dataKey="value"
              label={({ name, percent }) => `${name} ${(percent * 100).toFixed(0)}%`}
              labelLine={false}
            >
              {data.map((d, i) => (
                <Cell key={i} fill={SEVERITY_COLORS[d.name] || '#9CA3AF'} />
              ))}
            </Pie>
            <Tooltip formatter={(v) => v.toLocaleString()} />
          </PieChart>
        </ResponsiveContainer>
      </div>
      <div className="space-y-2">
        {data.map((d) => {
          const pct = total > 0 ? (d.value / total * 100).toFixed(1) : 0;
          return (
            <div key={d.name} className="flex items-center gap-2 text-sm">
              <span className="w-3 h-3 rounded-full" style={{ backgroundColor: SEVERITY_COLORS[d.name] || '#9CA3AF' }} />
              <span className="w-16 font-medium text-gray-700">{d.name}</span>
              <span className="text-gray-500">: {d.value.toLocaleString()} ({pct}%)</span>
            </div>
          );
        })}
        <div className="pt-1 border-t text-sm font-medium text-gray-700">
          Total: {total.toLocaleString()} tiket
        </div>
      </div>
    </div>
  );
}

function FaultPareto({ items, onFaultClick }) {
  if (!items?.length) return <p className="text-sm text-gray-400">Tidak ada data fault level.</p>;

  const maxCount = Math.max(...items.map(i => i.count));

  return (
    <div className="space-y-1.5">
      {items.map((item, idx) => {
        const barWidth = maxCount > 0 ? (item.count / maxCount) * 100 : 0;
        return (
          <div
            key={idx}
            className="flex items-center gap-2 py-1 px-1 rounded cursor-pointer hover:bg-blue-50 transition-colors"
            onClick={() => onFaultClick(item.name)}
          >
            <span className="w-28 text-xs text-gray-700 truncate text-right font-medium" title={item.name}>
              {item.name}
            </span>
            <div className="flex-1 bg-gray-100 rounded-full h-5 relative">
              <div
                className="h-5 rounded-full bg-blue-500 transition-all"
                style={{ width: `${Math.max(barWidth, 2)}%` }}
              />
            </div>
            <span className="w-20 text-xs text-gray-600 text-right">
              {item.count.toLocaleString()} ({item.pct}%)
            </span>
            <span className="w-12 text-xs text-red-500 text-right font-mono">
              {item.cumulative_pct}%
            </span>
          </div>
        );
      })}
      <div className="text-[10px] text-gray-400 text-right">↑ cumulative %</div>
    </div>
  );
}

function RcCategoryBars({ items, onRcClick }) {
  if (!items?.length) return <p className="text-sm text-gray-400">Tidak ada data RC category.</p>;

  const maxCount = Math.max(...items.map(i => i.count));

  return (
    <div className="space-y-1.5">
      {items.map((item, idx) => {
        const barWidth = maxCount > 0 ? (item.count / maxCount) * 100 : 0;
        return (
          <div
            key={idx}
            className="flex items-center gap-2 py-1 px-1 rounded cursor-pointer hover:bg-purple-50 transition-colors"
            onClick={() => onRcClick(item.name)}
          >
            <span className="w-28 text-xs text-gray-700 truncate text-right font-medium" title={item.name}>
              {item.name}
            </span>
            <div className="flex-1 bg-gray-100 rounded-full h-5 relative">
              <div
                className="h-5 rounded-full transition-all"
                style={{ width: `${Math.max(barWidth, 2)}%`, backgroundColor: RC_COLORS[idx % RC_COLORS.length] }}
              />
            </div>
            <span className="w-20 text-xs text-gray-600 text-right">
              {item.count.toLocaleString()} ({item.pct}%)
            </span>
          </div>
        );
      })}
    </div>
  );
}

function RcSeverityMatrix({ matrix }) {
  if (!matrix || Object.keys(matrix).length === 0) return null;

  const severities = ['Critical', 'Major', 'Minor', 'Low'];
  const rcNames = Object.keys(matrix);
  const allValues = rcNames.flatMap(rc => severities.map(s => matrix[rc]?.[s] || 0));
  const maxVal = Math.max(...allValues, 1);
  const minVal = Math.min(...allValues.filter(v => v > 0), 0);

  return (
    <div className="overflow-x-auto mt-3">
      <p className="text-xs font-semibold text-gray-500 uppercase mb-2">RC Category × Severity</p>
      <table className="text-xs border-collapse w-full">
        <thead>
          <tr>
            <th className="text-left px-2 py-1 text-gray-500 w-28"></th>
            {severities.map(s => (
              <th key={s} className="px-2 py-1 text-center text-gray-500 font-medium">{s}</th>
            ))}
            <th className="px-2 py-1 text-center text-gray-500 font-medium">Total</th>
          </tr>
        </thead>
        <tbody>
          {rcNames.map(rc => {
            const total = severities.reduce((sum, s) => sum + (matrix[rc]?.[s] || 0), 0);
            return (
              <tr key={rc} className="hover:bg-gray-50">
                <td className="px-2 py-1 font-medium text-gray-700 truncate max-w-[7rem]" title={rc}>{rc}</td>
                {severities.map(s => {
                  const val = matrix[rc]?.[s] || 0;
                  return (
                    <td key={s} className="px-2 py-1 text-center">
                      <span
                        className="inline-block px-2 py-0.5 rounded text-[10px] font-medium"
                        style={{
                          backgroundColor: interpolateHeatColor(val, minVal, maxVal),
                          color: val > 0 && (val - minVal) / (maxVal - minVal || 1) > 0.5 ? '#fff' : '#374151',
                        }}
                      >
                        {val > 0 ? val.toLocaleString() : '—'}
                      </span>
                    </td>
                  );
                })}
                <td className="px-2 py-1 text-center font-medium text-gray-700">{total.toLocaleString()}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function DistributionBars({ children, faultName, childLabel, onDrillDown }) {
  if (!children?.length) {
    return <p className="text-sm text-gray-400">Tidak ada data distribusi.</p>;
  }

  const maxCount = Math.max(...children.map(c => c.count));

  return (
    <div className="space-y-1.5">
      <p className="text-xs font-semibold text-gray-500 uppercase">
        {faultName} per {childLabel}
      </p>
      {children.map((child) => {
        const barWidth = maxCount > 0 ? (child.count / maxCount) * 100 : 0;
        return (
          <div
            key={child.entity_id}
            className="flex items-center gap-2 py-1 px-1 rounded cursor-pointer hover:bg-blue-50 transition-colors"
            onClick={() => onDrillDown && onDrillDown(child.entity_id)}
          >
            <span className="w-28 text-xs text-gray-700 truncate text-right font-medium" title={child.entity_name}>
              {child.entity_name}
            </span>
            <div className="flex-1 bg-gray-100 rounded-full h-5 relative">
              <div
                className={`h-5 rounded-full transition-all ${child.is_over ? 'bg-red-400' : child.is_under ? 'bg-green-400' : 'bg-blue-400'}`}
                style={{ width: `${Math.max(barWidth, 2)}%` }}
              />
            </div>
            <span className="w-20 text-xs text-gray-600 text-right">
              {child.count.toLocaleString()} ({child.actual_pct}%)
            </span>
            <span className="w-20 text-[10px] text-gray-400 text-right">
              exp: {child.expected_pct}%
            </span>
            {child.is_over && (
              <span className="inline-block w-2 h-2 rounded-full flex-shrink-0" style={{ backgroundColor: 'var(--status-warning-dot)' }} title={`Over-representation: +${child.diff_pp}pp`} />
            )}
          </div>
        );
      })}
    </div>
  );
}

function RepeatPatternsTable({ patterns, faultName }) {
  if (!patterns?.length) return null;

  return (
    <div className="mt-3">
      <p className="text-xs font-semibold text-gray-500 uppercase mb-2">
        Pola Repeat {faultName}
      </p>
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b">
              <th className="text-left px-2 py-1.5 text-gray-500">#</th>
              <th className="text-left px-2 py-1.5 text-gray-500">Site</th>
              <th className="text-right px-2 py-1.5 text-gray-500">Tiket</th>
              <th className="text-right px-2 py-1.5 text-gray-500">Interval (hari)</th>
              <th className="text-center px-2 py-1.5 text-gray-500">Pola</th>
            </tr>
          </thead>
          <tbody>
            {patterns.map((p, i) => (
              <tr key={i} className="border-b hover:bg-gray-50">
                <td className="px-2 py-1.5 text-gray-400">{i + 1}</td>
                <td className="px-2 py-1.5 font-medium text-gray-700 max-w-[10rem] truncate" title={p.site_name}>
                  {p.site_name || p.site_id}
                </td>
                <td className="px-2 py-1.5 text-right text-gray-600">{p.ticket_count}</td>
                <td className="px-2 py-1.5 text-right text-gray-600">avg {p.avg_gap_days} hari</td>
                <td className="px-2 py-1.5 text-center">
                  <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded text-[10px] font-medium bg-gray-50 text-gray-700 border border-gray-200">
                    <span className="inline-block w-1.5 h-1.5 rounded-full flex-shrink-0" style={{ backgroundColor: p.pattern === 'Regular' ? 'var(--status-critical-dot)' : p.pattern === 'Semi-regular' ? 'var(--status-warning-dot)' : 'var(--status-neutral-dot)' }} />
                    {p.pattern}
                  </span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function FaultRecommendations({ recs }) {
  if (!recs?.length) return null;

  return (
    <div className="mt-3">
      <p className="text-xs font-semibold text-gray-500 uppercase mb-2">Rekomendasi</p>
      <div className="space-y-1.5">
        {recs.map((r, i) => (
          <div
            key={i}
            className="text-sm px-3 py-2 rounded-r border"
            style={{
              borderLeft: `3px solid ${r.priority === 'critical' ? 'var(--status-critical-dot)' : 'var(--status-warning-dot)'}`,
              backgroundColor: 'var(--bg-secondary)',
              color: 'var(--text-secondary)',
            }}
          >
            {i + 1}. {r.text}
          </div>
        ))}
      </div>
    </div>
  );
}

function CrossDimensionOverviewCards({ overview }) {
  if (!overview) return null;

  const cards = [
    { label: `Tiket ${overview.fault_name}`, value: overview.volume?.toLocaleString(), color: 'blue' },
    { label: 'dari Total', value: `${overview.pct_of_total}%`, color: overview.pct_of_total > 30 ? 'red' : 'gray' },
    {
      label: `SLA (${overview.fault_name})`,
      value: `${overview.sla_pct}%`,
      sub: `vs ${overview.sla_overall}% overall`,
      color: overview.sla_delta < -3 ? 'red' : overview.sla_delta < 0 ? 'yellow' : 'green',
    },
    {
      label: `MTTR (${overview.fault_name})`,
      value: `${Math.round(overview.avg_mttr_min)} min`,
      sub: `vs ${Math.round(overview.mttr_overall)}m overall`,
      color: overview.avg_mttr_min > overview.mttr_overall * 1.2 ? 'red' : 'gray',
    },
  ];

  const dotColorMap = {
    blue: 'var(--accent-brand)',
    red: 'var(--status-critical-dot)',
    yellow: 'var(--status-warning-dot)',
    green: 'var(--status-good-dot)',
    gray: 'var(--status-neutral-dot)',
  };

  return (
    <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
      {cards.map((c, i) => (
        <div
          key={i}
          className="rounded-lg border border-gray-200 p-3"
          style={{ borderLeftWidth: '3px', borderLeftColor: dotColorMap[c.color] || dotColorMap.gray, backgroundColor: 'var(--bg-secondary)' }}
        >
          <div className="text-xl font-bold" style={{ color: 'var(--text-primary)' }}>{c.value}</div>
          <div className="text-xs mt-0.5" style={{ color: 'var(--text-muted)' }}>{c.label}</div>
          {c.sub && <div className="text-[10px] mt-0.5" style={{ color: 'var(--text-muted)' }}>{c.sub}</div>}
        </div>
      ))}
    </div>
  );
}

function TopSitesTable({ data, faultName }) {
  if (!data?.sites?.length) return null;

  return (
    <div className="mt-3">
      <p className="text-xs font-semibold text-gray-500 uppercase mb-2">
        Top Sites: {faultName}
      </p>
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b">
              <th className="text-left px-2 py-1.5 text-gray-500">#</th>
              <th className="text-left px-2 py-1.5 text-gray-500">Site</th>
              <th className="text-right px-2 py-1.5 text-gray-500">Tiket</th>
              <th className="text-right px-2 py-1.5 text-gray-500">MTTR (min)</th>
              <th className="text-right px-2 py-1.5 text-gray-500">Interval (hari)</th>
              <th className="text-center px-2 py-1.5 text-gray-500">Pola</th>
            </tr>
          </thead>
          <tbody>
            {data.sites.map((s, i) => (
              <tr key={i} className="border-b hover:bg-gray-50">
                <td className="px-2 py-1.5 text-gray-400">{i + 1}</td>
                <td className="px-2 py-1.5 font-medium text-gray-700 max-w-[10rem] truncate" title={s.site_name}>
                  {s.site_name || s.site_id}
                </td>
                <td className="px-2 py-1.5 text-right text-gray-600">{s.ticket_count}</td>
                <td className="px-2 py-1.5 text-right text-gray-600">{Math.round(s.avg_mttr_min)}</td>
                <td className="px-2 py-1.5 text-right text-gray-600">{s.avg_gap_days > 0 ? `avg ${s.avg_gap_days}` : '—'}</td>
                <td className="px-2 py-1.5 text-center">
                  <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded text-[10px] font-medium bg-gray-50 text-gray-700 border border-gray-200">
                    <span className="inline-block w-1.5 h-1.5 rounded-full flex-shrink-0" style={{ backgroundColor: s.pattern === 'Regular' ? 'var(--status-critical-dot)' : s.pattern === 'Semi-regular' ? 'var(--status-warning-dot)' : 'var(--status-neutral-dot)' }} />
                    {s.pattern}
                  </span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {data.narrative && (
        <p className="mt-2 text-xs text-gray-500 italic">{data.narrative}</p>
      )}
    </div>
  );
}

function MiniHeatmap({ data, faultName }) {
  if (!data?.cells?.length) return null;

  const flat = data.cells.flat().filter(v => v !== null && v !== undefined);
  if (!flat.length) return null;
  const minVal = Math.min(...flat);
  const maxVal = Math.max(...flat);

  return (
    <div className="mt-3">
      <p className="text-xs font-semibold text-gray-500 uppercase mb-2">
        Heatmap: {faultName} ({data.heatmap_type === 'week_x_day' ? 'Minggu × Hari' : 'Hari × Jam'})
      </p>
      <div className="overflow-x-auto">
        <table className="border-collapse">
          <thead>
            <tr>
              <th className="w-10" />
              {(data.x_labels || []).map((xl, i) => (
                <th key={i} className="px-0.5 py-0.5 text-[9px] font-medium text-gray-500 text-center min-w-[28px]">{xl}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {data.cells.map((row, yi) => (
              <tr key={yi}>
                <td className="pr-1 py-0 text-[9px] font-medium text-gray-500 text-right">{(data.y_labels || [])[yi]}</td>
                {row.map((val, xi) => (
                  <td key={xi} className="p-0">
                    <div
                      className="w-7 h-5 rounded-sm flex items-center justify-center text-[8px]"
                      style={{
                        backgroundColor: interpolateHeatColor(val || 0, minVal, maxVal),
                        color: val !== null && (val - minVal) / (maxVal - minVal || 1) > 0.6 ? '#fff' : '#374151',
                      }}
                      title={`${(data.y_labels || [])[yi]} × ${(data.x_labels || [])[xi]}: ${val ?? '-'}`}
                    >
                      {val ?? ''}
                    </div>
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {data.interpretation?.narrative && (
        <p className="mt-1 text-xs text-gray-500 italic">{data.interpretation.narrative}</p>
      )}
    </div>
  );
}

function NdcDistributionWidget({ entityLevel, entityId }) {
  const [ndcData, setNdcData] = useState(null);
  const [ndcLoading, setNdcLoading] = useState(false);
  const navigate = useNavigate();

  useEffect(() => {
    if (!entityId) { setNdcData(null); return; }
    const load = async () => {
      setNdcLoading(true);
      try {
        const url = entityLevel === 'site'
          ? `/api/ndc/site/${entityId}`
          : `/api/ndc/entity/${entityLevel}/${entityId}?limit=5`;
        const res = await fetch(url);
        const d = await res.json();
        setNdcData(Array.isArray(d) ? d : []);
      } catch {
        setNdcData([]);
      } finally {
        setNdcLoading(false);
      }
    };
    load();
  }, [entityLevel, entityId]);

  if (!entityId || ndcLoading) return null;
  if (!ndcData || ndcData.length === 0) return null;

  const items = entityLevel === 'site'
    ? Object.entries(ndcData.reduce((acc, r) => {
        acc[r.ndc_code] = (acc[r.ndc_code] || 0) + r.ticket_count;
        return acc;
      }, {})).map(([code, count]) => ({ ndc_code: code, ticket_count: count })).sort((a, b) => b.ticket_count - a.ticket_count).slice(0, 5)
    : ndcData.slice(0, 5);

  const maxTix = Math.max(...items.map(i => i.ticket_count || 0), 1);

  return (
    <div>
      <div className="flex items-center justify-between mb-2">
        <div className="flex items-center gap-2">
          <BookOpen size={14} className="text-[#1E40AF]" />
          <h4 className="text-xs font-semibold text-[#475569] uppercase tracking-wider">Distribusi NDC</h4>
        </div>
        <button
          onClick={() => navigate('/ndc')}
          className="text-xs text-[#1E40AF] hover:underline"
        >
          Lihat semua
        </button>
      </div>
      <div className="space-y-1.5">
        {items.map((item, i) => (
          <div key={i} className="flex items-center gap-2 text-xs">
            <span className="font-mono text-[#1E40AF] w-28 shrink-0 truncate">{item.ndc_code}</span>
            {item.title && <span className="text-[#475569] truncate flex-1 min-w-0">{item.title}</span>}
            <div className="w-20 shrink-0">
              <div className="h-1.5 bg-gray-100 rounded-full overflow-hidden">
                <div className="h-full bg-[#1E40AF] rounded-full" style={{ width: `${(item.ticket_count / maxTix) * 100}%` }} />
              </div>
            </div>
            <span className="font-mono text-[#0F172A] w-14 text-right shrink-0">{(item.ticket_count || 0).toLocaleString()}</span>
            {item.pct != null && <span className="font-mono text-[#475569] w-12 text-right shrink-0">{item.pct}%</span>}
          </div>
        ))}
      </div>
    </div>
  );
}

export default function GangguanPanel({
  overviewData,
  crossDimData,
  topSitesData,
  faultHeatmapData,
  loading,
  faultLevel,
  rcCategory,
  entityLevel,
  entityId,
  onFaultClick,
  onRcClick,
  onDistributionDrillDown,
}) {
  const isFiltered = !!(faultLevel || rcCategory);
  const filterName = faultLevel || rcCategory || '';

  if (loading) {
    return (
      <div className="bg-white rounded-lg border p-5">
        <div className="flex items-center justify-center py-12 text-gray-400">
          <RefreshCw size={24} className="animate-spin mr-2" /> Memuat data gangguan...
        </div>
      </div>
    );
  }

  return (
    <div className="bg-white rounded-lg border p-5 space-y-5">
      <div className="flex items-center gap-2">
        <AlertTriangle size={18} className="text-orange-500" />
        <h3 className="text-sm font-semibold text-gray-700 uppercase tracking-wide">
          Panel 3: Profil Gangguan {isFiltered ? `— ${filterName}` : '— Overview'}
        </h3>
      </div>

      {!isFiltered && overviewData && (
        <>
          <div className="space-y-4">
            <div>
              <p className="text-xs font-semibold text-gray-500 uppercase mb-2 flex items-center gap-1">
                <PieChartIcon size={14} /> Severity Mix
              </p>
              <SeverityDonut counts={overviewData.severity_mix?.counts || {}} total={overviewData.severity_mix?.total || 0} />
              {overviewData.severity_mix?.narrative && (
                <div className="mt-2 text-sm px-3 py-2 rounded border bg-gray-50 border-gray-200 text-gray-600">
                  {overviewData.severity_mix.narrative}
                </div>
              )}
            </div>

            <div>
              <p className="text-xs font-semibold text-gray-500 uppercase mb-2 flex items-center gap-1">
                <BarChart3 size={14} /> Fault Level (Pareto)
              </p>
              <FaultPareto items={overviewData.fault_pareto?.items} onFaultClick={onFaultClick} />
              {overviewData.fault_pareto?.narrative && (
                <div className="mt-2 text-sm px-3 py-2 rounded border bg-gray-50 border-gray-200 text-gray-600">
                  {overviewData.fault_pareto.narrative}
                </div>
              )}
            </div>

            <div>
              <p className="text-xs font-semibold text-gray-500 uppercase mb-2 flex items-center gap-1">
                <Layers size={14} /> Root Cause Category
              </p>
              <RcCategoryBars items={overviewData.rc_category?.items} onRcClick={onRcClick} />
              <RcSeverityMatrix matrix={overviewData.rc_category?.rc_severity_matrix} />
              {overviewData.rc_category?.narrative && (
                <div className="mt-2 text-sm px-3 py-2 rounded border bg-gray-50 border-gray-200 text-gray-600">
                  {overviewData.rc_category.narrative}
                </div>
              )}
            </div>
          </div>
        </>
      )}

      {isFiltered && crossDimData && (
        <>
          <button
            className="text-xs text-blue-600 hover:text-blue-800 underline"
            onClick={() => {
              if (onFaultClick) onFaultClick('');
              if (onRcClick) onRcClick('');
            }}
          >
            ← Kembali ke Overview
          </button>
          <CrossDimensionOverviewCards overview={crossDimData.overview} />

          {crossDimData.overview?.narrative && (
            <div className={`text-sm px-3 py-2 rounded border ${
              crossDimData.overview.sla_delta < -3
                ? 'bg-red-50 border-red-200 text-red-700'
                : 'bg-gray-50 border-gray-200 text-gray-600'
            }`}>
              {crossDimData.overview.narrative}
            </div>
          )}

          {entityLevel !== 'site' && (
            <div>
              <DistributionBars
                children={crossDimData.distribution?.children}
                faultName={filterName}
                childLabel={
                  entityLevel === 'area' ? 'Regional' :
                  entityLevel === 'regional' ? 'NOP' :
                  entityLevel === 'nop' ? 'TO' :
                  entityLevel === 'to' ? 'Site' : 'Child'
                }
                onDrillDown={onDistributionDrillDown}
              />
              {crossDimData.distribution?.narrative && (
                <div className={`mt-2 text-sm px-3 py-2 rounded border ${
                  crossDimData.distribution.narrative.includes('Over-representation')
                    ? 'bg-amber-50 border-amber-200 text-amber-700'
                    : 'bg-gray-50 border-gray-200 text-gray-600'
                }`}>
                  {crossDimData.distribution.narrative}
                </div>
              )}
            </div>
          )}

          <TopSitesTable data={topSitesData} faultName={filterName} />
          <MiniHeatmap data={faultHeatmapData} faultName={filterName} />
          <RepeatPatternsTable patterns={crossDimData.repeat_patterns} faultName={filterName} />
          <FaultRecommendations recs={crossDimData.recommendations} />
        </>
      )}

      <NdcDistributionWidget entityLevel={entityLevel} entityId={entityId} />

      {!overviewData && !crossDimData && !loading && (
        <div className="text-center py-8 text-gray-400 text-sm">
          Tidak ada data gangguan tersedia.
        </div>
      )}
    </div>
  );
}

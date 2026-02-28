import { useState, useEffect, useRef } from 'react';
import { FileText, Printer, RefreshCw } from 'lucide-react';
import axios from 'axios';

function StatusBadge({ status }) {
  if (!status) return null;
  const bgMap = {
    'KRITIS': 'bg-red-100 text-red-700 border-red-300',
    'PERLU PERHATIAN': 'bg-amber-100 text-amber-700 border-amber-300',
    'BAIK': 'bg-green-100 text-green-700 border-green-300',
    'SANGAT BAIK': 'bg-emerald-100 text-emerald-700 border-emerald-300',
  };
  return (
    <span className={`inline-flex items-center gap-1 px-3 py-1 rounded-lg text-sm font-bold border ${bgMap[status.status] || 'bg-gray-100 text-gray-700'}`}>
      {status.icon} {status.status}
    </span>
  );
}

function KpiCompactTable({ kpis, deltas }) {
  if (!kpis) return null;
  const items = [
    { label: 'Volume', val: kpis.total_volume >= 1000 ? `${(kpis.total_volume / 1000).toFixed(1)}K` : kpis.total_volume, delta: `${deltas?.volume_mom_pct > 0 ? '+' : ''}${deltas?.volume_mom_pct || 0}%` },
    { label: 'SLA', val: `${kpis.sla_pct}%`, delta: `${deltas?.sla_mom_pp > 0 ? '+' : ''}${deltas?.sla_mom_pp || 0}pp` },
    { label: 'MTTR', val: `${Math.round(kpis.avg_mttr_min)}m`, delta: `${deltas?.mttr_mom_pct > 0 ? '+' : ''}${deltas?.mttr_mom_pct || 0}%` },
    { label: 'Eskalasi', val: `${kpis.escalation_pct}%`, delta: `${deltas?.esc_mom_pp > 0 ? '+' : ''}${deltas?.esc_mom_pp || 0}pp` },
    { label: 'Auto', val: `${kpis.auto_resolve_pct}%`, delta: `${deltas?.auto_mom_pp > 0 ? '+' : ''}${deltas?.auto_mom_pp || 0}pp` },
  ];
  return (
    <table className="w-full text-xs">
      <thead><tr className="border-b">{items.map(i => <th key={i.label} className="px-2 py-1 text-gray-500 text-center">{i.label}</th>)}</tr></thead>
      <tbody><tr>{items.map(i => <td key={i.label} className="px-2 py-1.5 text-center"><div className="font-bold text-gray-800">{i.val}</div><div className="text-gray-400">{i.delta}</div></td>)}</tr></tbody>
    </table>
  );
}

function TrendSparklines({ trend3m }) {
  if (!trend3m) return null;
  const renderSparkline = (label, data, unit) => {
    if (!data?.length) return null;
    const vals = data.map(d => d.value);
    const first = vals[0];
    const last = vals[vals.length - 1];
    const change = first > 0 ? ((last - first) / first * 100) : 0;
    const dir = change > 2 ? '📈' : (change < -2 ? '📉' : '─');
    return (
      <div className="flex items-center gap-2 text-xs">
        <span className="text-gray-500 w-14">{label}:</span>
        <span className="text-gray-700">{vals.map(v => typeof v === 'number' ? (unit === '%' ? v.toFixed(1) : Math.round(v)) : v).join(' → ')}</span>
        <span>{dir}</span>
        <span className="text-gray-400">({change > 0 ? '+' : ''}{change.toFixed(1)}%)</span>
      </div>
    );
  };
  return (
    <div className="space-y-1.5">
      {renderSparkline('SLA', trend3m.sla, '%')}
      {renderSparkline('MTTR', trend3m.mttr, 'm')}
      {renderSparkline('Volume', trend3m.volume, '')}
    </div>
  );
}

function ChildRankingTable({ children }) {
  if (!children?.length) return null;
  return (
    <table className="w-full text-xs">
      <thead>
        <tr className="border-b">
          <th className="px-2 py-1 text-left text-gray-500">#</th>
          <th className="px-2 py-1 text-left text-gray-500">Nama</th>
          <th className="px-2 py-1 text-right text-gray-500">SLA%</th>
          <th className="px-2 py-1 text-right text-gray-500">MTTR</th>
          <th className="px-2 py-1 text-center text-gray-500">Trend</th>
          <th className="px-2 py-1 text-center text-gray-500">Status</th>
        </tr>
      </thead>
      <tbody>
        {children.map((c, i) => (
          <tr key={c.id} className="border-b border-gray-50">
            <td className="px-2 py-1 text-gray-400">{i + 1}</td>
            <td className="px-2 py-1 font-medium text-gray-700">{c.name || c.id}</td>
            <td className="px-2 py-1 text-right text-gray-600">{c.sla_pct}%</td>
            <td className="px-2 py-1 text-right text-gray-600">{Math.round(c.avg_mttr_min)}m</td>
            <td className="px-2 py-1 text-center">{c.trend_icon}</td>
            <td className="px-2 py-1 text-center">
              <span className="text-[10px]" style={{ color: c.status_color }}>{c.status_icon} {c.status_level}</span>
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function RecommendationList({ recommendations }) {
  if (!recommendations?.length) return null;
  return (
    <div className="space-y-1.5">
      {recommendations.map((r, i) => (
        <div key={i} className="text-xs">
          <p className="font-medium text-gray-700">
            {r.priority_info?.icon || '⚪'} {i + 1}. {r.message}
          </p>
          {r.action && <p className="text-gray-500 ml-4">▶ {r.action}</p>}
          {r.impact && <p className="text-gray-400 ml-4">📊 {r.impact}</p>}
        </div>
      ))}
    </div>
  );
}

const LEVEL_OPTIONS = [
  { value: 'area', label: 'Area' },
  { value: 'regional', label: 'Regional' },
  { value: 'nop', label: 'NOP' },
  { value: 'to', label: 'TO' },
];

export default function ReportCardPage() {
  const [level, setLevel] = useState('area');
  const [entityId, setEntityId] = useState('');
  const [period, setPeriod] = useState('');
  const [periods, setPeriods] = useState([]);
  const [entities, setEntities] = useState([]);
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const printRef = useRef();

  useEffect(() => {
    axios.get('/api/dashboard/periods').then(r => {
      const p = r.data.periods || [];
      setPeriods(p);
      if (p.length > 0) setPeriod(p[0]);
    }).catch(() => {});
  }, []);

  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    if (params.get('level')) setLevel(params.get('level'));
    if (params.get('id')) setEntityId(params.get('id'));
    if (params.get('period')) setPeriod(params.get('period'));
  }, []);

  useEffect(() => {
    const tbl = { area: 'master_area', regional: 'master_regional', nop: 'master_nop', to: 'master_to' }[level];
    const col = { area: 'area_id', regional: 'regional_id', nop: 'nop_id', to: 'to_id' }[level];
    const name = { area: 'area_name', regional: 'regional_name', nop: 'nop_name', to: 'to_name' }[level];
    if (tbl) {
      axios.get('/api/profiler/filter-options').then(r => {
        const opts = r.data;
        const list = {
          area: opts.areas || [],
          regional: opts.regionals || [],
          nop: opts.nops || [],
          to: opts.tos || [],
        }[level] || [];
        setEntities(list);
        if (list.length > 0 && !entityId) setEntityId(list[0].id);
      }).catch(() => {});
    }
  }, [level]);

  const handleGenerate = async () => {
    if (!entityId || !period) return;
    setLoading(true);
    try {
      const res = await axios.post('/api/report-card/generate', {
        entity_level: level,
        entity_id: entityId,
        period,
      });
      setData(res.data);
    } catch {
      setData(null);
    } finally {
      setLoading(false);
    }
  };

  const handlePrint = () => {
    window.print();
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between print:hidden">
        <div className="flex items-center gap-2">
          <FileText size={22} className="text-indigo-600" />
          <h2 className="text-xl font-bold text-gray-800">Report Card</h2>
        </div>
      </div>

      <div className="flex flex-wrap items-end gap-3 print:hidden">
        <div>
          <label className="text-xs text-gray-500 block mb-0.5">Level</label>
          <select value={level} onChange={(e) => { setLevel(e.target.value); setEntityId(''); }} className="border border-gray-300 rounded-lg px-3 py-1.5 text-sm">
            {LEVEL_OPTIONS.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
          </select>
        </div>
        <div>
          <label className="text-xs text-gray-500 block mb-0.5">Entity</label>
          <select value={entityId} onChange={(e) => setEntityId(e.target.value)} className="border border-gray-300 rounded-lg px-3 py-1.5 text-sm">
            <option value="">Pilih...</option>
            {entities.map(e => <option key={e.id} value={e.id}>{e.name || e.id}</option>)}
          </select>
        </div>
        <div>
          <label className="text-xs text-gray-500 block mb-0.5">Periode</label>
          <select value={period} onChange={(e) => setPeriod(e.target.value)} className="border border-gray-300 rounded-lg px-3 py-1.5 text-sm">
            {periods.map(p => <option key={p} value={p}>{p}</option>)}
          </select>
        </div>
        <button onClick={handleGenerate} disabled={loading || !entityId} className="px-4 py-1.5 bg-indigo-600 text-white rounded-lg text-sm font-medium hover:bg-indigo-700 disabled:opacity-50">
          {loading ? 'Generating...' : 'Generate'}
        </button>
        {data && (
          <button onClick={handlePrint} className="px-4 py-1.5 bg-gray-100 text-gray-700 rounded-lg text-sm font-medium hover:bg-gray-200 flex items-center gap-1">
            <Printer size={14} /> Print
          </button>
        )}
      </div>

      {loading && (
        <div className="flex items-center justify-center py-12 text-gray-400">
          <RefreshCw size={24} className="animate-spin mr-2" /> Generating report card...
        </div>
      )}

      {data && !loading && (
        <div ref={printRef} className="bg-white rounded-xl border border-gray-200 p-6 space-y-5 print:border-0 print:shadow-none print:p-4">
          <div className="flex items-start justify-between border-b pb-3">
            <div>
              <h3 className="text-lg font-bold text-gray-800">REPORT CARD</h3>
              <p className="text-sm text-gray-600">
                {data.entity?.name} | {data.entity?.level?.toUpperCase()} | Periode: {period}
              </p>
              <p className="text-xs text-gray-400">Generated: {new Date(data.generated_at).toLocaleString()} | Author: Dr. Adam M.</p>
            </div>
            <StatusBadge status={data.overall_status} />
          </div>

          {data.overall_status?.narrative && (
            <div className="text-sm text-gray-600 bg-gray-50 rounded-lg p-3 border border-gray-100">
              {data.overall_status.narrative}
            </div>
          )}

          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div className="border border-gray-200 rounded-lg p-3">
              <p className="text-xs font-semibold text-gray-500 uppercase mb-2">Profil</p>
              <div className="space-y-1 text-xs text-gray-600">
                <p>Level: {data.entity?.level}</p>
                {data.entity?.parent && <p>Parent: {data.entity.parent.name}</p>}
                <p>Children: {data.entity?.children_count}</p>
                <p>Sites: {data.entity?.site_count}</p>
              </div>
            </div>
            <div className="border border-gray-200 rounded-lg p-3">
              <p className="text-xs font-semibold text-gray-500 uppercase mb-2">KPI</p>
              <KpiCompactTable kpis={data.kpis} deltas={data.kpi_deltas} />
            </div>
          </div>

          <div className="border border-gray-200 rounded-lg p-3">
            <p className="text-xs font-semibold text-gray-500 uppercase mb-2">Tren (3 bulan)</p>
            <TrendSparklines trend3m={data.trend_3m} />
          </div>

          {data.children?.length > 0 && (
            <div className="border border-gray-200 rounded-lg p-3">
              <p className="text-xs font-semibold text-gray-500 uppercase mb-2">Child Ranking</p>
              <ChildRankingTable children={data.children} />
            </div>
          )}

          {data.recommendations?.length > 0 && (
            <div className="border border-gray-200 rounded-lg p-3">
              <p className="text-xs font-semibold text-gray-500 uppercase mb-2">Rekomendasi</p>
              <RecommendationList recommendations={data.recommendations} />
            </div>
          )}

          <div className="text-center text-xs text-gray-400 border-t pt-3">
            Dr. Adam M. | NOC-IS Analytics Platform v1.0
          </div>
        </div>
      )}
    </div>
  );
}

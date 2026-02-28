import { useState, useEffect } from 'react';
import PeriodPicker from './PeriodPicker';

const REPORT_TYPES = [
  { value: 'daily', label: 'Harian' },
  { value: 'weekly', label: 'Mingguan' },
  { value: 'monthly', label: 'Bulanan' },
  { value: 'quarterly', label: 'Triwulan' },
  { value: 'annual', label: 'Tahunan' },
];

const ENTITY_LEVELS = [
  { value: 'nasional', label: 'Nasional' },
  { value: 'area', label: 'Area' },
  { value: 'regional', label: 'Regional' },
  { value: 'witel', label: 'Witel' },
];

function ReportForm({ onGenerate, loading }) {
  const [reportType, setReportType] = useState('monthly');
  const [entityLevel, setEntityLevel] = useState('nasional');
  const [entityId, setEntityId] = useState('ALL');
  const [entities, setEntities] = useState([]);
  const [periodStart, setPeriodStart] = useState('');
  const [periodEnd, setPeriodEnd] = useState('');
  const [includePdf, setIncludePdf] = useState(true);
  const [includeExcel, setIncludeExcel] = useState(false);

  useEffect(() => {
    if (entityLevel === 'nasional') {
      setEntities([{ id: 'ALL', name: 'Seluruh Indonesia' }]);
      setEntityId('ALL');
      return;
    }
    const levelMap = { area: 'area', regional: 'regional', witel: 'witel' };
    const endpoint = levelMap[entityLevel];
    if (!endpoint) return;

    fetch(`/api/hierarchy/${endpoint}`)
      .then(r => r.json())
      .then(data => {
        const items = (data.items || data || []).map(item => ({
          id: item.area_id || item.regional_id || item.witel_id || item.id,
          name: item.area_name || item.regional_name || item.witel_name || item.name || item.id,
        }));
        setEntities(items);
        if (items.length > 0) setEntityId(items[0].id);
      })
      .catch(() => setEntities([]));
  }, [entityLevel]);

  const handleSubmit = (e) => {
    e.preventDefault();
    if (!periodStart || !periodEnd) return;
    onGenerate({
      report_type: reportType,
      entity_level: entityLevel,
      entity_id: entityId,
      period_start: periodStart,
      period_end: periodEnd,
      options: {
        include_pdf: includePdf,
        include_excel: includeExcel,
      },
    });
  };

  return (
    <form onSubmit={handleSubmit} className="bg-white rounded-xl border border-gray-200 p-6 space-y-5">
      <div>
        <label className="block text-sm font-medium text-gray-700 mb-2">Jenis Laporan</label>
        <div className="flex flex-wrap gap-2">
          {REPORT_TYPES.map(t => (
            <button
              key={t.value}
              type="button"
              onClick={() => setReportType(t.value)}
              className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
                reportType === t.value
                  ? 'bg-[#1B2A4A] text-white'
                  : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
              }`}
            >
              {t.label}
            </button>
          ))}
        </div>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">Level Entitas</label>
          <select
            value={entityLevel}
            onChange={e => setEntityLevel(e.target.value)}
            className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
          >
            {ENTITY_LEVELS.map(l => (
              <option key={l.value} value={l.value}>{l.label}</option>
            ))}
          </select>
        </div>
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">Entitas</label>
          <select
            value={entityId}
            onChange={e => setEntityId(e.target.value)}
            className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
          >
            {entities.map(ent => (
              <option key={ent.id} value={ent.id}>{ent.name}</option>
            ))}
          </select>
        </div>
      </div>

      <PeriodPicker
        reportType={reportType}
        onPeriodChange={(start, end) => { setPeriodStart(start); setPeriodEnd(end); }}
      />

      <div className="flex flex-wrap gap-6 items-center">
        <label className="flex items-center gap-2 text-sm">
          <input
            type="checkbox"
            checked={includePdf}
            onChange={e => setIncludePdf(e.target.checked)}
            className="rounded border-gray-300 text-blue-600"
          />
          PDF
        </label>
        {['monthly', 'quarterly', 'annual'].includes(reportType) && (
          <label className="flex items-center gap-2 text-sm">
            <input
              type="checkbox"
              checked={includeExcel}
              onChange={e => setIncludeExcel(e.target.checked)}
              className="rounded border-gray-300 text-blue-600"
            />
            Excel
          </label>
        )}
      </div>

      <button
        type="submit"
        disabled={loading || !periodStart || !periodEnd}
        className="w-full bg-[#1B2A4A] text-white py-3 rounded-lg font-medium hover:bg-[#243656] disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
      >
        {loading ? (
          <span className="flex items-center justify-center gap-2">
            <svg className="animate-spin h-5 w-5" viewBox="0 0 24 24">
              <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" fill="none" />
              <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
            </svg>
            Generating...
          </span>
        ) : 'Generate Laporan'}
      </button>
    </form>
  );
}

export default ReportForm;

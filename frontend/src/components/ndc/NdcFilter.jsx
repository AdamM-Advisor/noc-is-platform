import { Search, X } from 'lucide-react';

const CATEGORY_LABELS = {
  PWR: 'Power System',
  RAD: 'Radio Access',
  TRX: 'Transmission',
  AKT: 'Aktivitas',
  OTH: 'Other',
  COR: 'Core Network',
  CME: 'CME',
  LOG: 'Logic/SW',
  HW: 'Hardware',
};

function NdcFilter({ filters, onChange, categories }) {
  const update = (key, value) => onChange({ ...filters, [key]: value });
  const hasFilters = filters.category || filters.priority || filters.status || filters.search;

  return (
    <div className="flex flex-wrap items-center gap-3 mb-4">
      <div className="relative flex-1 min-w-[200px] max-w-xs">
        <Search size={16} className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400" />
        <input
          type="text"
          placeholder="Cari NDC code, judul..."
          value={filters.search}
          onChange={e => update('search', e.target.value)}
          className="w-full pl-9 pr-3 py-2 text-sm border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-100 focus:border-[#1E40AF]"
        />
      </div>
      <select
        value={filters.category}
        onChange={e => update('category', e.target.value)}
        className="text-sm border border-gray-200 rounded-lg px-3 py-2 bg-white focus:outline-none focus:ring-2 focus:ring-blue-100"
      >
        <option value="">Semua Kategori</option>
        {categories.map(c => (
          <option key={c.code} value={c.code}>
            {CATEGORY_LABELS[c.code] || c.code} ({c.count})
          </option>
        ))}
      </select>
      <select
        value={filters.priority}
        onChange={e => update('priority', e.target.value)}
        className="text-sm border border-gray-200 rounded-lg px-3 py-2 bg-white focus:outline-none focus:ring-2 focus:ring-blue-100"
      >
        <option value="">Semua Prioritas</option>
        <option value="HIGH">HIGH</option>
        <option value="MEDIUM">MEDIUM</option>
        <option value="LOW">LOW</option>
      </select>
      <select
        value={filters.status}
        onChange={e => update('status', e.target.value)}
        className="text-sm border border-gray-200 rounded-lg px-3 py-2 bg-white focus:outline-none focus:ring-2 focus:ring-blue-100"
      >
        <option value="">Semua Status</option>
        <option value="auto">Auto</option>
        <option value="reviewed">Reviewed</option>
        <option value="curated">Curated</option>
      </select>
      {hasFilters && (
        <button
          onClick={() => onChange({ ...filters, category: '', priority: '', status: '', search: '' })}
          className="flex items-center gap-1 text-sm text-[#475569] hover:text-[#0F172A] transition-colors"
        >
          <X size={14} />
          Reset
        </button>
      )}
    </div>
  );
}

export default NdcFilter;

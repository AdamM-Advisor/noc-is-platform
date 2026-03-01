import { useState, Fragment } from 'react';
import { ChevronDown, ChevronRight, ArrowUpDown } from 'lucide-react';
import NdcDetail from './NdcDetail';

const PRIORITY_COLORS = {
  HIGH: { bg: '#FEF2F2', text: '#991B1B', dot: '#DC2626' },
  MEDIUM: { bg: '#FFFBEB', text: '#92400E', dot: '#F59E0B' },
  LOW: { bg: '#F0FDF4', text: '#166534', dot: '#22C55E' },
};

const COLUMNS = [
  { key: 'ndc_code', label: 'Kode NDC', width: 'w-32' },
  { key: 'title', label: 'Judul', width: 'flex-1' },
  { key: 'category_code', label: 'Kategori', width: 'w-20' },
  { key: 'total_tickets', label: 'Volume', width: 'w-24', align: 'right' },
  { key: 'sla_breach_pct', label: 'Breach %', width: 'w-24', align: 'right' },
  { key: 'avg_mttr_min', label: 'MTTR (m)', width: 'w-24', align: 'right' },
  { key: 'priority_score', label: 'Skor', width: 'w-20', align: 'right' },
  { key: 'calculated_priority', label: 'Prioritas', width: 'w-24' },
];

function NdcTable({ entries, loading, sort_by, sort_dir, onSort }) {
  const [expandedCode, setExpandedCode] = useState(null);

  if (loading) {
    return (
      <div className="bg-white rounded-lg border border-gray-200 p-12 text-center">
        <div className="animate-spin w-8 h-8 border-2 border-[#1E40AF] border-t-transparent rounded-full mx-auto mb-3" />
        <p className="text-sm text-[#475569]">Memuat data NDC...</p>
      </div>
    );
  }

  if (!entries.length) {
    return (
      <div className="bg-white rounded-lg border border-gray-200 p-12 text-center">
        <p className="text-[#475569] text-sm">Belum ada data NDC. Klik "Refresh NDC" untuk generate.</p>
      </div>
    );
  }

  return (
    <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="bg-gray-50 border-b border-gray-200">
              <th className="w-8 px-3 py-3" />
              {COLUMNS.map(col => (
                <th
                  key={col.key}
                  onClick={() => onSort(col.key)}
                  className={`px-3 py-3 font-medium text-[#475569] cursor-pointer hover:text-[#0F172A] select-none ${col.align === 'right' ? 'text-right' : 'text-left'} ${col.width}`}
                >
                  <span className="inline-flex items-center gap-1">
                    {col.label}
                    {sort_by === col.key && (
                      <ArrowUpDown size={12} className="text-[#1E40AF]" />
                    )}
                  </span>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {entries.map(entry => {
              const isExpanded = expandedCode === entry.ndc_code;
              const prio = PRIORITY_COLORS[entry.calculated_priority] || PRIORITY_COLORS.LOW;
              return (
                <Fragment key={entry.ndc_code}>
                  <tr
                    onClick={() => setExpandedCode(isExpanded ? null : entry.ndc_code)}
                    className={`border-b border-gray-100 cursor-pointer transition-colors ${isExpanded ? 'bg-blue-50/50' : 'hover:bg-gray-50'}`}
                  >
                    <td className="px-3 py-2.5 text-[#475569]">
                      {isExpanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
                    </td>
                    <td className="px-3 py-2.5 font-mono text-xs text-[#1E40AF] font-medium">{entry.ndc_code}</td>
                    <td className="px-3 py-2.5 text-[#0F172A] font-medium max-w-[300px] truncate">{entry.title}</td>
                    <td className="px-3 py-2.5">
                      <span className="px-2 py-0.5 rounded text-xs font-medium bg-gray-100 text-[#475569]">{entry.category_code}</span>
                    </td>
                    <td className="px-3 py-2.5 text-right font-mono text-[#0F172A]">{(entry.total_tickets || 0).toLocaleString()}</td>
                    <td className="px-3 py-2.5 text-right font-mono text-[#0F172A]">{(entry.sla_breach_pct || 0).toFixed(1)}%</td>
                    <td className="px-3 py-2.5 text-right font-mono text-[#0F172A]">{(entry.avg_mttr_min || 0).toFixed(0)}</td>
                    <td className="px-3 py-2.5 text-right font-mono text-[#0F172A]">{(entry.priority_score || 0).toFixed(1)}</td>
                    <td className="px-3 py-2.5">
                      <span className="inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full text-xs font-medium" style={{ backgroundColor: prio.bg, color: prio.text }}>
                        <span className="w-1.5 h-1.5 rounded-full" style={{ backgroundColor: prio.dot }} />
                        {entry.calculated_priority}
                      </span>
                    </td>
                  </tr>
                  {isExpanded && (
                    <tr>
                      <td colSpan={COLUMNS.length + 1} className="p-0">
                        <NdcDetail code={entry.ndc_code} entry={entry} />
                      </td>
                    </tr>
                  )}
                </Fragment>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

export default NdcTable;

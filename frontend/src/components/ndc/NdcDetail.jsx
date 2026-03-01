import { useState, useEffect } from 'react';
import AlarmSnapshotTab from './tabs/AlarmSnapshotTab';
import SymptomsTab from './tabs/SymptomsTab';
import DiagnosticTreeTab from './tabs/DiagnosticTreeTab';
import SOPTab from './tabs/SOPTab';
import NdcCurationForm from './NdcCurationForm';

const TABS = [
  { id: 'alarm', label: 'Alarm Snapshot' },
  { id: 'symptoms', label: 'Symptoms' },
  { id: 'diagnostic', label: 'Diagnostic Tree' },
  { id: 'sop', label: 'SOP' },
];

function NdcDetail({ code, entry }) {
  const [activeTab, setActiveTab] = useState('alarm');
  const [detail, setDetail] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const load = async () => {
      setLoading(true);
      try {
        const res = await fetch(`/api/ndc/${code}`);
        const data = await res.json();
        setDetail(data);
      } catch (e) {
        console.error('Failed to load NDC detail:', e);
      } finally {
        setLoading(false);
      }
    };
    load();
  }, [code]);

  if (loading) {
    return (
      <div className="p-6 bg-white border-l-4 border-l-[#1E40AF] shadow-lg m-4 rounded-lg">
        <div className="flex items-center gap-3 text-sm text-[#475569]">
          <div className="animate-spin w-4 h-4 border-2 border-[#1E40AF] border-t-transparent rounded-full" />
          Memuat detail...
        </div>
      </div>
    );
  }

  if (!detail || detail.error) {
    return (
      <div className="p-6 bg-white border-l-4 border-l-[#1E40AF] shadow-lg m-4 rounded-lg text-sm text-[#475569]">
        Data belum tersedia. Jalankan Refresh NDC terlebih dahulu.
      </div>
    );
  }

  return (
    <div className="bg-white border-l-4 border-l-[#1E40AF] shadow-lg m-4 rounded-lg overflow-hidden">
      <div className="px-6 pt-5 pb-3 bg-gradient-to-r from-[#F8FAFC] to-white">
        <div className="flex items-start justify-between mb-4">
          <div>
            <h3 className="text-base font-bold text-[#0F172A]">
              <span className="text-[#1E40AF] font-mono mr-2">{detail.ndc_code}</span>
              <span className="text-[#334155]">—</span>
              <span className="ml-2">{detail.title}</span>
            </h3>
            <p className="text-sm text-[#64748B] mt-1">
              {detail.category_name} | {(detail.total_tickets || 0).toLocaleString()} tiket | {detail.first_seen} s/d {detail.last_seen}
            </p>
          </div>
          <NdcCurationForm code={code} status={detail.status} notes={detail.notes} />
        </div>

        <div className="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-6 gap-3 mb-4">
          <MiniKpi label="SLA Breach" value={`${(detail.sla_breach_pct || 0).toFixed(1)}%`} />
          <MiniKpi label="Auto-resolve" value={`${(detail.auto_resolve_pct || 0).toFixed(1)}%`} />
          <MiniKpi label="Avg MTTR" value={`${(detail.avg_mttr_min || 0).toFixed(0)}m`} />
          <MiniKpi label="Median MTTR" value={`${(detail.median_mttr_min || 0).toFixed(0)}m`} />
          <MiniKpi label="Eskalasi" value={`${(detail.escalation_pct || 0).toFixed(1)}%`} />
          <MiniKpi label="3T Sites" value={`${(detail.pct_in_3t || 0).toFixed(1)}%`} />
        </div>
      </div>

      <div className="h-[2px] bg-gradient-to-r from-[#1E40AF] via-[#3B82F6] to-[#93C5FD]" />

      <div className="border-b border-gray-200 px-6 bg-[#F8FAFC]">
        <nav className="flex gap-0">
          {TABS.map(tab => (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              className={`px-4 py-2.5 text-xs font-semibold border-b-2 transition-colors ${
                activeTab === tab.id
                  ? 'border-[#1E40AF] text-[#1E40AF] bg-white'
                  : 'border-transparent text-[#64748B] hover:text-[#0F172A] hover:bg-white/50'
              }`}
            >
              {tab.label}
            </button>
          ))}
        </nav>
      </div>

      <div className="px-6 py-5 bg-white">
        {activeTab === 'alarm' && <AlarmSnapshotTab data={detail.alarm_snapshot} coAlarms={detail.co_occurring_alarms} />}
        {activeTab === 'symptoms' && <SymptomsTab symptoms={detail.symptoms} code={code} />}
        {activeTab === 'diagnostic' && <DiagnosticTreeTab steps={detail.diagnostic_steps} code={code} />}
        {activeTab === 'sop' && <SOPTab paths={detail.resolution_paths} escalation={detail.escalation_matrix} preventive={detail.preventive_actions} code={code} />}
      </div>
    </div>
  );
}

function MiniKpi({ label, value }) {
  return (
    <div className="bg-white rounded-lg border border-gray-200 px-3 py-2.5 border-t-2 border-t-[#3B82F6] shadow-sm">
      <div className="text-xs font-medium text-[#64748B]">{label}</div>
      <div className="text-base font-bold text-[#0F172A] mt-0.5">{value}</div>
    </div>
  );
}

export default NdcDetail;

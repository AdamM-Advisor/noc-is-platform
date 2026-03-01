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
      <div className="p-6 bg-gray-50 border-t border-gray-200">
        <div className="flex items-center gap-3 text-sm text-[#475569]">
          <div className="animate-spin w-4 h-4 border-2 border-[#1E40AF] border-t-transparent rounded-full" />
          Memuat detail...
        </div>
      </div>
    );
  }

  if (!detail || detail.error) {
    return (
      <div className="p-6 bg-gray-50 border-t border-gray-200 text-sm text-[#475569]">
        Data belum tersedia. Jalankan Refresh NDC terlebih dahulu.
      </div>
    );
  }

  return (
    <div className="bg-gray-50 border-t border-gray-200">
      <div className="px-6 pt-4 pb-2">
        <div className="flex items-start justify-between mb-3">
          <div>
            <h3 className="text-sm font-semibold text-[#0F172A]">{detail.ndc_code} — {detail.title}</h3>
            <p className="text-xs text-[#475569] mt-0.5">
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

      <div className="border-b border-gray-200 px-6">
        <nav className="flex gap-0">
          {TABS.map(tab => (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              className={`px-3 py-2 text-xs font-medium border-b-2 transition-colors ${
                activeTab === tab.id
                  ? 'border-[#1E40AF] text-[#1E40AF]'
                  : 'border-transparent text-[#475569] hover:text-[#0F172A]'
              }`}
            >
              {tab.label}
            </button>
          ))}
        </nav>
      </div>

      <div className="px-6 py-4">
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
    <div className="bg-white rounded-lg border border-gray-200 px-3 py-2">
      <div className="text-xs text-[#475569]">{label}</div>
      <div className="text-sm font-semibold text-[#0F172A] mt-0.5">{value}</div>
    </div>
  );
}

export default NdcDetail;

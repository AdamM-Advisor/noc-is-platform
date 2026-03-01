import { useState } from 'react';
import { ChevronDown, ChevronRight } from 'lucide-react';

function SOPTab({ paths, escalation, preventive, code }) {
  return (
    <div className="space-y-6">
      <ResolutionPaths paths={paths} />
      <EscalationMatrix data={escalation} />
      <PreventiveActions data={preventive} />
    </div>
  );
}

function ResolutionPaths({ paths }) {
  const [expandedIdx, setExpandedIdx] = useState(null);

  if (!paths || paths.length === 0) {
    return (
      <div className="bg-white rounded-lg border border-gray-200 shadow-sm overflow-hidden">
        <div className="bg-[#F1F5F9] px-4 py-2 border-b border-gray-200">
          <h4 className="text-xs font-bold text-[#334155] uppercase tracking-wider">Resolution Paths</h4>
        </div>
        <div className="p-4">
          <p className="text-sm text-[#64748B]">Belum ada data resolution path.</p>
        </div>
      </div>
    );
  }

  return (
    <div className="bg-white rounded-lg border border-gray-200 shadow-sm overflow-hidden">
      <div className="bg-[#F1F5F9] px-4 py-2 border-b border-gray-200">
        <h4 className="text-xs font-bold text-[#334155] uppercase tracking-wider">Resolution Paths</h4>
      </div>
      <div className="p-4 space-y-2">
        {paths.map((path, i) => {
          const isOpen = expandedIdx === i;
          return (
            <div key={i} className="bg-[#FAFBFC] rounded-lg border border-gray-200 overflow-hidden shadow-sm">
              <div
                onClick={() => setExpandedIdx(isOpen ? null : i)}
                className="flex items-center gap-3 px-4 py-3 cursor-pointer hover:bg-gray-50 transition-colors"
              >
                <div className="flex-1 min-w-0">
                  <div className="text-sm text-[#0F172A] font-semibold">{path.path_name}</div>
                  <div className="text-xs text-[#64748B] mt-0.5">
                    {path.ticket_count} tiket | MTTR ~{(path.avg_mttr_min || 0).toFixed(0)}m | SLA {(path.sla_met_pct || 0).toFixed(1)}%
                  </div>
                </div>
                <div className="flex items-center gap-3 shrink-0">
                  <div className="text-right">
                    <div className="text-lg font-bold text-[#0F172A]">{(path.probability_pct || 0).toFixed(1)}%</div>
                    <div className="text-[10px] font-medium text-[#64748B]">probabilitas</div>
                  </div>
                  <div className="w-20">
                    <div className="h-3 bg-gray-200 rounded-full overflow-hidden">
                      <div
                        className="h-full bg-[#1E40AF] rounded-full transition-all"
                        style={{ width: `${Math.min(100, path.probability_pct || 0)}%` }}
                      />
                    </div>
                  </div>
                  {isOpen ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
                </div>
              </div>
              {isOpen && path.steps && path.steps.length > 0 && (
                <div className="border-t border-gray-200 px-4 py-3 bg-white">
                  <div className="space-y-1.5">
                    {path.steps.map((step, si) => (
                      <div key={si} className="flex items-start gap-2 text-sm">
                        <span className="text-[#1E40AF] font-mono text-xs font-bold mt-0.5">{step.step_number}.</span>
                        <span className="text-[#0F172A] font-medium">{step.step_text}</span>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}

function EscalationMatrix({ data }) {
  if (!data || data.length === 0) return null;

  const tierColors = {
    1: '#DBEAFE', 2: '#FEF9C3', 3: '#FED7AA', 4: '#FECACA',
  };

  return (
    <div className="bg-white rounded-lg border border-gray-200 shadow-sm overflow-hidden">
      <div className="bg-[#F1F5F9] px-4 py-2 border-b border-gray-200">
        <h4 className="text-xs font-bold text-[#334155] uppercase tracking-wider">Escalation Matrix</h4>
      </div>
      <table className="w-full text-sm">
        <thead>
          <tr className="bg-gray-50 border-b border-gray-200">
            <th className="px-4 py-2.5 text-left text-xs font-semibold text-[#475569]">Tier</th>
            <th className="px-4 py-2.5 text-left text-xs font-semibold text-[#475569]">Role</th>
            <th className="px-4 py-2.5 text-left text-xs font-semibold text-[#475569]">Action</th>
            <th className="px-4 py-2.5 text-left text-xs font-semibold text-[#475569]">Max Duration</th>
          </tr>
        </thead>
        <tbody>
          {data.map((row, i) => (
            <tr key={i} className="border-b border-gray-100 last:border-0">
              <td className="px-4 py-2.5">
                <span
                  className="inline-flex items-center justify-center w-7 h-7 rounded-full text-xs font-bold"
                  style={{ backgroundColor: tierColors[row.tier] || '#F1F5F9', color: '#0F172A' }}
                >
                  {row.tier}
                </span>
              </td>
              <td className="px-4 py-2.5 font-semibold text-[#0F172A]">{row.role}</td>
              <td className="px-4 py-2.5 text-[#334155]">{row.action}</td>
              <td className="px-4 py-2.5 font-medium text-[#334155]">{row.max_duration}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function PreventiveActions({ data }) {
  if (!data || data.length === 0) return null;

  const effortStyles = {
    LOW: { bg: '#DCFCE7', text: '#166534' },
    MEDIUM: { bg: '#FEF9C3', text: '#854D0E' },
    HIGH: { bg: '#FEF2F2', text: '#991B1B' },
  };

  return (
    <div className="bg-white rounded-lg border border-gray-200 shadow-sm overflow-hidden">
      <div className="bg-[#F1F5F9] px-4 py-2 border-b border-gray-200">
        <h4 className="text-xs font-bold text-[#334155] uppercase tracking-wider">Preventive Actions</h4>
      </div>
      <div className="p-4 space-y-2">
        {data.map((item, i) => {
          const effort = effortStyles[item.effort_level] || effortStyles.MEDIUM;
          return (
            <div key={i} className="flex items-center gap-3 bg-[#FAFBFC] rounded-lg border border-gray-200 p-3">
              <div className="flex-1">
                <div className="text-sm font-medium text-[#0F172A]">{item.action}</div>
                {item.expected_impact && <div className="text-xs text-[#64748B] mt-0.5">{item.expected_impact}</div>}
              </div>
              <span className="px-2.5 py-1 rounded-full text-xs font-semibold shrink-0" style={{ backgroundColor: effort.bg, color: effort.text }}>
                {item.effort_level}
              </span>
            </div>
          );
        })}
      </div>
    </div>
  );
}

export default SOPTab;

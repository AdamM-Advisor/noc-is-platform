import { useState } from 'react';
import { ChevronDown, ChevronRight, Clock } from 'lucide-react';

function DiagnosticTreeTab({ steps, code }) {
  const [expandedStep, setExpandedStep] = useState(null);

  if (!steps || steps.length === 0) {
    return <p className="text-sm text-[#475569]">Diagnostic tree belum tersedia.</p>;
  }

  return (
    <div className="bg-white rounded-lg border border-gray-200 shadow-sm overflow-hidden">
      <div className="bg-[#F1F5F9] px-4 py-2 border-b border-gray-200">
        <h4 className="text-xs font-bold text-[#334155] uppercase tracking-wider">Diagnostic Steps</h4>
      </div>
      <div className="p-4 space-y-2">
        {steps.map((step, i) => {
          const isExpanded = expandedStep === step.step_number;
          const pct = step.cumulative_resolve_pct || 0;
          return (
            <div key={i} className="bg-[#FAFBFC] rounded-lg border border-gray-200 overflow-hidden shadow-sm">
              <div
                onClick={() => setExpandedStep(isExpanded ? null : step.step_number)}
                className="flex items-center gap-3 px-4 py-3 cursor-pointer hover:bg-gray-50 transition-colors"
              >
                <div className="w-8 h-8 rounded-full bg-[#1E40AF] text-white text-sm font-bold flex items-center justify-center shrink-0">
                  {step.step_number}
                </div>
                <div className="flex-1 min-w-0">
                  <div className="text-sm text-[#0F172A] font-semibold truncate">{step.action}</div>
                </div>
                <div className="flex items-center gap-3 shrink-0 text-xs text-[#64748B]">
                  {step.avg_duration_min != null && (
                    <span className="flex items-center gap-1 font-medium">
                      <Clock size={12} />
                      {step.avg_duration_min}m
                    </span>
                  )}
                  {step.cumulative_resolve_pct != null && (
                    <div className="flex items-center gap-2 w-28">
                      <div className="flex-1 h-2 bg-gray-200 rounded-full overflow-hidden">
                        <div className="h-full bg-green-500 rounded-full" style={{ width: `${pct}%` }} />
                      </div>
                      <span className="font-mono text-xs font-semibold">{pct}%</span>
                    </div>
                  )}
                </div>
                {isExpanded ? <ChevronDown size={14} className="text-[#475569]" /> : <ChevronRight size={14} className="text-[#475569]" />}
              </div>

              {isExpanded && (
                <div className="px-4 pb-3 pt-0 border-t border-gray-200 bg-white">
                  <div className="pl-11 space-y-2 mt-3">
                    {step.expected_result && (
                      <div className="text-xs">
                        <span className="font-semibold text-[#475569]">Expected: </span>
                        <span className="font-medium text-[#0F172A]">{step.expected_result}</span>
                      </div>
                    )}
                    <div className="grid grid-cols-2 gap-3">
                      {step.if_yes && (
                        <div className="bg-green-50 border border-green-200 rounded-lg p-2.5 shadow-sm">
                          <div className="text-[10px] font-bold text-green-700 uppercase mb-1">YA</div>
                          <div className="text-xs font-medium text-green-800">{step.if_yes}</div>
                        </div>
                      )}
                      {step.if_no && (
                        <div className="bg-red-50 border border-red-200 rounded-lg p-2.5 shadow-sm">
                          <div className="text-[10px] font-bold text-red-700 uppercase mb-1">TIDAK</div>
                          <div className="text-xs font-medium text-red-800">{step.if_no}</div>
                        </div>
                      )}
                    </div>
                    {step.success_rate_at_step != null && (
                      <div className="text-xs font-medium text-[#64748B]">
                        Resolve di step ini: <span className="font-bold text-[#0F172A]">{step.success_rate_at_step}%</span>
                      </div>
                    )}
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

export default DiagnosticTreeTab;

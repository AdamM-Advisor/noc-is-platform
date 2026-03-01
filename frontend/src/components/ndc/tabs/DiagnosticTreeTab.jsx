import { useState } from 'react';
import { ChevronDown, ChevronRight, Clock } from 'lucide-react';

function DiagnosticTreeTab({ steps, code }) {
  const [expandedStep, setExpandedStep] = useState(null);

  if (!steps || steps.length === 0) {
    return <p className="text-sm text-[#475569]">Diagnostic tree belum tersedia.</p>;
  }

  return (
    <div className="space-y-2">
      {steps.map((step, i) => {
        const isExpanded = expandedStep === step.step_number;
        const pct = step.cumulative_resolve_pct || 0;
        return (
          <div key={i} className="bg-white rounded-lg border border-gray-200 overflow-hidden">
            <div
              onClick={() => setExpandedStep(isExpanded ? null : step.step_number)}
              className="flex items-center gap-3 px-4 py-3 cursor-pointer hover:bg-gray-50 transition-colors"
            >
              <div className="w-7 h-7 rounded-full bg-[#1E40AF] text-white text-xs font-semibold flex items-center justify-center shrink-0">
                {step.step_number}
              </div>
              <div className="flex-1 min-w-0">
                <div className="text-sm text-[#0F172A] font-medium truncate">{step.action}</div>
              </div>
              <div className="flex items-center gap-3 shrink-0 text-xs text-[#475569]">
                {step.avg_duration_min != null && (
                  <span className="flex items-center gap-1">
                    <Clock size={12} />
                    {step.avg_duration_min}m
                  </span>
                )}
                {step.cumulative_resolve_pct != null && (
                  <div className="flex items-center gap-2 w-28">
                    <div className="flex-1 h-1.5 bg-gray-100 rounded-full overflow-hidden">
                      <div className="h-full bg-green-500 rounded-full" style={{ width: `${pct}%` }} />
                    </div>
                    <span className="font-mono text-[10px]">{pct}%</span>
                  </div>
                )}
              </div>
              {isExpanded ? <ChevronDown size={14} className="text-[#475569]" /> : <ChevronRight size={14} className="text-[#475569]" />}
            </div>

            {isExpanded && (
              <div className="px-4 pb-3 pt-0 border-t border-gray-100">
                <div className="pl-10 space-y-2 mt-3">
                  {step.expected_result && (
                    <div className="text-xs">
                      <span className="text-[#475569]">Expected: </span>
                      <span className="text-[#0F172A]">{step.expected_result}</span>
                    </div>
                  )}
                  <div className="grid grid-cols-2 gap-3">
                    {step.if_yes && (
                      <div className="bg-green-50 border border-green-100 rounded-lg p-2.5">
                        <div className="text-[10px] font-semibold text-green-700 uppercase mb-1">YA</div>
                        <div className="text-xs text-green-800">{step.if_yes}</div>
                      </div>
                    )}
                    {step.if_no && (
                      <div className="bg-red-50 border border-red-100 rounded-lg p-2.5">
                        <div className="text-[10px] font-semibold text-red-700 uppercase mb-1">TIDAK</div>
                        <div className="text-xs text-red-800">{step.if_no}</div>
                      </div>
                    )}
                  </div>
                  {step.success_rate_at_step != null && (
                    <div className="text-xs text-[#475569]">
                      Resolve di step ini: {step.success_rate_at_step}%
                    </div>
                  )}
                </div>
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

export default DiagnosticTreeTab;

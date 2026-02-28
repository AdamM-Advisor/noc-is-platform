import { useState } from 'react';
import { ChevronRight, ChevronDown } from 'lucide-react';

export default function FormulaBox({ title, children, formulaText, defaultOpen = false }) {
  const [open, setOpen] = useState(defaultOpen);

  const content = formulaText || children;
  if (!content) return null;

  return (
    <div className="border rounded-lg overflow-hidden bg-gray-50 border-gray-200">
      <button
        onClick={() => setOpen(!open)}
        className="w-full flex items-center gap-2 px-3 py-2 text-xs font-medium text-gray-600 hover:bg-gray-100 transition-colors"
      >
        {open ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
        {title || 'Lihat formula kalkulasi'}
      </button>
      {open && (
        <div className="px-4 py-3 border-t border-gray-200 bg-white">
          <pre className="text-xs text-gray-700 font-mono whitespace-pre-wrap leading-relaxed">
            {typeof content === 'string' ? content : children}
          </pre>
        </div>
      )}
    </div>
  );
}

import { useState } from 'react';
import { Check, Edit3 } from 'lucide-react';

function NdcCurationForm({ code, status: initStatus, notes: initNotes }) {
  const [editing, setEditing] = useState(false);
  const [status, setStatus] = useState(initStatus || 'auto');
  const [notes, setNotes] = useState(initNotes || '');
  const [saving, setSaving] = useState(false);

  const save = async () => {
    setSaving(true);
    try {
      await fetch(`/api/ndc/${code}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ status, notes, reviewed_by: 'Dr. Adam M.' }),
      });
      setEditing(false);
    } catch (e) {
      console.error('Save failed:', e);
    } finally {
      setSaving(false);
    }
  };

  const statusStyles = {
    auto: { bg: '#F1F5F9', text: '#475569', dot: '#94A3B8' },
    reviewed: { bg: '#DBEAFE', text: '#1E40AF', dot: '#3B82F6' },
    curated: { bg: '#DCFCE7', text: '#166534', dot: '#22C55E' },
  };
  const st = statusStyles[status] || statusStyles.auto;

  if (!editing) {
    return (
      <div className="flex items-center gap-2">
        <span className="inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full text-xs font-medium" style={{ backgroundColor: st.bg, color: st.text }}>
          <span className="w-1.5 h-1.5 rounded-full" style={{ backgroundColor: st.dot }} />
          {status}
        </span>
        <button
          onClick={() => setEditing(true)}
          className="p-1 text-[#475569] hover:text-[#0F172A] transition-colors"
        >
          <Edit3 size={14} />
        </button>
      </div>
    );
  }

  return (
    <div className="flex items-center gap-2 bg-white border border-gray-200 rounded-lg p-2">
      <select
        value={status}
        onChange={e => setStatus(e.target.value)}
        className="text-xs border border-gray-200 rounded px-2 py-1 bg-white"
      >
        <option value="auto">Auto</option>
        <option value="reviewed">Reviewed</option>
        <option value="curated">Curated</option>
      </select>
      <input
        type="text"
        value={notes}
        onChange={e => setNotes(e.target.value)}
        placeholder="Catatan..."
        className="text-xs border border-gray-200 rounded px-2 py-1 w-32"
      />
      <button
        onClick={save}
        disabled={saving}
        className="p-1 text-green-600 hover:text-green-700 disabled:opacity-50"
      >
        <Check size={14} />
      </button>
    </div>
  );
}

export default NdcCurationForm;

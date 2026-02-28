import { useState, useEffect, useCallback } from 'react';
import { ChevronDown, ChevronRight, RotateCcw, Check } from 'lucide-react';
import client from '../../api/client';

function ThresholdTab() {
  const [categories, setCategories] = useState({});
  const [loading, setLoading] = useState(true);
  const [expanded, setExpanded] = useState({});
  const [editingKey, setEditingKey] = useState(null);
  const [editValue, setEditValue] = useState('');
  const [flashKey, setFlashKey] = useState(null);
  const [resetting, setResetting] = useState(false);

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      const res = await client.get('/threshold');
      setCategories(res.data.categories || {});
      const initial = {};
      Object.keys(res.data.categories || {}).forEach((cat) => {
        if (cat === 'MTTR' || cat === 'COST') initial[cat] = true;
      });
      setExpanded((prev) => {
        const merged = { ...initial };
        Object.keys(prev).forEach((k) => { merged[k] = prev[k]; });
        return merged;
      });
    } catch (e) {}
    setLoading(false);
  }, []);

  useEffect(() => { fetchData(); }, [fetchData]);

  const toggleCategory = (cat) => {
    setExpanded((p) => ({ ...p, [cat]: !p[cat] }));
  };

  const startEdit = (key, value) => {
    setEditingKey(key);
    setEditValue(String(value));
  };

  const saveEdit = async (key) => {
    const num = parseFloat(editValue);
    if (isNaN(num) || num <= 0) {
      setEditingKey(null);
      return;
    }
    try {
      await client.put(`/threshold/${key}`, { param_value: num });
      setFlashKey(key);
      setTimeout(() => setFlashKey(null), 1000);
      await fetchData();
    } catch (e) {}
    setEditingKey(null);
  };

  const handleKeyDown = (e, key) => {
    if (e.key === 'Enter') saveEdit(key);
    if (e.key === 'Escape') setEditingKey(null);
  };

  const resetAll = async () => {
    if (!window.confirm('Reset semua threshold ke nilai default? Semua perubahan akan hilang.')) return;
    setResetting(true);
    try {
      await client.post('/schema/seed-reset');
      await fetchData();
    } catch (e) {}
    setResetting(false);
  };

  const resetSingle = async (key) => {
    setResetting(true);
    try {
      await client.post('/schema/seed-reset');
      await fetchData();
    } catch (e) {}
    setResetting(false);
  };

  if (loading) {
    return <div className="flex items-center justify-center h-40 text-gray-400 text-sm">Memuat data threshold...</div>;
  }

  const catKeys = Object.keys(categories);

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-lg font-semibold text-[#1B2A4A]">Parameter Threshold</h2>
        <button
          onClick={resetAll}
          disabled={resetting}
          className="px-4 py-2 text-sm text-white bg-red-600 rounded-md hover:bg-red-700 flex items-center gap-1.5 disabled:opacity-50"
        >
          <RotateCcw size={14} />
          {resetting ? 'Mereset...' : 'Reset Semua ke Default'}
        </button>
      </div>

      {catKeys.length === 0 ? (
        <div className="text-center text-gray-400 text-sm py-10">Tidak ada data threshold</div>
      ) : (
        catKeys.map((cat) => (
          <div key={cat} className="border border-gray-200 rounded-lg bg-white overflow-hidden">
            <button
              onClick={() => toggleCategory(cat)}
              className="w-full flex items-center justify-between px-4 py-3 bg-gray-50 hover:bg-gray-100 transition-colors"
            >
              <div className="flex items-center gap-2">
                {expanded[cat] ? <ChevronDown size={16} className="text-[#1B2A4A]" /> : <ChevronRight size={16} className="text-gray-400" />}
                <span className="font-semibold text-sm text-[#1B2A4A]">{cat}</span>
                <span className="text-xs text-gray-400 bg-gray-200 px-2 py-0.5 rounded-full">{categories[cat].length}</span>
              </div>
            </button>
            {expanded[cat] && (
              <div className="divide-y divide-gray-100">
                <div className="grid grid-cols-12 gap-2 px-4 py-2 bg-gray-50 text-xs font-medium text-gray-500 uppercase tracking-wide">
                  <div className="col-span-3">Parameter</div>
                  <div className="col-span-2">Nilai</div>
                  <div className="col-span-2">Satuan</div>
                  <div className="col-span-4">Deskripsi</div>
                  <div className="col-span-1"></div>
                </div>
                {categories[cat].map((item) => (
                  <div
                    key={item.param_key}
                    className={`grid grid-cols-12 gap-2 px-4 py-2.5 items-center text-sm group transition-colors duration-500 ${
                      flashKey === item.param_key ? 'bg-green-50' : 'hover:bg-gray-50'
                    }`}
                  >
                    <div className="col-span-3 font-medium text-gray-800 font-mono text-xs">{item.param_key}</div>
                    <div className="col-span-2">
                      {editingKey === item.param_key ? (
                        <div className="flex items-center gap-1">
                          <input
                            type="number"
                            value={editValue}
                            onChange={(e) => setEditValue(e.target.value)}
                            onBlur={() => saveEdit(item.param_key)}
                            onKeyDown={(e) => handleKeyDown(e, item.param_key)}
                            autoFocus
                            className="w-full border border-[#1B2A4A] rounded px-2 py-1 text-sm focus:outline-none focus:ring-2 focus:ring-[#1B2A4A]"
                            min="0"
                            step="any"
                          />
                        </div>
                      ) : (
                        <span
                          onClick={() => startEdit(item.param_key, item.param_value)}
                          className="cursor-pointer px-2 py-1 rounded hover:bg-blue-50 hover:text-[#1B2A4A] font-semibold text-[#1B2A4A] inline-flex items-center gap-1"
                        >
                          {item.param_value}
                          {flashKey === item.param_key && <Check size={14} className="text-green-500" />}
                        </span>
                      )}
                    </div>
                    <div className="col-span-2 text-gray-500 text-xs">{item.param_unit || '-'}</div>
                    <div className="col-span-4 text-gray-500 text-xs">{item.description || '-'}</div>
                    <div className="col-span-1 flex justify-end">
                      <button
                        onClick={() => resetSingle(item.param_key)}
                        className="opacity-0 group-hover:opacity-100 transition-opacity p-1 text-gray-400 hover:text-orange-500 hover:bg-orange-50 rounded"
                        title="Reset ke default"
                      >
                        <RotateCcw size={14} />
                      </button>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        ))
      )}
    </div>
  );
}

export default ThresholdTab;

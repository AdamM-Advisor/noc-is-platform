import { useState, useEffect, useMemo } from 'react';

function ConfusionMatrix() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const load = async () => {
      try {
        const res = await fetch('/api/ndc/confusion-matrix');
        const d = await res.json();
        setData(d);
      } catch (e) {
        console.error('Failed to load confusion matrix:', e);
      } finally {
        setLoading(false);
      }
    };
    load();
  }, []);

  const matrix = useMemo(() => {
    if (!data || !data.data) return null;

    const categories = data.categories || [];
    const grid = {};
    let maxVal = 0;

    categories.forEach(cat => {
      grid[cat] = {};
      categories.forEach(c2 => { grid[cat][c2] = 0; });
    });

    data.data.forEach(row => {
      if (grid[row.inap_rc_category] && grid[row.inap_rc_category][row.confirmed_rc_category] !== undefined) {
        grid[row.inap_rc_category][row.confirmed_rc_category] += row.ticket_count;
        maxVal = Math.max(maxVal, row.ticket_count);
      }
    });

    return { categories, grid, maxVal };
  }, [data]);

  if (loading) {
    return (
      <div className="p-12 text-center">
        <div className="animate-spin w-8 h-8 border-2 border-[#1E40AF] border-t-transparent rounded-full mx-auto mb-3" />
        <p className="text-sm text-[#475569]">Memuat confusion matrix...</p>
      </div>
    );
  }

  if (!matrix || matrix.categories.length === 0) {
    return <p className="text-sm text-[#475569]">Data confusion matrix belum tersedia. Jalankan Refresh NDC.</p>;
  }

  const getColor = (val, isDiag) => {
    if (val === 0) return 'bg-gray-50';
    if (isDiag) {
      const intensity = Math.min(1, val / matrix.maxVal);
      const alpha = 0.15 + intensity * 0.6;
      return '';
    }
    const intensity = Math.min(1, val / matrix.maxVal);
    const alpha = 0.1 + intensity * 0.5;
    return '';
  };

  return (
    <div>
      <div className="mb-4">
        <h3 className="text-sm font-semibold text-[#0F172A]">Confusion Matrix — INAP vs Konfirmasi Engineer</h3>
        <p className="text-xs text-[#475569] mt-1">Perbandingan klasifikasi awal (INAP) dengan konfirmasi engineer di lapangan</p>
      </div>
      <div className="bg-white rounded-lg border border-gray-200 overflow-x-auto">
        <table className="text-xs">
          <thead>
            <tr>
              <th className="px-3 py-2 bg-gray-50 border-b border-r border-gray-200 text-left text-[#475569] font-medium min-w-[120px]">
                INAP \ Konfirmasi
              </th>
              {matrix.categories.map(cat => (
                <th key={cat} className="px-3 py-2 bg-gray-50 border-b border-gray-200 text-center text-[#475569] font-medium min-w-[80px]">
                  {cat}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {matrix.categories.map(rowCat => (
              <tr key={rowCat} className="border-b border-gray-100 last:border-0">
                <td className="px-3 py-2 border-r border-gray-200 font-medium text-[#0F172A] bg-gray-50">
                  {rowCat}
                </td>
                {matrix.categories.map(colCat => {
                  const val = matrix.grid[rowCat][colCat] || 0;
                  const isDiag = rowCat === colCat;
                  const maxForRow = Math.max(...matrix.categories.map(c => matrix.grid[rowCat][c] || 0), 1);
                  const intensity = val / maxForRow;
                  let bgColor = 'transparent';
                  if (val > 0) {
                    if (isDiag) {
                      bgColor = `rgba(30, 64, 175, ${0.1 + intensity * 0.4})`;
                    } else {
                      bgColor = `rgba(220, 38, 38, ${0.05 + intensity * 0.3})`;
                    }
                  }
                  return (
                    <td
                      key={colCat}
                      className="px-3 py-2 text-center font-mono border-gray-100"
                      style={{ backgroundColor: bgColor }}
                    >
                      {val > 0 ? val.toLocaleString() : '-'}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div className="flex items-center gap-4 mt-3 text-xs text-[#475569]">
        <span className="flex items-center gap-1">
          <span className="w-4 h-3 rounded" style={{ backgroundColor: 'rgba(30, 64, 175, 0.3)' }} /> Diagonal (match)
        </span>
        <span className="flex items-center gap-1">
          <span className="w-4 h-3 rounded" style={{ backgroundColor: 'rgba(220, 38, 38, 0.2)' }} /> Off-diagonal (mismatch)
        </span>
      </div>

      {data.data && data.data.length > 0 && (
        <div className="mt-6">
          <h4 className="text-xs font-semibold text-[#475569] uppercase tracking-wider mb-2">Top Misclassifications</h4>
          <div className="space-y-1.5">
            {data.data
              .filter(r => r.inap_rc_category !== r.confirmed_rc_category || r.inap_rc_1 !== r.confirmed_rc_1)
              .slice(0, 10)
              .map((row, i) => (
                <div key={i} className="flex items-center gap-3 bg-white border border-gray-200 rounded-lg px-3 py-2 text-xs">
                  <span className="text-[#0F172A] font-medium min-w-[150px]">{row.inap_rc_category}: {row.inap_rc_1}</span>
                  <span className="text-[#475569]">→</span>
                  <span className="text-[#0F172A] min-w-[150px]">{row.confirmed_rc_category}: {row.confirmed_rc_1}</span>
                  <span className="ml-auto font-mono text-[#475569]">{row.ticket_count.toLocaleString()} tiket</span>
                  <span className="font-mono text-red-600">{(row.match_pct || 0).toFixed(1)}%</span>
                </div>
              ))}
          </div>
        </div>
      )}
    </div>
  );
}

export default ConfusionMatrix;

import { useMemo, useState } from 'react';
import { RefreshCw } from 'lucide-react';

function interpolateColor(value, min, max) {
  if (value === null || value === undefined) return '#F3F4F6';
  const ratio = max > min ? (value - min) / (max - min) : 0;
  const r = Math.round(247 + (220 - 247) * ratio);
  const g = Math.round(247 + (38 - 247) * ratio);
  const b = Math.round(247 + (38 - 247) * ratio);
  return `rgb(${r}, ${g}, ${b})`;
}

export default function HeatmapAdaptive({ data, loading, onCellClick }) {
  const [hoveredCell, setHoveredCell] = useState(null);

  const { flatMin, flatMax } = useMemo(() => {
    if (!data?.cells?.length) return { flatMin: 0, flatMax: 0 };
    const flat = data.cells.flat().filter(v => v !== null && v !== undefined);
    return {
      flatMin: flat.length ? Math.min(...flat) : 0,
      flatMax: flat.length ? Math.max(...flat) : 0,
    };
  }, [data]);

  if (loading) {
    return (
      <div className="flex items-center justify-center py-12 text-gray-400">
        <RefreshCw size={24} className="animate-spin mr-2" /> Memuat heatmap...
      </div>
    );
  }

  if (!data || !data.cells?.length) {
    return (
      <div className="text-center py-8 text-gray-400 text-sm">
        Tidak ada data heatmap tersedia.
      </div>
    );
  }

  const xLabels = data.x_labels || [];
  const yLabels = data.y_labels || [];
  const cells = data.cells || [];

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <p className="text-xs text-gray-500 uppercase font-semibold">
          Heatmap: {data.heatmap_type === 'week_x_day' ? 'Minggu × Hari' : 'Hari × Jam'}
        </p>
        <div className="flex items-center gap-1 text-xs text-gray-400">
          <div className="w-4 h-3 rounded" style={{ backgroundColor: '#F7F7F7', border: '1px solid #E5E7EB' }} />
          <span>Min</span>
          <div className="w-16 h-3 rounded" style={{ background: 'linear-gradient(to right, #F7F7F7, #DC2626)' }} />
          <span>Max</span>
        </div>
      </div>

      <div className="overflow-x-auto">
        <table className="border-collapse">
          <thead>
            <tr>
              <th className="w-12" />
              {xLabels.map((xl, i) => (
                <th key={i} className="px-1 py-1 text-[10px] font-medium text-gray-500 text-center min-w-[32px]">
                  {xl}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {cells.map((row, yi) => (
              <tr key={yi}>
                <td className="pr-2 py-0.5 text-[10px] font-medium text-gray-500 text-right whitespace-nowrap">
                  {yLabels[yi] || yi}
                </td>
                {row.map((val, xi) => {
                  const isHovered = hoveredCell && hoveredCell[0] === yi && hoveredCell[1] === xi;
                  return (
                    <td
                      key={xi}
                      className="p-0"
                      onMouseEnter={() => setHoveredCell([yi, xi])}
                      onMouseLeave={() => setHoveredCell(null)}
                      onClick={() => onCellClick && onCellClick(yi, xi, val)}
                    >
                      <div
                        className={`w-8 h-7 rounded-sm flex items-center justify-center text-[9px] font-medium cursor-pointer transition-all ${
                          isHovered ? 'ring-2 ring-blue-500 ring-offset-1 z-10 scale-110' : ''
                        } ${
                          data.interpretation?.peak_cell?.[0] === yi && data.interpretation?.peak_cell?.[1] === xi
                            ? 'ring-1 ring-red-400'
                            : ''
                        }`}
                        style={{
                          backgroundColor: interpolateColor(val, flatMin, flatMax),
                          color: val !== null && val !== undefined && (val - flatMin) / (flatMax - flatMin || 1) > 0.6 ? '#fff' : '#374151',
                        }}
                        title={`${yLabels[yi]} × ${xLabels[xi]}: ${val !== null ? val : '-'}`}
                      >
                        {val !== null && val !== undefined ? val : ''}
                      </div>
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {data.interpretation?.narrative && (
        <div className={`text-sm px-3 py-2 rounded border ${
          data.interpretation.peak_factor > 2 ? 'bg-amber-50 border-amber-200 text-amber-700' : 'bg-gray-50 border-gray-200 text-gray-600'
        }`}>
          {data.interpretation.narrative}
        </div>
      )}

      <div className="flex gap-4 text-xs text-gray-500">
        <span>Min: <strong>{data.stats?.min ?? 0}</strong></span>
        <span>Max: <strong>{data.stats?.max ?? 0}</strong></span>
        <span>Rata-rata: <strong>{data.stats?.avg ?? 0}</strong></span>
      </div>
    </div>
  );
}

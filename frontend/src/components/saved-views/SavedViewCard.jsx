import { Pin, PinOff, Eye, GitCompare, Pencil, Trash2, Clock, MapPin } from 'lucide-react';
import DeltaBadge from './DeltaBadge';

const KPI_LABELS = {
  sla: 'SLA',
  mttr: 'MTTR',
  volume: 'Volume',
  escalation: 'Eskalasi',
  auto_resolve: 'Auto-resolve',
  repeat: 'Repeat',
};

const KPI_UNITS = {
  sla: '%',
  mttr: ' min',
  volume: '',
  escalation: '%',
  auto_resolve: '%',
  repeat: '%',
};

const behaviorColors = {
  CHRONIC: 'bg-red-100 text-red-800',
  DETERIORATING: 'bg-orange-100 text-orange-800',
  SPORADIC: 'bg-amber-100 text-amber-800',
  SEASONAL: 'bg-yellow-100 text-yellow-800',
  IMPROVING: 'bg-blue-100 text-blue-800',
  HEALTHY: 'bg-green-100 text-green-800',
};

export default function SavedViewCard({ view, onOpen, onCompare, onEdit, onDelete, onPin }) {
  const deltas = view.deltas || {};
  const hasDeltas = Object.keys(deltas).length > 0;

  const formatDate = (d) => {
    if (!d) return '';
    try {
      return new Date(d).toLocaleDateString('id-ID', { day: 'numeric', month: 'short', year: 'numeric' });
    } catch {
      return d;
    }
  };

  return (
    <div className={`bg-white rounded-lg border shadow-sm hover:shadow-md transition-shadow p-4 ${view.is_pinned ? 'border-blue-300 ring-1 ring-blue-100' : 'border-gray-200'}`}>
      <div className="flex items-start justify-between mb-3">
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2">
            {view.is_pinned && <Pin size={14} className="text-blue-500 shrink-0" />}
            <h3 className="font-semibold text-gray-800 truncate">{view.name}</h3>
          </div>
          {view.description && (
            <p className="text-xs text-gray-500 mt-0.5 line-clamp-2">{view.description}</p>
          )}
        </div>
        <button
          onClick={() => onPin(view.id)}
          className="p-1 hover:bg-gray-100 rounded text-gray-400 hover:text-blue-500 shrink-0"
          title={view.is_pinned ? 'Unpin' : 'Pin'}
        >
          {view.is_pinned ? <PinOff size={16} /> : <Pin size={16} />}
        </button>
      </div>

      <div className="flex flex-wrap gap-2 mb-3">
        <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs bg-blue-50 text-blue-700">
          <MapPin size={10} />
          {view.entity_level?.toUpperCase()}: {view.entity_name || view.entity_id}
        </span>
        {view.snapshot_behavior && (
          <span className={`px-2 py-0.5 rounded text-xs font-medium ${behaviorColors[view.snapshot_behavior] || 'bg-gray-100 text-gray-600'}`}>
            {view.snapshot_behavior}
          </span>
        )}
        {view.date_from && view.date_to && (
          <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs bg-gray-50 text-gray-600">
            <Clock size={10} />
            {view.date_from} — {view.date_to}
          </span>
        )}
      </div>

      {hasDeltas && (
        <div className="grid grid-cols-3 gap-2 mb-3 p-2 bg-gray-50 rounded-lg">
          {Object.entries(deltas).map(([kpi, d]) => (
            <div key={kpi} className="text-center">
              <div className="text-[10px] text-gray-500 uppercase">{KPI_LABELS[kpi] || kpi}</div>
              <DeltaBadge delta={d.delta} quality={d.quality} unit={KPI_UNITS[kpi] || ''} />
            </div>
          ))}
        </div>
      )}

      {!hasDeltas && view.snapshot_sla !== null && view.snapshot_sla !== undefined && (
        <div className="grid grid-cols-3 gap-2 mb-3 p-2 bg-gray-50 rounded-lg text-center">
          <div>
            <div className="text-[10px] text-gray-500 uppercase">SLA</div>
            <div className="text-sm font-semibold">{view.snapshot_sla?.toFixed(1)}%</div>
          </div>
          <div>
            <div className="text-[10px] text-gray-500 uppercase">MTTR</div>
            <div className="text-sm font-semibold">{Math.round(view.snapshot_mttr || 0)} min</div>
          </div>
          <div>
            <div className="text-[10px] text-gray-500 uppercase">Volume</div>
            <div className="text-sm font-semibold">{view.snapshot_volume || 0}</div>
          </div>
        </div>
      )}

      <div className="flex items-center justify-between pt-2 border-t border-gray-100">
        <div className="text-[10px] text-gray-400">
          {view.last_accessed_at ? `Terakhir dibuka: ${formatDate(view.last_accessed_at)}` : `Dibuat: ${formatDate(view.created_at)}`}
          {view.access_count > 0 && ` · ${view.access_count}x`}
        </div>
        <div className="flex items-center gap-1">
          <button onClick={() => onOpen(view)} className="p-1.5 hover:bg-blue-50 rounded text-blue-600" title="Buka">
            <Eye size={14} />
          </button>
          <button onClick={() => onCompare(view)} className="p-1.5 hover:bg-purple-50 rounded text-purple-600" title="Compare">
            <GitCompare size={14} />
          </button>
          <button onClick={() => onEdit(view)} className="p-1.5 hover:bg-gray-100 rounded text-gray-500" title="Edit">
            <Pencil size={14} />
          </button>
          <button onClick={() => onDelete(view.id)} className="p-1.5 hover:bg-red-50 rounded text-red-500" title="Delete">
            <Trash2 size={14} />
          </button>
        </div>
      </div>
    </div>
  );
}

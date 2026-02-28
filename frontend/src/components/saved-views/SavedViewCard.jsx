import { Pin, PinOff, Eye, GitCompare, Pencil, Trash2, Clock, MapPin } from 'lucide-react';
import StatusDot from '../ui/StatusDot';
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

function mapBehaviorToStatus(behavior) {
  if (!behavior) return 'neutral';
  const b = behavior.toUpperCase();
  if (b === 'CHRONIC' || b === 'DETERIORATING') return 'critical';
  if (b === 'SPORADIC' || b === 'SEASONAL') return 'warning';
  return 'neutral';
}

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
            <h3 className="font-semibold truncate" style={{ color: 'var(--text-primary)' }}>{view.name}</h3>
          </div>
          {view.description && (
            <p className="text-xs mt-0.5 line-clamp-2" style={{ color: 'var(--text-muted)' }}>{view.description}</p>
          )}
        </div>
        <button
          onClick={() => onPin(view.id)}
          className="p-1 hover:bg-gray-100 rounded shrink-0"
          style={{ color: 'var(--text-muted)' }}
          title={view.is_pinned ? 'Unpin' : 'Pin'}
        >
          {view.is_pinned ? <PinOff size={16} /> : <Pin size={16} />}
        </button>
      </div>

      <div className="flex flex-wrap gap-2 mb-3">
        <span
          className="inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs"
          style={{ backgroundColor: 'var(--bg-hover)', color: 'var(--text-secondary)' }}
        >
          <MapPin size={10} />
          {view.entity_level?.toUpperCase()}: {view.entity_name || view.entity_id}
        </span>
        {view.snapshot_behavior && (
          <span
            className="inline-flex items-center gap-1.5 px-2 py-0.5 rounded text-xs font-medium"
            style={{ backgroundColor: 'var(--bg-hover)', color: 'var(--text-secondary)' }}
          >
            <StatusDot status={mapBehaviorToStatus(view.snapshot_behavior)} size={6} />
            {view.snapshot_behavior}
          </span>
        )}
        {view.date_from && view.date_to && (
          <span
            className="inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs"
            style={{ backgroundColor: 'var(--bg-secondary)', color: 'var(--text-muted)' }}
          >
            <Clock size={10} />
            {view.date_from} — {view.date_to}
          </span>
        )}
      </div>

      {hasDeltas && (
        <div className="grid grid-cols-3 gap-2 mb-3 p-2 rounded-lg" style={{ backgroundColor: 'var(--bg-secondary)' }}>
          {Object.entries(deltas).map(([kpi, d]) => (
            <div key={kpi} className="text-center">
              <div className="text-[10px] uppercase" style={{ color: 'var(--text-muted)' }}>{KPI_LABELS[kpi] || kpi}</div>
              <DeltaBadge delta={d.delta} quality={d.quality} unit={KPI_UNITS[kpi] || ''} />
            </div>
          ))}
        </div>
      )}

      {!hasDeltas && view.snapshot_sla !== null && view.snapshot_sla !== undefined && (
        <div className="grid grid-cols-3 gap-2 mb-3 p-2 rounded-lg text-center" style={{ backgroundColor: 'var(--bg-secondary)' }}>
          <div>
            <div className="text-[10px] uppercase" style={{ color: 'var(--text-muted)' }}>SLA</div>
            <div className="text-sm font-semibold" style={{ color: 'var(--text-primary)' }}>{view.snapshot_sla?.toFixed(1)}%</div>
          </div>
          <div>
            <div className="text-[10px] uppercase" style={{ color: 'var(--text-muted)' }}>MTTR</div>
            <div className="text-sm font-semibold" style={{ color: 'var(--text-primary)' }}>{Math.round(view.snapshot_mttr || 0)} min</div>
          </div>
          <div>
            <div className="text-[10px] uppercase" style={{ color: 'var(--text-muted)' }}>Volume</div>
            <div className="text-sm font-semibold" style={{ color: 'var(--text-primary)' }}>{view.snapshot_volume || 0}</div>
          </div>
        </div>
      )}

      <div className="flex items-center justify-between pt-2 border-t border-gray-100">
        <div className="text-[10px]" style={{ color: 'var(--text-muted)' }}>
          {view.last_accessed_at ? `Terakhir dibuka: ${formatDate(view.last_accessed_at)}` : `Dibuat: ${formatDate(view.created_at)}`}
          {view.access_count > 0 && ` · ${view.access_count}x`}
        </div>
        <div className="flex items-center gap-1">
          <button onClick={() => onOpen(view)} className="p-1.5 hover:bg-gray-100 rounded" style={{ color: 'var(--accent-brand)' }} title="Buka">
            <Eye size={14} />
          </button>
          <button onClick={() => onCompare(view)} className="p-1.5 hover:bg-gray-100 rounded" style={{ color: 'var(--text-secondary)' }} title="Compare">
            <GitCompare size={14} />
          </button>
          <button onClick={() => onEdit(view)} className="p-1.5 hover:bg-gray-100 rounded" style={{ color: 'var(--text-muted)' }} title="Edit">
            <Pencil size={14} />
          </button>
          <button onClick={() => onDelete(view.id)} className="p-1.5 hover:bg-gray-100 rounded" style={{ color: 'var(--status-critical-dot)' }} title="Delete">
            <Trash2 size={14} />
          </button>
        </div>
      </div>
    </div>
  );
}

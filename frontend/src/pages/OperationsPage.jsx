import { useEffect, useMemo, useState } from 'react';
import {
  Activity,
  AlertTriangle,
  CheckCircle,
  Clock,
  Database,
  FileText,
  Layers,
  RefreshCw,
  ServerCog,
  TrendingUp,
} from 'lucide-react';
import client from '../api/client';
import LoadingWrapper from '../components/LoadingWrapper';
import StatusBanner from '../components/ui/StatusBanner';

const TABS = [
  { key: 'jobs', label: 'Jobs', icon: ServerCog },
  { key: 'lake', label: 'Lake', icon: Layers },
  { key: 'models', label: 'Models', icon: TrendingUp },
  { key: 'files', label: 'Files', icon: FileText },
];

function OperationsPage() {
  const [snapshot, setSnapshot] = useState(null);
  const [activeTab, setActiveTab] = useState('jobs');
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState(null);

  const loadSnapshot = async (silent = false) => {
    if (silent) setRefreshing(true);
    else setLoading(true);
    setError(null);
    try {
      const res = await client.get('/ops/summary');
      setSnapshot(res.data);
    } catch (err) {
      setError('Gagal memuat monitoring operasi.');
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  };

  useEffect(() => {
    loadSnapshot();
  }, []);

  const health = snapshot?.health;
  const banner = useMemo(() => {
    const status = health?.status === 'ok' ? 'good' : health?.status || 'neutral';
    const narrative = health?.issues?.length
      ? health.issues.map((item) => item.message).join(' ')
      : 'Pipeline, data lake, dan model catalog tidak melaporkan isu aktif.';
    return { status, narrative };
  }, [health]);

  return (
    <div>
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3 mb-4">
        <div>
          <h2 className="text-xl font-bold text-gray-800">Operations</h2>
          <p className="text-xs text-gray-500 mt-1">{snapshot?.generated_at ? `Updated ${formatDate(snapshot.generated_at)}` : ''}</p>
        </div>
        <button
          onClick={() => loadSnapshot(true)}
          disabled={refreshing}
          className="inline-flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white text-xs font-medium hover:bg-blue-700 disabled:opacity-60"
        >
          <RefreshCw size={15} className={refreshing ? 'animate-spin' : ''} />
          Refresh
        </button>
      </div>

      <LoadingWrapper loading={loading} error={error} onRetry={() => loadSnapshot()}>
        {snapshot && (
          <div className="space-y-6">
            <StatusBanner
              status={banner.status}
              title={`Operational Health: ${health?.status?.toUpperCase() || 'UNKNOWN'}`}
              narrative={banner.narrative}
            />

            <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-4">
              <MetricTile
                icon={ServerCog}
                label="Recent Jobs"
                value={formatNumber(snapshot.jobs.total_recent)}
                detail={`${snapshot.jobs.running_count} running, ${snapshot.jobs.failed_count} failed`}
                tone={snapshot.jobs.failed_count > 0 ? 'warning' : 'good'}
              />
              <MetricTile
                icon={Database}
                label="Lake Rows"
                value={formatNumber(snapshot.lake.row_count)}
                detail={`${snapshot.lake.total_recent} partitions`}
                tone={snapshot.lake.total_recent > 0 ? 'good' : 'warning'}
              />
              <MetricTile
                icon={TrendingUp}
                label="Model Runs"
                value={formatNumber(snapshot.models.total_recent)}
                detail={`${snapshot.models.backtest_count} backtests`}
                tone={snapshot.models.total_recent > 0 ? 'good' : 'neutral'}
              />
              <MetricTile
                icon={Activity}
                label="Latest F1"
                value={formatPercent(snapshot.models.latest_backtest_metrics?.f1)}
                detail={`Recall ${formatPercent(snapshot.models.latest_backtest_metrics?.recall)}`}
                tone={(snapshot.models.latest_backtest_metrics?.recall ?? 1) < 0.5 ? 'warning' : 'good'}
              />
            </div>

            <div className="flex gap-1 border-b border-gray-200">
              {TABS.map(({ key, label, icon: Icon }) => (
                <button
                  key={key}
                  onClick={() => setActiveTab(key)}
                  className={`flex items-center gap-2 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors ${
                    activeTab === key
                      ? 'border-blue-600 text-blue-600'
                      : 'border-transparent text-gray-500 hover:text-gray-700'
                  }`}
                >
                  <Icon size={16} />
                  {label}
                </button>
              ))}
            </div>

            {activeTab === 'jobs' && <JobsView jobs={snapshot.recent.jobs} summary={snapshot.jobs} />}
            {activeTab === 'lake' && <LakeView partitions={snapshot.recent.partitions} summary={snapshot.lake} />}
            {activeTab === 'models' && <ModelsView modelRuns={snapshot.recent.model_runs} summary={snapshot.models} />}
            {activeTab === 'files' && <FilesView files={snapshot.recent.files} summary={snapshot.files} />}
          </div>
        )}
      </LoadingWrapper>
    </div>
  );
}

function MetricTile({ icon: Icon, label, value, detail, tone }) {
  const iconClass = tone === 'warning' ? 'text-amber-600' : tone === 'good' ? 'text-green-600' : 'text-gray-500';
  return (
    <div className="bg-white rounded-xl border border-gray-200 p-4">
      <div className="flex items-center justify-between mb-3">
        <span className="text-xs font-medium text-gray-500">{label}</span>
        <Icon size={18} className={iconClass} />
      </div>
      <div className="text-2xl font-bold text-gray-900">{value}</div>
      <div className="text-xs text-gray-500 mt-1">{detail}</div>
    </div>
  );
}

function JobsView({ jobs, summary }) {
  return (
    <section className="bg-white rounded-xl border border-gray-200 overflow-hidden">
      <SectionHeader title="Job Pipeline" meta={`Success rate ${formatPercent(summary.success_rate)}`} />
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead className="bg-gray-50 text-xs text-gray-500">
            <tr>
              <th className="text-left px-4 py-3 font-medium">Job</th>
              <th className="text-left px-4 py-3 font-medium">Status</th>
              <th className="text-left px-4 py-3 font-medium">Phase</th>
              <th className="text-left px-4 py-3 font-medium">Progress</th>
              <th className="text-left px-4 py-3 font-medium">Updated</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {jobs.length === 0 && <EmptyRow colSpan={5} />}
            {jobs.map((job) => (
              <tr key={job.job_id} className="hover:bg-gray-50">
                <td className="px-4 py-3">
                  <div className="font-medium text-gray-800">{job.job_type}</div>
                  <div className="text-xs text-gray-400 font-mono">{job.job_id}</div>
                </td>
                <td className="px-4 py-3"><StatusPill status={job.status} /></td>
                <td className="px-4 py-3 text-gray-600">{job.progress_phase || '-'}</td>
                <td className="px-4 py-3 min-w-36">
                  <ProgressBar current={job.progress_current} total={job.progress_total} />
                </td>
                <td className="px-4 py-3 text-gray-500">{formatDate(job.updated_at)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function LakeView({ partitions, summary }) {
  return (
    <section className="bg-white rounded-xl border border-gray-200 overflow-hidden">
      <SectionHeader title="Parquet Lake" meta={`${summary.covered_months.length} months, ${summary.covered_sources.length} sources`} />
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-0 border-b border-gray-100">
        {Object.entries(summary.dataset_layer).map(([key, value]) => (
          <div key={key} className="px-4 py-3 border-r border-gray-100 last:border-r-0">
            <div className="text-xs text-gray-500">{key}</div>
            <div className="text-lg font-semibold text-gray-800">{formatNumber(value.row_count)} rows</div>
            <div className="text-xs text-gray-400">{value.partition_count} partitions</div>
          </div>
        ))}
      </div>
      <SimpleTable
        columns={['Partition', 'Layer', 'Rows', 'Size', 'Updated']}
        rows={partitions.map((item) => [
          item.partition_id,
          item.layer,
          formatNumber(item.row_count),
          formatBytes(item.size_bytes),
          formatDate(item.updated_at),
        ])}
      />
    </section>
  );
}

function ModelsView({ modelRuns, summary }) {
  return (
    <section className="bg-white rounded-xl border border-gray-200 overflow-hidden">
      <SectionHeader title="Model Monitoring" meta={`Risk runs ${summary.risk_run_count}, backtests ${summary.backtest_count}`} />
      <div className="grid grid-cols-1 md:grid-cols-3 gap-0 border-b border-gray-100">
        {['critical', 'high', 'medium'].map((level) => (
          <div key={level} className="px-4 py-3 border-r border-gray-100 last:border-r-0">
            <div className="text-xs text-gray-500 capitalize">{level} Risk</div>
            <div className="text-lg font-semibold text-gray-800">{summary.risk_distribution[level] || 0}</div>
          </div>
        ))}
      </div>
      <SimpleTable
        columns={['Model', 'Entity', 'Window', 'Status', 'Created']}
        rows={modelRuns.map((item) => [
          item.model_name,
          `${item.entity_level || '-'} / ${item.entity_id || '-'}`,
          `${item.window_start || '-'} to ${item.window_end || '-'}`,
          item.status,
          formatDate(item.created_at),
        ])}
      />
    </section>
  );
}

function FilesView({ files, summary }) {
  return (
    <section className="bg-white rounded-xl border border-gray-200 overflow-hidden">
      <SectionHeader title="File Catalog" meta={`${formatNumber(summary.row_count)} registered rows`} />
      <SimpleTable
        columns={['File', 'Type', 'Source', 'Status', 'Rows']}
        rows={files.map((item) => [
          item.filename || item.storage_uri,
          item.file_type || '-',
          item.source || '-',
          item.status,
          formatNumber(item.row_count),
        ])}
      />
    </section>
  );
}

function SectionHeader({ title, meta }) {
  return (
    <div className="flex items-center justify-between px-4 py-3 border-b border-gray-100">
      <h3 className="font-semibold text-gray-800 text-sm">{title}</h3>
      <span className="text-xs text-gray-500">{meta}</span>
    </div>
  );
}

function SimpleTable({ columns, rows }) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead className="bg-gray-50 text-xs text-gray-500">
          <tr>
            {columns.map((column) => (
              <th key={column} className="text-left px-4 py-3 font-medium">{column}</th>
            ))}
          </tr>
        </thead>
        <tbody className="divide-y divide-gray-100">
          {rows.length === 0 && <EmptyRow colSpan={columns.length} />}
          {rows.map((row, idx) => (
            <tr key={idx} className="hover:bg-gray-50">
              {row.map((cell, cellIdx) => (
                <td key={cellIdx} className="px-4 py-3 text-gray-700 max-w-xs truncate">{cell}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function EmptyRow({ colSpan }) {
  return (
    <tr>
      <td colSpan={colSpan} className="px-4 py-8 text-center text-sm text-gray-400">
        Tidak ada data.
      </td>
    </tr>
  );
}

function StatusPill({ status }) {
  const s = status || 'unknown';
  const classes = {
    completed: 'bg-green-50 text-green-700 border-green-200',
    running: 'bg-blue-50 text-blue-700 border-blue-200',
    queued: 'bg-gray-50 text-gray-700 border-gray-200',
    failed: 'bg-red-50 text-red-700 border-red-200',
    cancelled: 'bg-amber-50 text-amber-700 border-amber-200',
  };
  const Icon = s === 'failed' ? AlertTriangle : s === 'completed' ? CheckCircle : Clock;
  return (
    <span className={`inline-flex items-center gap-1.5 px-2 py-1 rounded-md border text-xs font-medium ${classes[s] || classes.queued}`}>
      <Icon size={13} />
      {s}
    </span>
  );
}

function ProgressBar({ current, total }) {
  const pct = total ? Math.min(100, Math.round(((current || 0) / total) * 100)) : 0;
  return (
    <div>
      <div className="h-1.5 bg-gray-100 rounded-full overflow-hidden">
        <div className="h-full bg-blue-600 rounded-full" style={{ width: `${pct}%` }} />
      </div>
      <div className="text-xs text-gray-400 mt-1">{total ? `${current || 0}/${total}` : '-'}</div>
    </div>
  );
}

function formatNumber(value) {
  const n = Number(value || 0);
  return new Intl.NumberFormat('id-ID').format(n);
}

function formatBytes(value) {
  const n = Number(value || 0);
  if (n >= 1024 * 1024 * 1024) return `${(n / (1024 * 1024 * 1024)).toFixed(1)} GB`;
  if (n >= 1024 * 1024) return `${(n / (1024 * 1024)).toFixed(1)} MB`;
  if (n >= 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${n} B`;
}

function formatPercent(value) {
  if (value === undefined || value === null || Number.isNaN(Number(value))) return '-';
  const n = Number(value);
  return n <= 1 ? `${Math.round(n * 100)}%` : `${Math.round(n)}%`;
}

function formatDate(value) {
  if (!value) return '-';
  try {
    return new Intl.DateTimeFormat('id-ID', {
      dateStyle: 'medium',
      timeStyle: 'short',
    }).format(new Date(value));
  } catch (_) {
    return value;
  }
}

export default OperationsPage;

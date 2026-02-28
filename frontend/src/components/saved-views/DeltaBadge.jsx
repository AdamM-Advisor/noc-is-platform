import { TrendingUp, TrendingDown, Minus } from 'lucide-react';

const qualityConfig = {
  improving: { bg: 'bg-green-100', text: 'text-green-700', icon: TrendingUp },
  worsening: { bg: 'bg-red-100', text: 'text-red-700', icon: TrendingDown },
  stable: { bg: 'bg-gray-100', text: 'text-gray-600', icon: Minus },
};

export default function DeltaBadge({ delta, quality, unit = '' }) {
  if (delta === undefined || delta === null) return null;
  const cfg = qualityConfig[quality] || qualityConfig.stable;
  const Icon = cfg.icon;
  const sign = delta > 0 ? '+' : '';

  return (
    <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium ${cfg.bg} ${cfg.text}`}>
      <Icon size={12} />
      {sign}{typeof delta === 'number' ? delta.toFixed(1) : delta}{unit}
    </span>
  );
}

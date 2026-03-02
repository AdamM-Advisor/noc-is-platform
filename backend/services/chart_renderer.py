import io
import base64
import logging

logger = logging.getLogger(__name__)

BLUE = '#1e40af'
RED = '#DC2626'
GREEN = '#16A34A'
YELLOW = '#D97706'
GRAY = '#6B7280'

_plt = None
_mticker = None

def _get_plt():
    global _plt, _mticker
    if _plt is None:
        try:
            import matplotlib
            matplotlib.use('Agg')
            import matplotlib.pyplot as plt
            import matplotlib.ticker as mticker
            _plt = plt
            _mticker = mticker
        except ImportError:
            logger.warning("matplotlib not available - chart rendering disabled")
            return None
    return _plt


_NO_CHART = "data:image/png;base64,"


class ChartRenderer:

    def _check_plt(self):
        plt = _get_plt()
        if plt is None:
            return None
        return plt

    def render_trend_line(self, data, title, target=None, ylabel=''):
        plt = self._check_plt()
        if plt is None:
            return _NO_CHART
        fig, ax = plt.subplots(figsize=(8, 3))
        periods = [d.get('period', '') for d in data]
        values = [d.get('value', 0) for d in data]
        ax.plot(periods, values, 'o-', color=BLUE, linewidth=2, markersize=6)
        if target:
            ax.axhline(y=target, color=RED, linestyle='--', label=f'Target {target}%', alpha=0.7)
        ax.set_title(title, fontsize=11, fontweight='bold')
        if ylabel:
            ax.set_ylabel(ylabel, fontsize=9)
        ax.grid(True, alpha=0.3)
        if target:
            ax.legend(fontsize=8)
        plt.xticks(fontsize=8, rotation=30)
        plt.tight_layout()
        return self._fig_to_base64(fig)

    def render_multi_trend(self, datasets, title, labels=None):
        plt = self._check_plt()
        if plt is None:
            return _NO_CHART
        fig, ax = plt.subplots(figsize=(8, 3.5))
        colors = [BLUE, RED, GREEN, YELLOW, GRAY]
        for i, ds in enumerate(datasets):
            periods = [d.get('period', '') for d in ds]
            values = [d.get('value', 0) for d in ds]
            label = labels[i] if labels and i < len(labels) else f'Series {i+1}'
            ax.plot(periods, values, 'o-', color=colors[i % len(colors)],
                    linewidth=2, markersize=5, label=label)
        ax.set_title(title, fontsize=11, fontweight='bold')
        ax.grid(True, alpha=0.3)
        ax.legend(fontsize=8)
        plt.xticks(fontsize=8, rotation=30)
        plt.tight_layout()
        return self._fig_to_base64(fig)

    def render_bar_chart(self, labels, values, title, color=None):
        plt = self._check_plt()
        if plt is None:
            return _NO_CHART
        fig, ax = plt.subplots(figsize=(8, 3.5))
        bars = ax.bar(labels, values, color=color or BLUE, alpha=0.85)
        ax.set_title(title, fontsize=11, fontweight='bold')
        ax.grid(True, alpha=0.2, axis='y')
        for bar, val in zip(bars, values):
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height(),
                    f'{val}', ha='center', va='bottom', fontsize=8)
        plt.xticks(fontsize=8, rotation=30)
        plt.tight_layout()
        return self._fig_to_base64(fig)

    def render_heatmap(self, cells, y_labels, x_labels, title):
        plt = self._check_plt()
        if plt is None:
            return _NO_CHART
        fig, ax = plt.subplots(figsize=(10, max(3, len(y_labels) * 0.5 + 1)))
        if not cells or not cells[0]:
            ax.text(0.5, 0.5, 'Tidak ada data', ha='center', va='center', fontsize=12)
            ax.set_title(title, fontsize=11, fontweight='bold')
            plt.tight_layout()
            return self._fig_to_base64(fig)
        im = ax.imshow(cells, cmap='YlOrRd', aspect='auto')
        ax.set_xticks(range(len(x_labels)))
        ax.set_xticklabels(x_labels, fontsize=7)
        ax.set_yticks(range(len(y_labels)))
        ax.set_yticklabels(y_labels, fontsize=8)
        ax.set_title(title, fontsize=11, fontweight='bold')
        fig.colorbar(im, ax=ax, shrink=0.8)
        plt.tight_layout()
        return self._fig_to_base64(fig)

    def render_pareto(self, labels, values, title):
        plt = self._check_plt()
        if plt is None:
            return _NO_CHART
        if not labels or not values:
            return self._empty_chart(title)
        sorted_data = sorted(zip(labels, values), key=lambda x: -x[1])
        labels_s = [d[0][:20] for d in sorted_data]
        values_s = [d[1] for d in sorted_data]
        total = sum(values_s) or 1
        cumulative = [sum(values_s[:i+1])/total*100 for i in range(len(values_s))]

        fig, ax1 = plt.subplots(figsize=(8, 4))
        ax1.bar(labels_s, values_s, color=BLUE, alpha=0.8)
        ax1.set_ylabel('Volume', fontsize=9)
        ax2 = ax1.twinx()
        ax2.plot(labels_s, cumulative, 'o-', color=RED, linewidth=2)
        ax2.axhline(y=80, color=RED, linestyle=':', alpha=0.5)
        ax2.set_ylabel('Kumulatif %', fontsize=9)
        ax1.set_title(title, fontsize=11, fontweight='bold')
        plt.xticks(fontsize=7, rotation=45, ha='right')
        plt.tight_layout()
        return self._fig_to_base64(fig)

    def render_radar(self, labels, values, title):
        plt = self._check_plt()
        if plt is None:
            return _NO_CHART
        import numpy as np
        n = len(labels)
        if n < 3:
            return self._empty_chart(title)
        angles = [i / float(n) * 2 * 3.14159 for i in range(n)]
        angles += angles[:1]
        values_plot = list(values) + [values[0]]
        fig, ax = plt.subplots(figsize=(5, 5), subplot_kw=dict(polar=True))
        ax.plot(angles, values_plot, 'o-', color=BLUE, linewidth=2)
        ax.fill(angles, values_plot, alpha=0.15, color=BLUE)
        ax.set_xticks(angles[:-1])
        ax.set_xticklabels(labels, fontsize=8)
        ax.set_title(title, fontsize=11, fontweight='bold', y=1.1)
        plt.tight_layout()
        return self._fig_to_base64(fig)

    def _empty_chart(self, title):
        plt = self._check_plt()
        if plt is None:
            return _NO_CHART
        fig, ax = plt.subplots(figsize=(6, 3))
        ax.text(0.5, 0.5, 'Data tidak tersedia', ha='center', va='center',
                fontsize=12, color=GRAY)
        ax.set_title(title, fontsize=11, fontweight='bold')
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.axis('off')
        plt.tight_layout()
        return self._fig_to_base64(fig)

    def _fig_to_base64(self, fig):
        plt = _get_plt()
        buf = io.BytesIO()
        fig.savefig(buf, format='png', dpi=150, bbox_inches='tight',
                    facecolor='white', edgecolor='none')
        plt.close(fig)
        buf.seek(0)
        return base64.b64encode(buf.read()).decode('utf-8')

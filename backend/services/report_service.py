import os
import time
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

from jinja2 import Environment, FileSystemLoader
from backend.database import get_connection, get_write_connection
from backend.services.excel_service import ExcelExporter
from backend.services.dashboard_service import (
    LEVEL_COLS, CHILD_LEVEL_MAP, determine_entity_status,
    determine_overall_status, generate_overall_narrative,
    _get_recent_periods, _get_entity_history, _compute_sla_trend,
    _compute_mttr_trend, _prev_month,
)
from backend.services.recommendation_service import RecommendationEngine

logger = logging.getLogger(__name__)

REPORT_DIR = Path("data/reports")
REPORT_DIR.mkdir(parents=True, exist_ok=True)

TEMPLATE_DIR = Path(__file__).parent.parent / "templates" / "reports"

TYPE_LABELS = {
    "daily": "HARIAN",
    "weekly": "MINGGUAN",
    "monthly": "BULANAN",
    "quarterly": "TRIWULAN",
    "annual": "TAHUNAN",
}

def _get_chart_renderer():
    from backend.services.chart_renderer import ChartRenderer
    return ChartRenderer()

excel_exporter = ExcelExporter()
rec_engine = RecommendationEngine()


class ReportGenerator:

    def __init__(self):
        self.env = Environment(
            loader=FileSystemLoader(str(TEMPLATE_DIR)),
            autoescape=True,
        )

    def generate(self, report_type, entity_level, entity_id,
                 period_start, period_end, options=None):
        options = options or {}
        report_id = self._create_history(report_type, entity_level, entity_id,
                                          period_start, period_end)
        self._update_status(report_id, 'generating')

        try:
            start_time = time.time()

            data = self._collect_data(report_type, entity_level, entity_id,
                                       period_start, period_end)

            narratives = self._generate_narratives(report_type, data)
            charts = self._render_charts(report_type, data)

            entity_name = data.get('entity_name', entity_id)
            period_label = self._make_period_label(report_type, period_start, period_end)
            status_info = data.get('status_info', {})

            template_data = {
                'report_type': report_type,
                'report_type_label': TYPE_LABELS.get(report_type, report_type.upper()),
                'entity_name': entity_name,
                'entity_level': entity_level,
                'entity_id': entity_id,
                'period_label': period_label,
                'period_start': period_start,
                'period_end': period_end,
                'generated_at': datetime.now().strftime('%d %b %Y %H:%M'),
                'status_class': status_info.get('css_class', 'baik'),
                'status_icon': status_info.get('icon', ''),
                'status_text': status_info.get('status', ''),
                **data,
                **narratives,
                **charts,
            }

            html = self._render_template(report_type, template_data)

            pdf_path = None
            if options.get('include_pdf', True):
                pdf_bytes = self._html_to_pdf(html)
                pdf_path = self._save_file(report_id, pdf_bytes, 'pdf')

            excel_path = None
            if options.get('include_excel') and report_type in ('monthly', 'quarterly', 'annual'):
                excel_bytes = self._generate_excel(report_type, data)
                excel_path = self._save_file(report_id, excel_bytes, 'xlsx')

            elapsed = int((time.time() - start_time) * 1000)

            self._update_completed(report_id, entity_name, period_label,
                                    pdf_path, excel_path, elapsed,
                                    options.get('ai_enhanced', False), data)

            return {
                "report_id": report_id,
                "status": "completed",
                "pdf_path": pdf_path,
                "excel_path": excel_path,
                "generation_time_ms": elapsed,
                "html": html,
            }
        except Exception as e:
            logger.error(f"Report generation failed: {e}", exc_info=True)
            self._update_status(report_id, 'failed', str(e))
            return {
                "report_id": report_id,
                "status": "failed",
                "error": str(e),
            }

    def preview(self, report_type, entity_level, entity_id,
                period_start, period_end):
        data = self._collect_data(report_type, entity_level, entity_id,
                                   period_start, period_end)
        narratives = self._generate_narratives(report_type, data)
        charts = self._render_charts(report_type, data)

        entity_name = data.get('entity_name', entity_id)
        period_label = self._make_period_label(report_type, period_start, period_end)
        status_info = data.get('status_info', {})

        template_data = {
            'report_type': report_type,
            'report_type_label': TYPE_LABELS.get(report_type, report_type.upper()),
            'entity_name': entity_name,
            'entity_level': entity_level,
            'entity_id': entity_id,
            'period_label': period_label,
            'period_start': period_start,
            'period_end': period_end,
            'generated_at': datetime.now().strftime('%d %b %Y %H:%M'),
            'status_class': status_info.get('css_class', 'baik'),
            'status_icon': status_info.get('icon', ''),
            'status_text': status_info.get('status', ''),
            **data,
            **narratives,
            **charts,
        }
        return self._render_template(report_type, template_data)

    def _collect_data(self, report_type, entity_level, entity_id,
                       period_start, period_end):
        with get_connection() as conn:
            is_nasional = entity_level == 'nasional'
            col_info = LEVEL_COLS.get(entity_level)

            if is_nasional:
                entity_name = 'Seluruh Indonesia'
                col = None
            elif col_info:
                col, tbl, name_col = col_info
                try:
                    row = conn.execute(f"SELECT {name_col} FROM {tbl} WHERE {col} = ?",
                                       [entity_id]).fetchone()
                    entity_name = row[0] if row else entity_id
                except Exception:
                    entity_name = entity_id
            else:
                entity_name = entity_id
                col = None

            period = period_end[:7] if period_end else period_start[:7] if period_start else "2025-07"

            kpis = self._get_kpis(conn, col, entity_id, period)
            prev = _prev_month(period)
            kpis_prev = self._get_kpis(conn, col, entity_id, prev)

            deltas = self._calc_deltas(kpis, kpis_prev)

            recent = _get_recent_periods(period, 3)
            if col:
                hist = _get_entity_history(conn, col, entity_id, recent)
            else:
                hist = self._get_national_history(conn, recent)
            sla_trend = _compute_sla_trend(hist)
            mttr_trend = _compute_mttr_trend(hist)

            status_info = self._calc_status(kpis, sla_trend)

            if is_nasional:
                children = self._get_national_children(conn, period)
            elif col:
                children = self._get_children(conn, entity_level, entity_id, period, col)
            else:
                children = []

            trend_data = [{"period": h["period"], "value": h.get("sla_pct", 0)} for h in hist]
            mttr_trend_data = [{"period": h["period"], "value": h.get("avg_mttr_min", 0)} for h in hist]
            vol_trend_data = [{"period": h["period"], "value": h.get("total_tickets", 0)} for h in hist]

            recs_kpis = {**kpis, **deltas}
            entity_info = {
                "name": entity_name, "level": entity_level,
                "child_type": CHILD_LEVEL_MAP.get(entity_level, "entitas"),
            }
            recs = rec_engine.generate(entity_info, recs_kpis,
                                        {**sla_trend, **mttr_trend},
                                        None, children, None)
            for r in recs:
                pinfo = {
                    "SEGERA": {"label": "SEGERA", "icon": "", "class": "segera"},
                    "MINGGU_INI": {"label": "MINGGU INI", "icon": "", "class": "minggu"},
                    "BULAN_INI": {"label": "BULAN INI", "icon": "", "class": "bulan"},
                    "RUTIN": {"label": "RUTIN", "icon": "", "class": "rutin"},
                }.get(r.get("priority", ""), {"label": "", "icon": "", "class": "rutin"})
                r["priority_icon"] = pinfo["icon"]
                r["priority_class"] = pinfo["class"]
                r["priority_label"] = pinfo["label"]

            top_children = sorted(children, key=lambda c: c.get('sla_pct', 100))[:10]

            result = {
                'entity_name': entity_name,
                'kpis': kpis,
                'kpis_prev': kpis_prev,
                'deltas': deltas,
                'sla_trend': sla_trend,
                'mttr_trend': mttr_trend,
                'status_info': status_info,
                'children': children,
                'children_kpis': children,
                'top_problem_children': top_children,
                'recommendations': recs,
                'trend_data': trend_data,
                'mttr_trend_data': mttr_trend_data,
                'vol_trend_data': vol_trend_data,
                'period': period,
                'generated_at': datetime.now().isoformat(),
                'entity_level': entity_level,
                'period_label': '',
                'risk_scores': [],
                'aging_tickets': [],
                'behavior_transition': [],
            }
            return result

    def _get_kpis(self, conn, col, entity_id, period):
        try:
            if col:
                row = conn.execute(f"""
                    SELECT SUM(total_tickets), SUM(total_sla_met), AVG(avg_mttr_min),
                           SUM(total_escalated), SUM(total_auto_resolved), SUM(total_repeat)
                    FROM summary_monthly WHERE year_month = ? AND {col} = ?
                """, [period, entity_id]).fetchone()
            else:
                row = conn.execute("""
                    SELECT SUM(total_tickets), SUM(total_sla_met), AVG(avg_mttr_min),
                           SUM(total_escalated), SUM(total_auto_resolved), SUM(total_repeat)
                    FROM summary_monthly WHERE year_month = ?
                """, [period]).fetchone()
        except Exception:
            return self._empty_kpis()
        if not row or not row[0]:
            return self._empty_kpis()
        vol = row[0] or 0
        met = row[1] or 0
        return {
            "sla_pct": round((met / vol * 100) if vol > 0 else 0, 1),
            "total_volume": vol,
            "avg_mttr_min": round(row[2] or 0, 0),
            "escalation_pct": round((row[3] or 0) / vol * 100 if vol > 0 else 0, 1),
            "auto_resolve_pct": round((row[4] or 0) / vol * 100 if vol > 0 else 0, 1),
            "repeat_pct": round((row[5] or 0) / vol * 100 if vol > 0 else 0, 1),
        }

    def _empty_kpis(self):
        return {"sla_pct": 0, "total_volume": 0, "avg_mttr_min": 0,
                "escalation_pct": 0, "auto_resolve_pct": 0, "repeat_pct": 0}

    def _calc_deltas(self, kpis, kpis_prev):
        return {
            "sla_mom_pp": round(kpis["sla_pct"] - kpis_prev["sla_pct"], 1),
            "volume_mom_pct": round(((kpis["total_volume"] - kpis_prev["total_volume"]) / kpis_prev["total_volume"] * 100) if kpis_prev["total_volume"] > 0 else 0, 1),
            "mttr_mom_pct": round(((kpis["avg_mttr_min"] - kpis_prev["avg_mttr_min"]) / kpis_prev["avg_mttr_min"] * 100) if kpis_prev["avg_mttr_min"] > 0 else 0, 1),
            "esc_mom_pp": round(kpis["escalation_pct"] - kpis_prev["escalation_pct"], 1),
            "auto_mom_pp": round(kpis["auto_resolve_pct"] - kpis_prev["auto_resolve_pct"], 1),
        }

    def _calc_status(self, kpis, sla_trend):
        sla = kpis.get("sla_pct", 0)
        sla_target = 90.0
        status = determine_entity_status(sla, sla_target, sla_trend.get("sla_quality", "stable"))
        status_map = {
            "KRITIS": {"css_class": "kritis", "icon": ""},
            "PERLU PERHATIAN": {"css_class": "perhatian", "icon": ""},
            "BAIK": {"css_class": "baik", "icon": ""},
            "SANGAT BAIK": {"css_class": "sangat-baik", "icon": ""},
        }
        info = status_map.get(status.get("status", ""), {"css_class": "baik", "icon": ""})
        return {**status, **info}

    def _get_children(self, conn, entity_level, entity_id, period, parent_col):
        child_level = CHILD_LEVEL_MAP.get(entity_level)
        if not child_level or child_level == "site":
            return []
        c_info = LEVEL_COLS.get(child_level)
        if not c_info:
            return []
        c_col, c_tbl, c_name = c_info
        try:
            rows = conn.execute(f"""
                SELECT sm.{c_col}, m.{c_name},
                       SUM(sm.total_tickets), SUM(sm.total_sla_met),
                       AVG(sm.avg_mttr_min),
                       SUM(sm.total_escalated), SUM(sm.total_auto_resolved), SUM(sm.total_repeat)
                FROM summary_monthly sm
                LEFT JOIN {c_tbl} m ON sm.{c_col} = m.{c_col}
                WHERE sm.year_month = ? AND sm.{parent_col} = ?
                GROUP BY sm.{c_col}, m.{c_name}
                ORDER BY SUM(sm.total_sla_met) * 1.0 / NULLIF(SUM(sm.total_tickets), 0) ASC
            """, [period, entity_id]).fetchall()
        except Exception:
            return []
        result = []
        for r in rows:
            vol = r[2] or 0
            met = r[3] or 0
            sla = round((met / vol * 100) if vol > 0 else 0, 1)
            result.append({
                "id": r[0], "name": r[1] or r[0],
                "volume": vol, "sla_pct": sla,
                "mttr": round(r[4] or 0, 0),
                "escalation_pct": round((r[5] or 0) / vol * 100 if vol > 0 else 0, 1),
                "auto_resolve_pct": round((r[6] or 0) / vol * 100 if vol > 0 else 0, 1),
                "repeat_pct": round((r[7] or 0) / vol * 100 if vol > 0 else 0, 1),
                "trend": "",
                "status": "BAIK" if sla >= 90 else "PERHATIAN" if sla >= 85 else "KRITIS",
                "behavior": "",
            })
        return result

    def _get_national_history(self, conn, periods):
        placeholders = ",".join(["?" for _ in periods])
        try:
            rows = conn.execute(f"""
                SELECT year_month,
                       SUM(total_sla_met) as met,
                       SUM(total_tickets) as tkts,
                       AVG(avg_mttr_min) as mttr
                FROM summary_monthly
                WHERE year_month IN ({placeholders})
                GROUP BY year_month ORDER BY year_month
            """, periods).fetchall()
        except Exception:
            return []
        result = []
        for r in rows:
            tkts = r[2] or 0
            met = r[1] or 0
            result.append({
                "period": r[0],
                "sla_pct": round((met / tkts * 100) if tkts > 0 else 0, 1),
                "total_tickets": tkts,
                "avg_mttr_min": round(r[3] or 0, 0),
            })
        return result

    def _get_national_children(self, conn, period):
        try:
            rows = conn.execute("""
                SELECT sm.area_id, m.area_name,
                       SUM(sm.total_tickets), SUM(sm.total_sla_met),
                       AVG(sm.avg_mttr_min),
                       SUM(sm.total_escalated), SUM(sm.total_auto_resolved), SUM(sm.total_repeat)
                FROM summary_monthly sm
                LEFT JOIN master_area m ON sm.area_id = m.area_id
                WHERE sm.year_month = ?
                GROUP BY sm.area_id, m.area_name
                ORDER BY SUM(sm.total_sla_met) * 1.0 / NULLIF(SUM(sm.total_tickets), 0) ASC
            """, [period]).fetchall()
        except Exception:
            return []
        result = []
        for r in rows:
            vol = r[2] or 0
            met = r[3] or 0
            sla = round((met / vol * 100) if vol > 0 else 0, 1)
            result.append({
                "id": r[0], "name": r[1] or r[0],
                "volume": vol, "sla_pct": sla,
                "mttr": round(r[4] or 0, 0),
                "escalation_pct": round((r[5] or 0) / vol * 100 if vol > 0 else 0, 1),
                "auto_resolve_pct": round((r[6] or 0) / vol * 100 if vol > 0 else 0, 1),
                "repeat_pct": round((r[7] or 0) / vol * 100 if vol > 0 else 0, 1),
                "trend": "",
                "status": "BAIK" if sla >= 90 else "PERHATIAN" if sla >= 85 else "KRITIS",
                "behavior": "",
            })
        return result

    def _generate_narratives(self, report_type, data):
        kpis = data.get('kpis', {})
        deltas = data.get('deltas', {})
        entity_name = data.get('entity_name', '')
        children = data.get('children', [])

        sla = kpis.get('sla_pct', 0)
        vol = kpis.get('total_volume', 0)
        mttr = kpis.get('avg_mttr_min', 0)
        sla_delta = deltas.get('sla_mom_pp', 0)

        parts = []
        if sla >= 95:
            parts.append(f"{entity_name} menunjukkan performa sangat baik dengan SLA {sla}%.")
        elif sla >= 90:
            parts.append(f"{entity_name} mencapai SLA {sla}%, memenuhi target.")
        elif sla >= 85:
            parts.append(f"{entity_name} perlu perhatian — SLA {sla}% di bawah target 90%.")
        else:
            parts.append(f"{entity_name} dalam kondisi kritis — SLA hanya {sla}%.")

        if sla_delta > 0:
            parts.append(f"SLA membaik {sla_delta:+.1f}pp dari periode sebelumnya.")
        elif sla_delta < 0:
            parts.append(f"SLA menurun {sla_delta:+.1f}pp dari periode sebelumnya.")

        parts.append(f"Total {vol} tiket diproses dengan MTTR rata-rata {mttr:.0f} menit.")

        if children:
            worst = min(children, key=lambda c: c.get('sla_pct', 100))
            if worst.get('sla_pct', 100) < 85:
                parts.append(f"Perhatian khusus: {worst.get('name', '?')} hanya {worst.get('sla_pct', 0)}% SLA.")

        exec_narrative = " ".join(parts)

        trend_parts = []
        trend_data = data.get('trend_data', [])
        if len(trend_data) >= 2:
            latest = trend_data[-1].get('value', 0)
            prev = trend_data[-2].get('value', 0)
            if latest > prev:
                trend_parts.append(f"Tren SLA menunjukkan perbaikan dari {prev}% ke {latest}%.")
            elif latest < prev:
                trend_parts.append(f"Tren SLA menurun dari {prev}% ke {latest}%.")
            else:
                trend_parts.append(f"Tren SLA stabil di {latest}%.")
        trend_narrative = " ".join(trend_parts) if trend_parts else "Data tren belum cukup."

        return {
            'executive_narrative': exec_narrative,
            'trend_narrative': trend_narrative,
            'comparison_label': 'vs Periode Sebelumnya',
        }

    def _render_charts(self, report_type, data):
        charts = {}
        trend_data = data.get('trend_data', [])
        if trend_data:
            charts['trend_chart_b64'] = _get_chart_renderer().render_trend_line(
                trend_data, 'Tren SLA (%)', target=90, ylabel='SLA %'
            )
        else:
            charts['trend_chart_b64'] = _get_chart_renderer()._empty_chart('Tren SLA')

        mttr_data = data.get('mttr_trend_data', [])
        if mttr_data:
            charts['mttr_chart_b64'] = _get_chart_renderer().render_trend_line(
                mttr_data, 'Tren MTTR (menit)', ylabel='Menit'
            )

        vol_data = data.get('vol_trend_data', [])
        if vol_data:
            charts['vol_chart_b64'] = _get_chart_renderer().render_bar_chart(
                [d['period'] for d in vol_data],
                [d['value'] for d in vol_data],
                'Volume Tiket per Periode'
            )

        children = data.get('children', [])
        if children:
            top10 = children[:10]
            charts['children_bar_b64'] = _get_chart_renderer().render_bar_chart(
                [(c.get('name') or c.get('id') or '')[:15] for c in top10],
                [c.get('sla_pct', 0) for c in top10],
                'SLA per Sub-Entitas (Bottom 10)',
                color='#DC2626'
            )

        kpis = data.get('kpis', {})
        radar_labels = ['SLA', 'MTTR_inv', 'Volume_inv', 'Esc_inv', 'Auto', 'Repeat_inv']
        sla = kpis.get('sla_pct', 0)
        mttr = min(kpis.get('avg_mttr_min', 0), 1440)
        vol = min(kpis.get('total_volume', 0), 1000)
        esc = kpis.get('escalation_pct', 0)
        auto = kpis.get('auto_resolve_pct', 0)
        repeat = kpis.get('repeat_pct', 0)
        radar_values = [
            sla,
            max(0, 100 - (mttr / 14.4)),
            max(0, 100 - (vol / 10)),
            max(0, 100 - esc * 5),
            auto,
            max(0, 100 - repeat * 4),
        ]
        charts['radar_b64'] = _get_chart_renderer().render_radar(radar_labels, radar_values, 'Profil KPI')

        return charts

    def _render_template(self, report_type, data):
        try:
            template = self.env.get_template(f"{report_type}.html")
        except Exception:
            template = self.env.get_template("monthly.html")
        return template.render(**data)

    def _html_to_pdf(self, html):
        from weasyprint import HTML
        return HTML(string=html).write_pdf()

    def _generate_excel(self, report_type, data):
        if report_type == 'monthly':
            return excel_exporter.generate_monthly_excel(data)
        elif report_type == 'quarterly':
            return excel_exporter.generate_quarterly_excel(data)
        elif report_type == 'annual':
            return excel_exporter.generate_annual_excel(data)
        return excel_exporter.generate_monthly_excel(data)

    def _save_file(self, report_id, content, ext):
        filename = f"report_{report_id}.{ext}"
        filepath = REPORT_DIR / filename
        with open(filepath, 'wb') as f:
            f.write(content)
        return str(filepath)

    def _make_period_label(self, report_type, period_start, period_end):
        try:
            MONTHS_ID = ["", "Januari", "Februari", "Maret", "April", "Mei", "Juni",
                         "Juli", "Agustus", "September", "Oktober", "November", "Desember"]
            if report_type == 'daily':
                d = datetime.strptime(period_start[:10], '%Y-%m-%d')
                return d.strftime(f'%d {MONTHS_ID[d.month]} %Y')
            elif report_type == 'weekly':
                d1 = datetime.strptime(period_start[:10], '%Y-%m-%d')
                d2 = datetime.strptime(period_end[:10], '%Y-%m-%d')
                wk = d1.isocalendar()[1]
                return f"Minggu {wk} ({d1.strftime('%d %b')} s/d {d2.strftime('%d %b %Y')})"
            elif report_type == 'monthly':
                parts = period_start[:7].split('-')
                return f"{MONTHS_ID[int(parts[1])]} {parts[0]}"
            elif report_type == 'quarterly':
                parts = period_start[:7].split('-')
                m = int(parts[1])
                q = (m - 1) // 3 + 1
                return f"Triwulan {q} {parts[0]}"
            elif report_type == 'annual':
                return f"Tahun {period_start[:4]}"
        except Exception:
            pass
        return f"{period_start} — {period_end}"

    def _create_history(self, report_type, entity_level, entity_id,
                         period_start, period_end):
        with get_write_connection() as conn:
            max_id = conn.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM report_history").fetchone()[0]
            now = datetime.now().isoformat()
            conn.execute("""
                INSERT INTO report_history (id, report_type, period_start, period_end,
                    entity_level, entity_id, generated_at, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')
            """, [max_id, report_type, period_start, period_end,
                  entity_level, entity_id, now])
            return max_id

    def _update_status(self, report_id, status, error_msg=None):
        with get_write_connection() as conn:
            if error_msg:
                conn.execute("UPDATE report_history SET status = ?, error_message = ? WHERE id = ?",
                             [status, error_msg, report_id])
            else:
                conn.execute("UPDATE report_history SET status = ? WHERE id = ?",
                             [status, report_id])

    def _update_completed(self, report_id, entity_name, period_label,
                           pdf_path, excel_path, elapsed, ai_enhanced, data):
        pdf_kb = 0
        excel_kb = 0
        if pdf_path and os.path.exists(pdf_path):
            pdf_kb = os.path.getsize(pdf_path) // 1024
        if excel_path and os.path.exists(excel_path):
            excel_kb = os.path.getsize(excel_path) // 1024

        snapshot = {
            "kpis": data.get("kpis", {}),
            "deltas": data.get("deltas", {}),
            "children_count": len(data.get("children", [])),
        }

        with get_write_connection() as conn:
            conn.execute("""
                UPDATE report_history SET
                    status = 'completed',
                    entity_name = ?,
                    period_label = ?,
                    generation_time_ms = ?,
                    ai_enhanced = ?,
                    pdf_path = ?,
                    pdf_size_kb = ?,
                    excel_path = ?,
                    excel_size_kb = ?,
                    metadata = ?
                WHERE id = ?
            """, [entity_name, period_label, elapsed, ai_enhanced,
                  pdf_path, pdf_kb, excel_path, excel_kb,
                  json.dumps(snapshot), report_id])

import logging
from datetime import datetime
from fastapi import APIRouter, Query
from fastapi.responses import Response
from pydantic import BaseModel
from backend.database import get_connection
from backend.services.dashboard_service import (
    determine_overall_status, generate_overall_narrative,
    _compute_sla_trend, _compute_mttr_trend, _get_entity_history,
    _get_recent_periods, _prev_month, LEVEL_COLS, CHILD_LEVEL_MAP, PARENT_LEVEL,
    determine_entity_status,
)
from backend.services.recommendation_service import RecommendationEngine

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/report-card", tags=["report-card"])
rec_engine = RecommendationEngine()


class ReportCardRequest(BaseModel):
    entity_level: str = "area"
    entity_id: str = ""
    period: str = ""


@router.post("/generate")
async def generate_report_card(req: ReportCardRequest):
    with get_connection() as conn:
        if not req.period:
            try:
                row = conn.execute("SELECT MAX(year_month) FROM summary_monthly").fetchone()
                req.period = row[0] if row and row[0] else "2025-07"
            except Exception:
                req.period = "2025-07"

        col_info = LEVEL_COLS.get(req.entity_level)
        if not col_info:
            return {"error": "Invalid entity_level"}
        col, tbl, name_col = col_info

        try:
            row = conn.execute(f"SELECT {name_col} FROM {tbl} WHERE {col} = ?", [req.entity_id]).fetchone()
            entity_name = row[0] if row else req.entity_id
        except Exception:
            entity_name = req.entity_id

        parent_info = None
        parent_lvl = PARENT_LEVEL.get(req.entity_level)
        if parent_lvl:
            p_col, p_tbl, p_name = LEVEL_COLS[parent_lvl]
            try:
                prow = conn.execute(f"""
                    SELECT m.{p_col}, p.{p_name}
                    FROM {tbl} m LEFT JOIN {p_tbl} p ON m.{p_col} = p.{p_col}
                    WHERE m.{col} = ?
                """, [req.entity_id]).fetchone()
                if prow:
                    parent_info = {"id": prow[0], "name": prow[1], "level": parent_lvl}
            except Exception:
                pass

        where = f"WHERE year_month = ? AND {col} = ?"
        params = [req.period, req.entity_id]

        try:
            agg = conn.execute(f"""
                SELECT SUM(total_tickets), SUM(total_sla_met), AVG(avg_mttr_min),
                       AVG(avg_response_min), SUM(total_escalated), SUM(total_auto_resolved)
                FROM summary_monthly {where}
            """, params).fetchone()
        except Exception:
            agg = None

        vol = agg[0] or 0 if agg else 0
        met = agg[1] or 0 if agg else 0
        mttr = agg[2] or 0 if agg else 0
        response = agg[3] or 0 if agg else 0
        esc = agg[4] or 0 if agg else 0
        auto_r = agg[5] or 0 if agg else 0
        sla_pct = (met / vol * 100) if vol > 0 else 0
        esc_pct = (esc / vol * 100) if vol > 0 else 0
        auto_pct = (auto_r / vol * 100) if vol > 0 else 0

        sla_target = 90.0
        try:
            t_row = conn.execute("SELECT param_value FROM master_threshold WHERE param_key = 'sla_target'").fetchone()
            if t_row:
                sla_target = t_row[0]
        except Exception:
            pass

        prev = _prev_month(req.period)
        try:
            prev_agg = conn.execute(f"""
                SELECT SUM(total_tickets), SUM(total_sla_met), AVG(avg_mttr_min),
                       SUM(total_escalated), SUM(total_auto_resolved)
                FROM summary_monthly WHERE year_month = ? AND {col} = ?
            """, [prev, req.entity_id]).fetchone()
        except Exception:
            prev_agg = None

        p_vol = prev_agg[0] or 0 if prev_agg else 0
        p_met = prev_agg[1] or 0 if prev_agg else 0
        p_mttr = prev_agg[2] or 0 if prev_agg else 0
        p_sla = (p_met / p_vol * 100) if p_vol > 0 else 0
        p_esc_pct = (prev_agg[3] / p_vol * 100) if prev_agg and p_vol > 0 else 0
        p_auto_pct = (prev_agg[4] / p_vol * 100) if prev_agg and p_vol > 0 else 0

        kpis = {
            "sla_pct": round(sla_pct, 1),
            "sla_target": sla_target,
            "total_volume": vol,
            "avg_mttr_min": round(mttr, 0),
            "avg_response_min": round(response, 0),
            "escalation_pct": round(esc_pct, 1),
            "auto_resolve_pct": round(auto_pct, 1),
        }
        kpi_deltas = {
            "sla_mom_pp": round(sla_pct - p_sla, 1),
            "volume_mom_pct": round(((vol - p_vol) / p_vol * 100) if p_vol > 0 else 0, 1),
            "mttr_mom_pct": round(((mttr - p_mttr) / p_mttr * 100) if p_mttr > 0 else 0, 1),
            "esc_mom_pp": round(esc_pct - p_esc_pct, 1),
            "auto_mom_pp": round(auto_pct - p_auto_pct, 1),
        }

        recent = _get_recent_periods(req.period, 3)
        hist = _get_entity_history(conn, col, req.entity_id, recent)
        sla_trend = _compute_sla_trend(hist)
        mttr_trend = _compute_mttr_trend(hist)
        trend = {**sla_trend, **mttr_trend}

        trend_3m = {
            "sla": [{"period": h["period"], "value": h["sla_pct"]} for h in hist],
            "mttr": [{"period": h["period"], "value": h.get("avg_mttr_min", 0)} for h in hist],
            "volume": [{"period": h["period"], "value": h.get("total_tickets", 0)} for h in hist],
        }

        vol_slope = 0
        if len(hist) >= 2:
            from backend.services.predictive_service import linear_regression_slope
            v_vals = [h.get("total_tickets", 0) for h in hist]
            vol_slope = linear_regression_slope(range(len(v_vals)), v_vals)
            vol_pct = (vol_slope / v_vals[0] * 100) if v_vals[0] > 0 else 0
        else:
            vol_pct = 0

        trend_direction = {
            "sla": sla_trend["sla_quality"],
            "mttr": mttr_trend["mttr_quality"],
            "volume": "rising" if vol_pct > 5 else ("falling" if vol_pct < -5 else "stable"),
        }

        children = []
        child_level = CHILD_LEVEL_MAP.get(req.entity_level)
        if child_level and child_level != "site":
            c_col, c_tbl, c_name = LEVEL_COLS.get(child_level, (None, None, None))
            if c_col:
                try:
                    c_rows = conn.execute(f"""
                        SELECT sm.{c_col}, m.{c_name},
                               SUM(sm.total_tickets) as vol,
                               SUM(sm.total_sla_met) as met,
                               AVG(sm.avg_mttr_min) as mttr
                        FROM summary_monthly sm
                        LEFT JOIN {c_tbl} m ON sm.{c_col} = m.{c_col}
                        WHERE sm.year_month = ? AND sm.{col} = ?
                        GROUP BY sm.{c_col}, m.{c_name}
                        ORDER BY SUM(sm.total_sla_met) * 1.0 / NULLIF(SUM(sm.total_tickets), 0) ASC
                    """, [req.period, req.entity_id]).fetchall()
                    for cr in c_rows:
                        c_sla = (cr[3] / cr[2] * 100) if cr[2] and cr[2] > 0 else 0
                        c_hist = _get_entity_history(conn, c_col, cr[0], recent)
                        c_trend = _compute_sla_trend(c_hist)
                        c_status = determine_entity_status(c_sla, sla_target, c_trend["sla_quality"])
                        trend_icon = "📉" if c_trend["sla_quality"] == "worsening" else ("📈" if c_trend["sla_quality"] == "improving" else "─")
                        children.append({
                            "id": cr[0], "name": cr[1] or cr[0],
                            "sla_pct": round(c_sla, 1),
                            "avg_mttr_min": round(cr[4] or 0, 0),
                            "total_volume": cr[2] or 0,
                            "trend_icon": trend_icon,
                            "trend_quality": c_trend["sla_quality"],
                            "status_level": c_status["status"],
                            "status_icon": c_status["icon"],
                            "status_color": c_status["color"],
                        })
                except Exception as e:
                    logger.warning(f"Failed to get children: {e}")

        children_count = len(children)
        site_count = 0
        try:
            if req.entity_level == "to":
                site_count = conn.execute("SELECT COUNT(*) FROM master_site WHERE to_id = ? AND status = 'ACTIVE'", [req.entity_id]).fetchone()[0]
            elif req.entity_level == "area":
                site_count = conn.execute("""
                    SELECT COUNT(*) FROM master_site s
                    JOIN master_to t ON s.to_id = t.to_id
                    JOIN master_nop n ON t.nop_id = n.nop_id
                    JOIN master_regional r ON n.regional_id = r.regional_id
                    WHERE r.area_id = ? AND s.status = 'ACTIVE'
                """, [req.entity_id]).fetchone()[0]
            else:
                site_count = vol
        except Exception:
            site_count = 0

        overall_status = determine_overall_status(
            {**kpis, "volume_mom_pct": kpi_deltas["volume_mom_pct"], "mttr_mom_pct": kpi_deltas["mttr_mom_pct"]},
            trend, None, children
        )
        narrative = generate_overall_narrative(entity_name, overall_status, kpis, trend, children)

        entity_info = {
            "name": entity_name,
            "level": req.entity_level,
            "parent": parent_info,
            "children_count": children_count,
            "site_count": site_count,
        }
        recs_kpis = {**kpis, **kpi_deltas}
        recs = rec_engine.generate(
            {**entity_info, "child_type": CHILD_LEVEL_MAP.get(req.entity_level, "entitas")},
            recs_kpis, trend, None, children, None
        )
        for r in recs:
            r["priority_info"] = {
                "SEGERA": {"label": "SEGERA", "icon": "🔴", "color": "#DC2626"},
                "MINGGU_INI": {"label": "MINGGU INI", "icon": "🟡", "color": "#D97706"},
                "BULAN_INI": {"label": "BULAN INI", "icon": "🔵", "color": "#2563EB"},
                "RUTIN": {"label": "RUTIN", "icon": "🟢", "color": "#16A34A"},
            }.get(r["priority"], {"label": r["priority"], "icon": "⚪", "color": "#6B7280"})

    return {
        "entity": entity_info,
        "overall_status": {**overall_status, "narrative": narrative},
        "kpis": kpis,
        "kpi_deltas": kpi_deltas,
        "trend_3m": trend_3m,
        "trend_direction": trend_direction,
        "children": children,
        "recommendations": recs,
        "generated_at": datetime.now().isoformat(),
    }

import logging
from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional
from backend.database import get_connection
from backend.services.dashboard_service import get_dashboard_data, get_available_periods, get_parent_entities
from backend.services.recommendation_service import RecommendationEngine

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/dashboard", tags=["dashboard"])

rec_engine = RecommendationEngine()


class DashboardRequest(BaseModel):
    period: str = ""
    view_level: str = "area"
    parent_filter: Optional[str] = None


@router.post("/overview")
async def dashboard_overview(req: DashboardRequest):
    with get_connection() as conn:
        if not req.period:
            periods = get_available_periods(conn)
            req.period = periods[0] if periods else "2025-07"

        data = get_dashboard_data(conn, req.period, req.view_level, req.parent_filter)

    entity = {
        "name": "Jaringan NOC-IS",
        "level": req.view_level,
        "child_type": {"area": "Regional", "regional": "NOP", "nop": "TO", "to": "Site"}.get(req.view_level, "entitas"),
    }
    kpis = data["kpi_snapshot"]
    trend = {
        "sla_quality": "neutral",
        "sla_slope": 0,
        "mttr_quality": "neutral",
        "mttr_slope": 0,
    }
    chart_data = data.get("charts", {})
    sla_trend_data = chart_data.get("sla_trend", [])
    if len(sla_trend_data) >= 2:
        sla_vals = [p["value"] for p in sla_trend_data]
        from backend.services.predictive_service import linear_regression_slope
        slope = linear_regression_slope(range(len(sla_vals)), sla_vals)
        trend["sla_slope"] = round(slope, 2)
        trend["sla_quality"] = "worsening" if slope < -0.5 else ("improving" if slope > 0.5 else "neutral")
        consecutive = 0
        for i in range(len(sla_vals) - 1, 0, -1):
            if sla_vals[i] < sla_vals[i-1]:
                consecutive += 1
            else:
                break
        trend["consecutive"] = consecutive

    recs = rec_engine.generate(entity, kpis, trend, None, data["entities"], None)
    for r in recs:
        r["priority_info"] = {
            "SEGERA": {"label": "SEGERA", "icon": "", "color": "#DC2626"},
            "MINGGU_INI": {"label": "MINGGU INI", "icon": "", "color": "#D97706"},
            "BULAN_INI": {"label": "BULAN INI", "icon": "", "color": "#2563EB"},
            "RUTIN": {"label": "RUTIN", "icon": "", "color": "#16A34A"},
        }.get(r["priority"], {"label": r["priority"], "icon": "", "color": "#6B7280"})

    data["recommendations"] = recs
    return data


@router.get("/periods")
async def get_periods():
    with get_connection() as conn:
        periods = get_available_periods(conn)
        return {"periods": periods}


@router.get("/parent-options")
async def get_parent_options(view_level: str = "area"):
    with get_connection() as conn:
        parents = get_parent_entities(conn, view_level)
        return {"parents": parents}

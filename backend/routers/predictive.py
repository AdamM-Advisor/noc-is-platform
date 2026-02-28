import logging
from datetime import datetime, timedelta
from fastapi import APIRouter, Query
from typing import Optional
from backend.database import get_connection, get_write_connection
from backend.services.predictive_service import (
    calculate_risk_score,
    classify_risk,
    interpret_risk_aggregation,
    forecast_volume,
    predict_sla_breach,
    detect_pattern,
    generate_maintenance_schedule,
    get_monthly_volume_data,
    get_monthly_sla_data,
    get_child_sites,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/profiler", tags=["profiler-predictive"])

LEVEL_NAME_TABLE = {
    "area": ("master_area", "area_id", "area_name"),
    "regional": ("master_regional", "regional_id", "regional_name"),
    "nop": ("master_nop", "nop_id", "nop_name"),
    "to": ("master_to", "to_id", "to_name"),
    "site": ("master_site", "site_id", "site_name"),
}

CHILD_LEVEL_MAP = {
    "area": "regional",
    "regional": "nop",
    "nop": "to",
    "to": "site",
}

LEVEL_COL_MAP = {
    "area": "area_id",
    "regional": "regional_id",
    "nop": "nop_id",
    "to": "to_id",
    "site": "site_id",
}


def _get_entity_name(conn, level, entity_id):
    info = LEVEL_NAME_TABLE.get(level)
    if not info:
        return entity_id
    tbl, col, name_col = info
    try:
        row = conn.execute(f"SELECT {name_col} FROM {tbl} WHERE {col} = ?", [entity_id]).fetchone()
        return row[0] if row else entity_id
    except Exception:
        return entity_id


def _batch_compute_risk_scores(conn, sites, date_to):
    results = []
    for site in sites:
        site_id = site["site_id"]
        site_name = site.get("site_name", site_id)
        try:
            risk = calculate_risk_score(conn, site_id, date_to)
            results.append({
                "site_id": site_id,
                "site_name": site_name,
                "risk_score": risk["risk_score"],
                "status": risk["status"],
                "top_component": risk["top_component"],
                "top_component_label": risk.get("top_component_label", ""),
                "components": risk["components"],
            })
        except Exception:
            results.append({
                "site_id": site_id,
                "site_name": site_name,
                "risk_score": 0,
                "status": classify_risk(0),
                "top_component": "frequency",
                "top_component_label": "Frekuensi",
                "components": {},
            })
    return results


def _store_risk_scores(results, date_to):
    try:
        with get_write_connection() as wconn:
            now = datetime.now().isoformat()
            for r in results:
                try:
                    wconn.execute(
                        "DELETE FROM site_risk_scores WHERE site_id = ?",
                        [r["site_id"]]
                    )
                    comps = r.get("components", {})
                    wconn.execute("""
                        INSERT INTO site_risk_scores (
                            site_id, calculated_at, risk_score,
                            frequency_score, recency_score, severity_score,
                            mttr_trend_score, repeat_score, device_score, escalation_score,
                            risk_level, top_component
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, [
                        r["site_id"], now, r["risk_score"],
                        comps.get("frequency", 0), comps.get("recency", 0),
                        comps.get("severity", 0), comps.get("mttr_trend", 0),
                        comps.get("repeat", 0), comps.get("device", 0),
                        comps.get("escalation", 0),
                        r["status"]["level"], r["top_component"],
                    ])
                except Exception as e:
                    logger.warning(f"Failed to store risk score for {r['site_id']}: {e}")
    except Exception as e:
        logger.warning(f"Failed to store risk scores: {e}")


@router.get("/risk-score")
async def get_risk_score(
    site_id: str = Query(...),
    date_to: str = Query(""),
):
    if not date_to:
        date_to = datetime.now().strftime("%Y-%m-%d")

    with get_connection() as conn:
        site_name = _get_entity_name(conn, "site", site_id)
        risk = calculate_risk_score(conn, site_id, date_to)

    return {
        "site_id": site_id,
        "site_name": site_name,
        "date_to": date_to,
        **risk,
    }


@router.get("/risk-aggregation")
async def get_risk_aggregation(
    entity_level: str = Query(...),
    entity_id: str = Query(...),
    date_to: str = Query(""),
    limit: int = Query(10),
):
    if not date_to:
        date_to = datetime.now().strftime("%Y-%m-%d")

    with get_connection() as conn:
        entity_name = _get_entity_name(conn, entity_level, entity_id)
        sites = get_child_sites(conn, entity_level, entity_id)

        if not sites:
            return {
                "entity_level": entity_level,
                "entity_id": entity_id,
                "entity_name": entity_name,
                "total_sites": 0,
                "avg_risk": 0,
                "distribution": {"high": 0, "medium": 0, "low": 0},
                "top_sites": [],
                "narrative": "Tidak ada site ditemukan.",
                "chart_data": [],
            }

        risk_results = _batch_compute_risk_scores(conn, sites, date_to)

    _store_risk_scores(risk_results, date_to)

    n_high = sum(1 for r in risk_results if r["risk_score"] >= 70)
    n_medium = sum(1 for r in risk_results if 40 <= r["risk_score"] < 70)
    n_low = sum(1 for r in risk_results if r["risk_score"] < 40)
    total = len(risk_results)
    avg_risk = sum(r["risk_score"] for r in risk_results) / total if total > 0 else 0

    sorted_by_risk = sorted(risk_results, key=lambda r: -r["risk_score"])
    top_sites = sorted_by_risk[:limit]

    worst = None
    if sorted_by_risk:
        worst = {
            "name": sorted_by_risk[0]["site_name"],
            "score": sorted_by_risk[0]["risk_score"],
        }

    interp = interpret_risk_aggregation(n_high, n_medium, n_low, total, worst)

    chart_data = [
        {"level": "HIGH", "count": n_high, "color": "#DC2626"},
        {"level": "MEDIUM", "count": n_medium, "color": "#D97706"},
        {"level": "LOW", "count": n_low, "color": "#16A34A"},
    ]

    return {
        "entity_level": entity_level,
        "entity_id": entity_id,
        "entity_name": entity_name,
        "total_sites": total,
        "avg_risk": round(avg_risk, 1),
        "distribution": {"high": n_high, "medium": n_medium, "low": n_low},
        "pct_high": round(n_high / total * 100, 1) if total > 0 else 0,
        "worst_site": worst,
        "top_sites": top_sites,
        "narrative": interp["narrative"],
        "chart_data": chart_data,
    }


@router.get("/forecast")
async def get_forecast(
    entity_level: str = Query(...),
    entity_id: str = Query(...),
    date_from: str = Query(""),
    date_to: str = Query(""),
    horizon: int = Query(3),
):
    with get_connection() as conn:
        entity_name = _get_entity_name(conn, entity_level, entity_id)
        monthly_data = get_monthly_volume_data(conn, entity_level, entity_id, date_from, date_to)

    if not monthly_data or len(monthly_data) < 3:
        return {
            "entity_level": entity_level,
            "entity_id": entity_id,
            "entity_name": entity_name,
            "error": "Minimal 3 bulan data untuk forecast",
            "historical": [],
            "forecasts": [],
        }

    result = forecast_volume(monthly_data, horizon)

    capacity_alert = None

    return {
        "entity_level": entity_level,
        "entity_id": entity_id,
        "entity_name": entity_name,
        "capacity_alert": capacity_alert,
        **result,
    }


@router.get("/sla-breach")
async def get_sla_breach(
    entity_level: str = Query(...),
    entity_id: str = Query(...),
    date_from: str = Query(""),
    date_to: str = Query(""),
    target: float = Query(90.0),
    horizon_weeks: int = Query(8),
):
    with get_connection() as conn:
        entity_name = _get_entity_name(conn, entity_level, entity_id)
        sla_data = get_monthly_sla_data(conn, entity_level, entity_id, date_from, date_to)

        children_breach = []
        child_level = CHILD_LEVEL_MAP.get(entity_level)
        if child_level and entity_level != "site":
            child_col = LEVEL_COL_MAP.get(child_level)
            parent_col = LEVEL_COL_MAP.get(entity_level)
            if child_col and parent_col:
                try:
                    if child_level == "site":
                        child_sites = get_child_sites(conn, entity_level, entity_id)
                        for cs in child_sites[:50]:
                            child_sla = get_monthly_sla_data(conn, "site", cs["site_id"], date_from, date_to)
                            if len(child_sla) >= 3:
                                child_result = predict_sla_breach(child_sla, target, horizon_weeks)
                                if child_result.get("status") in ("breach_predicted", "already_breached"):
                                    proj_4w = child_result.get("current_sla", 0) + child_result.get("slope_per_period", 0) * 4
                                    bw = child_result.get("breach_week")
                                    children_breach.append({
                                        "entity_id": cs["site_id"],
                                        "entity_name": cs.get("site_name", cs["site_id"]),
                                        "current_sla": child_result.get("current_sla", 0),
                                        "projected_4w": round(proj_4w, 2),
                                        "breach_week": bw,
                                        "breach_in": bw,
                                        "status": child_result["status"],
                                        "slope": child_result.get("slope_per_period", 0),
                                    })
                except Exception:
                    pass

            children_breach.sort(
                key=lambda c: (0 if c["status"] == "already_breached" else 1, c.get("breach_week") or 999)
            )

    children_breach_narrative = None
    if children_breach:
        n_breached = sum(1 for c in children_breach if c["status"] == "already_breached")
        n_predicted = sum(1 for c in children_breach if c["status"] == "breach_predicted")
        parts = []
        if n_breached:
            parts.append(f" {n_breached} entitas sudah breach")
        if n_predicted:
            parts.append(f" {n_predicted} entitas diprediksi breach")
        children_breach_narrative = ". ".join(parts) + "." if parts else None

    if not sla_data or len(sla_data) < 3:
        return {
            "entity_level": entity_level,
            "entity_id": entity_id,
            "entity_name": entity_name,
            "error": "Minimal 3 periode data SLA",
            "children_breach": children_breach,
            "children_breach_narrative": children_breach_narrative,
        }

    result = predict_sla_breach(sla_data, target, horizon_weeks)

    return {
        "entity_level": entity_level,
        "entity_id": entity_id,
        "entity_name": entity_name,
        "children_breach": children_breach,
        "children_breach_narrative": children_breach_narrative,
        **result,
    }


@router.get("/pattern")
async def get_pattern(
    site_id: str = Query(...),
    date_from: str = Query(""),
    date_to: str = Query(""),
):
    if not date_from:
        date_from = (datetime.now().replace(day=1) - timedelta(days=365)).strftime("%Y-%m-%d")
    if not date_to:
        date_to = datetime.now().strftime("%Y-%m-%d")

    with get_connection() as conn:
        site_name = _get_entity_name(conn, "site", site_id)
        result = detect_pattern(conn, site_id, date_from, date_to)

    return {
        "site_id": site_id,
        "site_name": site_name,
        **result,
    }


@router.get("/pattern-batch")
async def get_pattern_batch(
    entity_level: str = Query(...),
    entity_id: str = Query(...),
    date_from: str = Query(""),
    date_to: str = Query(""),
    limit: int = Query(50),
):
    if not date_from:
        date_from = (datetime.now().replace(day=1) - timedelta(days=365)).strftime("%Y-%m-%d")
    if not date_to:
        date_to = datetime.now().strftime("%Y-%m-%d")

    with get_connection() as conn:
        entity_name = _get_entity_name(conn, entity_level, entity_id)
        sites = get_child_sites(conn, entity_level, entity_id)

        scatter_data = []
        for site in sites[:limit]:
            site_id = site["site_id"]
            pat = detect_pattern(conn, site_id, date_from, date_to)
            if pat.get("avg_gap_days") is not None:
                risk = calculate_risk_score(conn, site_id, date_to)
                try:
                    tc_row = conn.execute(
                        "SELECT COUNT(*) FROM noc_tickets WHERE site_id = ? AND occured_time >= ? AND occured_time <= ?",
                        [site_id, date_from, date_to]
                    ).fetchone()
                    ticket_count = tc_row[0] if tc_row else 0
                except Exception:
                    ticket_count = pat.get("n_intervals", 0) + 1
                scatter_data.append({
                    "site_id": site_id,
                    "site_name": site.get("site_name", site_id),
                    "avg_gap_days": pat["avg_gap_days"],
                    "cv": pat.get("cv", 1),
                    "pattern_detected": pat.get("pattern_detected", False),
                    "pattern": "konsisten" if pat.get("pattern_detected") else "acak",
                    "risk_score": risk["risk_score"],
                    "ticket_count": ticket_count,
                    "n_intervals": pat.get("n_intervals", 0),
                    "predicted_next": pat.get("predicted_next"),
                    "days_until_next": pat.get("days_until_next"),
                })

    scatter_data.sort(key=lambda s: -s["risk_score"])

    n_pattern = sum(1 for s in scatter_data if s["pattern_detected"])
    consistent_sites = [
        s for s in scatter_data
        if s["pattern_detected"] and s.get("cv", 1) < 0.4
    ]
    consistent_sites.sort(key=lambda s: s.get("cv", 1))

    narrative = (
        f"{n_pattern} dari {len(scatter_data)} site memiliki pola gangguan terdeteksi. "
        f"Site dengan pola reguler dan risk tinggi perlu prioritas PM."
    ) if scatter_data else "Tidak ada data pattern."

    return {
        "entity_level": entity_level,
        "entity_id": entity_id,
        "entity_name": entity_name,
        "scatter_data": scatter_data,
        "consistent_sites": consistent_sites,
        "total_sites": len(scatter_data),
        "n_pattern_detected": n_pattern,
        "narrative": narrative,
    }


@router.get("/maintenance-calendar")
async def get_maintenance_calendar(
    entity_level: str = Query(...),
    entity_id: str = Query(...),
    date_to: str = Query(""),
    target_month: Optional[int] = Query(None),
    target_year: Optional[int] = Query(None),
):
    if not date_to:
        date_to = datetime.now().strftime("%Y-%m-%d")

    if target_month is None:
        now = datetime.now()
        next_month = now.month + 1 if now.month < 12 else 1
        target_month = next_month
        if target_year is None:
            target_year = now.year if now.month < 12 else now.year + 1
    elif target_year is None:
        target_year = datetime.now().year

    with get_connection() as conn:
        entity_name = _get_entity_name(conn, entity_level, entity_id)
        sites = get_child_sites(conn, entity_level, entity_id)

        if entity_level == "site":
            sites = [{"site_id": entity_id, "site_name": entity_name, "equipment_age_years": 0}]
            try:
                row = conn.execute(
                    "SELECT equipment_age_years FROM master_site WHERE site_id = ?",
                    [entity_id]
                ).fetchone()
                if row and row[0]:
                    sites[0]["equipment_age_years"] = float(row[0])
            except Exception:
                pass

        items = []
        date_from_pattern = (datetime.now().replace(day=1) - timedelta(days=365)).strftime("%Y-%m-%d")

        for site in sites[:100]:
            site_id = site["site_id"]
            risk = calculate_risk_score(conn, site_id, date_to)
            pat = detect_pattern(conn, site_id, date_from_pattern, date_to)

            items.append({
                "site_id": site_id,
                "site_name": site.get("site_name", site_id),
                "risk_score": risk["risk_score"],
                "pattern": pat,
                "device_age": site.get("equipment_age_years", 0),
                "target_month": target_month,
                "target_year": target_year,
            })

    result = generate_maintenance_schedule(items)

    return {
        "entity_level": entity_level,
        "entity_id": entity_id,
        "entity_name": entity_name,
        "target_month": target_month,
        "target_year": target_year,
        **result,
    }

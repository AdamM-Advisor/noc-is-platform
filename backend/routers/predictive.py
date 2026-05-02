import logging
from datetime import datetime, timedelta
from fastapi import APIRouter, BackgroundTasks, Query
from typing import Optional
from backend.database import get_connection, get_write_connection
from backend.services.predictive_service import (
    calculate_risk_score,
    batch_calculate_risk_scores,
    classify_risk,
    interpret_risk_aggregation,
    forecast_volume,
    predict_sla_breach,
    detect_pattern,
    batch_detect_patterns,
    generate_maintenance_schedule,
    get_monthly_volume_data,
    get_monthly_sla_data,
    get_child_sites,
)
from backend.services.operational_catalog_service import create_job
from backend.services.sarimax_service import SarimaxRunConfig, latest_sarimax_forecast, run_sarimax_volume_forecast

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


def _batch_compute_risk_scores(conn, sites, date_to, entity_level=None, entity_id=None):
    site_ids = [s["site_id"] for s in sites]
    name_map = {s["site_id"]: s.get("site_name", s["site_id"]) for s in sites}

    batch_results = batch_calculate_risk_scores(conn, site_ids, date_to, entity_level=entity_level, entity_id=entity_id)

    results = []
    for sid in site_ids:
        risk = batch_results.get(sid)
        if risk:
            results.append({
                "site_id": sid,
                "site_name": name_map[sid],
                "risk_score": risk["risk_score"],
                "status": risk["status"],
                "top_component": risk["top_component"],
                "top_component_label": risk.get("top_component_label", ""),
                "components": risk["components"],
            })
        else:
            results.append({
                "site_id": sid,
                "site_name": name_map[sid],
                "risk_score": 0,
                "status": classify_risk(0),
                "top_component": "frequency",
                "top_component_label": "Frekuensi",
                "components": {},
            })
    return results


def _store_risk_scores(results, date_to):
    if not results:
        return
    try:
        with get_write_connection() as wconn:
            now = datetime.now().isoformat()
            seen = set()
            unique_results = []
            for r in results:
                if r["site_id"] not in seen:
                    seen.add(r["site_id"])
                    unique_results.append(r)
            results = unique_results
            site_ids = [r["site_id"].replace("'", "''") for r in results]
            id_list = ",".join([f"'{sid}'" for sid in site_ids])
            wconn.execute(f"DELETE FROM site_risk_scores WHERE site_id IN ({id_list})")

            values = []
            for r in results:
                comps = r.get("components", {})
                sid = r["site_id"].replace("'", "''")
                level = r["status"]["level"].replace("'", "''")
                tc = r["top_component"].replace("'", "''")
                values.append(f"""('{sid}', '{now}', {r["risk_score"]},
                    {comps.get("frequency", 0)}, {comps.get("recency", 0)},
                    {comps.get("severity", 0)}, {comps.get("mttr_trend", 0)},
                    {comps.get("repeat", 0)}, {comps.get("device", 0)},
                    {comps.get("escalation", 0)},
                    '{level}', '{tc}')""")

            batch_size = 500
            for i in range(0, len(values), batch_size):
                chunk = values[i:i+batch_size]
                wconn.execute(f"""
                    INSERT INTO site_risk_scores (
                        site_id, calculated_at, risk_score,
                        frequency_score, recency_score, severity_score,
                        mttr_trend_score, repeat_score, device_score, escalation_score,
                        risk_level, top_component
                    ) VALUES {','.join(chunk)}
                """)
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

        risk_results = _batch_compute_risk_scores(conn, sites, date_to, entity_level=entity_level, entity_id=entity_id)

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

    cached_sarimax = latest_sarimax_forecast(
        entity_level,
        entity_id,
        _year_month_or_none(date_from),
        _year_month_or_none(date_to),
    )
    if cached_sarimax:
        return {
            "entity_level": entity_level,
            "entity_id": entity_id,
            "entity_name": entity_name,
            "capacity_alert": None,
            "cached": True,
            "cache_source": "model_run_catalog",
            **cached_sarimax,
        }

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
        "cached": False,
        **result,
    }


def _year_month_or_none(value: str) -> str | None:
    if not value or len(value) < 7:
        return None
    candidate = value[:7]
    if len(candidate) == 7 and candidate[4] == "-":
        return candidate
    return None


@router.post("/forecast/sarimax/run")
async def run_sarimax_forecast_job(
    background_tasks: BackgroundTasks,
    entity_level: str = Query("site"),
    entity_id: Optional[str] = Query(None),
    window_start: str = Query(...),
    window_end: str = Query(...),
    horizon: int = Query(3),
    limit: int = Query(100),
    min_points: int = Query(6),
):
    job = create_job(
        "sarimax_forecast",
        payload={
            "entity_level": entity_level,
            "entity_id": entity_id,
            "window_start": window_start,
            "window_end": window_end,
            "horizon": horizon,
            "limit": limit,
            "min_points": min_points,
        },
        source="sarimax",
    )
    background_tasks.add_task(
        run_sarimax_volume_forecast,
        SarimaxRunConfig(
            entity_level=entity_level,
            entity_id=entity_id,
            window_start=window_start,
            window_end=window_end,
            horizon=horizon,
            limit=limit,
            min_points=min_points,
            job_id=job["job_id"],
        ),
    )
    return {"status": "started", "job_id": job["job_id"]}


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
        limited_sites = sites[:limit]

        site_ids = [s["site_id"] for s in limited_sites]
        name_map = {s["site_id"]: s.get("site_name", s["site_id"]) for s in limited_sites}

        batch_risks = batch_calculate_risk_scores(conn, site_ids, date_to, entity_level=entity_level, entity_id=entity_id)
        batch_patterns = batch_detect_patterns(conn, site_ids, date_from, date_to)

        safe_ids = [sid.replace("'", "''") for sid in site_ids]
        id_list_sql = ",".join([f"'{sid}'" for sid in safe_ids])
        ticket_counts = {}
        try:
            tc_rows = conn.execute(f"""
                SELECT site_id, COUNT(*) FROM noc_tickets
                WHERE site_id IN ({id_list_sql})
                  AND occured_time >= ? AND occured_time <= ?
                GROUP BY site_id
            """, [date_from, date_to]).fetchall()
            ticket_counts = {r[0]: r[1] for r in tc_rows}
        except Exception:
            pass

        scatter_data = []
        for site in limited_sites:
            site_id = site["site_id"]
            pat = batch_patterns.get(site_id, {"pattern_detected": False})
            if pat.get("avg_gap_days") is not None:
                risk = batch_risks.get(site_id, {"risk_score": 0})
                scatter_data.append({
                    "site_id": site_id,
                    "site_name": name_map[site_id],
                    "avg_gap_days": pat["avg_gap_days"],
                    "cv": pat.get("cv", 1),
                    "pattern_detected": pat.get("pattern_detected", False),
                    "pattern": "konsisten" if pat.get("pattern_detected") else "acak",
                    "risk_score": risk["risk_score"],
                    "ticket_count": ticket_counts.get(site_id, pat.get("n_intervals", 0) + 1),
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
        limited_sites = sites[:100]

        site_ids = [s["site_id"] for s in limited_sites]
        batch_risks = batch_calculate_risk_scores(conn, site_ids, date_to, entity_level=entity_level, entity_id=entity_id)
        batch_patterns = batch_detect_patterns(conn, site_ids, date_from_pattern, date_to)

        for site in limited_sites:
            site_id = site["site_id"]
            risk = batch_risks.get(site_id, {"risk_score": 0})
            pat = batch_patterns.get(site_id, {"pattern_detected": False})

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

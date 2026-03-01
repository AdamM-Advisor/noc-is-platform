import math
from statistics import mean as stat_mean, stdev as stat_stdev
from fastapi import APIRouter, Query
from pydantic import BaseModel
from typing import Optional, List
from backend.database import get_connection
from backend.services.behavior_service import (
    get_behavior_with_meta, interpret_sla, interpret_mttr,
    interpret_escalation, interpret_auto_resolve, interpret_repeat,
    interpret_volume, linear_slope, generate_summary_narrative,
    generate_recommendations
)

router = APIRouter(prefix="/profiler", tags=["profiler"])

CHILD_LEVEL_MAP = {
    "area": "regional",
    "regional": "nop",
    "nop": "to",
    "to": "site",
}

LEVEL_COL_MAP = {
    "area": ("calc_area_id", "area_id"),
    "regional": ("calc_regional_id", "regional_id"),
    "nop": ("calc_nop_id", "nop_id"),
    "to": ("calc_to_id", "to_id"),
    "site": ("site_id", "site_id"),
}

LEVEL_NAME_TABLE = {
    "area": ("master_area", "area_id", "area_name"),
    "regional": ("master_regional", "regional_id", "regional_name"),
    "nop": ("master_nop", "nop_id", "nop_name"),
    "to": ("master_to", "to_id", "to_name"),
    "site": ("master_site", "site_id", "site_name"),
}

PARENT_CHAIN = {
    "regional": [("area", "area_id", "master_area", "area_id", "area_name")],
    "nop": [
        ("regional", "regional_id", "master_regional", "regional_id", "regional_name"),
        ("area", "area_id", "master_regional", "regional_id", "regional_name"),
    ],
    "to": [
        ("nop", "nop_id", "master_nop", "nop_id", "nop_name"),
    ],
    "site": [
        ("to", "to_id", "master_to", "to_id", "to_name"),
    ],
}

GRANULARITY_LABELS = {
    "monthly": "bulan",
    "weekly": "minggu",
    "daily": "hari",
    "hourly": "jam",
    "quarterly": "triwulan",
}


class ProfileRequest(BaseModel):
    entity_level: str
    entity_id: str
    granularity: str = "monthly"
    date_from: str = ""
    date_to: str = ""
    type_ticket: str = ""
    severities: List[str] = []
    fault_level: str = ""
    rc_category: str = ""


def _build_filters(req, prefix=""):
    conditions = []
    params = []
    p = prefix

    if req.date_from:
        conditions.append(f"{p}year_month >= ?")
        params.append(req.date_from)
    if req.date_to:
        conditions.append(f"{p}year_month <= ?")
        params.append(req.date_to)
    if req.type_ticket:
        conditions.append(f"{p}type_ticket = ?")
        params.append(req.type_ticket)
    if req.severities and len(req.severities) > 0:
        placeholders = ",".join(["?" for _ in req.severities])
        conditions.append(f"{p}severity IN ({placeholders})")
        params.extend(req.severities)
    if req.fault_level:
        conditions.append(f"{p}fault_level = ?")
        params.append(req.fault_level)
    return conditions, params


def _get_entity_name(conn, level, entity_id):
    info = LEVEL_NAME_TABLE.get(level)
    if not info:
        return entity_id
    table, id_col, name_col = info
    row = conn.execute(f"SELECT {name_col} FROM {table} WHERE {id_col} = ?", [entity_id]).fetchone()
    return row[0] if row else entity_id


def _get_parent_chain(conn, level, entity_id):
    chain = []
    current_level = level
    current_id = entity_id

    while True:
        name = _get_entity_name(conn, current_level, current_id)
        chain.insert(0, {"level": current_level, "id": current_id, "name": name})

        info = LEVEL_NAME_TABLE.get(current_level)
        if not info:
            break

        table, id_col, _ = info

        if current_level == "regional":
            row = conn.execute(f"SELECT area_id FROM master_regional WHERE regional_id = ?", [current_id]).fetchone()
            if row:
                current_level = "area"
                current_id = row[0]
            else:
                break
        elif current_level == "nop":
            row = conn.execute(f"SELECT regional_id FROM master_nop WHERE nop_id = ?", [current_id]).fetchone()
            if row:
                current_level = "regional"
                current_id = row[0]
            else:
                break
        elif current_level == "to":
            row = conn.execute(f"SELECT nop_id FROM master_to WHERE to_id = ?", [current_id]).fetchone()
            if row:
                current_level = "nop"
                current_id = row[0]
            else:
                break
        elif current_level == "site":
            row = conn.execute(f"SELECT to_id FROM master_site WHERE site_id = ?", [current_id]).fetchone()
            if row:
                current_level = "to"
                current_id = row[0]
            else:
                break
        else:
            break

    return chain


def _get_child_counts(conn, level, entity_id):
    counts = {}
    if level == "area":
        counts["regional"] = conn.execute("SELECT COUNT(*) FROM master_regional WHERE area_id = ? AND status = 'ACTIVE'", [entity_id]).fetchone()[0]
        counts["nop"] = conn.execute("SELECT COUNT(*) FROM master_nop WHERE regional_id IN (SELECT regional_id FROM master_regional WHERE area_id = ? AND status = 'ACTIVE') AND status = 'ACTIVE'", [entity_id]).fetchone()[0]
        counts["to"] = conn.execute("SELECT COUNT(*) FROM master_to WHERE nop_id IN (SELECT nop_id FROM master_nop WHERE regional_id IN (SELECT regional_id FROM master_regional WHERE area_id = ?)) AND status = 'ACTIVE'", [entity_id]).fetchone()[0]
        counts["site"] = conn.execute("SELECT COUNT(*) FROM master_site WHERE to_id IN (SELECT to_id FROM master_to WHERE nop_id IN (SELECT nop_id FROM master_nop WHERE regional_id IN (SELECT regional_id FROM master_regional WHERE area_id = ?))) AND status = 'ACTIVE'", [entity_id]).fetchone()[0]
    elif level == "regional":
        counts["nop"] = conn.execute("SELECT COUNT(*) FROM master_nop WHERE regional_id = ? AND status = 'ACTIVE'", [entity_id]).fetchone()[0]
        counts["to"] = conn.execute("SELECT COUNT(*) FROM master_to WHERE nop_id IN (SELECT nop_id FROM master_nop WHERE regional_id = ?) AND status = 'ACTIVE'", [entity_id]).fetchone()[0]
        counts["site"] = conn.execute("SELECT COUNT(*) FROM master_site WHERE to_id IN (SELECT to_id FROM master_to WHERE nop_id IN (SELECT nop_id FROM master_nop WHERE regional_id = ?)) AND status = 'ACTIVE'", [entity_id]).fetchone()[0]
    elif level == "nop":
        counts["to"] = conn.execute("SELECT COUNT(*) FROM master_to WHERE nop_id = ? AND status = 'ACTIVE'", [entity_id]).fetchone()[0]
        counts["site"] = conn.execute("SELECT COUNT(*) FROM master_site WHERE to_id IN (SELECT to_id FROM master_to WHERE nop_id = ?) AND status = 'ACTIVE'", [entity_id]).fetchone()[0]
    elif level == "to":
        counts["site"] = conn.execute("SELECT COUNT(*) FROM master_site WHERE to_id = ? AND status = 'ACTIVE'", [entity_id]).fetchone()[0]

    return counts


def _get_site_composition(conn, level, entity_id):
    if level == "site":
        row = conn.execute("SELECT site_class FROM master_site WHERE site_id = ?", [entity_id]).fetchone()
        return {row[0] if row else "Unknown": 1} if row else {}

    if level == "to":
        where = "to_id = ?"
    elif level == "nop":
        where = "to_id IN (SELECT to_id FROM master_to WHERE nop_id = ?)"
    elif level == "regional":
        where = "to_id IN (SELECT to_id FROM master_to WHERE nop_id IN (SELECT nop_id FROM master_nop WHERE regional_id = ?))"
    elif level == "area":
        where = "to_id IN (SELECT to_id FROM master_to WHERE nop_id IN (SELECT nop_id FROM master_nop WHERE regional_id IN (SELECT regional_id FROM master_regional WHERE area_id = ?)))"
    else:
        return {}

    rows = conn.execute(f"SELECT COALESCE(site_class, 'Unknown') as cls, COUNT(*) FROM master_site WHERE {where} AND status = 'ACTIVE' GROUP BY cls ORDER BY COUNT(*) DESC", [entity_id]).fetchall()
    return {r[0]: r[1] for r in rows}


def _get_entity_col(level):
    return LEVEL_COL_MAP.get(level, ("site_id", "site_id"))


def _aggregate_kpis(conn, level, entity_id, filters_cond, filters_params):
    summary_col, _ = _get_entity_col(level)
    sm_col = summary_col.replace("calc_", "") if summary_col.startswith("calc_") else summary_col

    where = [f"{sm_col} = ?"]
    params = [entity_id]
    where.extend(filters_cond)
    params.extend(filters_params)

    where_str = " AND ".join(where) if where else "1=1"

    row = conn.execute(f"""
        SELECT 
            COALESCE(SUM(total_tickets), 0) as total_tickets,
            COALESCE(SUM(total_sla_met), 0) as total_sla_met,
            CASE WHEN SUM(total_tickets) > 0 THEN SUM(total_sla_met) * 100.0 / SUM(total_tickets) ELSE 0 END as sla_pct,
            COALESCE(AVG(avg_mttr_min), 0) as avg_mttr_min,
            COALESCE(SUM(total_escalated), 0) as total_escalated,
            CASE WHEN SUM(total_tickets) > 0 THEN SUM(total_escalated) * 100.0 / SUM(total_tickets) ELSE 0 END as escalation_pct,
            COALESCE(SUM(total_auto_resolved), 0) as total_auto_resolved,
            CASE WHEN SUM(total_tickets) > 0 THEN SUM(total_auto_resolved) * 100.0 / SUM(total_tickets) ELSE 0 END as auto_resolve_pct,
            COALESCE(SUM(total_repeat), 0) as total_repeat,
            CASE WHEN SUM(total_tickets) > 0 THEN SUM(total_repeat) * 100.0 / SUM(total_tickets) ELSE 0 END as repeat_pct,
            COUNT(DISTINCT year_month) as month_count
        FROM summary_monthly
        WHERE {where_str}
    """, params).fetchone()

    total = row[0] or 0
    month_count = row[10] or 1

    months_data = conn.execute(f"""
        SELECT year_month,
            SUM(total_tickets) as total_tickets,
            CASE WHEN SUM(total_tickets) > 0 THEN SUM(total_sla_met) * 100.0 / SUM(total_tickets) ELSE 0 END as sla_pct,
            AVG(avg_mttr_min) as avg_mttr_min,
            SUM(count_critical) as count_critical,
            SUM(count_major) as count_major,
            SUM(count_minor) as count_minor,
            SUM(count_low) as count_low
        FROM summary_monthly
        WHERE {where_str}
        GROUP BY year_month
        ORDER BY year_month
    """, params).fetchall()

    months_list = []
    for m in months_data:
        months_list.append({
            "year_month": m[0],
            "total_tickets": m[1] or 0,
            "sla_pct": m[2] or 0,
            "avg_mttr_min": m[3] or 0,
            "count_critical": m[4] or 0,
            "count_major": m[5] or 0,
            "count_minor": m[6] or 0,
            "count_low": m[7] or 0,
        })

    sla_values = [m["sla_pct"] for m in months_list]
    sla_slope = linear_slope(sla_values) if len(sla_values) >= 2 else 0

    vol_values = [m["total_tickets"] for m in months_list]
    mom_change = 0
    if len(vol_values) >= 2 and vol_values[-2] > 0:
        mom_change = ((vol_values[-1] - vol_values[-2]) / vol_values[-2]) * 100

    return {
        "total_tickets": total,
        "avg_volume": total / max(month_count, 1),
        "sla_pct": row[2] or 0,
        "sla_target": 90.0,
        "avg_mttr_min": row[3] or 0,
        "escalation_pct": row[5] or 0,
        "auto_resolve_pct": row[7] or 0,
        "repeat_pct": row[9] or 0,
        "sla_trend_slope": sla_slope,
        "sla_trend_months": len(months_list),
        "volume_mom_change": mom_change,
        "month_count": month_count,
    }, months_list


@router.post("/generate")
async def generate_profile(req: ProfileRequest):
    filters_cond, filters_params = _build_filters(req)

    with get_connection() as conn:
        entity_name = _get_entity_name(conn, req.entity_level, req.entity_id)
        parent_chain = _get_parent_chain(conn, req.entity_level, req.entity_id)
        child_counts = _get_child_counts(conn, req.entity_level, req.entity_id)
        site_composition = _get_site_composition(conn, req.entity_level, req.entity_id)

        kpis, months_list = _aggregate_kpis(conn, req.entity_level, req.entity_id, filters_cond, filters_params)

        behavior = get_behavior_with_meta(req.entity_level, months_list)

        sla_interp = interpret_sla(kpis["sla_pct"], kpis["sla_target"])
        mttr_interp = interpret_mttr(kpis["avg_mttr_min"])
        esc_interp = interpret_escalation(kpis["escalation_pct"])
        ar_interp = interpret_auto_resolve(kpis["auto_resolve_pct"])
        rep_interp = interpret_repeat(kpis["repeat_pct"])
        vol_interp = interpret_volume(kpis["volume_mom_change"])

        child_level = CHILD_LEVEL_MAP.get(req.entity_level)
        children_summary = None
        if child_level:
            children_summary = _compute_children_summary(conn, req.entity_level, req.entity_id, child_level, filters_cond, filters_params)

        gran_label = GRANULARITY_LABELS.get(req.granularity, "bulan")
        narrative = generate_summary_narrative(entity_name, req.entity_level, kpis, behavior, children_summary, gran_label)
        recommendations = generate_recommendations(kpis, behavior, children_summary)

        overall_status = "SEHAT"
        if behavior["label"] in ("CHRONIC", "DETERIORATING"):
            overall_status = "PERLU PERHATIAN"
        elif behavior["label"] in ("SPORADIC", "SEASONAL"):
            overall_status = "MONITORING"

    return {
        "identity": {
            "level": req.entity_level,
            "id": req.entity_id,
            "name": entity_name,
            "parent_chain": parent_chain,
            "child_counts": child_counts,
            "site_composition": site_composition,
        },
        "kpis": {
            "volume": {"value": kpis["total_tickets"], "avg": kpis["avg_volume"], "mom_change": kpis["volume_mom_change"], "interpretation": vol_interp},
            "sla_pct": {"value": kpis["sla_pct"], "target": kpis["sla_target"], "interpretation": sla_interp},
            "avg_mttr_min": {"value": kpis["avg_mttr_min"], "interpretation": mttr_interp},
            "escalation_pct": {"value": kpis["escalation_pct"], "interpretation": esc_interp},
            "auto_resolve_pct": {"value": kpis["auto_resolve_pct"], "interpretation": ar_interp},
            "repeat_pct": {"value": kpis["repeat_pct"], "interpretation": rep_interp},
        },
        "behavior": behavior,
        "overall_status": overall_status,
        "child_composition": children_summary,
        "summary_narrative": narrative,
        "recommendations": recommendations,
        "months_data": months_list,
    }


def _compute_children_summary(conn, parent_level, parent_id, child_level, filters_cond, filters_params):
    child_col_sm = LEVEL_COL_MAP[child_level][1]
    parent_col_sm = LEVEL_COL_MAP[parent_level][1]

    where = [f"{parent_col_sm} = ?"]
    params = [parent_id]
    where.extend(filters_cond)
    params.extend(filters_params)
    where_str = " AND ".join(where)

    rows = conn.execute(f"""
        SELECT {child_col_sm},
            SUM(total_tickets) as total_tickets,
            CASE WHEN SUM(total_tickets) > 0 THEN SUM(total_sla_met) * 100.0 / SUM(total_tickets) ELSE 0 END as sla_pct
        FROM summary_monthly
        WHERE {where_str}
        GROUP BY {child_col_sm}
        ORDER BY total_tickets DESC
    """, params).fetchall()

    total_all_tickets = sum(r[1] or 0 for r in rows)
    by_behavior = {}
    worst = None
    worst_sla = 999

    for r in rows:
        child_id = r[0]
        if not child_id:
            continue
        child_months = conn.execute(f"""
            SELECT year_month,
                SUM(total_tickets) as total_tickets,
                CASE WHEN SUM(total_tickets) > 0 THEN SUM(total_sla_met) * 100.0 / SUM(total_tickets) ELSE 0 END as sla_pct,
                SUM(count_critical) as count_critical,
                SUM(count_major) as count_major
            FROM summary_monthly
            WHERE {where_str} AND {child_col_sm} = ?
            GROUP BY year_month ORDER BY year_month
        """, params + [child_id]).fetchall()

        child_months_list = [
            {"total_tickets": m[1] or 0, "sla_pct": m[2] or 0, "count_critical": m[3] or 0, "count_major": m[4] or 0}
            for m in child_months
        ]

        beh = get_behavior_with_meta(child_level, child_months_list)
        label = beh["label"]
        by_behavior[label] = by_behavior.get(label, 0) + 1

        child_sla = r[2] or 0
        child_tickets = r[1] or 0
        if child_sla < worst_sla:
            worst_sla = child_sla
            child_name = _get_entity_name(conn, child_level, child_id)
            contribution = (child_tickets / total_all_tickets * 100) if total_all_tickets > 0 else 0
            worst = {
                "id": child_id,
                "name": child_name,
                "behavior": label,
                "contribution_pct": contribution,
                "chronic_count": 1,
            }

    type_labels = {"regional": "Regional", "nop": "NOP", "to": "TO", "site": "Site"}

    return {
        "total": len(rows),
        "type_label": type_labels.get(child_level, child_level),
        "by_behavior": by_behavior,
        "worst": worst,
    }


@router.get("/children")
async def get_children(
    entity_level: str = Query(...),
    entity_id: str = Query(...),
    date_from: str = Query(""),
    date_to: str = Query(""),
    type_ticket: str = Query(""),
    severities: str = Query(""),
    fault_level: str = Query(""),
    rc_category: str = Query(""),
    sort: str = Query("sla_pct"),
    order: str = Query("asc"),
    page: int = Query(1),
    per_page: int = Query(20),
):
    child_level = CHILD_LEVEL_MAP.get(entity_level)
    if not child_level:
        if entity_level == "site":
            return {"data": [], "total": 0, "page": 1, "per_page": per_page, "narrative": "Site tidak memiliki child entity.", "child_level": "ticket"}
        return {"data": [], "total": 0, "page": 1, "per_page": per_page, "narrative": "Level tidak dikenal.", "child_level": None}

    class FakeReq:
        pass
    req = FakeReq()
    req.date_from = date_from
    req.date_to = date_to
    req.type_ticket = type_ticket
    req.severities = [s.strip() for s in severities.split(",") if s.strip()] if severities else []
    req.fault_level = fault_level
    req.rc_category = rc_category

    filters_cond, filters_params = _build_filters(req)

    child_col_sm = LEVEL_COL_MAP[child_level][1]
    parent_col_sm = LEVEL_COL_MAP[entity_level][1]

    where = [f"{parent_col_sm} = ?"]
    params = [entity_id]
    where.extend(filters_cond)
    params.extend(filters_params)
    where_str = " AND ".join(where)

    with get_connection() as conn:
        rows = conn.execute(f"""
            SELECT {child_col_sm},
                SUM(total_tickets) as total_tickets,
                CASE WHEN SUM(total_tickets) > 0 THEN SUM(total_sla_met) * 100.0 / SUM(total_tickets) ELSE 0 END as sla_pct,
                AVG(avg_mttr_min) as avg_mttr_min,
                CASE WHEN SUM(total_tickets) > 0 THEN SUM(total_escalated) * 100.0 / SUM(total_tickets) ELSE 0 END as escalation_pct,
                CASE WHEN SUM(total_tickets) > 0 THEN SUM(total_auto_resolved) * 100.0 / SUM(total_tickets) ELSE 0 END as auto_resolve_pct,
                CASE WHEN SUM(total_tickets) > 0 THEN SUM(total_repeat) * 100.0 / SUM(total_tickets) ELSE 0 END as repeat_pct
            FROM summary_monthly
            WHERE {where_str}
            GROUP BY {child_col_sm}
            HAVING {child_col_sm} IS NOT NULL
        """, params).fetchall()

        children = []
        total_all = sum(r[1] or 0 for r in rows)

        for r in rows:
            child_id = r[0]
            if not child_id:
                continue

            child_months = conn.execute(f"""
                SELECT year_month,
                    SUM(total_tickets) as total_tickets,
                    CASE WHEN SUM(total_tickets) > 0 THEN SUM(total_sla_met) * 100.0 / SUM(total_tickets) ELSE 0 END as sla_pct,
                    SUM(count_critical) as count_critical,
                    SUM(count_major) as count_major
                FROM summary_monthly
                WHERE {where_str} AND {child_col_sm} = ?
                GROUP BY year_month ORDER BY year_month
            """, params + [child_id]).fetchall()

            child_months_list = [
                {"total_tickets": m[1] or 0, "sla_pct": m[2] or 0, "count_critical": m[3] or 0, "count_major": m[4] or 0}
                for m in child_months
            ]

            beh = get_behavior_with_meta(child_level, child_months_list)

            sla_vals = [m["sla_pct"] for m in child_months_list]
            trend = "stable"
            if len(sla_vals) >= 2:
                slope = linear_slope(sla_vals)
                if slope > 0.5:
                    trend = "up"
                elif slope < -0.5:
                    trend = "down"

            risk_score = _calc_risk_score(r[2] or 0, r[3] or 0, r[4] or 0, r[6] or 0, beh["label"])

            child_name = _get_entity_name(conn, child_level, child_id)

            children.append({
                "entity_id": child_id,
                "name": child_name,
                "total_tickets": r[1] or 0,
                "sla_pct": round(r[2] or 0, 1),
                "avg_mttr_min": round(r[3] or 0, 0),
                "escalation_pct": round(r[4] or 0, 1),
                "auto_resolve_pct": round(r[5] or 0, 1),
                "repeat_pct": round(r[6] or 0, 1),
                "risk_score": risk_score,
                "trend_direction": trend,
                "behavior_label": beh["label"],
                "behavior_icon": beh["icon"],
                "contribution_pct": round((r[1] or 0) / max(total_all, 1) * 100, 1),
            })

        sort_key = sort if sort in ("sla_pct", "avg_mttr_min", "total_tickets", "risk_score", "name", "escalation_pct", "repeat_pct") else "risk_score"
        reverse = order.lower() == "desc"
        if sort_key == "name":
            children.sort(key=lambda c: c["name"], reverse=reverse)
        else:
            children.sort(key=lambda c: c.get(sort_key, 0), reverse=reverse)

        total_children = len(children)
        start = (page - 1) * per_page
        paginated = children[start:start + per_page]

        worst = max(children, key=lambda c: c["risk_score"]) if children else None
        narrative = ""
        if worst and total_all > 0:
            median_sla = sorted([c["sla_pct"] for c in children])[len(children) // 2] if children else 0
            narrative = (
                f"{worst['name']} menyumbang {worst['contribution_pct']:.0f}% tiket dan memiliki risiko tertinggi "
                f"(skor {worst['risk_score']}). "
            )
            if worst["sla_pct"] < median_sla:
                potential = (median_sla - worst["sla_pct"]) * worst["contribution_pct"] / 100
                narrative += f"Jika SLA {worst['name']} naik ke median {median_sla:.1f}%, SLA keseluruhan naik ~{potential:.1f}pp."

    return {
        "data": paginated,
        "total": total_children,
        "page": page,
        "per_page": per_page,
        "child_level": child_level,
        "narrative": narrative,
    }


def _calc_risk_score(sla_pct, mttr, esc_pct, rep_pct, behavior_label):
    score = 0
    if sla_pct < 85:
        score += 30
    elif sla_pct < 90:
        score += 20
    elif sla_pct < 95:
        score += 10

    if mttr > 1440:
        score += 25
    elif mttr > 720:
        score += 15
    elif mttr > 240:
        score += 5

    if esc_pct > 7:
        score += 15
    elif esc_pct > 3:
        score += 8

    if rep_pct > 25:
        score += 20
    elif rep_pct > 10:
        score += 10

    behavior_scores = {"CHRONIC": 10, "DETERIORATING": 8, "SPORADIC": 5, "SEASONAL": 2, "IMPROVING": 0, "HEALTHY": 0}
    score += behavior_scores.get(behavior_label, 0)

    return min(score, 100)


@router.get("/peer-ranking")
async def get_peer_ranking(
    entity_level: str = Query(...),
    entity_id: str = Query(...),
    kpi: str = Query("sla_pct"),
    date_from: str = Query(""),
    date_to: str = Query(""),
    type_ticket: str = Query(""),
    severities: str = Query(""),
    fault_level: str = Query(""),
):
    class FakeReq:
        pass
    req = FakeReq()
    req.date_from = date_from
    req.date_to = date_to
    req.type_ticket = type_ticket
    req.severities = [s.strip() for s in severities.split(",") if s.strip()] if severities else []
    req.fault_level = fault_level
    req.rc_category = ""

    filters_cond, filters_params = _build_filters(req)

    entity_col_sm = LEVEL_COL_MAP[entity_level][1]

    kpi_expr_map = {
        "sla_pct": "CASE WHEN SUM(total_tickets) > 0 THEN SUM(total_sla_met) * 100.0 / SUM(total_tickets) ELSE 0 END",
        "avg_mttr_min": "AVG(avg_mttr_min)",
        "escalation_pct": "CASE WHEN SUM(total_tickets) > 0 THEN SUM(total_escalated) * 100.0 / SUM(total_tickets) ELSE 0 END",
        "auto_resolve_pct": "CASE WHEN SUM(total_tickets) > 0 THEN SUM(total_auto_resolved) * 100.0 / SUM(total_tickets) ELSE 0 END",
        "repeat_pct": "CASE WHEN SUM(total_tickets) > 0 THEN SUM(total_repeat) * 100.0 / SUM(total_tickets) ELSE 0 END",
        "total_tickets": "SUM(total_tickets)",
    }

    kpi_expr = kpi_expr_map.get(kpi, kpi_expr_map["sla_pct"])

    where = filters_cond[:]
    params = filters_params[:]
    where_str = " AND ".join(where) if where else "1=1"

    higher_is_better = kpi in ("sla_pct", "auto_resolve_pct")
    sort_dir = "DESC" if higher_is_better else "ASC"

    with get_connection() as conn:
        rows = conn.execute(f"""
            SELECT {entity_col_sm}, {kpi_expr} as kpi_value
            FROM summary_monthly
            WHERE {where_str}
            GROUP BY {entity_col_sm}
            HAVING {entity_col_sm} IS NOT NULL
            ORDER BY kpi_value {sort_dir}
        """, params).fetchall()

        peers = []
        current_rank = 0
        current_value = 0
        values = []

        for idx, r in enumerate(rows):
            peer_id = r[0]
            value = round(r[1] or 0, 1)
            values.append(value)
            is_current = peer_id == entity_id
            if is_current:
                current_rank = idx + 1
                current_value = value

            name = _get_entity_name(conn, entity_level, peer_id)
            peers.append({
                "id": peer_id,
                "name": name,
                "value": value,
                "is_current": is_current,
            })

    total = len(peers)
    if total == 0:
        return {"peers": [], "current_rank": 0, "total": 0, "percentile": 0, "median": 0, "target": 90.0, "narrative": "Tidak ada data peer."}

    sorted_vals = sorted(values)
    median = sorted_vals[len(sorted_vals) // 2]

    if total <= 1:
        percentile = 100
    else:
        if higher_is_better:
            percentile = round((total - current_rank) / (total - 1) * 100) if total > 1 else 100
        else:
            percentile = round((current_rank - 1) / (total - 1) * 100) if total > 1 else 100

    kpi_labels = {
        "sla_pct": "SLA",
        "avg_mttr_min": "MTTR",
        "escalation_pct": "Eskalasi",
        "auto_resolve_pct": "Auto-resolve",
        "repeat_pct": "Repeat",
        "total_tickets": "Volume",
    }
    kpi_label = kpi_labels.get(kpi, kpi)

    narrative = f"Berada di peringkat {current_rank} dari {total} {entity_level.title()}"
    if percentile >= 75:
        narrative += " (kuartil atas — 25% terbaik)."
    elif percentile >= 50:
        narrative += " (di atas median peer)."
    elif percentile >= 25:
        narrative += " (di bawah median — kuartil ketiga)."
    else:
        narrative += " (kuartil bawah — 25% terendah)."

    if current_rank == 1:
        narrative = f"Peringkat terbaik dari {total} {entity_level.title()}."
    elif current_rank == total:
        narrative = f" Peringkat terendah dari {total} {entity_level.title()}."

    diff = current_value - median
    if higher_is_better:
        if diff >= 0:
            narrative += f" {kpi_label} {current_value:.1f} berada {diff:.1f} di atas median peer ({median:.1f})."
        else:
            narrative += f" {kpi_label} {current_value:.1f} berada {abs(diff):.1f} di bawah median peer ({median:.1f})."
    else:
        if diff <= 0:
            narrative += f" {kpi_label} {current_value:.1f} lebih baik {abs(diff):.1f} dari median peer ({median:.1f})."
        else:
            narrative += f" {kpi_label} {current_value:.1f} lebih buruk {diff:.1f} dari median peer ({median:.1f})."

    return {
        "peers": peers,
        "current_rank": current_rank,
        "total": total,
        "percentile": percentile,
        "median": round(median, 1),
        "target": 90.0,
        "narrative": narrative,
    }


@router.get("/filter-options")
async def get_filter_options():
    with get_connection() as conn:
        areas = conn.execute("SELECT area_id, area_name FROM master_area WHERE status = 'ACTIVE' ORDER BY area_name").fetchall()
        regionals = conn.execute("SELECT regional_id, regional_name, area_id FROM master_regional WHERE status = 'ACTIVE' ORDER BY regional_name").fetchall()
        nops = conn.execute("SELECT nop_id, nop_name, regional_id FROM master_nop WHERE status = 'ACTIVE' ORDER BY nop_name").fetchall()
        tos = conn.execute("SELECT to_id, to_name, nop_id FROM master_to WHERE status = 'ACTIVE' ORDER BY to_name").fetchall()

        periods = conn.execute("SELECT DISTINCT year_month FROM summary_monthly ORDER BY year_month").fetchall()

        severities = conn.execute("SELECT DISTINCT severity FROM summary_monthly WHERE severity IS NOT NULL ORDER BY severity").fetchall()
        types = conn.execute("SELECT DISTINCT type_ticket FROM summary_monthly WHERE type_ticket IS NOT NULL ORDER BY type_ticket").fetchall()
        fault_levels = conn.execute("SELECT DISTINCT fault_level FROM summary_monthly WHERE fault_level IS NOT NULL ORDER BY fault_level").fetchall()

    return {
        "areas": [{"id": a[0], "name": a[1]} for a in areas],
        "regionals": [{"id": r[0], "name": r[1], "area_id": r[2]} for r in regionals],
        "nops": [{"id": n[0], "name": n[1], "regional_id": n[2]} for n in nops],
        "tos": [{"id": t[0], "name": t[1], "nop_id": t[2]} for t in tos],
        "periods": [p[0] for p in periods],
        "severities": [s[0] for s in severities],
        "types": [t[0] for t in types],
        "fault_levels": [f[0] for f in fault_levels],
    }


@router.get("/sites")
async def get_sites(to_id: str = None):
    with get_connection() as conn:
        if to_id:
            rows = conn.execute(
                "SELECT site_id, site_name, to_id FROM master_site WHERE to_id = ? AND status = 'ACTIVE' ORDER BY site_name",
                [to_id]
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT site_id, site_name, to_id FROM master_site WHERE status = 'ACTIVE' ORDER BY site_name LIMIT 500"
            ).fetchall()
    return [{"id": r[0], "name": r[1] or r[0], "to_id": r[2]} for r in rows]


POSITIVE_UP_KPIS = {"sla_pct", "auto_resolve_pct"}
TREND_THRESHOLDS = {
    "sla_pct": 0.5,
    "avg_mttr_min": 30,
    "total_tickets": 5,
    "escalation_pct": 0.5,
    "auto_resolve_pct": 0.5,
    "repeat_pct": 0.5,
}

KPI_LABELS_ID = {
    "sla_pct": "SLA",
    "avg_mttr_min": "MTTR",
    "total_tickets": "Volume",
    "escalation_pct": "Eskalasi",
    "auto_resolve_pct": "Auto-resolve",
    "repeat_pct": "Repeat",
}

MONTH_NAMES = {
    "01": "Jan", "02": "Feb", "03": "Mar", "04": "Apr",
    "05": "Mei", "06": "Jun", "07": "Jul", "08": "Agu",
    "09": "Sep", "10": "Okt", "11": "Nov", "12": "Des",
}

KPI_EXPR_MAP_TREND = {
    "sla_pct": "CASE WHEN SUM(total_tickets) > 0 THEN SUM(total_sla_met) * 100.0 / SUM(total_tickets) ELSE 0 END",
    "avg_mttr_min": "AVG(avg_mttr_min)",
    "total_tickets": "SUM(total_tickets)",
    "escalation_pct": "CASE WHEN SUM(total_tickets) > 0 THEN SUM(total_escalated) * 100.0 / SUM(total_tickets) ELSE 0 END",
    "auto_resolve_pct": "CASE WHEN SUM(total_tickets) > 0 THEN SUM(total_auto_resolved) * 100.0 / SUM(total_tickets) ELSE 0 END",
    "repeat_pct": "CASE WHEN SUM(total_tickets) > 0 THEN SUM(total_repeat) * 100.0 / SUM(total_tickets) ELSE 0 END",
}


def _linear_regression(values):
    n = len(values)
    if n < 2:
        return 0.0, values[0] if values else 0.0
    x_mean = (n - 1) / 2.0
    y_mean = stat_mean(values)
    num = sum((i - x_mean) * (v - y_mean) for i, v in enumerate(values))
    den = sum((i - x_mean) ** 2 for i in range(n))
    slope = num / den if den != 0 else 0.0
    intercept = y_mean - slope * x_mean
    return slope, intercept


def _classify_trend_direction(kpi_name, slope, period_label="bulan"):
    thr = TREND_THRESHOLDS.get(kpi_name, 0.5)
    positive_up = kpi_name in POSITIVE_UP_KPIS
    kpi_label = KPI_LABELS_ID.get(kpi_name, kpi_name)

    if abs(slope) <= thr:
        return {
            "direction": "stable", "icon": "─", "quality": "neutral",
            "narrative": f"Tren {kpi_label} stabil (perubahan < ±{thr}/{period_label})"
        }

    if slope > 0:
        if positive_up:
            return {
                "direction": "up", "icon": "", "quality": "improving",
                "narrative": f"Tren {kpi_label} naik (membaik) {slope:.1f}/{period_label}"
            }
        else:
            return {
                "direction": "up", "icon": "", "quality": "worsening",
                "narrative": f"Tren {kpi_label} naik (memburuk) {slope:.1f}/{period_label}"
            }
    else:
        if positive_up:
            return {
                "direction": "down", "icon": "", "quality": "worsening",
                "narrative": f"Tren {kpi_label} turun (memburuk) {abs(slope):.1f}/{period_label}"
            }
        else:
            return {
                "direction": "down", "icon": "", "quality": "improving",
                "narrative": f"Tren {kpi_label} turun (membaik) {abs(slope):.1f}/{period_label}"
            }


def _detect_anomalies(data_points, kpi_name):
    values = [d["value"] for d in data_points]
    if len(values) < 3:
        return []
    mean_val = stat_mean(values)
    std_val = stat_stdev(values) if len(values) > 2 else 0
    if std_val == 0:
        return []

    anomalies = []
    kpi_label = KPI_LABELS_ID.get(kpi_name, kpi_name)
    for dp in data_points:
        z = (dp["value"] - mean_val) / std_val
        if abs(z) > 2:
            direction = "di atas" if z > 0 else "di bawah"
            sev = "significant" if abs(z) > 3 else "moderate"
            if abs(z) > 3:
                narr = (
                    f" ANOMALI SIGNIFIKAN pada {dp['period']}: "
                    f"{kpi_label} = {dp['value']:.1f} ({abs(z):.1f}σ {direction} rata-rata "
                    f"{mean_val:.1f} ± {std_val:.1f}). Investigasi penyebab diperlukan."
                )
            else:
                narr = (
                    f" Anomali pada {dp['period']}: {kpi_label} = {dp['value']:.1f} "
                    f"(z-score: {z:.1f}). Rata-rata historis: {mean_val:.1f} ± {std_val:.1f}."
                )
            anomalies.append({
                "period": dp["period"],
                "value": dp["value"],
                "z_score": round(z, 2),
                "mean": round(mean_val, 1),
                "std": round(std_val, 1),
                "severity": sev,
                "narrative": narr,
            })
    return anomalies


def _count_consecutive_worsening(values, kpi_name):
    positive_up = kpi_name in POSITIVE_UP_KPIS
    count = 0
    for i in range(1, len(values)):
        if positive_up:
            worse = values[i] < values[i - 1]
        else:
            worse = values[i] > values[i - 1]
        if worse:
            count += 1
        else:
            count = 0
    return count


@router.get("/trends")
async def get_trends(
    entity_level: str = Query(...),
    entity_id: str = Query(...),
    kpi: str = Query("sla_pct"),
    granularity: str = Query("monthly"),
    date_from: str = Query(""),
    date_to: str = Query(""),
    type_ticket: str = Query(""),
    severities: str = Query(""),
    fault_level: str = Query(""),
):
    class FakeReq:
        pass
    req = FakeReq()
    req.date_from = date_from
    req.date_to = date_to
    req.type_ticket = type_ticket
    req.severities = [s.strip() for s in severities.split(",") if s.strip()] if severities else []
    req.fault_level = fault_level

    filters_cond, filters_params = _build_filters(req)

    entity_col_sm = LEVEL_COL_MAP[entity_level][1]
    kpi_expr = KPI_EXPR_MAP_TREND.get(kpi, KPI_EXPR_MAP_TREND["sla_pct"])

    where = [f"{entity_col_sm} = ?"]
    params = [entity_id]
    where.extend(filters_cond)
    params.extend(filters_params)
    where_str = " AND ".join(where)

    with get_connection() as conn:
        rows = conn.execute(f"""
            SELECT year_month, {kpi_expr} as kpi_value
            FROM summary_monthly
            WHERE {where_str}
            GROUP BY year_month
            ORDER BY year_month
        """, params).fetchall()

        data_points = []
        for r in rows:
            ym = r[0]
            parts = ym.split("-")
            label = f"{MONTH_NAMES.get(parts[1], parts[1])} {parts[0]}" if len(parts) == 2 else ym
            data_points.append({
                "period": ym,
                "value": round(r[1] or 0, 2),
                "label": label,
            })

        values = [d["value"] for d in data_points]

        if len(values) < 2:
            return {
                "data_points": data_points,
                "trend": {"slope": 0, "intercept": 0, "direction": "stable", "quality": "neutral",
                          "icon": "─", "narrative": "Data tidak cukup untuk analisis tren.",
                          "consecutive_worsening": 0, "projection_3m": None},
                "anomalies": [],
                "stats": {"mean": values[0] if values else 0, "std": 0, "min": values[0] if values else 0, "max": values[0] if values else 0},
                "target": 90.0 if kpi == "sla_pct" else None,
                "band": {"upper": [], "lower": []},
            }

        slope, intercept = _linear_regression(values)
        trend_info = _classify_trend_direction(kpi, slope)

        consec = _count_consecutive_worsening(values, kpi)
        kpi_label = KPI_LABELS_ID.get(kpi, kpi)
        thr = TREND_THRESHOLDS.get(kpi, 0.5)

        if trend_info["quality"] == "worsening" and consec >= 3:
            trend_info["narrative"] += (
                f"  Tren memburuk selama {consec} periode berturut-turut. Intervensi segera diperlukan."
            )
        if trend_info["quality"] == "worsening" and abs(slope) > 2 * thr:
            trend_info["narrative"] += (
                f"  Penurunan CEPAT — laju {abs(slope):.1f} per bulan jauh di atas batas normal ({thr})."
            )

        projection = None
        if trend_info["quality"] == "worsening" and len(values) >= 4:
            projection = round(values[-1] + slope * 3, 1)
            trend_info["narrative"] += (
                f" Jika berlanjut, {kpi_label} akan mencapai ~{projection} dalam 3 bulan."
            )

        mean_val = stat_mean(values)
        std_val = stat_stdev(values) if len(values) > 2 else 0

        trend_line = [round(slope * i + intercept, 2) for i in range(len(values))]
        upper_band = [round(mean_val + 2 * std_val, 2)] * len(values) if std_val > 0 else []
        lower_band = [round(mean_val - 2 * std_val, 2)] * len(values) if std_val > 0 else []

        anomalies = _detect_anomalies(data_points, kpi)

        target = None
        if kpi == "sla_pct":
            target = 90.0

    return {
        "data_points": data_points,
        "trend": {
            "slope": round(slope, 3),
            "intercept": round(intercept, 2),
            "direction": trend_info["direction"],
            "quality": trend_info["quality"],
            "icon": trend_info["icon"],
            "narrative": trend_info["narrative"],
            "consecutive_worsening": consec,
            "projection_3m": projection,
        },
        "anomalies": anomalies,
        "stats": {
            "mean": round(mean_val, 2),
            "std": round(std_val, 2),
            "min": round(min(values), 2),
            "max": round(max(values), 2),
        },
        "target": target,
        "band": {"upper": upper_band, "lower": lower_band},
        "trend_line": trend_line,
    }


HEATMAP_CONFIG = {
    "monthly": {"type": "week_x_day", "y_labels": ["W1", "W2", "W3", "W4", "W5"],
                "x_labels": ["Sen", "Sel", "Rab", "Kam", "Jum", "Sab", "Min"]},
    "weekly": {"type": "day_x_hour", "y_labels": ["Sen", "Sel", "Rab", "Kam", "Jum", "Sab", "Min"],
               "x_labels": [f"{h:02d}" for h in range(24)]},
}


@router.get("/heatmap")
async def get_heatmap(
    entity_level: str = Query(...),
    entity_id: str = Query(...),
    granularity: str = Query("monthly"),
    date_from: str = Query(""),
    date_to: str = Query(""),
    metric: str = Query("volume"),
    type_ticket: str = Query(""),
    severities: str = Query(""),
    fault_level: str = Query(""),
):
    config = HEATMAP_CONFIG.get(granularity, HEATMAP_CONFIG["monthly"])
    entity_col_ticket = LEVEL_COL_MAP[entity_level][0]

    with get_connection() as conn:
        has_tickets = conn.execute("SELECT COUNT(*) FROM noc_tickets LIMIT 1").fetchone()[0]
        if has_tickets == 0:
            return {
                "heatmap_type": config["type"],
                "y_labels": config["y_labels"],
                "x_labels": config["x_labels"],
                "cells": [],
                "stats": {"min": 0, "max": 0, "avg": 0},
                "interpretation": {"narrative": "Tidak ada data tiket untuk heatmap.", "peak_factor": 0},
            }

        where_parts = [f"{entity_col_ticket} = ?"]
        params = [entity_id]
        if date_from:
            where_parts.append("calc_year_month >= ?")
            params.append(date_from)
        if date_to:
            where_parts.append("calc_year_month <= ?")
            params.append(date_to)
        if type_ticket:
            where_parts.append("type_ticket = ?")
            params.append(type_ticket)
        if severities:
            sev_list = [s.strip() for s in severities.split(",") if s.strip()]
            if sev_list:
                placeholders = ",".join(["?" for _ in sev_list])
                where_parts.append(f"severity IN ({placeholders})")
                params.extend(sev_list)
        if fault_level:
            where_parts.append("fault_level = ?")
            params.append(fault_level)

        where_str = " AND ".join(where_parts)

        if config["type"] == "week_x_day":
            rows = conn.execute(f"""
                SELECT
                    calc_week_of_month as week_num,
                    calc_day_of_week as day_of_week,
                    COUNT(*) as ticket_count
                FROM noc_tickets
                WHERE {where_str}
                GROUP BY 1, 2
                ORDER BY 1, 2
            """, params).fetchall()

            cells = [[None] * 7 for _ in range(5)]
            for r in rows:
                w = (r[0] or 1) - 1
                d = r[1] or 0
                if 0 <= w < 5 and 0 <= d < 7:
                    cells[w][d] = r[2]

        elif config["type"] == "day_x_hour":
            rows = conn.execute(f"""
                SELECT
                    calc_day_of_week as day_of_week,
                    calc_hour_of_day as hour,
                    COUNT(*) as ticket_count
                FROM noc_tickets
                WHERE {where_str}
                GROUP BY 1, 2
                ORDER BY 1, 2
            """, params).fetchall()

            cells = [[None] * 24 for _ in range(7)]
            for r in rows:
                d = r[0] or 0
                h = r[1] or 0
                if 0 <= d < 7 and 0 <= h < 24:
                    cells[d][h] = r[2]
        else:
            cells = []

        flat = [v for row in cells for v in row if v is not None]
        if not flat:
            return {
                "heatmap_type": config["type"],
                "y_labels": config["y_labels"],
                "x_labels": config["x_labels"],
                "cells": cells,
                "stats": {"min": 0, "max": 0, "avg": 0},
                "interpretation": {"narrative": "Data tidak tersedia untuk heatmap.", "peak_factor": 0},
            }

        avg_val = stat_mean(flat)
        max_val = max(flat)
        min_val = min(flat)

        peak_y, peak_x, peak_val = 0, 0, 0
        low_y, low_x, low_val = 0, 0, float("inf")
        for yi, row in enumerate(cells):
            for xi, v in enumerate(row):
                if v is not None:
                    if v > peak_val:
                        peak_y, peak_x, peak_val = yi, xi, v
                    if v < low_val:
                        low_y, low_x, low_val = yi, xi, v

        peak_factor = peak_val / avg_val if avg_val > 0 else 0
        low_factor = low_val / avg_val if avg_val > 0 else 0

        parts = []
        y_labels = config["y_labels"]
        x_labels = config["x_labels"]

        if peak_factor > 2.0:
            parts.append(
                f" Hotspot: {y_labels[peak_y]} {x_labels[peak_x]} mengalami "
                f"{peak_factor:.1f}× volume rata-rata ({peak_val:,} vs avg {avg_val:,.0f}). "
                f"Pertimbangkan penambahan resource pada periode ini."
            )
        elif peak_factor > 1.5:
            parts.append(
                f"Puncak gangguan: {y_labels[peak_y]} {x_labels[peak_x]} "
                f"({peak_val:,} tiket, {peak_factor:.1f}× rata-rata)."
            )
        else:
            parts.append(f"Puncak: {y_labels[peak_y]} {x_labels[peak_x]} ({peak_val:,} tiket).")

        if low_factor < 0.5:
            parts.append(
                f"Periode paling tenang: {y_labels[low_y]} {x_labels[low_x]} "
                f"({low_val:,} tiket) — peluang maintenance window."
            )

        weekend_labels = {"Sab", "Min", "Sat", "Sun"}
        if any(l in weekend_labels for l in x_labels):
            weekday_vals = []
            weekend_vals = []
            for row in cells:
                for xi, v in enumerate(row):
                    if v is not None:
                        if x_labels[xi] in weekend_labels:
                            weekend_vals.append(v)
                        else:
                            weekday_vals.append(v)
            if weekday_vals and weekend_vals:
                wd_avg = stat_mean(weekday_vals)
                we_avg = stat_mean(weekend_vals)
                if wd_avg > 0:
                    diff_pct = (we_avg - wd_avg) / wd_avg * 100
                    parts.append(f"Weekend rata-rata {we_avg:,.0f} tiket ({diff_pct:+.0f}% dari weekday).")

    return {
        "heatmap_type": config["type"],
        "y_labels": config["y_labels"],
        "x_labels": config["x_labels"],
        "cells": cells,
        "stats": {"min": int(min_val), "max": int(max_val), "avg": round(avg_val, 1)},
        "interpretation": {
            "narrative": " ".join(parts),
            "peak_factor": round(peak_factor, 2),
            "peak_cell": [peak_y, peak_x],
            "low_cell": [low_y, low_x],
        },
    }


@router.get("/child-trends")
async def get_child_trends(
    entity_level: str = Query(...),
    entity_id: str = Query(...),
    kpi: str = Query("sla_pct"),
    granularity: str = Query("monthly"),
    date_from: str = Query(""),
    date_to: str = Query(""),
    type_ticket: str = Query(""),
    severities: str = Query(""),
    fault_level: str = Query(""),
):
    child_level = CHILD_LEVEL_MAP.get(entity_level)
    if not child_level:
        return {
            "parent_trend": {"slope": 0, "direction": "stable", "quality": "neutral"},
            "children": [],
            "summary": {"n_improving": 0, "n_stable": 0, "n_worsening": 0, "total": 0},
            "narrative": "Tidak ada child entity.",
        }

    class FakeReq:
        pass
    req = FakeReq()
    req.date_from = date_from
    req.date_to = date_to
    req.type_ticket = type_ticket
    req.severities = [s.strip() for s in severities.split(",") if s.strip()] if severities else []
    req.fault_level = fault_level

    filters_cond, filters_params = _build_filters(req)

    parent_col_sm = LEVEL_COL_MAP[entity_level][1]
    child_col_sm = LEVEL_COL_MAP[child_level][1]
    kpi_expr = KPI_EXPR_MAP_TREND.get(kpi, KPI_EXPR_MAP_TREND["sla_pct"])

    where = [f"{parent_col_sm} = ?"]
    params = [entity_id]
    where.extend(filters_cond)
    params.extend(filters_params)
    where_str = " AND ".join(where)

    with get_connection() as conn:
        parent_rows = conn.execute(f"""
            SELECT year_month, {kpi_expr} as kpi_value
            FROM summary_monthly
            WHERE {where_str}
            GROUP BY year_month ORDER BY year_month
        """, params).fetchall()

        parent_values = [r[1] or 0 for r in parent_rows]
        parent_slope, _ = _linear_regression(parent_values) if len(parent_values) >= 2 else (0, 0)
        parent_trend = _classify_trend_direction(kpi, parent_slope)

        child_ids = conn.execute(f"""
            SELECT DISTINCT {child_col_sm}
            FROM summary_monthly
            WHERE {where_str} AND {child_col_sm} IS NOT NULL
        """, params).fetchall()

        children = []
        for (child_id,) in child_ids:
            child_rows = conn.execute(f"""
                SELECT year_month, {kpi_expr} as kpi_value
                FROM summary_monthly
                WHERE {where_str} AND {child_col_sm} = ?
                GROUP BY year_month ORDER BY year_month
            """, params + [child_id]).fetchall()

            child_values = [r[1] or 0 for r in child_rows]
            if len(child_values) >= 2:
                c_slope, _ = _linear_regression(child_values)
            else:
                c_slope = 0

            c_trend = _classify_trend_direction(kpi, c_slope)
            child_name = _get_entity_name(conn, child_level, child_id)

            children.append({
                "entity_id": child_id,
                "entity_name": child_name,
                "slope": round(c_slope, 2),
                "direction": c_trend["direction"],
                "quality": c_trend["quality"],
                "icon": c_trend["icon"],
                "current_value": round(child_values[-1], 1) if child_values else 0,
            })

    children.sort(key=lambda c: c["slope"])

    n_improving = sum(1 for c in children if c["quality"] == "improving")
    n_stable = sum(1 for c in children if c["quality"] == "neutral")
    n_worsening = sum(1 for c in children if c["quality"] == "worsening")
    total = len(children)

    type_labels = {"regional": "Regional", "nop": "NOP", "to": "TO", "site": "Site"}
    child_type_label = type_labels.get(child_level, child_level)

    parts = [
        f"Dari {total} {child_type_label}: {n_improving} membaik, "
        f"{n_stable} stabil, {n_worsening} memburuk."
    ]

    if parent_trend["quality"] in ("neutral", "improving") and n_worsening >= 2:
        worst = [c for c in children if c["quality"] == "worsening"][:3]
        worst_names = ", ".join(f"{c['entity_name']} ({c['slope']:+.1f}pp/bln)" for c in worst)
        entity_name = _get_entity_name(conn, entity_level, entity_id) if conn else entity_id
        parts.append(
            f" Meskipun secara keseluruhan "
            f"{'stabil' if parent_trend['quality'] == 'neutral' else 'membaik'}, "
            f"{n_worsening} {child_type_label} ({n_worsening / total * 100:.0f}%) "
            f"menunjukkan penurunan ({worst_names}). "
            f"Tren negatif ini ter-mask oleh performa baik {child_type_label} lain."
        )

    if n_worsening == total and total > 0:
        parts.append(
            f" SEMUA {child_type_label} menunjukkan tren memburuk. "
            f"Masalah bersifat sistemik."
        )

    return {
        "parent_trend": {
            "slope": round(parent_slope, 3),
            "direction": parent_trend["direction"],
            "quality": parent_trend["quality"],
        },
        "children": children,
        "summary": {
            "n_improving": n_improving,
            "n_stable": n_stable,
            "n_worsening": n_worsening,
            "total": total,
        },
        "narrative": " ".join(parts),
    }

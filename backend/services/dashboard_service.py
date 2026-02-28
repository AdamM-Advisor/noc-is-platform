import logging
from datetime import datetime
from backend.database import get_connection
from backend.services.predictive_service import linear_regression_slope

logger = logging.getLogger(__name__)

CHILD_TYPE_LABELS = {
    "area": "Regional",
    "regional": "NOP",
    "nop": "TO",
    "to": "Site",
}

CHILD_LEVEL_MAP = {
    "area": "regional",
    "regional": "nop",
    "nop": "to",
    "to": "site",
}

LEVEL_COLS = {
    "area": ("area_id", "master_area", "area_name"),
    "regional": ("regional_id", "master_regional", "regional_name"),
    "nop": ("nop_id", "master_nop", "nop_name"),
    "to": ("to_id", "master_to", "to_name"),
}

PARENT_LEVEL = {
    "regional": "area",
    "nop": "regional",
    "to": "nop",
}

PARENT_FK = {
    "regional": ("area_id", "master_area", "area_name"),
    "nop": ("regional_id", "master_regional", "regional_name"),
    "to": ("nop_id", "master_nop", "nop_name"),
}


def determine_overall_status(kpis, trend, risk_summary, children):
    sla = kpis.get("sla_pct", 0)
    target = kpis.get("sla_target", 90)
    sla_trend = trend.get("sla_quality", "neutral")
    has_high_risk = risk_summary.get("pct_high", 0) > 10 if risk_summary else False
    n_critical = sum(1 for c in children if c.get("status_level") in ("KRITIS", "critical"))

    if sla < target - 5 and sla_trend == "worsening":
        return {"status": "KRITIS", "icon": "🔴", "color": "#DC2626",
                "rule": "SLA jauh di bawah target DAN tren memburuk"}
    if sla < target and sla_trend == "worsening" and has_high_risk:
        return {"status": "KRITIS", "icon": "🔴", "color": "#DC2626",
                "rule": "SLA di bawah target + tren memburuk + risk tinggi"}
    if sla < target or sla_trend == "worsening" or has_high_risk:
        return {"status": "PERLU PERHATIAN", "icon": "🟡", "color": "#D97706",
                "rule": "SLA di bawah target ATAU tren memburuk ATAU risk tinggi"}
    if sla >= target + 3 and sla_trend == "improving":
        return {"status": "SANGAT BAIK", "icon": "🏆", "color": "#059669",
                "rule": "SLA di atas target+3pp DAN tren membaik"}
    return {"status": "BAIK", "icon": "🟢", "color": "#16A34A",
            "rule": "SLA memenuhi target, tren tidak memburuk, risk terkendali"}


def determine_entity_status(sla, target, sla_quality):
    if sla < target - 5 and sla_quality == "worsening":
        return {"status": "KRITIS", "icon": "🔴", "color": "#DC2626"}
    if sla < target and sla_quality == "worsening":
        return {"status": "KRITIS", "icon": "🔴", "color": "#DC2626"}
    if sla < target or sla_quality == "worsening":
        return {"status": "PERLU PERHATIAN", "icon": "🟡", "color": "#D97706"}
    if sla >= target + 3 and sla_quality == "improving":
        return {"status": "SANGAT BAIK", "icon": "🏆", "color": "#059669"}
    return {"status": "BAIK", "icon": "🟢", "color": "#16A34A"}


def generate_overall_narrative(entity_name, status, kpis, trend, children):
    parts = []
    sla = kpis.get("sla_pct", 0)
    target = kpis.get("sla_target", 90)
    gap = sla - target

    if gap >= 0:
        parts.append(f"{entity_name} menunjukkan SLA {sla:.1f}% ({gap:+.1f}pp dari target {target}%).")
    else:
        parts.append(f"{entity_name} menunjukkan SLA {sla:.1f}% ({abs(gap):.1f}pp di bawah target {target}%).")

    sla_quality = trend.get("sla_quality", "neutral")
    if sla_quality == "worsening":
        parts.append(f"Tren SLA menurun {abs(trend.get('sla_slope', 0)):.1f}pp/bulan.")
    elif sla_quality == "improving":
        parts.append(f"Tren SLA membaik {trend.get('sla_slope', 0):.1f}pp/bulan.")

    n_critical = sum(1 for c in children if c.get("status_level") in ("KRITIS",))
    if n_critical > 0:
        parts.append(f"{n_critical} dari {len(children)} entitas berada dalam status Kritis.")

    vol_change = kpis.get("volume_mom_pct", 0)
    mttr_change = kpis.get("mttr_mom_pct", 0)
    vol_word = "naik" if vol_change > 5 else ("turun" if vol_change < -5 else "stabil")
    mttr_word = "naik" if mttr_change > 5 else ("turun" if mttr_change < -5 else "stabil")
    parts.append(f"Volume {vol_word} ({vol_change:+.1f}% MoM), MTTR {mttr_word} ({mttr_change:+.1f}% MoM).")

    return " ".join(parts)


def _compute_sla_trend(periods_data):
    if len(periods_data) < 2:
        return {"sla_quality": "neutral", "sla_slope": 0, "consecutive": 0}

    sla_values = [p["sla_pct"] for p in periods_data]
    slope = linear_regression_slope(range(len(sla_values)), sla_values)

    consecutive = 0
    for i in range(len(sla_values) - 1, 0, -1):
        if sla_values[i] < sla_values[i - 1]:
            consecutive += 1
        else:
            break

    if slope < -0.5:
        quality = "worsening"
    elif slope > 0.5:
        quality = "improving"
    else:
        quality = "neutral"

    return {"sla_quality": quality, "sla_slope": round(slope, 2), "consecutive": consecutive}


def _compute_mttr_trend(periods_data):
    if len(periods_data) < 2:
        return {"mttr_quality": "neutral", "mttr_slope": 0}
    mttr_values = [p.get("avg_mttr_min", 0) for p in periods_data]
    if mttr_values[0] == 0:
        return {"mttr_quality": "neutral", "mttr_slope": 0}
    slope = linear_regression_slope(range(len(mttr_values)), mttr_values)
    pct_slope = slope / mttr_values[0] * 100 if mttr_values[0] > 0 else 0
    if pct_slope > 5:
        quality = "worsening"
    elif pct_slope < -5:
        quality = "improving"
    else:
        quality = "neutral"
    return {"mttr_quality": quality, "mttr_slope": round(pct_slope, 2)}


def get_available_periods(conn):
    try:
        rows = conn.execute(
            "SELECT DISTINCT year_month FROM summary_monthly ORDER BY year_month DESC"
        ).fetchall()
        return [r[0] for r in rows]
    except Exception:
        return []


def get_parent_entities(conn, view_level):
    parent_lvl = PARENT_LEVEL.get(view_level)
    if not parent_lvl:
        return []
    fk_col, tbl, name_col = PARENT_FK[view_level]
    try:
        rows = conn.execute(
            f"SELECT {fk_col}, {name_col} FROM {tbl} WHERE status = 'ACTIVE' ORDER BY {name_col}"
        ).fetchall()
        return [{"id": r[0], "name": r[1]} for r in rows]
    except Exception:
        return []


def get_dashboard_data(conn, period, view_level, parent_filter=None):
    col, tbl, name_col = LEVEL_COLS.get(view_level, ("area_id", "master_area", "area_name"))
    child_level = CHILD_LEVEL_MAP.get(view_level)
    sla_target = 90.0
    try:
        row = conn.execute("SELECT param_value FROM master_threshold WHERE param_key = 'sla_target'").fetchone()
        if row:
            sla_target = row[0]
    except Exception:
        pass

    where_clause = f"WHERE year_month = ?"
    params = [period]

    if parent_filter:
        parent_level = PARENT_LEVEL.get(view_level)
        if parent_level:
            parent_col = LEVEL_COLS[parent_level][0]
            where_clause += f" AND {parent_col} = ?"
            params.append(parent_filter)

    agg_query = f"""
        SELECT
            SUM(total_tickets) as total_vol,
            SUM(total_sla_met) as total_met,
            SUM(total_tickets) as total_tkts,
            SUM(total_escalated) as total_esc,
            SUM(total_auto_resolved) as total_auto,
            AVG(avg_mttr_min) as avg_mttr,
            AVG(avg_response_min) as avg_response
        FROM summary_monthly
        {where_clause}
    """
    try:
        agg = conn.execute(agg_query, params).fetchone()
    except Exception:
        agg = None

    total_vol = agg[0] or 0 if agg else 0
    total_met = agg[1] or 0 if agg else 0
    total_tkts = agg[2] or 0 if agg else 0
    total_esc = agg[3] or 0 if agg else 0
    total_auto = agg[4] or 0 if agg else 0
    avg_mttr = agg[5] or 0 if agg else 0
    avg_response = agg[6] or 0 if agg else 0
    sla_pct = (total_met / total_tkts * 100) if total_tkts > 0 else 0
    esc_pct = (total_esc / total_tkts * 100) if total_tkts > 0 else 0
    auto_pct = (total_auto / total_tkts * 100) if total_tkts > 0 else 0

    prev_period = _prev_month(period)
    prev_params = [prev_period] + params[1:]
    try:
        prev_agg = conn.execute(agg_query.replace("year_month = ?", "year_month = ?", 1), prev_params).fetchone()
    except Exception:
        prev_agg = None

    prev_vol = prev_agg[0] or 0 if prev_agg else 0
    prev_met = prev_agg[1] or 0 if prev_agg else 0
    prev_tkts = prev_agg[2] or 0 if prev_agg else 0
    prev_mttr = prev_agg[5] or 0 if prev_agg else 0
    prev_sla = (prev_met / prev_tkts * 100) if prev_tkts > 0 else 0
    prev_esc_pct = (prev_agg[3] / prev_tkts * 100) if prev_agg and prev_tkts > 0 else 0
    prev_auto_pct = (prev_agg[4] / prev_tkts * 100) if prev_agg and prev_tkts > 0 else 0

    vol_mom = ((total_vol - prev_vol) / prev_vol * 100) if prev_vol > 0 else 0
    sla_mom = sla_pct - prev_sla
    mttr_mom = ((avg_mttr - prev_mttr) / prev_mttr * 100) if prev_mttr > 0 else 0
    esc_mom = esc_pct - prev_esc_pct
    auto_mom = auto_pct - prev_auto_pct

    kpis = {
        "total_volume": total_vol,
        "sla_pct": round(sla_pct, 1),
        "sla_target": sla_target,
        "avg_mttr_min": round(avg_mttr, 0),
        "avg_response_min": round(avg_response, 0),
        "escalation_pct": round(esc_pct, 1),
        "auto_resolve_pct": round(auto_pct, 1),
        "volume_mom_pct": round(vol_mom, 1),
        "sla_mom_pp": round(sla_mom, 1),
        "mttr_mom_pct": round(mttr_mom, 1),
        "esc_mom_pp": round(esc_mom, 1),
        "auto_mom_pp": round(auto_mom, 1),
    }

    entity_query = f"""
        SELECT
            sm.{col},
            m.{name_col},
            SUM(sm.total_tickets) as vol,
            SUM(sm.total_sla_met) as met,
            SUM(sm.total_tickets) as tkts,
            AVG(sm.avg_mttr_min) as mttr,
            SUM(sm.total_escalated) as esc,
            SUM(sm.total_auto_resolved) as auto_r
        FROM summary_monthly sm
        LEFT JOIN {tbl} m ON sm.{col} = m.{col}
        {where_clause.replace('WHERE', 'WHERE sm.')}
        GROUP BY sm.{col}, m.{name_col}
        ORDER BY SUM(sm.total_sla_met) * 1.0 / NULLIF(SUM(sm.total_tickets), 0) ASC
    """
    try:
        entity_rows = conn.execute(entity_query, params).fetchall()
    except Exception:
        entity_rows = []

    recent_periods = _get_recent_periods(period, 3)
    entities = []
    for er in entity_rows:
        eid = er[0]
        ename = er[1] or eid
        evol = er[2] or 0
        emet = er[3] or 0
        etkts = er[4] or 0
        emttr = er[5] or 0
        eesc = er[6] or 0
        eauto = er[7] or 0
        esla = (emet / etkts * 100) if etkts > 0 else 0
        eesc_pct = (eesc / etkts * 100) if etkts > 0 else 0

        hist = _get_entity_history(conn, col, eid, recent_periods)
        trend_info = _compute_sla_trend(hist)
        entity_status = determine_entity_status(esla, sla_target, trend_info["sla_quality"])

        trend_icon = "📉" if trend_info["sla_quality"] == "worsening" else ("📈" if trend_info["sla_quality"] == "improving" else "─")

        entities.append({
            "id": eid,
            "name": ename,
            "sla_pct": round(esla, 1),
            "avg_mttr_min": round(emttr, 0),
            "total_volume": evol,
            "escalation_pct": round(eesc_pct, 1),
            "trend_icon": trend_icon,
            "trend_quality": trend_info["sla_quality"],
            "status_level": entity_status["status"],
            "status_icon": entity_status["icon"],
            "status_color": entity_status["color"],
        })

    all_hist = _get_aggregate_history(conn, recent_periods + _get_recent_periods(period, 6)[3:], parent_filter, view_level)
    trend = _compute_sla_trend(all_hist)
    trend.update(_compute_mttr_trend(all_hist))

    overall_status = determine_overall_status(kpis, trend, None, entities)

    entity_label = LEVEL_COLS.get(view_level, ("", "", ""))[2].replace("_name", "").title() if view_level in LEVEL_COLS else "Jaringan"
    narrative = generate_overall_narrative(
        f"Jaringan NOC-IS" if not parent_filter else entity_label,
        overall_status, kpis, trend, entities
    )

    sla_trend_chart = [{"period": h["period"], "value": h["sla_pct"]} for h in all_hist[-6:]]
    vol_trend_chart = [{"period": h["period"], "value": h.get("total_tickets", 0)} for h in all_hist[-6:]]

    behavior_dist = {"chronic": 0, "deteriorating": 0, "sporadic": 0, "seasonal": 0, "improving": 0, "healthy": 0, "total": len(entities)}
    risk_dist = {"high": 0, "medium": 0, "low": 0, "total": len(entities)}
    for e in entities:
        s = e["sla_pct"]
        if s < sla_target - 5:
            risk_dist["high"] += 1
        elif s < sla_target:
            risk_dist["medium"] += 1
        else:
            risk_dist["low"] += 1

        if e["status_level"] == "KRITIS":
            behavior_dist["chronic"] += 1
        elif e["status_level"] == "PERLU PERHATIAN" and e["trend_quality"] == "worsening":
            behavior_dist["deteriorating"] += 1
        elif e["status_level"] == "PERLU PERHATIAN":
            behavior_dist["sporadic"] += 1
        elif e["status_level"] == "SANGAT BAIK":
            behavior_dist["improving"] += 1
        else:
            behavior_dist["healthy"] += 1

    return {
        "overall_status": {**overall_status, "narrative": narrative},
        "kpi_snapshot": kpis,
        "entities": entities,
        "charts": {
            "sla_trend": sla_trend_chart,
            "volume_trend": vol_trend_chart,
            "sla_target": sla_target,
            "risk_distribution": risk_dist,
            "behavior_distribution": behavior_dist,
        },
        "period": period,
        "view_level": view_level,
        "parent_filter": parent_filter,
    }


def _prev_month(ym):
    try:
        y, m = int(ym[:4]), int(ym[5:7])
        if m == 1:
            return f"{y-1}-12"
        return f"{y}-{m-1:02d}"
    except Exception:
        return ym


def _get_recent_periods(period, n):
    periods = [period]
    ym = period
    for _ in range(n - 1):
        ym = _prev_month(ym)
        periods.insert(0, ym)
    return periods


def _get_entity_history(conn, col, entity_id, periods):
    if not periods:
        return []
    placeholders = ",".join("?" for _ in periods)
    try:
        rows = conn.execute(f"""
            SELECT year_month,
                   SUM(total_sla_met) as met,
                   SUM(total_tickets) as tkts,
                   AVG(avg_mttr_min) as mttr
            FROM summary_monthly
            WHERE {col} = ? AND year_month IN ({placeholders})
            GROUP BY year_month
            ORDER BY year_month
        """, [entity_id] + periods).fetchall()
        return [
            {"period": r[0], "sla_pct": round((r[1] / r[2] * 100) if r[2] > 0 else 0, 1),
             "total_tickets": r[2] or 0, "avg_mttr_min": r[3] or 0}
            for r in rows
        ]
    except Exception:
        return []


def _get_aggregate_history(conn, periods, parent_filter=None, view_level=None):
    if not periods:
        return []
    placeholders = ",".join("?" for _ in periods)
    params = list(periods)
    extra_where = ""
    if parent_filter and view_level:
        parent_level = PARENT_LEVEL.get(view_level)
        if parent_level:
            parent_col = LEVEL_COLS[parent_level][0]
            extra_where = f" AND {parent_col} = ?"
            params.append(parent_filter)
    try:
        rows = conn.execute(f"""
            SELECT year_month,
                   SUM(total_sla_met) as met,
                   SUM(total_tickets) as tkts,
                   AVG(avg_mttr_min) as mttr
            FROM summary_monthly
            WHERE year_month IN ({placeholders}) {extra_where}
            GROUP BY year_month
            ORDER BY year_month
        """, params).fetchall()
        return [
            {"period": r[0], "sla_pct": round((r[1] / r[2] * 100) if r[2] > 0 else 0, 1),
             "total_tickets": r[2] or 0, "avg_mttr_min": r[3] or 0}
            for r in rows
        ]
    except Exception:
        return []

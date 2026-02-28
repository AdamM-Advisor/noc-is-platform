import logging
from backend.database import get_connection
from backend.services.dashboard_service import (
    LEVEL_COLS, CHILD_LEVEL_MAP, determine_entity_status,
    _get_recent_periods, _get_entity_history, _compute_sla_trend,
)

logger = logging.getLogger(__name__)

KPI_LABELS = {
    "sla_pct": "SLA",
    "avg_mttr_min": "MTTR",
    "total_tickets": "Volume",
    "escalation_pct": "Eskalasi",
    "auto_resolve_pct": "Auto-resolve",
    "repeat_pct": "Repeat",
}

POSITIVE_UP = {"sla_pct", "auto_resolve_pct"}
NEGATIVE_UP = {"avg_mttr_min", "total_tickets", "escalation_pct", "repeat_pct"}


def validate_comparison(profile_a, profile_b):
    errors = []
    if profile_a["entity_level"] != profile_b["entity_level"]:
        errors.append("Perbandingan harus pada level yang sama.")
    a_key = (profile_a["entity_id"], profile_a.get("date_from"), profile_a.get("date_to"))
    b_key = (profile_b["entity_id"], profile_b.get("date_from"), profile_b.get("date_to"))
    if a_key == b_key:
        filters_a = profile_a.get("filters") or {}
        filters_b = profile_b.get("filters") or {}
        if filters_a == filters_b:
            errors.append("Kedua profil identik — tidak ada yang dibandingkan.")
    return {"valid": len(errors) == 0, "errors": errors}


def detect_comparison_type(profile_a, profile_b):
    same_entity = profile_a["entity_id"] == profile_b["entity_id"]
    same_period = (profile_a.get("date_from") == profile_b.get("date_from") and
                   profile_a.get("date_to") == profile_b.get("date_to"))
    if same_entity and not same_period:
        return "temporal"
    if not same_entity and same_period:
        return "entity"
    return "fault"


def aggregate_kpis(conn, entity_level, entity_id, date_from=None, date_to=None):
    col_info = LEVEL_COLS.get(entity_level)
    if not col_info:
        return {}
    col = col_info[0]

    where = f"WHERE {col} = ?"
    params = [entity_id]
    if date_from:
        where += " AND year_month >= ?"
        params.append(date_from[:7] if len(date_from) > 7 else date_from)
    if date_to:
        where += " AND year_month <= ?"
        params.append(date_to[:7] if len(date_to) > 7 else date_to)

    try:
        row = conn.execute(f"""
            SELECT SUM(total_tickets), SUM(total_sla_met), AVG(avg_mttr_min),
                   SUM(total_escalated), SUM(total_auto_resolved), SUM(total_repeat)
            FROM summary_monthly {where}
        """, params).fetchone()
    except Exception:
        return {}

    if not row or not row[0]:
        return {"sla_pct": 0, "avg_mttr_min": 0, "total_tickets": 0,
                "escalation_pct": 0, "auto_resolve_pct": 0, "repeat_pct": 0}

    vol = row[0] or 0
    met = row[1] or 0
    mttr = row[2] or 0
    esc = row[3] or 0
    auto_r = row[4] or 0
    repeat = row[5] or 0

    return {
        "sla_pct": round((met / vol * 100) if vol > 0 else 0, 1),
        "avg_mttr_min": round(mttr, 0),
        "total_tickets": vol,
        "escalation_pct": round((esc / vol * 100) if vol > 0 else 0, 1),
        "auto_resolve_pct": round((auto_r / vol * 100) if vol > 0 else 0, 1),
        "repeat_pct": round((repeat / vol * 100) if vol > 0 else 0, 1),
    }


def calculate_deltas(kpis_a, kpis_b):
    deltas = {}
    for kpi in ["sla_pct", "avg_mttr_min", "total_tickets",
                "escalation_pct", "auto_resolve_pct", "repeat_pct"]:
        val_a = kpis_a.get(kpi)
        val_b = kpis_b.get(kpi)
        if val_a is None or val_b is None:
            continue

        diff = val_b - val_a
        pct_change = (diff / val_a * 100) if val_a != 0 else 0

        if abs(diff) < 0.1:
            quality = "stable"
        elif kpi in POSITIVE_UP:
            quality = "improving" if diff > 0 else "worsening"
        else:
            quality = "improving" if diff < 0 else "worsening"

        deltas[kpi] = {
            "label": KPI_LABELS.get(kpi, kpi),
            "value_a": val_a,
            "value_b": val_b,
            "delta": round(diff, 2),
            "pct_change": round(pct_change, 1),
            "quality": quality,
            "icon": "✅" if quality == "improving" else ("❌" if quality == "worsening" else "─"),
        }
    return deltas


def get_entity_status(conn, entity_level, entity_id, date_from, date_to):
    kpis = aggregate_kpis(conn, entity_level, entity_id, date_from, date_to)
    sla = kpis.get("sla_pct", 0)
    sla_target = 90.0
    try:
        t = conn.execute("SELECT param_value FROM master_threshold WHERE param_key = 'sla_target'").fetchone()
        if t:
            sla_target = t[0]
    except Exception:
        pass
    period = date_to or date_from or ""
    periods = _get_recent_periods(period[:7] if period else "2025-07", 3)
    col = LEVEL_COLS.get(entity_level, ("area_id",))[0]
    hist = _get_entity_history(conn, col, entity_id, periods)
    trend = _compute_sla_trend(hist)
    status = determine_entity_status(sla, sla_target, trend["sla_quality"])
    return status


def get_children_delta(conn, entity_level, entity_id_a, entity_id_b,
                        date_from_a, date_to_a, date_from_b, date_to_b, comparison_type):
    child_level = CHILD_LEVEL_MAP.get(entity_level)
    if not child_level or child_level == "site":
        return []

    col_info = LEVEL_COLS.get(entity_level)
    c_info = LEVEL_COLS.get(child_level)
    if not col_info or not c_info:
        return []

    parent_col = col_info[0]
    c_col, c_tbl, c_name = c_info

    def get_child_kpis(eid, dfrom, dto):
        where = f"WHERE sm.{parent_col} = ?"
        params = [eid]
        if dfrom:
            where += " AND sm.year_month >= ?"
            params.append(dfrom[:7] if len(dfrom) > 7 else dfrom)
        if dto:
            where += " AND sm.year_month <= ?"
            params.append(dto[:7] if len(dto) > 7 else dto)
        try:
            rows = conn.execute(f"""
                SELECT sm.{c_col}, m.{c_name},
                       SUM(sm.total_tickets), SUM(sm.total_sla_met), AVG(sm.avg_mttr_min)
                FROM summary_monthly sm
                LEFT JOIN {c_tbl} m ON sm.{c_col} = m.{c_col}
                {where}
                GROUP BY sm.{c_col}, m.{c_name}
            """, params).fetchall()
            return {r[0]: {"name": r[1] or r[0], "vol": r[2] or 0, "met": r[3] or 0,
                           "sla": round((r[3] / r[2] * 100) if r[2] and r[2] > 0 else 0, 1),
                           "mttr": round(r[4] or 0, 0)} for r in rows}
        except Exception:
            return {}

    children_a = get_child_kpis(entity_id_a, date_from_a, date_to_a)
    children_b = get_child_kpis(entity_id_b, date_from_b, date_to_b)

    all_ids = set(list(children_a.keys()) + list(children_b.keys()))
    result = []
    for cid in sorted(all_ids):
        ca = children_a.get(cid, {})
        cb = children_b.get(cid, {})
        sla_a = ca.get("sla", 0)
        sla_b = cb.get("sla", 0)
        delta = round(sla_b - sla_a, 1)
        quality = "improving" if delta > 0.1 else ("worsening" if delta < -0.1 else "stable")
        result.append({
            "id": cid,
            "name": ca.get("name") or cb.get("name") or cid,
            "sla_a": sla_a,
            "sla_b": sla_b,
            "mttr_a": ca.get("mttr", 0),
            "mttr_b": cb.get("mttr", 0),
            "delta": delta,
            "quality": quality,
            "icon": "✅" if quality == "improving" else ("❌" if quality == "worsening" else "─"),
        })
    result.sort(key=lambda x: x["delta"])
    return result


def normalize_radar(kpis_a, kpis_b):
    axes = ["SLA", "MTTR_inv", "Volume_inv", "Esc_inv", "Auto", "Repeat_inv"]
    kpi_keys = ["sla_pct", "avg_mttr_min", "total_tickets", "escalation_pct", "auto_resolve_pct", "repeat_pct"]
    invert = {1, 2, 3, 5}

    vals_a = []
    vals_b = []
    for i, key in enumerate(kpi_keys):
        va = kpis_a.get(key, 0)
        vb = kpis_b.get(key, 0)
        max_val = max(abs(va), abs(vb), 1)

        na = (va / max_val) * 100
        nb = (vb / max_val) * 100

        if i in invert:
            na = 100 - na
            nb = 100 - nb

        vals_a.append(round(max(0, min(100, na)), 1))
        vals_b.append(round(max(0, min(100, nb)), 1))

    composite_a = round(sum(vals_a) / len(vals_a), 1)
    composite_b = round(sum(vals_b) / len(vals_b), 1)

    return {
        "axes": axes,
        "values_a": vals_a,
        "values_b": vals_b,
        "composite_a": composite_a,
        "composite_b": composite_b,
    }


def check_composition_similarity(conn, entity_level, entity_id_a, entity_id_b):
    col_info = LEVEL_COLS.get(entity_level)
    if not col_info:
        return {"similar": True, "total_diff_pp": 0}

    col = col_info[0]

    def get_comp(eid):
        try:
            rows = conn.execute(f"""
                SELECT s.classification, COUNT(*) as cnt
                FROM master_site s
                JOIN master_to t ON s.to_id = t.to_id
                JOIN master_nop n ON t.nop_id = n.nop_id
                JOIN master_regional r ON n.regional_id = r.regional_id
                WHERE (s.{col} = ? OR t.{col} = ? OR n.{col} = ? OR r.{col} = ?)
                  AND s.status = 'ACTIVE'
                GROUP BY s.classification
            """, [eid, eid, eid, eid]).fetchall()
        except Exception:
            return {}
        total = sum(r[1] for r in rows)
        if total == 0:
            return {}
        return {r[0]: round(r[1] / total * 100, 1) for r in rows}

    comp_a = get_comp(entity_id_a)
    comp_b = get_comp(entity_id_b)

    all_classes = set(list(comp_a.keys()) + list(comp_b.keys()))
    total_diff = sum(abs(comp_a.get(c, 0) - comp_b.get(c, 0)) for c in all_classes)
    similar = total_diff < 20

    result = {
        "similar": similar,
        "total_diff_pp": round(total_diff, 1),
        "composition_a": comp_a,
        "composition_b": comp_b,
    }

    if not similar:
        result["warning"] = (
            f"Komposisi site berbeda signifikan (gap {total_diff:.0f}pp). "
            f"Perbandingan SLA total bisa MISLEADING. "
            f"Lihat perbandingan per klasifikasi untuk analisis fair."
        )

    return result


def generate_comparison_narrative(entity_name, comparison_type, deltas, status_a, status_b, children_delta):
    parts = []

    n_improving = sum(1 for d in deltas.values() if d["quality"] == "improving")
    n_worsening = sum(1 for d in deltas.values() if d["quality"] == "worsening")
    total = len(deltas)

    if n_improving > n_worsening + 1:
        parts.append(f"{entity_name} menunjukkan perbaikan signifikan. {n_improving} dari {total} KPI membaik.")
    elif n_worsening > n_improving + 1:
        parts.append(f"{entity_name} menunjukkan penurunan. {n_worsening} dari {total} KPI memburuk.")
    else:
        parts.append(f"{entity_name} relatif stabil dengan perubahan minor.")

    improving_items = [(k, v) for k, v in deltas.items() if v["quality"] == "improving"]
    worsening_items = [(k, v) for k, v in deltas.items() if v["quality"] == "worsening"]

    if improving_items:
        best = max(improving_items, key=lambda x: abs(x[1]["pct_change"]))
        parts.append(f"Perbaikan terbesar: {best[1]['label']} ({best[1]['delta']:+.1f}).")
    if worsening_items:
        worst = max(worsening_items, key=lambda x: abs(x[1]["pct_change"]))
        parts.append(f"Penurunan: {worst[1]['label']} ({worst[1]['delta']:+.1f}).")

    if status_a.get("status") != status_b.get("status"):
        parts.append(f"Status berubah dari {status_a.get('status', '?')} ke {status_b.get('status', '?')}.")

    if children_delta:
        n_worse = sum(1 for c in children_delta if c["quality"] == "worsening")
        if n_worse > 0:
            worst_child = min(children_delta, key=lambda c: c["delta"])
            parts.append(f"Perhatian: {worst_child['name']} memburuk ({worst_child['delta']:+.1f}pp).")

    return " ".join(parts)


def generate_comparison(conn, profile_a, profile_b):
    validation = validate_comparison(profile_a, profile_b)
    if not validation["valid"]:
        return {"error": True, "errors": validation["errors"]}

    comparison_type = detect_comparison_type(profile_a, profile_b)

    kpis_a = aggregate_kpis(conn, profile_a["entity_level"], profile_a["entity_id"],
                            profile_a.get("date_from"), profile_a.get("date_to"))
    kpis_b = aggregate_kpis(conn, profile_b["entity_level"], profile_b["entity_id"],
                            profile_b.get("date_from"), profile_b.get("date_to"))

    deltas = calculate_deltas(kpis_a, kpis_b)

    status_a = get_entity_status(conn, profile_a["entity_level"], profile_a["entity_id"],
                                  profile_a.get("date_from"), profile_a.get("date_to"))
    status_b = get_entity_status(conn, profile_b["entity_level"], profile_b["entity_id"],
                                  profile_b.get("date_from"), profile_b.get("date_to"))

    children_delta = get_children_delta(
        conn, profile_a["entity_level"],
        profile_a["entity_id"], profile_b["entity_id"],
        profile_a.get("date_from"), profile_a.get("date_to"),
        profile_b.get("date_from"), profile_b.get("date_to"),
        comparison_type,
    )

    entity_name_a = profile_a.get("entity_name") or profile_a["entity_id"]
    entity_name_b = profile_b.get("entity_name") or profile_b["entity_id"]
    entity_label = entity_name_a if comparison_type == "temporal" else f"{entity_name_a} vs {entity_name_b}"

    narrative = generate_comparison_narrative(
        entity_label, comparison_type, deltas, status_a, status_b, children_delta
    )

    radar = normalize_radar(kpis_a, kpis_b)

    composition_check = None
    if comparison_type == "entity":
        composition_check = check_composition_similarity(
            conn, profile_a["entity_level"],
            profile_a["entity_id"], profile_b["entity_id"]
        )

    col = LEVEL_COLS.get(profile_a["entity_level"], ("area_id",))[0]
    period_a = (profile_a.get("date_to") or profile_a.get("date_from") or "2025-07")[:7]
    period_b = (profile_b.get("date_to") or profile_b.get("date_from") or "2025-07")[:7]
    periods_a = _get_recent_periods(period_a, 3)
    periods_b = _get_recent_periods(period_b, 3)
    hist_a = _get_entity_history(conn, col, profile_a["entity_id"], periods_a)
    hist_b = _get_entity_history(conn, col, profile_b["entity_id"], periods_b)
    trend_a = [{"period": h["period"], "value": h["sla_pct"]} for h in hist_a]
    trend_b = [{"period": h["period"], "value": h["sla_pct"]} for h in hist_b]

    return {
        "comparison_type": comparison_type,
        "profile_a": {
            "entity_level": profile_a["entity_level"],
            "entity_id": profile_a["entity_id"],
            "entity_name": entity_name_a,
            "date_from": profile_a.get("date_from"),
            "date_to": profile_a.get("date_to"),
        },
        "profile_b": {
            "entity_level": profile_b["entity_level"],
            "entity_id": profile_b["entity_id"],
            "entity_name": entity_name_b,
            "date_from": profile_b.get("date_from"),
            "date_to": profile_b.get("date_to"),
        },
        "kpis_a": kpis_a,
        "kpis_b": kpis_b,
        "deltas": deltas,
        "status_a": status_a,
        "status_b": status_b,
        "children_delta": children_delta,
        "narrative": narrative,
        "radar": radar,
        "composition_check": composition_check,
        "trend_a": trend_a,
        "trend_b": trend_b,
    }

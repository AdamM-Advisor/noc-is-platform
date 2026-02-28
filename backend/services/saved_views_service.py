import logging
import json
from datetime import datetime
from backend.database import get_connection

logger = logging.getLogger(__name__)

LEVEL_COLS = {
    "area": ("area_id", "master_area", "area_name"),
    "regional": ("regional_id", "master_regional", "regional_name"),
    "nop": ("nop_id", "master_nop", "nop_name"),
    "to": ("to_id", "master_to", "to_name"),
}


def _row_to_dict(row, columns):
    return dict(zip(columns, row)) if row else None


def _get_columns():
    return [
        "id", "name", "description", "entity_level", "entity_id", "entity_name",
        "granularity", "date_from", "date_to", "type_ticket", "severities",
        "fault_level", "rc_category",
        "snapshot_sla", "snapshot_mttr", "snapshot_volume",
        "snapshot_escalation", "snapshot_auto_resolve", "snapshot_repeat",
        "snapshot_behavior", "snapshot_status", "snapshot_risk_score",
        "created_at", "updated_at", "last_accessed_at",
        "access_count", "is_pinned", "sort_order", "url_params",
    ]


def list_saved_views(conn):
    cols = _get_columns()
    rows = conn.execute(f"""
        SELECT {', '.join(cols)} FROM saved_views
        ORDER BY is_pinned DESC, sort_order ASC, last_accessed_at DESC NULLS LAST, created_at DESC
    """).fetchall()
    return [_row_to_dict(r, cols) for r in rows]


def get_saved_view(conn, view_id):
    cols = _get_columns()
    row = conn.execute(
        f"SELECT {', '.join(cols)} FROM saved_views WHERE id = ?", [view_id]
    ).fetchone()
    return _row_to_dict(row, cols)


def create_saved_view(conn, data):
    now = datetime.now().isoformat()
    max_id = conn.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM saved_views").fetchone()[0]
    conn.execute("""
        INSERT INTO saved_views (
            id, name, description, entity_level, entity_id, entity_name,
            granularity, date_from, date_to, type_ticket, severities,
            fault_level, rc_category,
            snapshot_sla, snapshot_mttr, snapshot_volume,
            snapshot_escalation, snapshot_auto_resolve, snapshot_repeat,
            snapshot_behavior, snapshot_status, snapshot_risk_score,
            created_at, updated_at, is_pinned, sort_order, url_params,
            access_count, last_accessed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, NULL)
    """, [
        max_id,
        data.get("name", ""),
        data.get("description", ""),
        data.get("entity_level", ""),
        data.get("entity_id", ""),
        data.get("entity_name", ""),
        data.get("granularity", "monthly"),
        data.get("date_from", ""),
        data.get("date_to", ""),
        data.get("type_ticket", ""),
        json.dumps(data.get("severities", [])) if data.get("severities") else None,
        data.get("fault_level", ""),
        data.get("rc_category", ""),
        data.get("snapshot_sla"),
        data.get("snapshot_mttr"),
        data.get("snapshot_volume"),
        data.get("snapshot_escalation"),
        data.get("snapshot_auto_resolve"),
        data.get("snapshot_repeat"),
        data.get("snapshot_behavior", ""),
        data.get("snapshot_status", ""),
        data.get("snapshot_risk_score"),
        now, now,
        data.get("is_pinned", False),
        data.get("sort_order", 0),
        data.get("url_params", ""),
    ])
    return {"id": max_id, "created": True}


def update_saved_view(conn, view_id, data):
    now = datetime.now().isoformat()
    sets = []
    params = []
    for key in ["name", "description", "is_pinned", "sort_order"]:
        if key in data:
            sets.append(f"{key} = ?")
            params.append(data[key])
    if sets:
        sets.append("updated_at = ?")
        params.append(now)
        params.append(view_id)
        conn.execute(f"UPDATE saved_views SET {', '.join(sets)} WHERE id = ?", params)
    return {"updated": True}


def delete_saved_view(conn, view_id):
    conn.execute("DELETE FROM saved_views WHERE id = ?", [view_id])
    return {"deleted": True}


def record_access(conn, view_id):
    now = datetime.now().isoformat()
    conn.execute("""
        UPDATE saved_views
        SET last_accessed_at = ?, access_count = access_count + 1
        WHERE id = ?
    """, [now, view_id])
    return {"accessed": True}


def toggle_pin(conn, view_id):
    row = conn.execute("SELECT is_pinned FROM saved_views WHERE id = ?", [view_id]).fetchone()
    if not row:
        return {"error": "Not found"}
    new_pin = not row[0]
    if new_pin:
        pinned_count = conn.execute("SELECT COUNT(*) FROM saved_views WHERE is_pinned = TRUE").fetchone()[0]
        if pinned_count >= 5:
            return {"error": "Maksimal 5 pinned views", "is_pinned": False}
    conn.execute("UPDATE saved_views SET is_pinned = ?, updated_at = ? WHERE id = ?",
                 [new_pin, datetime.now().isoformat(), view_id])
    return {"is_pinned": new_pin}


def reorder_pinned(conn, order_list):
    for item in order_list:
        conn.execute("UPDATE saved_views SET sort_order = ? WHERE id = ?",
                     [item["sort_order"], item["id"]])
    return {"reordered": True}


def get_current_kpis(conn, entity_level, entity_id, date_from=None, date_to=None):
    col_info = LEVEL_COLS.get(entity_level)
    if not col_info:
        return {}
    col = col_info[0]

    where = f"WHERE {col} = ?"
    params = [entity_id]
    if date_from:
        where += " AND year_month >= ?"
        params.append(date_from[:7])
    if date_to:
        where += " AND year_month <= ?"
        params.append(date_to[:7])

    try:
        row = conn.execute(f"""
            SELECT SUM(total_tickets), SUM(total_sla_met), AVG(avg_mttr_min),
                   SUM(total_escalated), SUM(total_auto_resolved), SUM(total_repeat)
            FROM summary_monthly {where}
        """, params).fetchone()
    except Exception:
        return {}

    if not row or not row[0]:
        return {}

    vol = row[0] or 0
    met = row[1] or 0
    mttr = row[2] or 0
    esc = row[3] or 0
    auto_r = row[4] or 0
    repeat = row[5] or 0

    return {
        "sla": round((met / vol * 100) if vol > 0 else 0, 1),
        "mttr": round(mttr, 0),
        "volume": vol,
        "escalation": round((esc / vol * 100) if vol > 0 else 0, 1),
        "auto_resolve": round((auto_r / vol * 100) if vol > 0 else 0, 1),
        "repeat": round((repeat / vol * 100) if vol > 0 else 0, 1),
    }


def get_saved_view_with_delta(conn, view_id):
    view = get_saved_view(conn, view_id)
    if not view:
        return None

    current = get_current_kpis(conn, view["entity_level"], view["entity_id"],
                                view.get("date_from"), view.get("date_to"))

    deltas = {}
    positive_up = {"sla", "auto_resolve"}
    for kpi in ["sla", "mttr", "volume", "escalation", "auto_resolve", "repeat"]:
        snap_val = view.get(f"snapshot_{kpi}")
        curr_val = current.get(kpi)
        if snap_val is not None and curr_val is not None:
            diff = curr_val - snap_val
            if abs(diff) < 0.1:
                quality = "stable"
            elif kpi in positive_up:
                quality = "improving" if diff > 0 else "worsening"
            else:
                quality = "improving" if diff < 0 else "worsening"
            deltas[kpi] = {
                "snapshot": snap_val,
                "current": curr_val,
                "delta": round(diff, 2),
                "quality": quality,
            }

    return {**view, "deltas": deltas, "current_kpis": current}

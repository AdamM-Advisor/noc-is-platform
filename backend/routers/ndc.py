import logging
from fastapi import APIRouter, Query, Body, BackgroundTasks
from typing import Optional
from backend.services.ndc_service import (
    full_refresh, get_ndc_list, get_ndc_detail, get_confusion_matrix,
    get_ndc_for_entity, get_ndc_for_site, update_ndc_curation,
)
from backend.database import get_connection, get_write_connection

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ndc", tags=["ndc"])


@router.get("")
async def list_ndc(
    category: Optional[str] = Query(None),
    priority: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    sort_by: str = Query("total_tickets"),
    sort_dir: str = Query("desc"),
    limit: int = Query(100),
    offset: int = Query(0),
):
    return get_ndc_list(category, priority, status, search, sort_by, sort_dir, limit, offset)


@router.get("/confusion-matrix")
async def confusion_matrix():
    return get_confusion_matrix()


@router.get("/export")
async def export_ndc(format: str = Query("json")):
    data = get_ndc_list(limit=9999, offset=0)
    return data


@router.post("/refresh")
async def refresh_ndc(background_tasks: BackgroundTasks):
    background_tasks.add_task(full_refresh)
    return {"status": "started", "message": "NDC refresh dimulai di background"}


@router.get("/refresh-status")
async def refresh_status():
    with get_connection() as conn:
        count = conn.execute("SELECT COUNT(*) FROM ndc_entries").fetchone()[0]
    return {"entries": count, "status": "idle"}


@router.get("/entity/{entity_level}/{entity_id}")
async def ndc_for_entity(
    entity_level: str,
    entity_id: str,
    limit: int = Query(10),
):
    return get_ndc_for_entity(entity_level, entity_id, limit)


@router.get("/site/{site_id}")
async def ndc_for_site(site_id: str):
    return get_ndc_for_site(site_id)


@router.get("/{code}")
async def detail_ndc(code: str):
    result = get_ndc_detail(code)
    if not result:
        return {"error": "NDC not found"}
    return result


@router.put("/{code}")
async def update_ndc(
    code: str,
    body: dict = Body(...),
):
    return update_ndc_curation(
        code,
        status=body.get("status"),
        notes=body.get("notes"),
        differentiator=body.get("differentiator"),
        reviewed_by=body.get("reviewed_by"),
    )


@router.get("/{code}/alarm-snapshot")
async def alarm_snapshot(code: str):
    with get_connection() as conn:
        cols = [d[0] for d in conn.execute("SELECT * FROM ndc_alarm_snapshot LIMIT 0").description]
        snap = conn.execute("SELECT * FROM ndc_alarm_snapshot WHERE ndc_code = ?", [code]).fetchone()
        if not snap:
            return {"error": "No alarm snapshot"}

        co_cols = [d[0] for d in conn.execute("SELECT * FROM ndc_co_occurring_alarms LIMIT 0").description]
        co_rows = conn.execute("SELECT * FROM ndc_co_occurring_alarms WHERE ndc_code = ? ORDER BY co_occurrence_pct DESC", [code]).fetchall()

        return {
            "snapshot": dict(zip(cols, snap)),
            "co_occurring_alarms": [dict(zip(co_cols, r)) for r in co_rows],
        }


@router.get("/{code}/symptoms")
async def get_symptoms(code: str):
    with get_connection() as conn:
        cols = [d[0] for d in conn.execute("SELECT * FROM ndc_symptoms LIMIT 0").description]
        rows = conn.execute("SELECT * FROM ndc_symptoms WHERE ndc_code = ? ORDER BY symptom_type, sort_order", [code]).fetchall()
        return [dict(zip(cols, r)) for r in rows]


@router.post("/{code}/symptoms")
async def add_symptom(code: str, body: dict = Body(...)):
    with get_write_connection() as wconn:
        max_id = wconn.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM ndc_symptoms").fetchone()[0]
        wconn.execute("""
            INSERT INTO ndc_symptoms (id, ndc_code, symptom_text, symptom_type, frequency_pct, confidence, source, sort_order, is_auto_generated, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, FALSE, CURRENT_TIMESTAMP)
        """, [max_id, code, body.get("symptom_text", ""), body.get("symptom_type", "primary"),
              body.get("frequency_pct"), body.get("confidence", "medium"), body.get("source", "manual"),
              body.get("sort_order", 0)])
    return {"status": "added", "id": max_id}


@router.put("/{code}/symptoms/{symptom_id}")
async def update_symptom(code: str, symptom_id: int, body: dict = Body(...)):
    with get_write_connection() as wconn:
        updates = []
        params = []
        for field in ["symptom_text", "symptom_type", "frequency_pct", "confidence", "source", "negative_note", "redirect_ndc", "sort_order", "reviewed"]:
            if field in body:
                updates.append(f"{field} = ?")
                params.append(body[field])
        if updates:
            params.append(symptom_id)
            wconn.execute(f"UPDATE ndc_symptoms SET {', '.join(updates)} WHERE id = ?", params)
    return {"status": "updated"}


@router.delete("/{code}/symptoms/{symptom_id}")
async def delete_symptom(code: str, symptom_id: int):
    with get_write_connection() as wconn:
        wconn.execute("DELETE FROM ndc_symptoms WHERE id = ? AND ndc_code = ?", [symptom_id, code])
    return {"status": "deleted"}


@router.get("/{code}/diagnostic-tree")
async def get_diagnostic_tree(code: str):
    with get_connection() as conn:
        cols = [d[0] for d in conn.execute("SELECT * FROM ndc_diagnostic_steps LIMIT 0").description]
        rows = conn.execute("SELECT * FROM ndc_diagnostic_steps WHERE ndc_code = ? ORDER BY step_number", [code]).fetchall()
        return [dict(zip(cols, r)) for r in rows]


@router.put("/{code}/diagnostic-tree")
async def update_diagnostic_tree(code: str, body: list = Body(...)):
    with get_write_connection() as wconn:
        wconn.execute("DELETE FROM ndc_diagnostic_steps WHERE ndc_code = ?", [code])
        for step in body:
            max_id = wconn.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM ndc_diagnostic_steps").fetchone()[0]
            wconn.execute("""
                INSERT INTO ndc_diagnostic_steps (id, ndc_code, step_number, action, expected_result, if_yes, if_yes_goto_step, if_no, if_no_goto_step, if_no_redirect_ndc, avg_duration_min, success_rate_at_step, cumulative_resolve_pct, is_auto_generated, reviewed, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE, CURRENT_TIMESTAMP)
            """, [max_id, code, step.get("step_number", 0), step.get("action", ""),
                  step.get("expected_result"), step.get("if_yes"), step.get("if_yes_goto_step"),
                  step.get("if_no"), step.get("if_no_goto_step"), step.get("if_no_redirect_ndc"),
                  step.get("avg_duration_min"), step.get("success_rate_at_step"),
                  step.get("cumulative_resolve_pct"), step.get("is_auto_generated", False)])
    return {"status": "updated"}


@router.get("/{code}/sop")
async def get_sop(code: str):
    with get_connection() as conn:
        path_cols = [d[0] for d in conn.execute("SELECT * FROM ndc_resolution_paths LIMIT 0").description]
        path_rows = conn.execute("SELECT * FROM ndc_resolution_paths WHERE ndc_code = ? ORDER BY sort_order", [code]).fetchall()
        paths = []
        for pr in path_rows:
            p = dict(zip(path_cols, pr))
            step_cols = [d[0] for d in conn.execute("SELECT * FROM ndc_resolution_steps LIMIT 0").description]
            steps = conn.execute("SELECT * FROM ndc_resolution_steps WHERE path_id = ? ORDER BY step_number", [p['id']]).fetchall()
            p['steps'] = [dict(zip(step_cols, s)) for s in steps]
            paths.append(p)

        esc_cols = [d[0] for d in conn.execute("SELECT * FROM ndc_escalation_matrix LIMIT 0").description]
        esc_rows = conn.execute("SELECT * FROM ndc_escalation_matrix WHERE ndc_code = ? ORDER BY tier", [code]).fetchall()

        prev_cols = [d[0] for d in conn.execute("SELECT * FROM ndc_preventive_actions LIMIT 0").description]
        prev_rows = conn.execute("SELECT * FROM ndc_preventive_actions WHERE ndc_code = ? ORDER BY sort_order", [code]).fetchall()

        return {
            "resolution_paths": paths,
            "escalation_matrix": [dict(zip(esc_cols, r)) for r in esc_rows],
            "preventive_actions": [dict(zip(prev_cols, r)) for r in prev_rows],
        }


@router.post("/{code}/sop/preventive")
async def add_preventive(code: str, body: dict = Body(...)):
    with get_write_connection() as wconn:
        max_id = wconn.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM ndc_preventive_actions").fetchone()[0]
        wconn.execute("""
            INSERT INTO ndc_preventive_actions (id, ndc_code, action, expected_impact, effort_level, sort_order)
            VALUES (?, ?, ?, ?, ?, ?)
        """, [max_id, code, body.get("action", ""), body.get("expected_impact"),
              body.get("effort_level", "MEDIUM"), body.get("sort_order", 0)])
    return {"status": "added", "id": max_id}


@router.put("/{code}/sop/preventive/{action_id}")
async def update_preventive(code: str, action_id: int, body: dict = Body(...)):
    with get_write_connection() as wconn:
        updates = []
        params = []
        for field in ["action", "expected_impact", "effort_level", "sort_order"]:
            if field in body:
                updates.append(f"{field} = ?")
                params.append(body[field])
        if updates:
            params.append(action_id)
            wconn.execute(f"UPDATE ndc_preventive_actions SET {', '.join(updates)} WHERE id = ?", params)
    return {"status": "updated"}


@router.delete("/{code}/sop/preventive/{action_id}")
async def delete_preventive(code: str, action_id: int):
    with get_write_connection() as wconn:
        wconn.execute("DELETE FROM ndc_preventive_actions WHERE id = ? AND ndc_code = ?", [action_id, code])
    return {"status": "deleted"}


@router.put("/{code}/sop/escalation")
async def update_escalation(code: str, body: list = Body(...)):
    with get_write_connection() as wconn:
        wconn.execute("DELETE FROM ndc_escalation_matrix WHERE ndc_code = ?", [code])
        for entry in body:
            max_id = wconn.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM ndc_escalation_matrix").fetchone()[0]
            wconn.execute("""
                INSERT INTO ndc_escalation_matrix (id, ndc_code, tier, role, action, max_duration, sort_order)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [max_id, code, entry.get("tier", 1), entry.get("role", ""),
                  entry.get("action", ""), entry.get("max_duration"), entry.get("sort_order", 0)])
    return {"status": "updated"}

from fastapi import APIRouter, HTTPException, Query
from backend.database import get_write_connection, get_connection
from backend.services.backup_service import create_backup

router = APIRouter(prefix="/data")


@router.delete("/tickets")
async def delete_tickets(
    period: str = Query(None),
    source: str = Query(None),
):
    if not period and not source:
        raise HTTPException(status_code=400, detail="At least one filter (period or source) is required")

    try:
        create_backup()
    except Exception:
        pass

    conditions = []
    params = []
    if period:
        conditions.append("calc_year_month = ?")
        params.append(period)
    if source:
        conditions.append("calc_source = ?")
        params.append(source)

    where_clause = " AND ".join(conditions)

    with get_write_connection() as conn:
        count = conn.execute(
            f"SELECT COUNT(*) FROM noc_tickets WHERE {where_clause}", params
        ).fetchone()[0]

        conn.execute(f"DELETE FROM noc_tickets WHERE {where_clause}", params)

    if period:
        from backend.services.summary_service import refresh_summaries
        refresh_summaries([period])

    return {"deleted": count, "period": period, "source": source}


@router.delete("/tickets/by-import/{import_id}")
async def delete_by_import(import_id: int):
    with get_connection() as conn:
        row = conn.execute(
            "SELECT file_type, period FROM import_logs WHERE id = ?", [import_id]
        ).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Import not found")

    file_type, period = row

    try:
        create_backup()
    except Exception:
        pass

    with get_write_connection() as conn:
        if period:
            count = conn.execute("""
                SELECT COUNT(*) FROM noc_tickets
                WHERE calc_source = ? AND calc_year_month = ?
            """, [file_type, period]).fetchone()[0]

            conn.execute("""
                DELETE FROM noc_tickets
                WHERE calc_source = ? AND calc_year_month = ?
            """, [file_type, period])
        else:
            count = conn.execute(
                "SELECT COUNT(*) FROM noc_tickets WHERE calc_source = ?", [file_type]
            ).fetchone()[0]
            conn.execute("DELETE FROM noc_tickets WHERE calc_source = ?", [file_type])

    if period:
        from backend.services.summary_service import refresh_summaries
        refresh_summaries([period])

    return {"deleted": count, "import_id": import_id, "period": period, "source": file_type}

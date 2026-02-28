from fastapi import APIRouter, HTTPException
from backend.database import get_connection, get_write_connection

router = APIRouter(prefix="/orphans")


@router.get("")
async def list_orphans():
    try:
        with get_connection() as conn:
            rows = conn.execute("""
                SELECT id, source, level, value, suggested_match, resolved,
                       resolved_to, first_seen, ticket_count
                FROM orphan_log
                WHERE resolved = FALSE
                ORDER BY level, value
            """).fetchall()
            columns = ["id", "source", "level", "value", "suggested_match",
                        "resolved", "resolved_to", "first_seen", "ticket_count"]
            items = [dict(zip(columns, row)) for row in rows]

        grouped = {}
        for item in items:
            level = item["level"]
            if level not in grouped:
                grouped[level] = []
            grouped[level].append(item)

        return {"orphans": grouped, "total": len(items)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{orphan_id}/resolve")
async def resolve_orphan(orphan_id: int, data: dict):
    resolved_to = data.get("resolved_to")
    if not resolved_to:
        raise HTTPException(status_code=400, detail="resolved_to is required")

    try:
        with get_write_connection() as conn:
            row = conn.execute(
                "SELECT level, value FROM orphan_log WHERE id = ?", [orphan_id]
            ).fetchone()

            if not row:
                raise HTTPException(status_code=404, detail="Orphan not found")

            level, value = row

            conn.execute("""
                UPDATE orphan_log SET
                    resolved = TRUE,
                    resolved_to = ?,
                    resolved_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, [resolved_to, orphan_id])

            if level == "area":
                conn.execute("""
                    UPDATE noc_tickets SET calc_area_id = ?
                    WHERE area = ? AND (calc_area_id IS NULL)
                """, [resolved_to, value])
            elif level == "regional":
                conn.execute("""
                    UPDATE noc_tickets SET calc_regional_id = ?
                    WHERE regional = ? AND (calc_regional_id IS NULL)
                """, [resolved_to, value])
            elif level == "nop":
                conn.execute("""
                    UPDATE noc_tickets SET calc_nop_id = ?
                    WHERE nop = ? AND (calc_nop_id IS NULL)
                """, [resolved_to, value])
            elif level == "to":
                conn.execute("""
                    UPDATE noc_tickets SET calc_to_id = ?
                    WHERE cluster_to = ? AND (calc_to_id IS NULL)
                """, [resolved_to, value])

        return {"status": "resolved", "orphan_id": orphan_id, "resolved_to": resolved_to}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

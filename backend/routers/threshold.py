from fastapi import APIRouter, HTTPException
from backend.database import get_connection, get_write_connection

router = APIRouter(prefix="/threshold")


@router.get("")
async def get_all_thresholds():
    try:
        with get_connection() as conn:
            rows = conn.execute(
                "SELECT param_key, param_value, param_unit, category, description FROM master_threshold ORDER BY category, param_key"
            ).fetchall()
            columns = ["param_key", "param_value", "param_unit", "category", "description"]
            items = [dict(zip(columns, row)) for row in rows]

        categories = {}
        for item in items:
            cat = item["category"]
            if cat not in categories:
                categories[cat] = []
            categories[cat].append(item)

        return {"categories": categories}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{param_key}")
async def get_threshold(param_key: str):
    try:
        with get_connection() as conn:
            row = conn.execute(
                "SELECT param_key, param_value, param_unit, category, description FROM master_threshold WHERE param_key = ?",
                [param_key],
            ).fetchone()

        if not row:
            raise HTTPException(status_code=404, detail=f"Threshold '{param_key}' not found")

        columns = ["param_key", "param_value", "param_unit", "category", "description"]
        return dict(zip(columns, row))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{param_key}")
async def update_threshold(param_key: str, data: dict):
    param_value = data.get("param_value")
    if param_value is None:
        raise HTTPException(status_code=400, detail="param_value is required")

    try:
        with get_write_connection() as conn:
            exists = conn.execute(
                "SELECT COUNT(*) FROM master_threshold WHERE param_key = ?", [param_key]
            ).fetchone()[0]
            if exists == 0:
                raise HTTPException(status_code=404, detail=f"Threshold '{param_key}' not found")

            conn.execute(
                "UPDATE master_threshold SET param_value = ?, updated_at = CURRENT_TIMESTAMP WHERE param_key = ?",
                [param_value, param_key],
            )

        with get_connection() as conn:
            row = conn.execute(
                "SELECT param_key, param_value, param_unit, category, description FROM master_threshold WHERE param_key = ?",
                [param_key],
            ).fetchone()
            columns = ["param_key", "param_value", "param_unit", "category", "description"]
            return dict(zip(columns, row))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

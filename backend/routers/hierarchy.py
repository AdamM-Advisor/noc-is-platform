from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
from backend.database import get_connection, get_write_connection

router = APIRouter(prefix="/master")

LEVELS = {
    "area": {
        "table": "master_area",
        "id_col": "area_id",
        "name_col": "area_name",
        "parent_id_col": None,
        "child_table": "master_regional",
        "child_parent_col": "area_id",
        "columns": ["area_id", "area_name", "area_alias", "description", "status", "created_at", "updated_at"],
        "editable": ["area_name", "area_alias", "description", "status"],
    },
    "regional": {
        "table": "master_regional",
        "id_col": "regional_id",
        "name_col": "regional_name",
        "parent_id_col": "area_id",
        "child_table": "master_nop",
        "child_parent_col": "regional_id",
        "columns": ["regional_id", "regional_name", "area_id", "regional_alias_site_master", "regional_alias_ticket", "description", "status", "created_at", "updated_at"],
        "editable": ["regional_name", "area_id", "regional_alias_site_master", "regional_alias_ticket", "description", "status"],
    },
    "nop": {
        "table": "master_nop",
        "id_col": "nop_id",
        "name_col": "nop_name",
        "parent_id_col": "regional_id",
        "child_table": "master_to",
        "child_parent_col": "nop_id",
        "columns": ["nop_id", "nop_name", "regional_id", "nop_alias_site_master", "nop_alias_ticket", "description", "status", "created_at", "updated_at"],
        "editable": ["nop_name", "regional_id", "nop_alias_site_master", "nop_alias_ticket", "description", "status"],
    },
    "to": {
        "table": "master_to",
        "id_col": "to_id",
        "name_col": "to_name",
        "parent_id_col": "nop_id",
        "child_table": "master_site",
        "child_parent_col": "to_id",
        "columns": ["to_id", "to_name", "nop_id", "to_alias_site_master", "to_alias_ticket", "description", "status", "created_at", "updated_at"],
        "editable": ["to_name", "nop_id", "to_alias_site_master", "to_alias_ticket", "description", "status"],
    },
}


class AreaCreate(BaseModel):
    area_id: str
    area_name: str
    area_alias: Optional[str] = None
    description: Optional[str] = None
    status: Optional[str] = "ACTIVE"


class AreaUpdate(BaseModel):
    area_name: Optional[str] = None
    area_alias: Optional[str] = None
    description: Optional[str] = None
    status: Optional[str] = None


class RegionalCreate(BaseModel):
    regional_id: str
    regional_name: str
    area_id: str
    regional_alias_site_master: Optional[str] = None
    regional_alias_ticket: Optional[str] = None
    description: Optional[str] = None
    status: Optional[str] = "ACTIVE"


class RegionalUpdate(BaseModel):
    regional_name: Optional[str] = None
    area_id: Optional[str] = None
    regional_alias_site_master: Optional[str] = None
    regional_alias_ticket: Optional[str] = None
    description: Optional[str] = None
    status: Optional[str] = None


class NopCreate(BaseModel):
    nop_id: str
    nop_name: str
    regional_id: str
    nop_alias_site_master: Optional[str] = None
    nop_alias_ticket: Optional[str] = None
    description: Optional[str] = None
    status: Optional[str] = "ACTIVE"


class NopUpdate(BaseModel):
    nop_name: Optional[str] = None
    regional_id: Optional[str] = None
    nop_alias_site_master: Optional[str] = None
    nop_alias_ticket: Optional[str] = None
    description: Optional[str] = None
    status: Optional[str] = None


class ToCreate(BaseModel):
    to_id: str
    to_name: str
    nop_id: str
    to_alias_site_master: Optional[str] = None
    to_alias_ticket: Optional[str] = None
    description: Optional[str] = None
    status: Optional[str] = "ACTIVE"


class ToUpdate(BaseModel):
    to_name: Optional[str] = None
    nop_id: Optional[str] = None
    to_alias_site_master: Optional[str] = None
    to_alias_ticket: Optional[str] = None
    description: Optional[str] = None
    status: Optional[str] = None


class DeleteBody(BaseModel):
    cascade: Optional[bool] = False


def _fetch_all(conn, table, columns, filters=None):
    query = f'SELECT {", ".join(columns)} FROM {table}'
    params = []
    if filters:
        clauses = []
        for k, v in filters.items():
            clauses.append(f"{k} = ?")
            params.append(v)
        query += " WHERE " + " AND ".join(clauses)
    query += f" ORDER BY {columns[1]}"
    rows = conn.execute(query, params).fetchall()
    return [dict(zip(columns, row)) for row in rows]


def _get_by_id(conn, table, id_col, id_val, columns):
    row = conn.execute(
        f'SELECT {", ".join(columns)} FROM {table} WHERE {id_col} = ?', [id_val]
    ).fetchone()
    if not row:
        return None
    return dict(zip(columns, row))


def _count_active_children(conn, child_table, parent_col, parent_id):
    result = conn.execute(
        f"SELECT COUNT(*) FROM {child_table} WHERE {parent_col} = ? AND status = 'ACTIVE'",
        [parent_id],
    ).fetchone()
    return result[0]


def _cascade_deactivate(conn, level_key, parent_id):
    meta = LEVELS[level_key]
    child_table = meta["child_table"]
    child_parent_col = meta["child_parent_col"]
    child_id_col = None

    if level_key == "area":
        child_id_col = "regional_id"
        next_level = "regional"
    elif level_key == "regional":
        child_id_col = "nop_id"
        next_level = "nop"
    elif level_key == "nop":
        child_id_col = "to_id"
        next_level = "to"
    elif level_key == "to":
        conn.execute(
            f"UPDATE master_site SET status = 'INACTIVE', updated_at = CURRENT_TIMESTAMP WHERE to_id = ? AND status = 'ACTIVE'",
            [parent_id],
        )
        return

    children = conn.execute(
        f"SELECT {child_id_col} FROM {child_table} WHERE {child_parent_col} = ? AND status = 'ACTIVE'",
        [parent_id],
    ).fetchall()

    for (child_id,) in children:
        _cascade_deactivate(conn, next_level, child_id)

    conn.execute(
        f"UPDATE {child_table} SET status = 'INACTIVE', updated_at = CURRENT_TIMESTAMP WHERE {child_parent_col} = ? AND status = 'ACTIVE'",
        [parent_id],
    )


# ── Area endpoints ──

@router.get("/area")
async def list_areas():
    try:
        with get_connection() as conn:
            return _fetch_all(conn, "master_area", LEVELS["area"]["columns"])
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/area")
async def create_area(body: AreaCreate):
    try:
        with get_write_connection() as conn:
            conn.execute(
                "INSERT INTO master_area (area_id, area_name, area_alias, description, status) VALUES (?, ?, ?, ?, ?)",
                [body.area_id, body.area_name, body.area_alias, body.description, body.status],
            )
        return {"status": "created", "area_id": body.area_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/area/{area_id}")
async def update_area(area_id: str, body: AreaUpdate):
    try:
        updates = {k: v for k, v in body.dict().items() if v is not None}
        if not updates:
            raise HTTPException(status_code=400, detail="No fields to update")
        set_clause = ", ".join(f"{k} = ?" for k in updates)
        params = list(updates.values()) + [area_id]
        with get_write_connection() as conn:
            existing = conn.execute("SELECT area_id FROM master_area WHERE area_id = ?", [area_id]).fetchone()
            if not existing:
                raise HTTPException(status_code=404, detail="Area not found")
            conn.execute(
                f"UPDATE master_area SET {set_clause}, updated_at = CURRENT_TIMESTAMP WHERE area_id = ?",
                params,
            )
        return {"status": "updated", "area_id": area_id}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/area/{area_id}")
async def delete_area(area_id: str, body: Optional[DeleteBody] = None):
    cascade = body.cascade if body else False
    try:
        with get_write_connection() as conn:
            existing = conn.execute("SELECT status FROM master_area WHERE area_id = ?", [area_id]).fetchone()
            if not existing:
                raise HTTPException(status_code=404, detail="Area not found")
            if not cascade:
                active_children = _count_active_children(conn, "master_regional", "area_id", area_id)
                if active_children > 0:
                    raise HTTPException(
                        status_code=409,
                        detail=f"Cannot deactivate: {active_children} active regional(s) exist. Use cascade=true to deactivate all children.",
                    )
            else:
                _cascade_deactivate(conn, "area", area_id)
            conn.execute(
                "UPDATE master_area SET status = 'INACTIVE', updated_at = CURRENT_TIMESTAMP WHERE area_id = ?",
                [area_id],
            )
        return {"status": "deactivated", "area_id": area_id, "cascade": cascade}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Regional endpoints ──

@router.get("/regional")
async def list_regionals(area_id: Optional[str] = None):
    try:
        filters = {}
        if area_id:
            filters["area_id"] = area_id
        with get_connection() as conn:
            return _fetch_all(conn, "master_regional", LEVELS["regional"]["columns"], filters)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/regional")
async def create_regional(body: RegionalCreate):
    try:
        with get_write_connection() as conn:
            parent = conn.execute("SELECT area_id FROM master_area WHERE area_id = ?", [body.area_id]).fetchone()
            if not parent:
                raise HTTPException(status_code=400, detail=f"Area '{body.area_id}' not found")
            conn.execute(
                "INSERT INTO master_regional (regional_id, regional_name, area_id, regional_alias_site_master, regional_alias_ticket, description, status) VALUES (?, ?, ?, ?, ?, ?, ?)",
                [body.regional_id, body.regional_name, body.area_id, body.regional_alias_site_master, body.regional_alias_ticket, body.description, body.status],
            )
        return {"status": "created", "regional_id": body.regional_id}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/regional/{regional_id}")
async def update_regional(regional_id: str, body: RegionalUpdate):
    try:
        updates = {k: v for k, v in body.dict().items() if v is not None}
        if not updates:
            raise HTTPException(status_code=400, detail="No fields to update")
        set_clause = ", ".join(f"{k} = ?" for k in updates)
        params = list(updates.values()) + [regional_id]
        with get_write_connection() as conn:
            existing = conn.execute("SELECT regional_id FROM master_regional WHERE regional_id = ?", [regional_id]).fetchone()
            if not existing:
                raise HTTPException(status_code=404, detail="Regional not found")
            conn.execute(
                f"UPDATE master_regional SET {set_clause}, updated_at = CURRENT_TIMESTAMP WHERE regional_id = ?",
                params,
            )
        return {"status": "updated", "regional_id": regional_id}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/regional/{regional_id}")
async def delete_regional(regional_id: str, body: Optional[DeleteBody] = None):
    cascade = body.cascade if body else False
    try:
        with get_write_connection() as conn:
            existing = conn.execute("SELECT status FROM master_regional WHERE regional_id = ?", [regional_id]).fetchone()
            if not existing:
                raise HTTPException(status_code=404, detail="Regional not found")
            if not cascade:
                active_children = _count_active_children(conn, "master_nop", "regional_id", regional_id)
                if active_children > 0:
                    raise HTTPException(
                        status_code=409,
                        detail=f"Cannot deactivate: {active_children} active NOP(s) exist. Use cascade=true to deactivate all children.",
                    )
            else:
                _cascade_deactivate(conn, "regional", regional_id)
            conn.execute(
                "UPDATE master_regional SET status = 'INACTIVE', updated_at = CURRENT_TIMESTAMP WHERE regional_id = ?",
                [regional_id],
            )
        return {"status": "deactivated", "regional_id": regional_id, "cascade": cascade}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── NOP endpoints ──

@router.get("/nop")
async def list_nops(regional_id: Optional[str] = None):
    try:
        filters = {}
        if regional_id:
            filters["regional_id"] = regional_id
        with get_connection() as conn:
            return _fetch_all(conn, "master_nop", LEVELS["nop"]["columns"], filters)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/nop")
async def create_nop(body: NopCreate):
    try:
        with get_write_connection() as conn:
            parent = conn.execute("SELECT regional_id FROM master_regional WHERE regional_id = ?", [body.regional_id]).fetchone()
            if not parent:
                raise HTTPException(status_code=400, detail=f"Regional '{body.regional_id}' not found")
            conn.execute(
                "INSERT INTO master_nop (nop_id, nop_name, regional_id, nop_alias_site_master, nop_alias_ticket, description, status) VALUES (?, ?, ?, ?, ?, ?, ?)",
                [body.nop_id, body.nop_name, body.regional_id, body.nop_alias_site_master, body.nop_alias_ticket, body.description, body.status],
            )
        return {"status": "created", "nop_id": body.nop_id}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/nop/{nop_id}")
async def update_nop(nop_id: str, body: NopUpdate):
    try:
        updates = {k: v for k, v in body.dict().items() if v is not None}
        if not updates:
            raise HTTPException(status_code=400, detail="No fields to update")
        set_clause = ", ".join(f"{k} = ?" for k in updates)
        params = list(updates.values()) + [nop_id]
        with get_write_connection() as conn:
            existing = conn.execute("SELECT nop_id FROM master_nop WHERE nop_id = ?", [nop_id]).fetchone()
            if not existing:
                raise HTTPException(status_code=404, detail="NOP not found")
            conn.execute(
                f"UPDATE master_nop SET {set_clause}, updated_at = CURRENT_TIMESTAMP WHERE nop_id = ?",
                params,
            )
        return {"status": "updated", "nop_id": nop_id}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/nop/{nop_id}")
async def delete_nop(nop_id: str, body: Optional[DeleteBody] = None):
    cascade = body.cascade if body else False
    try:
        with get_write_connection() as conn:
            existing = conn.execute("SELECT status FROM master_nop WHERE nop_id = ?", [nop_id]).fetchone()
            if not existing:
                raise HTTPException(status_code=404, detail="NOP not found")
            if not cascade:
                active_children = _count_active_children(conn, "master_to", "nop_id", nop_id)
                if active_children > 0:
                    raise HTTPException(
                        status_code=409,
                        detail=f"Cannot deactivate: {active_children} active TO(s) exist. Use cascade=true to deactivate all children.",
                    )
            else:
                _cascade_deactivate(conn, "nop", nop_id)
            conn.execute(
                "UPDATE master_nop SET status = 'INACTIVE', updated_at = CURRENT_TIMESTAMP WHERE nop_id = ?",
                [nop_id],
            )
        return {"status": "deactivated", "nop_id": nop_id, "cascade": cascade}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── TO endpoints ──

@router.get("/to")
async def list_tos(nop_id: Optional[str] = None):
    try:
        filters = {}
        if nop_id:
            filters["nop_id"] = nop_id
        with get_connection() as conn:
            return _fetch_all(conn, "master_to", LEVELS["to"]["columns"], filters)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/to")
async def create_to(body: ToCreate):
    try:
        with get_write_connection() as conn:
            parent = conn.execute("SELECT nop_id FROM master_nop WHERE nop_id = ?", [body.nop_id]).fetchone()
            if not parent:
                raise HTTPException(status_code=400, detail=f"NOP '{body.nop_id}' not found")
            conn.execute(
                "INSERT INTO master_to (to_id, to_name, nop_id, to_alias_site_master, to_alias_ticket, description, status) VALUES (?, ?, ?, ?, ?, ?, ?)",
                [body.to_id, body.to_name, body.nop_id, body.to_alias_site_master, body.to_alias_ticket, body.description, body.status],
            )
        return {"status": "created", "to_id": body.to_id}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/to/{to_id}")
async def update_to(to_id: str, body: ToUpdate):
    try:
        updates = {k: v for k, v in body.dict().items() if v is not None}
        if not updates:
            raise HTTPException(status_code=400, detail="No fields to update")
        set_clause = ", ".join(f"{k} = ?" for k in updates)
        params = list(updates.values()) + [to_id]
        with get_write_connection() as conn:
            existing = conn.execute("SELECT to_id FROM master_to WHERE to_id = ?", [to_id]).fetchone()
            if not existing:
                raise HTTPException(status_code=404, detail="TO not found")
            conn.execute(
                f"UPDATE master_to SET {set_clause}, updated_at = CURRENT_TIMESTAMP WHERE to_id = ?",
                params,
            )
        return {"status": "updated", "to_id": to_id}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/to/{to_id}")
async def delete_to(to_id: str, body: Optional[DeleteBody] = None):
    cascade = body.cascade if body else False
    try:
        with get_write_connection() as conn:
            existing = conn.execute("SELECT status FROM master_to WHERE to_id = ?", [to_id]).fetchone()
            if not existing:
                raise HTTPException(status_code=404, detail="TO not found")
            if not cascade:
                active_children = _count_active_children(conn, "master_site", "to_id", to_id)
                if active_children > 0:
                    raise HTTPException(
                        status_code=409,
                        detail=f"Cannot deactivate: {active_children} active site(s) exist. Use cascade=true to deactivate all children.",
                    )
            else:
                _cascade_deactivate(conn, "to", to_id)
            conn.execute(
                "UPDATE master_to SET status = 'INACTIVE', updated_at = CURRENT_TIMESTAMP WHERE to_id = ?",
                [to_id],
            )
        return {"status": "deactivated", "to_id": to_id, "cascade": cascade}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Tree endpoint ──

@router.get("/hierarchy/tree")
async def get_hierarchy_tree():
    try:
        with get_connection() as conn:
            areas = conn.execute(
                "SELECT area_id, area_name, area_alias, status FROM master_area ORDER BY area_name"
            ).fetchall()

            regionals = conn.execute(
                "SELECT regional_id, regional_name, area_id, regional_alias_site_master, regional_alias_ticket, status FROM master_regional ORDER BY regional_name"
            ).fetchall()

            nops = conn.execute(
                "SELECT nop_id, nop_name, regional_id, nop_alias_site_master, nop_alias_ticket, status FROM master_nop ORDER BY nop_name"
            ).fetchall()

            tos = conn.execute(
                "SELECT to_id, to_name, nop_id, to_alias_site_master, to_alias_ticket, status FROM master_to ORDER BY to_name"
            ).fetchall()

            site_counts = {}
            rows = conn.execute(
                "SELECT to_id, COUNT(*) FROM master_site WHERE status = 'ACTIVE' GROUP BY to_id"
            ).fetchall()
            for to_id, cnt in rows:
                site_counts[to_id] = cnt

        reg_by_area = {}
        for r in regionals:
            reg_by_area.setdefault(r[2], []).append(r)

        nop_by_reg = {}
        for n in nops:
            nop_by_reg.setdefault(n[2], []).append(n)

        to_by_nop = {}
        for t in tos:
            to_by_nop.setdefault(t[2], []).append(t)

        tree = []
        for a in areas:
            area_node = {
                "area_id": a[0], "area_name": a[1], "area_alias": a[2], "status": a[3],
                "type": "area", "children": [],
            }
            for r in reg_by_area.get(a[0], []):
                reg_node = {
                    "regional_id": r[0], "regional_name": r[1], "area_id": r[2],
                    "regional_alias_site_master": r[3], "regional_alias_ticket": r[4], "status": r[5],
                    "type": "regional", "children": [],
                }
                for n in nop_by_reg.get(r[0], []):
                    nop_node = {
                        "nop_id": n[0], "nop_name": n[1], "regional_id": n[2],
                        "nop_alias_site_master": n[3], "nop_alias_ticket": n[4], "status": n[5],
                        "type": "nop", "children": [],
                    }
                    for t in to_by_nop.get(n[0], []):
                        to_node = {
                            "to_id": t[0], "to_name": t[1], "nop_id": t[2],
                            "to_alias_site_master": t[3], "to_alias_ticket": t[4], "status": t[5],
                            "type": "to", "site_count": site_counts.get(t[0], 0),
                        }
                        nop_node["children"].append(to_node)
                    reg_node["children"].append(nop_node)
                area_node["children"].append(reg_node)
            tree.append(area_node)

        return tree
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Stats endpoint ──

@router.get("/hierarchy/stats")
async def get_hierarchy_stats():
    try:
        with get_connection() as conn:
            area_count = conn.execute("SELECT COUNT(*) FROM master_area WHERE status = 'ACTIVE'").fetchone()[0]
            regional_count = conn.execute("SELECT COUNT(*) FROM master_regional WHERE status = 'ACTIVE'").fetchone()[0]
            nop_count = conn.execute("SELECT COUNT(*) FROM master_nop WHERE status = 'ACTIVE'").fetchone()[0]
            to_count = conn.execute("SELECT COUNT(*) FROM master_to WHERE status = 'ACTIVE'").fetchone()[0]
            site_count = conn.execute("SELECT COUNT(*) FROM master_site WHERE status = 'ACTIVE'").fetchone()[0]

        return {
            "area": area_count,
            "regional": regional_count,
            "nop": nop_count,
            "to": to_count,
            "site": site_count,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

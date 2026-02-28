import logging
import math
import csv
import io
from fastapi import APIRouter, Query, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional
from backend.database import get_connection, get_write_connection
from backend.services.enrichment_service import enrich_site

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/master", tags=["site"])


class SiteCreate(BaseModel):
    site_id: str
    site_name: str
    to_id: Optional[str] = None
    site_class: str
    site_flag: str
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    province: Optional[str] = None
    city: Optional[str] = None
    district: Optional[str] = None
    address: Optional[str] = None
    timezone: Optional[str] = "Asia/Jakarta"
    primary_equipment_type: Optional[str] = None
    equipment_count: Optional[int] = None
    equipment_age_years: Optional[float] = None
    commissioning_date: Optional[str] = None


class SiteUpdate(BaseModel):
    site_name: Optional[str] = None
    to_id: Optional[str] = None
    site_class: Optional[str] = None
    site_flag: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    province: Optional[str] = None
    city: Optional[str] = None
    district: Optional[str] = None
    address: Optional[str] = None
    timezone: Optional[str] = None
    primary_equipment_type: Optional[str] = None
    equipment_count: Optional[int] = None
    equipment_age_years: Optional[float] = None
    commissioning_date: Optional[str] = None


class SiteExportRequest(BaseModel):
    area_id: Optional[str] = None
    regional_id: Optional[str] = None
    nop_id: Optional[str] = None
    site_class: Optional[str] = None
    site_flag: Optional[str] = None
    status: Optional[str] = "ACTIVE"
    search: Optional[str] = None


def _build_site_query(filters: dict, count_only=False):
    if count_only:
        select = "SELECT COUNT(*) AS total"
    else:
        select = """SELECT s.site_id, s.site_name, s.to_id, s.site_class, s.site_flag,
            s.site_category, s.site_sub_class, s.upgrade_potential, s.est_technology,
            s.est_transmission, s.est_power, s.est_sector, s.complexity_level,
            s.est_opex_level, s.strategy_focus, s.latitude, s.longitude,
            s.province, s.city, s.district, s.address, s.timezone,
            s.primary_equipment_type, s.equipment_count, s.equipment_age_years,
            s.commissioning_date, s.status, s.source, s.created_at, s.updated_at,
            COALESCE(vh.to_name, '') as to_name,
            COALESCE(vh.nop_id, '') as nop_id, COALESCE(vh.nop_name, '') as nop_name,
            COALESCE(vh.regional_id, '') as regional_id, COALESCE(vh.regional_name, '') as regional_name,
            COALESCE(vh.area_id, '') as area_id, COALESCE(vh.area_name, '') as area_name"""

    base = f"""
    {select}
    FROM master_site s
    LEFT JOIN v_hierarchy vh ON s.to_id = vh.to_id
    """

    conditions = []
    params = []

    if filters.get("status"):
        conditions.append("s.status = ?")
        params.append(filters["status"])

    if filters.get("area_id"):
        conditions.append("vh.area_id = ?")
        params.append(filters["area_id"])

    if filters.get("regional_id"):
        conditions.append("vh.regional_id = ?")
        params.append(filters["regional_id"])

    if filters.get("nop_id"):
        conditions.append("vh.nop_id = ?")
        params.append(filters["nop_id"])

    if filters.get("site_class"):
        conditions.append("s.site_class = ?")
        params.append(filters["site_class"])

    if filters.get("site_flag"):
        conditions.append("s.site_flag = ?")
        params.append(filters["site_flag"])

    if filters.get("search"):
        search_term = f"%{filters['search']}%"
        conditions.append("(LOWER(s.site_id) LIKE LOWER(?) OR LOWER(s.site_name) LIKE LOWER(?))")
        params.extend([search_term, search_term])

    if conditions:
        base += " WHERE " + " AND ".join(conditions)

    return base, params


@router.get("/site")
def list_sites(
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
    area_id: Optional[str] = None,
    regional_id: Optional[str] = None,
    nop_id: Optional[str] = None,
    site_class: Optional[str] = Query(None, alias="class"),
    site_flag: Optional[str] = Query(None, alias="flag"),
    status: Optional[str] = "ACTIVE",
    search: Optional[str] = None,
    sort_by: Optional[str] = "site_id",
    sort_dir: Optional[str] = "asc",
):
    filters = {
        "area_id": area_id,
        "regional_id": regional_id,
        "nop_id": nop_id,
        "site_class": site_class,
        "site_flag": site_flag,
        "status": status,
        "search": search,
    }

    allowed_sort = {
        "site_id", "site_name", "site_class", "site_flag", "status",
        "area_name", "regional_name", "nop_name", "to_name",
        "created_at", "updated_at",
    }
    if sort_by not in allowed_sort:
        sort_by = "site_id"
    if sort_dir not in ("asc", "desc"):
        sort_dir = "asc"

    with get_connection() as conn:
        count_query, count_params = _build_site_query(filters, count_only=True)
        total = conn.execute(count_query, count_params).fetchone()[0]

        data_query, data_params = _build_site_query(filters, count_only=False)

        sort_col = f"vh.{sort_by}" if sort_by in ("area_name", "regional_name", "nop_name", "to_name") else f"s.{sort_by}"
        data_query += f" ORDER BY {sort_col} {sort_dir.upper()}"
        data_query += f" LIMIT {per_page} OFFSET {(page - 1) * per_page}"

        rows = conn.execute(data_query, data_params).fetchall()
        columns = [desc[0] for desc in conn.description]

    items = [dict(zip(columns, row)) for row in rows]
    total_pages = math.ceil(total / per_page) if total > 0 else 1

    return {
        "items": items,
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": total_pages,
    }


@router.get("/site/{site_id}")
def get_site(site_id: str):
    with get_connection() as conn:
        row = conn.execute("""
            SELECT s.*, 
                COALESCE(vh.to_name, '') as to_name,
                COALESCE(vh.nop_id, '') as nop_id, COALESCE(vh.nop_name, '') as nop_name,
                COALESCE(vh.regional_id, '') as regional_id, COALESCE(vh.regional_name, '') as regional_name,
                COALESCE(vh.area_id, '') as area_id, COALESCE(vh.area_name, '') as area_name
            FROM master_site s
            LEFT JOIN v_hierarchy vh ON s.to_id = vh.to_id
            WHERE s.site_id = ?
        """, [site_id]).fetchone()

        if not row:
            raise HTTPException(status_code=404, detail=f"Site {site_id} not found")

        columns = [desc[0] for desc in conn.description]

    return dict(zip(columns, row))


@router.post("/site")
def create_site(body: SiteCreate):
    if body.to_id:
        with get_connection() as conn:
            exists = conn.execute(
                "SELECT 1 FROM master_to WHERE to_id = ? AND status = 'ACTIVE'",
                [body.to_id]
            ).fetchone()
            if not exists:
                raise HTTPException(status_code=400, detail=f"TO {body.to_id} does not exist or is inactive")

    enrichment = enrich_site(body.site_class, body.site_flag)

    with get_write_connection() as conn:
        existing = conn.execute("SELECT 1 FROM master_site WHERE site_id = ?", [body.site_id]).fetchone()
        if existing:
            raise HTTPException(status_code=409, detail=f"Site {body.site_id} already exists")

        conn.execute("""
            INSERT INTO master_site (
                site_id, site_name, to_id, site_class, site_flag,
                site_category, site_sub_class, upgrade_potential, est_technology,
                est_transmission, est_power, est_sector, complexity_level,
                est_opex_level, strategy_focus,
                latitude, longitude, province, city, district, address, timezone,
                primary_equipment_type, equipment_count, equipment_age_years,
                commissioning_date, status, source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ACTIVE', 'manual')
        """, [
            body.site_id, body.site_name, body.to_id, body.site_class, body.site_flag,
            enrichment["site_category"], enrichment["site_sub_class"],
            enrichment["upgrade_potential"], enrichment["est_technology"],
            enrichment["est_transmission"], enrichment["est_power"],
            enrichment["est_sector"], enrichment["complexity_level"],
            enrichment["est_opex_level"], enrichment["strategy_focus"],
            body.latitude, body.longitude, body.province, body.city,
            body.district, body.address, body.timezone,
            body.primary_equipment_type, body.equipment_count,
            body.equipment_age_years, body.commissioning_date,
        ])

    return {"status": "created", "site_id": body.site_id, "enrichment": enrichment}


@router.put("/site/{site_id}")
def update_site(site_id: str, body: SiteUpdate):
    with get_connection() as conn:
        existing = conn.execute("SELECT site_class, site_flag FROM master_site WHERE site_id = ?", [site_id]).fetchone()
        if not existing:
            raise HTTPException(status_code=404, detail=f"Site {site_id} not found")
        old_class, old_flag = existing

    if body.to_id is not None:
        with get_connection() as conn:
            exists = conn.execute(
                "SELECT 1 FROM master_to WHERE to_id = ? AND status = 'ACTIVE'",
                [body.to_id]
            ).fetchone()
            if not exists:
                raise HTTPException(status_code=400, detail=f"TO {body.to_id} does not exist or is inactive")

    new_class = body.site_class if body.site_class is not None else old_class
    new_flag = body.site_flag if body.site_flag is not None else old_flag

    enrichment = None
    if new_class != old_class or new_flag != old_flag:
        enrichment = enrich_site(new_class, new_flag)

    updates = []
    params = []

    field_map = {
        "site_name": body.site_name,
        "to_id": body.to_id,
        "site_class": body.site_class,
        "site_flag": body.site_flag,
        "latitude": body.latitude,
        "longitude": body.longitude,
        "province": body.province,
        "city": body.city,
        "district": body.district,
        "address": body.address,
        "timezone": body.timezone,
        "primary_equipment_type": body.primary_equipment_type,
        "equipment_count": body.equipment_count,
        "equipment_age_years": body.equipment_age_years,
        "commissioning_date": body.commissioning_date,
    }

    for field, value in field_map.items():
        if value is not None:
            updates.append(f"{field} = ?")
            params.append(value)

    if enrichment:
        for field, value in enrichment.items():
            updates.append(f"{field} = ?")
            params.append(value)

    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")

    updates.append("updated_at = CURRENT_TIMESTAMP")
    params.append(site_id)

    with get_write_connection() as conn:
        conn.execute(
            f"UPDATE master_site SET {', '.join(updates)} WHERE site_id = ?",
            params
        )

    result = {"status": "updated", "site_id": site_id}
    if enrichment:
        result["enrichment_recalculated"] = True
        result["enrichment"] = enrichment
    return result


@router.delete("/site/{site_id}")
def delete_site(site_id: str):
    with get_write_connection() as conn:
        existing = conn.execute("SELECT status FROM master_site WHERE site_id = ?", [site_id]).fetchone()
        if not existing:
            raise HTTPException(status_code=404, detail=f"Site {site_id} not found")

        conn.execute(
            "UPDATE master_site SET status = 'INACTIVE', updated_at = CURRENT_TIMESTAMP WHERE site_id = ?",
            [site_id]
        )

    return {"status": "deactivated", "site_id": site_id}


@router.post("/site/export")
def export_sites(body: SiteExportRequest):
    filters = {
        "area_id": body.area_id,
        "regional_id": body.regional_id,
        "nop_id": body.nop_id,
        "site_class": body.site_class,
        "site_flag": body.site_flag,
        "status": body.status,
        "search": body.search,
    }

    with get_connection() as conn:
        data_query, data_params = _build_site_query(filters, count_only=False)
        data_query += " ORDER BY s.site_id ASC"

        rows = conn.execute(data_query, data_params).fetchall()
        columns = [desc[0] for desc in conn.description]

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(columns)
    for row in rows:
        writer.writerow(row)

    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=sites_export.csv"},
    )

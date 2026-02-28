import logging
import math
import csv
import io
import time
import tempfile
import os
from fastapi import APIRouter, Query, HTTPException, UploadFile, File
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional, List
from backend.database import get_connection, get_write_connection
from backend.services.enrichment_service import enrich_site

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/master", tags=["site"])

LAT_MIN, LAT_MAX = -11.0, 6.0
LON_MIN, LON_MAX = 95.0, 141.0


def _assign_timezone(longitude):
    if longitude is None:
        return "Asia/Jakarta"
    if longitude < 115.0:
        return "Asia/Jakarta"
    elif longitude < 131.0:
        return "Asia/Makassar"
    else:
        return "Asia/Jayapura"


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


@router.get("/site/stats")
def get_site_stats():
    with get_connection() as conn:
        total = conn.execute("SELECT COUNT(*) FROM master_site").fetchone()[0]
        active = conn.execute("SELECT COUNT(*) FROM master_site WHERE status = 'ACTIVE'").fetchone()[0]
        inactive = total - active

        class_rows = conn.execute(
            "SELECT site_class, COUNT(*) as cnt FROM master_site WHERE status = 'ACTIVE' GROUP BY site_class ORDER BY cnt DESC"
        ).fetchall()
        by_class = {r[0]: r[1] for r in class_rows}

        flag_rows = conn.execute(
            "SELECT site_flag, COUNT(*) as cnt FROM master_site WHERE status = 'ACTIVE' GROUP BY site_flag ORDER BY cnt DESC"
        ).fetchall()
        by_flag = {r[0]: r[1] for r in flag_rows}

        cat_rows = conn.execute(
            "SELECT site_category, COUNT(*) as cnt FROM master_site WHERE status = 'ACTIVE' AND site_category IS NOT NULL GROUP BY site_category ORDER BY cnt DESC"
        ).fetchall()
        by_category = {r[0]: r[1] for r in cat_rows}

        with_coords = conn.execute(
            "SELECT COUNT(*) FROM master_site WHERE status = 'ACTIVE' AND latitude IS NOT NULL AND longitude IS NOT NULL"
        ).fetchone()[0]
        with_equipment = conn.execute(
            "SELECT COUNT(*) FROM master_site WHERE status = 'ACTIVE' AND primary_equipment_type IS NOT NULL"
        ).fetchone()[0]
        with_derived = conn.execute(
            "SELECT COUNT(*) FROM master_site WHERE status = 'ACTIVE' AND site_category IS NOT NULL"
        ).fetchone()[0]

    return {
        "total": total,
        "active": active,
        "inactive": inactive,
        "by_class": by_class,
        "by_flag": by_flag,
        "by_category": by_category,
        "enrichment": {
            "with_coordinates": with_coords,
            "with_equipment": with_equipment,
            "with_derived": with_derived,
        },
    }


@router.get("/site/template/{template_type}")
def download_template(template_type: str):
    if template_type == "coordinates":
        headers = ["site_id", "latitude", "longitude", "province", "city", "district"]
        rows = [
            ["ADL001", "-3.9725", "122.515", "Sulawesi Tenggara", "Kendari", "Kadia"],
            ["ADL002", "-3.9801", "122.498", "Sulawesi Tenggara", "Kendari", "Mandonga"],
            ["JKT001", "-6.2088", "106.8456", "DKI Jakarta", "Jakarta Pusat", "Gambir"],
        ]
    elif template_type == "equipment":
        headers = ["site_id", "primary_equipment_type", "equipment_count", "equipment_age_years", "commissioning_date"]
        rows = [
            ["ADL001", "MW Outdoor", "3", "5.2", "2020-03-15"],
            ["ADL002", "BTS Indoor", "2", "3.1", "2022-01-10"],
            ["JKT001", "Multi-Tech", "5", "2.0", "2023-06-20"],
        ]
    elif template_type == "enriched":
        headers = [
            "SITE_ID", "SITE_NAME", "ID_REGION_NETWORK", "NOP_NAME", "SITEAREA_TO",
            "CLASS", "FLAG_SITE", "LATITUDE", "LONGITUDE", "PROVINCE", "CITY", "DISTRICT",
            "EQUIPMENT_TYPE", "EQUIPMENT_COUNT", "EQUIPMENT_AGE", "COMMISSIONING_DATE",
        ]
        rows = [
            ["ADL001", "ADILAM 1", "SULTRA", "NOP KENDARI", "TO KENDARI", "Gold", "Site Reguler",
             "-3.9725", "122.515", "Sulawesi Tenggara", "Kendari", "Kadia", "MW Outdoor", "3", "5.2", "2020-03-15"],
        ]
    else:
        raise HTTPException(status_code=400, detail=f"Unknown template type: {template_type}")

    output = io.StringIO()
    output.write("# Isi site_id sesuai master site yang ada\n")
    writer = csv.writer(output)
    writer.writerow(headers)
    for row in rows:
        writer.writerow(row)

    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=template_{template_type}.csv"},
    )


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


def _validate_coordinate_row(row, existing_ids):
    site_id = str(row.get("site_id", "")).strip()
    errors = []
    if not site_id:
        return None, "site_id kosong"
    if site_id not in existing_ids:
        return None, f"site_id {site_id} tidak ditemukan di master_site"

    lat = row.get("latitude")
    lon = row.get("longitude")
    try:
        lat = float(lat) if lat is not None and str(lat).strip() != "" else None
        lon = float(lon) if lon is not None and str(lon).strip() != "" else None
    except (ValueError, TypeError):
        return None, f"latitude/longitude bukan angka valid"

    if lat is not None and (lat < LAT_MIN or lat > LAT_MAX):
        return None, f"latitude {lat} di luar range Indonesia ({LAT_MIN} s/d {LAT_MAX})"
    if lon is not None and (lon < LON_MIN or lon > LON_MAX):
        return None, f"longitude {lon} di luar range Indonesia ({LON_MIN} s/d {LON_MAX})"

    return {"site_id": site_id, "latitude": lat, "longitude": lon,
            "province": str(row.get("province", "")).strip() or None,
            "city": str(row.get("city", "")).strip() or None,
            "district": str(row.get("district", "")).strip() or None}, None


@router.post("/site/bulk-update")
def bulk_update_sites(body: dict):
    update_type = body.get("update_type", "coordinates")
    data = body.get("data", [])
    if not data:
        raise HTTPException(status_code=400, detail="data array is empty")

    with get_connection() as conn:
        all_ids = set(r[0] for r in conn.execute("SELECT site_id FROM master_site").fetchall())

    updated = 0
    skipped = 0
    errors = []

    valid_rows = []
    for row in data:
        site_id = str(row.get("site_id", "")).strip()
        if not site_id or site_id not in all_ids:
            skipped += 1
            errors.append({"site_id": site_id, "reason": "site_id tidak ditemukan"})
            continue

        if update_type in ("coordinates", "mixed"):
            lat = row.get("latitude")
            lon = row.get("longitude")
            try:
                lat = float(lat) if lat is not None and str(lat).strip() != "" else None
                lon = float(lon) if lon is not None and str(lon).strip() != "" else None
            except (ValueError, TypeError):
                skipped += 1
                errors.append({"site_id": site_id, "reason": "latitude/longitude bukan angka"})
                continue
            if lat is not None and (lat < LAT_MIN or lat > LAT_MAX):
                skipped += 1
                errors.append({"site_id": site_id, "reason": f"latitude {lat} di luar range"})
                continue
            if lon is not None and (lon < LON_MIN or lon > LON_MAX):
                skipped += 1
                errors.append({"site_id": site_id, "reason": f"longitude {lon} di luar range"})
                continue

        valid_rows.append(row)

    if valid_rows:
        with get_write_connection() as conn:
            for row in valid_rows:
                site_id = str(row["site_id"]).strip()
                sets = []
                params = []

                if update_type in ("coordinates", "mixed"):
                    lat = row.get("latitude")
                    lon = row.get("longitude")
                    if lat is not None:
                        lat = float(lat)
                        sets.append("latitude = ?")
                        params.append(lat)
                    if lon is not None:
                        lon = float(lon)
                        sets.append("longitude = ?")
                        params.append(lon)
                        tz = _assign_timezone(lon)
                        sets.append("timezone = ?")
                        params.append(tz)
                    for f in ("province", "city", "district"):
                        val = str(row.get(f, "")).strip()
                        if val:
                            sets.append(f"{f} = ?")
                            params.append(val)

                if update_type in ("equipment", "mixed"):
                    for f, cast in [("primary_equipment_type", str), ("equipment_count", int),
                                    ("equipment_age_years", float), ("commissioning_date", str)]:
                        val = row.get(f)
                        if val is not None and str(val).strip() != "":
                            try:
                                sets.append(f"{f} = ?")
                                params.append(cast(val))
                            except (ValueError, TypeError):
                                pass

                if sets:
                    sets.append("updated_at = CURRENT_TIMESTAMP")
                    params.append(site_id)
                    conn.execute(f"UPDATE master_site SET {', '.join(sets)} WHERE site_id = ?", params)
                    updated += 1

    return {"updated": updated, "skipped": skipped, "errors": errors[:100]}


@router.post("/site/bulk-update/upload")
async def bulk_update_upload(file: UploadFile = File(...), update_type: str = "coordinates"):
    import pandas as pd

    content = await file.read()
    filename = file.filename or ""

    try:
        if filename.endswith(".xlsx") or filename.endswith(".xls"):
            df = pd.read_excel(io.BytesIO(content))
        else:
            df = pd.read_csv(io.BytesIO(content), comment="#")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Gagal parse file: {str(e)}")

    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    data = df.where(df.notna(), None).to_dict(orient="records")

    return bulk_update_sites({"update_type": update_type, "data": data})


@router.post("/site/import-enriched")
async def import_enriched_site_master(file: UploadFile = File(...)):
    import pandas as pd
    from backend.services.site_master_processor import process_site_master
    from backend.services.enrichment_service import enrich_site as enrich_fn

    content = await file.read()
    filename = file.filename or ""

    tmp_dir = tempfile.mkdtemp()
    tmp_path = os.path.join(tmp_dir, filename)
    with open(tmp_path, "wb") as f:
        f.write(content)

    try:
        result = process_site_master(tmp_path)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing site master: {str(e)}")
    finally:
        try:
            os.remove(tmp_path)
            os.rmdir(tmp_dir)
        except Exception:
            pass

    try:
        if filename.endswith(".xlsx") or filename.endswith(".xls"):
            df = pd.read_excel(io.BytesIO(content))
        else:
            df = pd.read_csv(io.BytesIO(content), comment="#")
    except Exception:
        df = pd.DataFrame()

    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    coord_header_map = {
        "latitude": "latitude", "lat": "latitude",
        "longitude": "longitude", "lon": "longitude", "long": "longitude",
        "province": "province", "provinsi": "province",
        "city": "city", "kota": "city", "kabupaten": "city",
        "district": "district", "kecamatan": "district",
    }
    equip_header_map = {
        "equipment_type": "primary_equipment_type", "primary_equipment_type": "primary_equipment_type",
        "equipment_count": "equipment_count",
        "equipment_age": "equipment_age_years", "equipment_age_years": "equipment_age_years",
        "commissioning_date": "commissioning_date",
    }

    coord_cols = {coord_header_map[c]: c for c in df.columns if c in coord_header_map}
    equip_cols = {equip_header_map[c]: c for c in df.columns if c in equip_header_map}

    coords_updated = 0
    equipment_updated = 0

    site_id_col = None
    for col in ["site_id", "siteid"]:
        if col in df.columns:
            site_id_col = col
            break

    if site_id_col and (coord_cols or equip_cols):
        with get_connection() as conn:
            all_ids = set(r[0] for r in conn.execute("SELECT site_id FROM master_site").fetchall())

        with get_write_connection() as conn:
            for _, row in df.iterrows():
                sid = str(row.get(site_id_col, "")).strip()
                if not sid or sid not in all_ids:
                    continue

                sets = []
                params = []

                if coord_cols:
                    lat_col = coord_cols.get("latitude")
                    lon_col = coord_cols.get("longitude")
                    has_coord = False
                    if lat_col:
                        try:
                            lat = float(row[lat_col])
                            if LAT_MIN <= lat <= LAT_MAX:
                                sets.append("latitude = ?")
                                params.append(lat)
                                has_coord = True
                        except (ValueError, TypeError):
                            pass
                    if lon_col:
                        try:
                            lon = float(row[lon_col])
                            if LON_MIN <= lon <= LON_MAX:
                                sets.append("longitude = ?")
                                params.append(lon)
                                sets.append("timezone = ?")
                                params.append(_assign_timezone(lon))
                                has_coord = True
                        except (ValueError, TypeError):
                            pass
                    for target, src in coord_cols.items():
                        if target in ("latitude", "longitude"):
                            continue
                        val = str(row.get(src, "")).strip()
                        if val and val != "nan":
                            sets.append(f"{target} = ?")
                            params.append(val)

                    if has_coord:
                        coords_updated += 1

                if equip_cols:
                    has_equip = False
                    for target, src in equip_cols.items():
                        val = row.get(src)
                        if val is not None and str(val).strip() != "" and str(val) != "nan":
                            try:
                                if target == "equipment_count":
                                    val = int(float(val))
                                elif target == "equipment_age_years":
                                    val = float(val)
                                else:
                                    val = str(val).strip()
                                sets.append(f"{target} = ?")
                                params.append(val)
                                has_equip = True
                            except (ValueError, TypeError):
                                pass
                    if has_equip:
                        equipment_updated += 1

                if sets:
                    sets.append("updated_at = CURRENT_TIMESTAMP")
                    params.append(sid)
                    conn.execute(f"UPDATE master_site SET {', '.join(sets)} WHERE site_id = ?", params)

    result["coordinates_updated"] = coords_updated
    result["equipment_updated"] = equipment_updated

    return result


@router.post("/site/recalculate-derived")
def recalculate_derived():
    start = time.time()

    with get_connection() as conn:
        rows = conn.execute(
            "SELECT site_id, site_class, site_flag FROM master_site WHERE status = 'ACTIVE'"
        ).fetchall()

    recalculated = 0
    batch_size = 500
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        with get_write_connection() as conn:
            for site_id, site_class, site_flag in batch:
                enrichment = enrich_site(site_class, site_flag)
                sets = []
                params = []
                for field, value in enrichment.items():
                    sets.append(f"{field} = ?")
                    params.append(value)
                sets.append("updated_at = CURRENT_TIMESTAMP")
                params.append(site_id)
                conn.execute(f"UPDATE master_site SET {', '.join(sets)} WHERE site_id = ?", params)
                recalculated += 1

    duration = round(time.time() - start, 1)
    return {"recalculated": recalculated, "duration_sec": duration}

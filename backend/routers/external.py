import logging
import io
import csv
import math
from datetime import date, datetime
from typing import Optional, List
from fastapi import APIRouter, Query, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from backend.database import get_connection, get_write_connection

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/external", tags=["external"])


class HolidayUpdate(BaseModel):
    date: str
    holiday_name: str
    is_cuti_bersama: bool = False


class AnnotationCreate(BaseModel):
    date: str
    date_end: Optional[str] = None
    area_id: Optional[str] = None
    regional_id: Optional[str] = None
    province: Optional[str] = None
    annotation_type: str = "custom"
    title: str
    description: Optional[str] = None
    severity: str = "info"
    color: Optional[str] = None
    icon: Optional[str] = "\U0001f4cc"
    show_on_chart: bool = True


class AnnotationUpdate(BaseModel):
    date: Optional[str] = None
    date_end: Optional[str] = None
    area_id: Optional[str] = None
    regional_id: Optional[str] = None
    province: Optional[str] = None
    annotation_type: Optional[str] = None
    title: Optional[str] = None
    description: Optional[str] = None
    severity: Optional[str] = None
    color: Optional[str] = None
    icon: Optional[str] = None
    show_on_chart: Optional[bool] = None


@router.post("/calendar/generate")
async def generate_calendar_endpoint(body: dict):
    year = body.get("year")
    if not year or not isinstance(year, int) or year < 2020 or year > 2030:
        raise HTTPException(400, "Tahun harus antara 2020-2030")
    from backend.services.calendar_service import generate_calendar
    result = generate_calendar(year)
    return result


@router.get("/calendar")
async def get_calendar(
    year: Optional[int] = None,
    month: Optional[int] = None,
    type: Optional[str] = None,
):
    with get_connection() as conn:
        sql = "SELECT * FROM ext_calendar WHERE 1=1"
        params = []
        if year:
            sql += " AND year = ?"
            params.append(year)
        if month:
            sql += " AND month = ?"
            params.append(month)
        if type == "holiday":
            sql += " AND (is_holiday = TRUE OR is_cuti_bersama = TRUE)"
        elif type == "ramadan":
            sql += " AND is_ramadan = TRUE"
        elif type == "weekend":
            sql += " AND is_weekend = TRUE"
        sql += " ORDER BY date"
        rows = conn.execute(sql, params).fetchall()
        cols = [d[0] for d in conn.description]
        result = []
        for row in rows:
            d = dict(zip(cols, row))
            d["date"] = str(d["date"])
            result.append(d)
        return result


@router.put("/calendar/holiday")
async def update_holiday(body: HolidayUpdate):
    with get_write_connection() as conn:
        existing = conn.execute("SELECT * FROM ext_calendar WHERE date = ?", [body.date]).fetchone()
        if not existing:
            raise HTTPException(404, "Tanggal tidak ditemukan di kalender")

        day_type = "Cuti Bersama" if body.is_cuti_bersama else "Libur Nasional"
        conn.execute(
            """UPDATE ext_calendar SET 
               is_holiday = ?, is_cuti_bersama = ?, holiday_name = ?, day_type = ?
               WHERE date = ?""",
            [not body.is_cuti_bersama, body.is_cuti_bersama, body.holiday_name, day_type, body.date]
        )

        cols = [d[0] for d in conn.description] if conn.description else []
        updated = conn.execute("SELECT * FROM ext_calendar WHERE date = ?", [body.date]).fetchone()
        if updated:
            ucols = [d[0] for d in conn.description]
            result = dict(zip(ucols, updated))
            result["date"] = str(result["date"])
            return result
    return {"status": "updated"}


@router.delete("/calendar/holiday/{date_str}")
async def delete_holiday(date_str: str):
    with get_write_connection() as conn:
        conn.execute(
            """UPDATE ext_calendar SET 
               is_holiday = FALSE, is_cuti_bersama = FALSE, holiday_name = NULL,
               day_type = CASE WHEN is_weekend THEN 'Akhir Pekan' ELSE 'Kerja' END
               WHERE date = ?""",
            [date_str]
        )
    return {"status": "deleted", "date": date_str}


@router.post("/weather/upload")
async def upload_weather(file: UploadFile = File(...)):
    import pandas as pd
    content = await file.read()
    filename = file.filename or ""

    try:
        if filename.endswith(".csv"):
            df = pd.read_csv(io.BytesIO(content))
        else:
            df = pd.read_excel(io.BytesIO(content))
    except Exception as e:
        raise HTTPException(400, f"Gagal membaca file: {str(e)}")

    col_map = {}
    for c in df.columns:
        cl = c.strip().lower().replace(" ", "_")
        col_map[c] = cl
    df.rename(columns=col_map, inplace=True)

    required = ["date", "province"]
    for r in required:
        if r not in df.columns:
            raise HTTPException(400, f"Kolom '{r}' wajib ada")

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df = df.dropna(subset=["date"])

    for col in ["rainfall_mm", "temperature_avg_c", "temperature_max_c", "temperature_min_c",
                 "humidity_avg_pct", "wind_speed_avg_kmh"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "is_extreme" not in df.columns:
        rainfall = df.get("rainfall_mm", pd.Series(dtype=float)).fillna(0)
        wind = df.get("wind_speed_avg_kmh", pd.Series(dtype=float)).fillna(0)
        df["is_extreme"] = (rainfall > 50) | (wind > 40)

    with get_write_connection() as conn:
        max_id = conn.execute("SELECT COALESCE(MAX(id), 0) FROM ext_weather").fetchone()[0]
        rows = []
        for i, row in df.iterrows():
            max_id += 1
            rows.append((
                max_id,
                row["date"],
                str(row.get("province", "")),
                str(row.get("city", "")) if pd.notna(row.get("city")) else None,
                row.get("rainfall_mm") if pd.notna(row.get("rainfall_mm")) else None,
                row.get("temperature_avg_c") if pd.notna(row.get("temperature_avg_c")) else None,
                row.get("temperature_max_c") if pd.notna(row.get("temperature_max_c")) else None,
                row.get("temperature_min_c") if pd.notna(row.get("temperature_min_c")) else None,
                row.get("humidity_avg_pct") if pd.notna(row.get("humidity_avg_pct")) else None,
                row.get("wind_speed_avg_kmh") if pd.notna(row.get("wind_speed_avg_kmh")) else None,
                str(row.get("weather_condition", "")) if pd.notna(row.get("weather_condition")) else None,
                bool(row.get("is_extreme", False)),
                "BMKG",
            ))

        conn.executemany(
            """INSERT INTO ext_weather
               (id, date, province, city, rainfall_mm, temperature_avg_c,
                temperature_max_c, temperature_min_c, humidity_avg_pct,
                wind_speed_avg_kmh, weather_condition, is_extreme, source)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            rows
        )

        extreme_rows = [r for r in rows if r[11]]
        ann_max_id = conn.execute("SELECT COALESCE(MAX(id), 0) FROM ext_annotations").fetchone()[0]
        ann_count = 0
        for r in extreme_rows:
            ann_max_id += 1
            condition = r[10] or "Cuaca Ekstrem"
            rainfall = r[4] or 0
            province = r[2]
            desc = f"Curah hujan {rainfall}mm"
            if r[9]:
                desc += f", angin {r[9]}km/h"
            conn.execute(
                """INSERT INTO ext_annotations
                   (id, date, annotation_type, title, description, severity,
                    color, icon, show_on_chart, source, province)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                [ann_max_id, r[1], 'weather',
                 f'Cuaca Ekstrem: {condition} ({province})',
                 desc, 'warning', '#FF6B00', '\U0001f327\ufe0f', True, 'auto_weather', province]
            )
            ann_count += 1

    return {
        "imported": len(rows),
        "extreme_days": len(extreme_rows),
        "annotations_created": ann_count,
    }


@router.get("/weather")
async def get_weather(
    from_date: Optional[str] = Query(None, alias="from"),
    to_date: Optional[str] = Query(None, alias="to"),
    province: Optional[str] = None,
):
    with get_connection() as conn:
        sql = "SELECT * FROM ext_weather WHERE 1=1"
        params = []
        if from_date:
            sql += " AND date >= ?"
            params.append(from_date)
        if to_date:
            sql += " AND date <= ?"
            params.append(to_date)
        if province:
            sql += " AND province = ?"
            params.append(province)
        sql += " ORDER BY date DESC LIMIT 500"
        rows = conn.execute(sql, params).fetchall()
        cols = [d[0] for d in conn.description]
        result = []
        for row in rows:
            d = dict(zip(cols, row))
            d["date"] = str(d["date"])
            if d.get("created_at"):
                d["created_at"] = str(d["created_at"])
            result.append(d)

        stats = conn.execute("SELECT COUNT(*), COUNT(DISTINCT province), MIN(date), MAX(date) FROM ext_weather").fetchone()
        summary = {
            "total_records": stats[0],
            "provinces": stats[1],
            "date_range": [str(stats[2]) if stats[2] else None, str(stats[3]) if stats[3] else None],
        }

        extreme_count = conn.execute("SELECT COUNT(*) FROM ext_weather WHERE is_extreme = TRUE").fetchone()[0]
        summary["extreme_days"] = extreme_count

        return {"data": result, "summary": summary}


@router.get("/weather/template")
async def weather_template():
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["date", "province", "city", "rainfall_mm", "temperature_avg_c",
                      "humidity_avg_pct", "wind_speed_avg_kmh", "weather_condition"])
    writer.writerow(["2025-01-01", "Jawa Barat", "Bandung", "15.2", "24.5", "78", "12", "Berawan"])
    writer.writerow(["2025-01-02", "Jawa Barat", "Bandung", "45.8", "23.1", "92", "25", "Hujan Lebat"])
    writer.writerow(["2025-01-01", "DKI Jakarta", "", "22.3", "28.2", "75", "8", "Hujan"])
    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=weather_template.csv"}
    )


@router.delete("/weather")
async def delete_weather(
    from_date: Optional[str] = Query(None, alias="from"),
    to_date: Optional[str] = Query(None, alias="to"),
    province: Optional[str] = None,
):
    with get_write_connection() as conn:
        sql = "DELETE FROM ext_weather WHERE 1=1"
        params = []
        if from_date:
            sql += " AND date >= ?"
            params.append(from_date)
        if to_date:
            sql += " AND date <= ?"
            params.append(to_date)
        if province:
            sql += " AND province = ?"
            params.append(province)
        result = conn.execute(sql, params)

        ann_sql = "DELETE FROM ext_annotations WHERE source = 'auto_weather'"
        ann_params = []
        if from_date:
            ann_sql += " AND date >= ?"
            ann_params.append(from_date)
        if to_date:
            ann_sql += " AND date <= ?"
            ann_params.append(to_date)
        if province:
            ann_sql += " AND province = ?"
            ann_params.append(province)
        conn.execute(ann_sql, ann_params)

    return {"status": "deleted"}


@router.post("/pln/upload")
async def upload_pln(file: UploadFile = File(...)):
    import pandas as pd
    content = await file.read()
    filename = file.filename or ""

    try:
        if filename.endswith(".csv"):
            df = pd.read_csv(io.BytesIO(content))
        else:
            df = pd.read_excel(io.BytesIO(content))
    except Exception as e:
        raise HTTPException(400, f"Gagal membaca file: {str(e)}")

    col_map = {}
    for c in df.columns:
        cl = c.strip().lower().replace(" ", "_")
        col_map[c] = cl
    df.rename(columns=col_map, inplace=True)

    required = ["date", "province"]
    for r in required:
        if r not in df.columns:
            raise HTTPException(400, f"Kolom '{r}' wajib ada")

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df = df.dropna(subset=["date"])

    if "duration_hours" in df.columns:
        df["duration_hours"] = pd.to_numeric(df["duration_hours"], errors="coerce")

    with get_write_connection() as conn:
        max_id = conn.execute("SELECT COALESCE(MAX(id), 0) FROM ext_pln_outage").fetchone()[0]
        rows = []
        for i, row in df.iterrows():
            max_id += 1
            rows.append((
                max_id,
                row["date"],
                str(row.get("province", "")),
                str(row.get("city", "")) if pd.notna(row.get("city")) else None,
                str(row.get("district", "")) if pd.notna(row.get("district")) else None,
                str(row.get("outage_type", "")) if pd.notna(row.get("outage_type")) else None,
                str(row.get("start_time", "")) if pd.notna(row.get("start_time")) else None,
                str(row.get("end_time", "")) if pd.notna(row.get("end_time")) else None,
                row.get("duration_hours") if pd.notna(row.get("duration_hours")) else None,
                str(row.get("affected_area", "")) if pd.notna(row.get("affected_area")) else None,
                "PLN",
            ))

        conn.executemany(
            """INSERT INTO ext_pln_outage
               (id, date, province, city, district, outage_type,
                start_time, end_time, duration_hours, affected_area, source)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            rows
        )

        ann_max_id = conn.execute("SELECT COALESCE(MAX(id), 0) FROM ext_annotations").fetchone()[0]
        ann_count = 0
        for r in rows:
            ann_max_id += 1
            outage_type = r[5] or "Gangguan"
            city = r[3] or r[2]
            duration = r[8]
            affected = r[9] or ""
            severity = "info"
            if duration and duration > 6:
                severity = "critical"
            elif duration and duration > 2:
                severity = "warning"
            desc = f"Durasi: {duration or '?'} jam"
            if affected:
                desc += f". {affected}"
            conn.execute(
                """INSERT INTO ext_annotations
                   (id, date, annotation_type, title, description, severity,
                    color, icon, show_on_chart, source, province)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                [ann_max_id, r[1], 'pln',
                 f'{outage_type} PLN: {city}',
                 desc, severity, '#FFD700', '\u26a1', True, 'auto_pln', r[2]]
            )
            ann_count += 1

    return {"imported": len(rows), "annotations_created": ann_count}


@router.get("/pln")
async def get_pln(
    from_date: Optional[str] = Query(None, alias="from"),
    to_date: Optional[str] = Query(None, alias="to"),
    province: Optional[str] = None,
):
    with get_connection() as conn:
        sql = "SELECT * FROM ext_pln_outage WHERE 1=1"
        params = []
        if from_date:
            sql += " AND date >= ?"
            params.append(from_date)
        if to_date:
            sql += " AND date <= ?"
            params.append(to_date)
        if province:
            sql += " AND province = ?"
            params.append(province)
        sql += " ORDER BY date DESC LIMIT 500"
        rows = conn.execute(sql, params).fetchall()
        cols = [d[0] for d in conn.description]
        result = []
        for row in rows:
            d = dict(zip(cols, row))
            d["date"] = str(d["date"])
            for k in ["start_time", "end_time", "created_at"]:
                if d.get(k):
                    d[k] = str(d[k])
            result.append(d)

        stats = conn.execute("SELECT COUNT(*), COUNT(DISTINCT province), MIN(date), MAX(date) FROM ext_pln_outage").fetchone()
        summary = {
            "total_records": stats[0],
            "provinces": stats[1],
            "date_range": [str(stats[2]) if stats[2] else None, str(stats[3]) if stats[3] else None],
        }
        return {"data": result, "summary": summary}


@router.get("/pln/template")
async def pln_template():
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["date", "province", "city", "district", "outage_type",
                      "start_time", "end_time", "duration_hours", "affected_area"])
    writer.writerow(["2025-01-15", "DKI Jakarta", "Jakarta Selatan", "Kebayoran Baru",
                      "Pemadaman Bergilir", "2025-01-15 08:00", "2025-01-15 14:00", "6", "Blok M area"])
    writer.writerow(["2025-01-20", "Jawa Barat", "Bandung", "Coblong",
                      "Gangguan", "2025-01-20 10:00", "2025-01-20 12:30", "2.5", "Dago area"])
    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=pln_template.csv"}
    )


@router.delete("/pln")
async def delete_pln(
    from_date: Optional[str] = Query(None, alias="from"),
    to_date: Optional[str] = Query(None, alias="to"),
):
    with get_write_connection() as conn:
        sql = "DELETE FROM ext_pln_outage WHERE 1=1"
        params = []
        if from_date:
            sql += " AND date >= ?"
            params.append(from_date)
        if to_date:
            sql += " AND date <= ?"
            params.append(to_date)
        conn.execute(sql, params)

        ann_sql = "DELETE FROM ext_annotations WHERE source = 'auto_pln'"
        ann_params = []
        if from_date:
            ann_sql += " AND date >= ?"
            ann_params.append(from_date)
        if to_date:
            ann_sql += " AND date <= ?"
            ann_params.append(to_date)
        conn.execute(ann_sql, ann_params)

    return {"status": "deleted"}


@router.get("/annotations")
async def get_annotations(
    from_date: Optional[str] = Query(None, alias="from"),
    to_date: Optional[str] = Query(None, alias="to"),
    type: Optional[str] = None,
    area_id: Optional[str] = None,
):
    with get_connection() as conn:
        sql = "SELECT * FROM ext_annotations WHERE 1=1"
        params = []
        if from_date:
            sql += " AND date >= ?"
            params.append(from_date)
        if to_date:
            sql += " AND date <= ?"
            params.append(to_date)
        if type:
            types = [t.strip() for t in type.split(",")]
            placeholders = ",".join(["?" for _ in types])
            sql += f" AND annotation_type IN ({placeholders})"
            params.extend(types)
        if area_id:
            sql += " AND (area_id = ? OR area_id IS NULL)"
            params.append(area_id)
        sql += " ORDER BY date"
        rows = conn.execute(sql, params).fetchall()
        cols = [d[0] for d in conn.description]
        result = []
        for row in rows:
            d = dict(zip(cols, row))
            d["date"] = str(d["date"])
            if d.get("date_end"):
                d["date_end"] = str(d["date_end"])
            if d.get("created_at"):
                d["created_at"] = str(d["created_at"])
            result.append(d)
        return result


@router.post("/annotation")
async def create_annotation(body: AnnotationCreate):
    with get_write_connection() as conn:
        max_id = conn.execute("SELECT COALESCE(MAX(id), 0) FROM ext_annotations").fetchone()[0] + 1
        conn.execute(
            """INSERT INTO ext_annotations
               (id, date, date_end, area_id, regional_id, province,
                annotation_type, title, description, severity,
                color, icon, show_on_chart, source)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            [max_id, body.date, body.date_end, body.area_id, body.regional_id,
             body.province, body.annotation_type, body.title, body.description,
             body.severity, body.color, body.icon, body.show_on_chart, 'manual']
        )
        return {"id": max_id, "status": "created"}


@router.put("/annotation/{ann_id}")
async def update_annotation(ann_id: int, body: AnnotationUpdate):
    with get_write_connection() as conn:
        existing = conn.execute("SELECT * FROM ext_annotations WHERE id = ?", [ann_id]).fetchone()
        if not existing:
            raise HTTPException(404, "Anotasi tidak ditemukan")

        updates = []
        params = []
        for field in ["date", "date_end", "area_id", "regional_id", "province",
                       "annotation_type", "title", "description", "severity",
                       "color", "icon", "show_on_chart"]:
            val = getattr(body, field, None)
            if val is not None:
                updates.append(f"{field} = ?")
                params.append(val)

        if updates:
            params.append(ann_id)
            conn.execute(f"UPDATE ext_annotations SET {', '.join(updates)} WHERE id = ?", params)

        return {"id": ann_id, "status": "updated"}


@router.delete("/annotation/{ann_id}")
async def delete_annotation(ann_id: int):
    with get_write_connection() as conn:
        conn.execute("DELETE FROM ext_annotations WHERE id = ?", [ann_id])
    return {"id": ann_id, "status": "deleted"}


def _pearson(x_vals, y_vals):
    n = len(x_vals)
    if n < 3:
        return 0.0
    mean_x = sum(x_vals) / n
    mean_y = sum(y_vals) / n
    num = sum((x - mean_x) * (y - mean_y) for x, y in zip(x_vals, y_vals))
    den_x = math.sqrt(sum((x - mean_x) ** 2 for x in x_vals))
    den_y = math.sqrt(sum((y - mean_y) ** 2 for y in y_vals))
    if den_x == 0 or den_y == 0:
        return 0.0
    return round(num / (den_x * den_y), 4)


def _interpret(r):
    ar = abs(r)
    if ar >= 0.8:
        return "sangat kuat"
    elif ar >= 0.6:
        return "kuat"
    elif ar >= 0.4:
        return "moderat"
    elif ar >= 0.2:
        return "lemah"
    return "sangat lemah"


@router.get("/correlation/weather")
async def correlation_weather(
    province: Optional[str] = None,
    from_date: Optional[str] = Query(None, alias="from"),
    to_date: Optional[str] = Query(None, alias="to"),
):
    with get_connection() as conn:
        weather_count = conn.execute("SELECT COUNT(*) FROM ext_weather").fetchone()[0]
        ticket_count = conn.execute("SELECT COUNT(*) FROM noc_tickets").fetchone()[0]

        if weather_count == 0:
            return {"error": "no_data", "message": "Upload data cuaca terlebih dahulu untuk analisis korelasi."}
        if ticket_count == 0:
            return {"error": "no_data", "message": "Belum ada data tiket untuk analisis korelasi."}

        w_sql = """
            SELECT date, AVG(rainfall_mm) as avg_rainfall, 
                   MAX(CAST(is_extreme AS INTEGER)) as has_extreme
            FROM ext_weather WHERE 1=1
        """
        w_params = []
        if province:
            w_sql += " AND province = ?"
            w_params.append(province)
        if from_date:
            w_sql += " AND date >= ?"
            w_params.append(from_date)
        if to_date:
            w_sql += " AND date <= ?"
            w_params.append(to_date)
        w_sql += " GROUP BY date"

        t_sql = """
            SELECT CAST(occured_time AS DATE) as tdate, COUNT(*) as cnt
            FROM noc_tickets WHERE occured_time IS NOT NULL
        """
        t_params = []
        if from_date:
            t_sql += " AND CAST(occured_time AS DATE) >= ?"
            t_params.append(from_date)
        if to_date:
            t_sql += " AND CAST(occured_time AS DATE) <= ?"
            t_params.append(to_date)
        t_sql += " GROUP BY CAST(occured_time AS DATE)"

        weather_rows = conn.execute(w_sql, w_params).fetchall()
        ticket_rows = conn.execute(t_sql, t_params).fetchall()

        weather_map = {str(r[0]): (r[1] or 0, r[2] or 0) for r in weather_rows}
        ticket_map = {str(r[0]): r[1] for r in ticket_rows}

        common_dates = set(weather_map.keys()) & set(ticket_map.keys())
        if len(common_dates) < 3:
            return {
                "error": "insufficient_data",
                "message": f"Data terlalu sedikit ({len(common_dates)} hari overlap). Butuh minimal 3 hari."
            }

        rainfall_vals = []
        extreme_vals = []
        ticket_vals = []
        data_points = []

        for d in sorted(common_dates):
            rain, ext = weather_map[d]
            tix = ticket_map[d]
            rainfall_vals.append(rain)
            extreme_vals.append(ext)
            ticket_vals.append(tix)
            data_points.append({"date": d, "rainfall_mm": round(rain, 1), "tickets": tix})

        corr_rain = _pearson(rainfall_vals, ticket_vals)
        corr_ext = _pearson(extreme_vals, ticket_vals)

        normal_tickets = [t for r, e, t in zip(rainfall_vals, extreme_vals, ticket_vals) if e == 0]
        extreme_tickets = [t for r, e, t in zip(rainfall_vals, extreme_vals, ticket_vals) if e == 1]

        avg_normal = round(sum(normal_tickets) / len(normal_tickets), 1) if normal_tickets else 0
        avg_extreme = round(sum(extreme_tickets) / len(extreme_tickets), 1) if extreme_tickets else 0
        increase = round((avg_extreme - avg_normal) / avg_normal * 100, 1) if avg_normal > 0 else 0

        return {
            "correlation_rainfall_vs_tickets": corr_rain,
            "correlation_extreme_vs_tickets": corr_ext,
            "interpretation": f"Korelasi {_interpret(corr_rain)} antara curah hujan dan jumlah gangguan",
            "data_points": data_points[:100],
            "extreme_impact": {
                "avg_tickets_normal": avg_normal,
                "avg_tickets_extreme": avg_extreme,
                "increase_pct": increase,
            },
            "total_days": len(common_dates),
        }


@router.get("/correlation/pln")
async def correlation_pln(
    province: Optional[str] = None,
    from_date: Optional[str] = Query(None, alias="from"),
    to_date: Optional[str] = Query(None, alias="to"),
):
    with get_connection() as conn:
        pln_count = conn.execute("SELECT COUNT(*) FROM ext_pln_outage").fetchone()[0]
        ticket_count = conn.execute("SELECT COUNT(*) FROM noc_tickets").fetchone()[0]

        if pln_count == 0:
            return {"error": "no_data", "message": "Upload data PLN terlebih dahulu untuk analisis korelasi."}
        if ticket_count == 0:
            return {"error": "no_data", "message": "Belum ada data tiket untuk analisis korelasi."}

        p_sql = """
            SELECT date, COUNT(*) as outage_count
            FROM ext_pln_outage WHERE 1=1
        """
        p_params = []
        if province:
            p_sql += " AND province = ?"
            p_params.append(province)
        if from_date:
            p_sql += " AND date >= ?"
            p_params.append(from_date)
        if to_date:
            p_sql += " AND date <= ?"
            p_params.append(to_date)
        p_sql += " GROUP BY date"

        t_sql = """
            SELECT CAST(occured_time AS DATE) as tdate, 
                   COUNT(*) as cnt,
                   COUNT(CASE WHEN LOWER(COALESCE(rc_category,'')) LIKE '%power%' THEN 1 END) as power_cnt
            FROM noc_tickets WHERE occured_time IS NOT NULL
        """
        t_params = []
        if from_date:
            t_sql += " AND CAST(occured_time AS DATE) >= ?"
            t_params.append(from_date)
        if to_date:
            t_sql += " AND CAST(occured_time AS DATE) <= ?"
            t_params.append(to_date)
        t_sql += " GROUP BY CAST(occured_time AS DATE)"

        pln_rows = conn.execute(p_sql, p_params).fetchall()
        ticket_rows = conn.execute(t_sql, t_params).fetchall()

        pln_dates = {str(r[0]) for r in pln_rows}
        ticket_map = {str(r[0]): (r[1], r[2]) for r in ticket_rows}

        all_dates = set(ticket_map.keys())
        if len(all_dates) < 3:
            return {"error": "insufficient_data", "message": "Data tiket terlalu sedikit."}

        pln_day_tickets = []
        pln_day_power = []
        no_pln_tickets = []
        no_pln_power = []
        has_pln_flags = []
        all_tickets = []

        for d in sorted(all_dates):
            total, power = ticket_map[d]
            is_pln = 1 if d in pln_dates else 0
            has_pln_flags.append(is_pln)
            all_tickets.append(total)
            if is_pln:
                pln_day_tickets.append(total)
                pln_day_power.append(power)
            else:
                no_pln_tickets.append(total)
                no_pln_power.append(power)

        corr_pln = _pearson(has_pln_flags, all_tickets)

        avg_no_pln = round(sum(no_pln_tickets) / len(no_pln_tickets), 1) if no_pln_tickets else 0
        avg_with_pln = round(sum(pln_day_tickets) / len(pln_day_tickets), 1) if pln_day_tickets else 0
        avg_power_no = round(sum(no_pln_power) / len(no_pln_power), 1) if no_pln_power else 0
        avg_power_with = round(sum(pln_day_power) / len(pln_day_power), 1) if pln_day_power else 0
        increase = round((avg_with_pln - avg_no_pln) / avg_no_pln * 100, 1) if avg_no_pln > 0 else 0

        return {
            "correlation_pln_vs_tickets": corr_pln,
            "interpretation": f"Korelasi {_interpret(corr_pln)} antara gangguan PLN dan jumlah tiket",
            "pln_impact": {
                "avg_tickets_no_pln": avg_no_pln,
                "avg_tickets_with_pln": avg_with_pln,
                "avg_power_rc_no_pln": avg_power_no,
                "avg_power_rc_with_pln": avg_power_with,
                "increase_pct": increase,
            },
            "total_days": len(all_dates),
            "pln_days": len(pln_day_tickets),
        }


@router.get("/correlation/calendar")
async def correlation_calendar(
    from_date: Optional[str] = Query(None, alias="from"),
    to_date: Optional[str] = Query(None, alias="to"),
):
    with get_connection() as conn:
        cal_count = conn.execute("SELECT COUNT(*) FROM ext_calendar").fetchone()[0]
        ticket_count = conn.execute("SELECT COUNT(*) FROM noc_tickets").fetchone()[0]

        if cal_count == 0:
            return {"error": "no_data", "message": "Kalender belum di-generate."}
        if ticket_count == 0:
            return {"error": "no_data", "message": "Belum ada data tiket."}

        t_sql = """
            SELECT CAST(occured_time AS DATE) as tdate, COUNT(*) as cnt
            FROM noc_tickets WHERE occured_time IS NOT NULL
        """
        t_params = []
        if from_date:
            t_sql += " AND CAST(occured_time AS DATE) >= ?"
            t_params.append(from_date)
        if to_date:
            t_sql += " AND CAST(occured_time AS DATE) <= ?"
            t_params.append(to_date)
        t_sql += " GROUP BY CAST(occured_time AS DATE)"

        ticket_rows = conn.execute(t_sql, t_params).fetchall()
        ticket_map = {str(r[0]): r[1] for r in ticket_rows}

        if not ticket_map:
            return {"error": "insufficient_data", "message": "Tidak ada data tiket dalam periode tersebut."}

        c_sql = "SELECT date, day_type, is_ramadan FROM ext_calendar WHERE 1=1"
        c_params = []
        if from_date:
            c_sql += " AND date >= ?"
            c_params.append(from_date)
        if to_date:
            c_sql += " AND date <= ?"
            c_params.append(to_date)
        cal_rows = conn.execute(c_sql, c_params).fetchall()

        weekday_tickets = []
        weekend_tickets = []
        holiday_tickets = []
        ramadan_tickets = []
        non_ramadan_tickets = []

        for cdate, day_type, is_ramadan in cal_rows:
            d = str(cdate)
            if d not in ticket_map:
                continue
            cnt = ticket_map[d]

            if day_type == "Kerja":
                weekday_tickets.append(cnt)
            elif day_type == "Akhir Pekan":
                weekend_tickets.append(cnt)
            elif day_type in ("Libur Nasional", "Cuti Bersama"):
                holiday_tickets.append(cnt)

            if is_ramadan:
                ramadan_tickets.append(cnt)
            else:
                non_ramadan_tickets.append(cnt)

        avg_weekday = round(sum(weekday_tickets) / len(weekday_tickets), 1) if weekday_tickets else 0
        avg_weekend = round(sum(weekend_tickets) / len(weekend_tickets), 1) if weekend_tickets else 0
        avg_holiday = round(sum(holiday_tickets) / len(holiday_tickets), 1) if holiday_tickets else 0
        avg_ramadan = round(sum(ramadan_tickets) / len(ramadan_tickets), 1) if ramadan_tickets else 0

        weekend_red = round((avg_weekend - avg_weekday) / avg_weekday * 100, 1) if avg_weekday > 0 else 0
        holiday_red = round((avg_holiday - avg_weekday) / avg_weekday * 100, 1) if avg_weekday > 0 else 0
        avg_non_ramadan = round(sum(non_ramadan_tickets) / len(non_ramadan_tickets), 1) if non_ramadan_tickets else 0
        ramadan_inc = round((avg_ramadan - avg_non_ramadan) / avg_non_ramadan * 100, 1) if avg_non_ramadan > 0 else 0

        parts = []
        if avg_weekday > 0:
            parts.append(f"Volume turun ~{abs(weekend_red):.0f}% saat weekend/libur")
        if avg_ramadan > 0 and ramadan_inc != 0:
            direction = "naik" if ramadan_inc > 0 else "turun"
            parts.append(f"{direction} ~{abs(ramadan_inc):.0f}% saat Ramadan")
        interpretation = ", ".join(parts) if parts else "Data belum cukup untuk interpretasi"

        return {
            "avg_tickets_weekday": avg_weekday,
            "avg_tickets_weekend": avg_weekend,
            "avg_tickets_holiday": avg_holiday,
            "avg_tickets_ramadan": avg_ramadan,
            "weekend_reduction_pct": weekend_red,
            "holiday_reduction_pct": holiday_red,
            "ramadan_increase_pct": ramadan_inc,
            "interpretation": interpretation,
            "days_analyzed": {
                "weekday": len(weekday_tickets),
                "weekend": len(weekend_tickets),
                "holiday": len(holiday_tickets),
                "ramadan": len(ramadan_tickets),
            },
        }

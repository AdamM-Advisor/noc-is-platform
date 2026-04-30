import threading
import logging
from fastapi import APIRouter, HTTPException, Query
from backend.database import get_write_connection, get_connection
from backend.services.backup_service import create_backup
from backend.services.job_status_adapter import (
    LEGACY_RESYNC_JOB_TYPE,
    LEGACY_UPLOAD_JOB_TYPE,
    has_active_operational_job,
    legacy_job_status,
    progress_result,
)
from backend.services.operational_catalog_service import create_job, get_job, update_job

router = APIRouter(prefix="/data")
logger = logging.getLogger(__name__)

_resync_lock = threading.Lock()


@router.get("/coverage")
async def get_data_coverage():
    sources = ["swfm_event", "swfm_incident", "swfm_realtime", "fault_center"]
    coverage = {}
    total_tickets = 0

    with get_connection() as conn:
        try:
            rows = conn.execute("""
                SELECT calc_year_month, calc_source, COUNT(*) as cnt
                FROM noc_tickets
                WHERE calc_year_month IS NOT NULL AND calc_source IS NOT NULL
                GROUP BY calc_year_month, calc_source
                ORDER BY calc_year_month, calc_source
            """).fetchall()
        except Exception:
            rows = []

        import_rows = []
        try:
            import_rows = conn.execute("""
                SELECT id, period, file_type, rows_imported, orphan_count, status, imported_at
                FROM import_logs
                ORDER BY imported_at DESC
            """).fetchall()
            import_cols = [d[0] for d in conn.description]
        except Exception:
            import_cols = []

    for ym, src, cnt in rows:
        if ym not in coverage:
            coverage[ym] = {}
        coverage[ym][src] = {"exists": True, "count": cnt}
        total_tickets += cnt

    import_by_period = {}
    for row in import_rows:
        imp = dict(zip(import_cols, row))
        key = (imp.get("period", ""), imp.get("file_type", ""))
        if key not in import_by_period:
            import_by_period[key] = []
        import_by_period[key].append(imp)

    for ym in coverage:
        for src in sources:
            if src not in coverage[ym]:
                coverage[ym][src] = {"exists": False}
            else:
                cell = coverage[ym][src]
                imports = import_by_period.get((ym, src), [])
                if imports:
                    cell["imports"] = imports
                    cell["has_orphans"] = any(i.get("orphan_count", 0) > 0 for i in imports)

    months = sorted(coverage.keys())
    sources_active = list(set(src for ym in coverage for src, info in coverage[ym].items() if info.get("exists")))

    return {
        "coverage": coverage,
        "summary": {
            "total_tickets": total_tickets,
            "months_covered": len(months),
            "sources_active": sorted(sources_active),
        },
    }


@router.get("/tickets/count")
async def preview_delete_count(
    year_month: str = Query(None),
    source: str = Query(None),
    from_month: str = Query(None),
    to_month: str = Query(None),
):
    conditions = []
    params = []

    if year_month:
        conditions.append("calc_year_month = ?")
        params.append(year_month)
    if source:
        conditions.append("calc_source = ?")
        params.append(source)
    if from_month:
        conditions.append("calc_year_month >= ?")
        params.append(from_month)
    if to_month:
        conditions.append("calc_year_month <= ?")
        params.append(to_month)

    if not conditions:
        raise HTTPException(status_code=400, detail="At least one filter required")

    where = " AND ".join(conditions)

    with get_connection() as conn:
        count = conn.execute(f"SELECT COUNT(*) FROM noc_tickets WHERE {where}", params).fetchone()[0]

    return {"count": count, "filters": {"year_month": year_month, "source": source, "from_month": from_month, "to_month": to_month}}


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

        if period and source:
            try:
                conn.execute(
                    "UPDATE import_logs SET status = 'deleted' WHERE period = ? AND file_type = ?",
                    [period, source]
                )
            except Exception:
                pass

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

        try:
            conn.execute("UPDATE import_logs SET status = 'deleted' WHERE id = ?", [import_id])
        except Exception:
            pass

    if period:
        from backend.services.summary_service import refresh_summaries
        refresh_summaries([period])

    return {"deleted": count, "import_id": import_id, "period": period, "source": file_type}


@router.delete("/tickets/by-period")
async def delete_by_period(body: dict):
    year_month = body.get("year_month")
    source = body.get("source")
    if not year_month:
        raise HTTPException(status_code=400, detail="year_month is required")

    try:
        create_backup()
    except Exception:
        pass

    conditions = ["calc_year_month = ?"]
    params = [year_month]
    if source:
        conditions.append("calc_source = ?")
        params.append(source)

    where = " AND ".join(conditions)

    with get_write_connection() as conn:
        count = conn.execute(f"SELECT COUNT(*) FROM noc_tickets WHERE {where}", params).fetchone()[0]
        conn.execute(f"DELETE FROM noc_tickets WHERE {where}", params)

        if source:
            try:
                conn.execute(
                    "UPDATE import_logs SET status = 'deleted' WHERE period = ? AND file_type = ?",
                    [year_month, source]
                )
            except Exception:
                pass

    from backend.services.summary_service import refresh_summaries
    refresh_summaries([year_month])

    return {"deleted": count, "period": year_month, "source": source}


@router.delete("/tickets/by-source")
async def delete_by_source(body: dict):
    source = body.get("source")
    if not source:
        raise HTTPException(status_code=400, detail="source is required")

    try:
        create_backup()
    except Exception:
        pass

    with get_write_connection() as conn:
        affected_months = [r[0] for r in conn.execute(
            "SELECT DISTINCT calc_year_month FROM noc_tickets WHERE calc_source = ? AND calc_year_month IS NOT NULL",
            [source]
        ).fetchall()]

        count = conn.execute("SELECT COUNT(*) FROM noc_tickets WHERE calc_source = ?", [source]).fetchone()[0]
        conn.execute("DELETE FROM noc_tickets WHERE calc_source = ?", [source])

        try:
            conn.execute("UPDATE import_logs SET status = 'deleted' WHERE file_type = ?", [source])
        except Exception:
            pass

    if affected_months:
        from backend.services.summary_service import refresh_summaries
        refresh_summaries(affected_months)

    return {"deleted": count, "source": source, "affected_months": affected_months}


@router.delete("/tickets/by-period-range")
async def delete_by_period_range(body: dict):
    from_month = body.get("from")
    to_month = body.get("to")
    source = body.get("source")

    if not from_month or not to_month:
        raise HTTPException(status_code=400, detail="from and to are required")

    try:
        create_backup()
    except Exception:
        pass

    conditions = ["calc_year_month >= ?", "calc_year_month <= ?"]
    params = [from_month, to_month]
    if source:
        conditions.append("calc_source = ?")
        params.append(source)

    where = " AND ".join(conditions)

    with get_write_connection() as conn:
        affected_months = [r[0] for r in conn.execute(
            f"SELECT DISTINCT calc_year_month FROM noc_tickets WHERE {where}",
            params
        ).fetchall()]

        count = conn.execute(f"SELECT COUNT(*) FROM noc_tickets WHERE {where}", params).fetchone()[0]
        conn.execute(f"DELETE FROM noc_tickets WHERE {where}", params)

        if source:
            try:
                conn.execute(
                    "UPDATE import_logs SET status = 'deleted' WHERE period >= ? AND period <= ? AND file_type = ?",
                    [from_month, to_month, source]
                )
            except Exception:
                pass

    if affected_months:
        from backend.services.summary_service import refresh_summaries
        refresh_summaries(affected_months)

    return {"deleted": count, "from": from_month, "to": to_month, "source": source, "affected_months": affected_months}


@router.post("/resync")
async def start_resync():
    if has_active_operational_job(LEGACY_UPLOAD_JOB_TYPE):
        raise HTTPException(
            status_code=409,
            detail="Upload sedang berjalan. Tunggu upload selesai sebelum sinkronisasi.",
        )

    with _resync_lock:
        if has_active_operational_job(LEGACY_RESYNC_JOB_TYPE):
            raise HTTPException(
                status_code=409,
                detail="Sinkronisasi sedang berjalan. Mohon tunggu...",
            )

        job = create_job(LEGACY_RESYNC_JOB_TYPE, payload={"requested_from": "api"})
        job_id = job["job_id"]
        update_job(
            job_id,
            status="running",
            result=progress_result("Memulai..."),
            progress_phase="starting",
            progress_current=0,
            progress_total=0,
        )

    def run_resync():
        try:
            def update_progress(phase, detail="", row=0, total=0):
                update_job(
                    job_id,
                    status="running",
                    result=progress_result(detail),
                    progress_phase=phase,
                    progress_current=row,
                    progress_total=total,
                )

            from backend.services.resync_service import resync_hierarchy
            result = resync_hierarchy(progress_callback=update_progress)
            total = int(result.get("total_tickets") or 0)

            update_job(
                job_id,
                status="completed",
                result=result,
                progress_phase="completed",
                progress_current=total,
                progress_total=total,
            )

        except Exception as e:
            logger.exception(f"Resync failed for job {job_id}")
            update_job(
                job_id,
                status="failed",
                result=progress_result(str(e)),
                error_message=str(e),
                progress_phase="failed",
            )

    thread = threading.Thread(target=run_resync, daemon=True)
    thread.start()

    return {"job_id": job_id, "status": "processing"}


@router.get("/resync/status/{job_id}")
async def resync_status(job_id: str):
    try:
        job = get_job(job_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.get("job_type") != LEGACY_RESYNC_JOB_TYPE:
        raise HTTPException(status_code=404, detail="Job not found")
    return legacy_job_status(job)

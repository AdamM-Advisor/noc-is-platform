from fastapi import APIRouter, HTTPException
from backend.database import get_connection, get_write_connection

router = APIRouter(prefix="/imports")


@router.get("")
async def list_imports():
    try:
        with get_connection() as conn:
            rows = conn.execute("""
                SELECT id, filename, file_type, file_size_mb, period,
                       rows_total, rows_imported, rows_skipped, rows_error,
                       orphan_count, processing_time_sec, status, imported_at
                FROM import_logs
                ORDER BY imported_at DESC
            """).fetchall()
            columns = ["id", "filename", "file_type", "file_size_mb", "period",
                        "rows_total", "rows_imported", "rows_skipped", "rows_error",
                        "orphan_count", "processing_time_sec", "status", "imported_at"]
            return [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{import_id}")
async def get_import(import_id: int):
    try:
        with get_connection() as conn:
            row = conn.execute("""
                SELECT id, filename, file_type, file_size_mb, period,
                       rows_total, rows_imported, rows_skipped, rows_error,
                       orphan_count, processing_time_sec, status, error_message, imported_at
                FROM import_logs WHERE id = ?
            """, [import_id]).fetchone()

            if not row:
                raise HTTPException(status_code=404, detail="Import not found")

            columns = ["id", "filename", "file_type", "file_size_mb", "period",
                        "rows_total", "rows_imported", "rows_skipped", "rows_error",
                        "orphan_count", "processing_time_sec", "status", "error_message", "imported_at"]
            return dict(zip(columns, row))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{import_id}")
async def delete_import(import_id: int):
    try:
        with get_connection() as conn:
            row = conn.execute(
                "SELECT file_type, period FROM import_logs WHERE id = ?", [import_id]
            ).fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="Import not found")

        file_type, period = row

        from backend.services.backup_service import create_backup
        try:
            create_backup()
        except Exception:
            pass

        deleted = 0
        with get_write_connection() as conn:
            if period:
                result = conn.execute("""
                    SELECT COUNT(*) FROM noc_tickets
                    WHERE calc_source = ? AND calc_year_month = ?
                """, [file_type, period]).fetchone()
                deleted = result[0]
                conn.execute("""
                    DELETE FROM noc_tickets
                    WHERE calc_source = ? AND calc_year_month = ?
                """, [file_type, period])
            else:
                result = conn.execute("""
                    SELECT COUNT(*) FROM noc_tickets WHERE calc_source = ?
                """, [file_type]).fetchone()
                deleted = result[0]
                conn.execute("DELETE FROM noc_tickets WHERE calc_source = ?", [file_type])

            conn.execute("DELETE FROM import_logs WHERE id = ?", [import_id])

        if period:
            from backend.services.summary_service import refresh_summaries
            refresh_summaries([period])

        return {"deleted_rows": deleted, "summaries_refreshed": [period] if period else []}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

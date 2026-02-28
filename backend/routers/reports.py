import os
import logging
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse, HTMLResponse
from pydantic import BaseModel
from typing import Optional
from backend.database import get_connection, get_write_connection
from backend.services.report_service import ReportGenerator

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/reports", tags=["reports"])
generator = ReportGenerator()


class GenerateRequest(BaseModel):
    report_type: str
    entity_level: str
    entity_id: str
    period_start: str
    period_end: str
    options: Optional[dict] = None


class PreviewRequest(BaseModel):
    report_type: str
    entity_level: str
    entity_id: str
    period_start: str
    period_end: str


@router.post("/generate")
async def generate_report(req: GenerateRequest):
    opts = req.options or {}
    opts.setdefault('include_pdf', True)
    result = generator.generate(
        req.report_type, req.entity_level, req.entity_id,
        req.period_start, req.period_end, opts
    )
    pdf_url = f"/api/reports/{result['report_id']}/pdf" if result.get('pdf_path') else None
    excel_url = f"/api/reports/{result['report_id']}/excel" if result.get('excel_path') else None
    return {
        "report_id": result["report_id"],
        "status": result["status"],
        "pdf_url": pdf_url,
        "excel_url": excel_url,
        "generation_time_ms": result.get("generation_time_ms"),
        "error": result.get("error"),
    }


@router.get("")
async def list_reports(
    report_type: Optional[str] = None,
    entity_level: Optional[str] = None,
    page: int = Query(1, ge=1),
    per_page: int = Query(10, ge=1, le=100),
):
    with get_connection() as conn:
        where = []
        params = []
        if report_type:
            where.append("report_type = ?")
            params.append(report_type)
        if entity_level:
            where.append("entity_level = ?")
            params.append(entity_level)

        where_clause = f"WHERE {' AND '.join(where)}" if where else ""

        total = conn.execute(
            f"SELECT COUNT(*) FROM report_history {where_clause}", params
        ).fetchone()[0]

        offset = (page - 1) * per_page
        rows = conn.execute(f"""
            SELECT id, report_type, entity_level, entity_id, entity_name,
                   period_label, period_start, period_end,
                   generated_at, status, generation_time_ms,
                   ai_enhanced, pdf_size_kb, excel_size_kb, error_message
            FROM report_history {where_clause}
            ORDER BY generated_at DESC
            LIMIT ? OFFSET ?
        """, params + [per_page, offset]).fetchall()

    cols = ["id", "report_type", "entity_level", "entity_id", "entity_name",
            "period_label", "period_start", "period_end",
            "generated_at", "status", "generation_time_ms",
            "ai_enhanced", "pdf_size_kb", "excel_size_kb", "error_message"]

    reports = []
    for r in rows:
        d = dict(zip(cols, r))
        d["pdf_url"] = f"/api/reports/{d['id']}/pdf" if d.get("pdf_size_kb") else None
        d["excel_url"] = f"/api/reports/{d['id']}/excel" if d.get("excel_size_kb") else None
        reports.append(d)

    return {
        "reports": reports,
        "total": total,
        "page": page,
        "per_page": per_page,
    }


@router.get("/{report_id}")
async def get_report(report_id: int):
    with get_connection() as conn:
        row = conn.execute(
            "SELECT * FROM report_history WHERE id = ?", [report_id]
        ).fetchone()
    if not row:
        raise HTTPException(404, "Report not found")
    cols = [d[0] for d in conn.execute("DESCRIBE report_history").fetchall()]
    return dict(zip(cols, row))


@router.get("/{report_id}/pdf")
async def download_pdf(report_id: int):
    with get_connection() as conn:
        row = conn.execute(
            "SELECT pdf_path, entity_name, report_type, period_label FROM report_history WHERE id = ?",
            [report_id]
        ).fetchone()
    if not row or not row[0]:
        raise HTTPException(404, "PDF not found")
    path = row[0]
    if not os.path.exists(path):
        raise HTTPException(404, "PDF file missing")
    filename = f"Laporan_{row[2]}_{row[1]}_{row[3]}.pdf".replace(" ", "_")
    return FileResponse(path, media_type="application/pdf", filename=filename)


@router.get("/{report_id}/excel")
async def download_excel(report_id: int):
    with get_connection() as conn:
        row = conn.execute(
            "SELECT excel_path, entity_name, report_type, period_label FROM report_history WHERE id = ?",
            [report_id]
        ).fetchone()
    if not row or not row[0]:
        raise HTTPException(404, "Excel not found")
    path = row[0]
    if not os.path.exists(path):
        raise HTTPException(404, "Excel file missing")
    filename = f"Lampiran_{row[2]}_{row[1]}_{row[3]}.xlsx".replace(" ", "_")
    return FileResponse(
        path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=filename
    )


@router.get("/{report_id}/preview")
async def preview_report(report_id: int):
    with get_connection() as conn:
        row = conn.execute(
            "SELECT report_type, entity_level, entity_id, period_start, period_end FROM report_history WHERE id = ?",
            [report_id]
        ).fetchone()
    if not row:
        raise HTTPException(404, "Report not found")
    ps = str(row[3]) if row[3] else ""
    pe = str(row[4]) if row[4] else ""
    html = generator.preview(row[0], row[1], row[2], ps, pe)
    return HTMLResponse(content=html)


@router.post("/preview")
async def preview_new(req: PreviewRequest):
    html = generator.preview(
        req.report_type, req.entity_level, req.entity_id,
        req.period_start, req.period_end
    )
    return HTMLResponse(content=html)


@router.delete("/{report_id}")
async def delete_report(report_id: int):
    with get_connection() as conn:
        row = conn.execute(
            "SELECT pdf_path, excel_path FROM report_history WHERE id = ?",
            [report_id]
        ).fetchone()
    if not row:
        raise HTTPException(404, "Report not found")
    for path in [row[0], row[1]]:
        if path and os.path.exists(path):
            try:
                os.remove(path)
            except Exception:
                pass
    with get_write_connection() as conn:
        conn.execute("DELETE FROM report_history WHERE id = ?", [report_id])
    return {"deleted": True}

import os
import uuid
import threading
import logging
import pandas as pd
from fastapi import APIRouter, UploadFile, File, Header, HTTPException, Request
from backend.config import SINGLE_UPLOAD_LIMIT_MB, CHUNK_SIZE_MB, UPLOAD_DIR
from backend.services.upload_service import save_upload, save_chunk, assemble_chunks, get_chunk_status
from backend.services.file_detector import detect_file_type
from backend.services.header_normalizer import normalize_headers

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/upload")

ALLOWED_EXTENSIONS = {".xlsx", ".csv", ".parquet"}

_processing_jobs = {}
_processing_lock = threading.Lock()


def validate_extension(filename: str):
    ext = os.path.splitext(filename)[1].lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Tipe file tidak didukung: {ext}. Gunakan .xlsx, .csv, atau .parquet",
        )


@router.post("/single")
async def upload_single(file: UploadFile = File(...)):
    validate_extension(file.filename)
    content = await file.read()
    size_mb = len(content) / (1024 * 1024)

    if size_mb > SINGLE_UPLOAD_LIMIT_MB:
        raise HTTPException(
            status_code=413,
            detail=f"File terlalu besar ({size_mb:.1f} MB). Gunakan chunked upload untuk file > {SINGLE_UPLOAD_LIMIT_MB} MB.",
        )

    result = save_upload(file.filename, content)
    return result


@router.post("/chunk")
async def upload_chunk(
    request: Request,
    x_upload_id: str = Header(...),
    x_chunk_index: int = Header(...),
    x_total_chunks: int = Header(...),
    x_filename: str = Header(...),
):
    validate_extension(x_filename)
    body = await request.body()

    chunk_size_mb = len(body) / (1024 * 1024)
    if chunk_size_mb > CHUNK_SIZE_MB + 0.5:
        raise HTTPException(
            status_code=413,
            detail=f"Chunk terlalu besar ({chunk_size_mb:.1f} MB). Max {CHUNK_SIZE_MB} MB.",
        )

    result = save_chunk(x_upload_id, x_chunk_index, x_total_chunks, body)
    return result


@router.post("/chunk/complete")
async def complete_chunked_upload(data: dict):
    upload_id = data.get("upload_id")
    filename = data.get("filename")
    total_chunks = data.get("total_chunks")

    if not all([upload_id, filename, total_chunks]):
        raise HTTPException(status_code=400, detail="Missing required fields")

    validate_extension(filename)
    result = assemble_chunks(upload_id, filename, total_chunks)
    return result


@router.get("/chunk/status/{upload_id}")
async def chunk_status(upload_id: str):
    result = get_chunk_status(upload_id)
    return result


@router.post("/detect")
async def detect_type(data: dict):
    filename = data.get("filename", "")
    if not filename:
        raise HTTPException(status_code=400, detail="filename is required")

    file_path = os.path.join(UPLOAD_DIR, filename)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail=f"File not found: {filename}")

    try:
        ext = os.path.splitext(filename)[1].lower()
        if ext in ('.xlsx', '.xls'):
            df = pd.read_excel(file_path, engine='openpyxl', nrows=5)
        elif ext == '.csv':
            df = pd.read_csv(file_path, nrows=5)
        elif ext == '.parquet':
            df = pd.read_parquet(file_path)
            df = df.head(5)
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported: {ext}")

        headers = list(df.columns)
        detection = detect_file_type(filename, headers)
        detection["total_columns"] = len(headers)
        detection["sample_headers"] = headers[:10]
        return detection
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Detection failed: {str(e)}")


@router.post("/process")
async def process_upload(data: dict):
    filename = data.get("filename", "")
    file_type_override = data.get("file_type", "auto")

    if not filename:
        raise HTTPException(status_code=400, detail="filename is required")

    file_path = os.path.join(UPLOAD_DIR, filename)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail=f"File not found: {filename}")

    with _processing_lock:
        active = [j for j in _processing_jobs.values() if j["status"] == "processing"]
        if active:
            raise HTTPException(
                status_code=409,
                detail="Sedang memproses upload lain. Mohon tunggu...",
            )

        job_id = str(uuid.uuid4())[:8]

        _processing_jobs[job_id] = {
            "status": "processing",
            "filename": filename,
            "file_type": None,
            "progress": {"phase": "reading", "detail": "Memulai...", "row": 0, "total": 0},
            "result": None,
            "error": None,
        }

    thread = threading.Thread(
        target=_run_processing,
        args=(job_id, file_path, filename, file_type_override),
        daemon=True,
    )
    thread.start()

    return {"job_id": job_id, "status": "processing"}


@router.get("/process/status/{job_id}")
async def process_status(job_id: str):
    job = _processing_jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


def _run_processing(job_id, file_path, filename, file_type_override):
    try:
        def update_progress(phase, detail="", row=0, total=0):
            if job_id in _processing_jobs:
                _processing_jobs[job_id]["progress"] = {
                    "phase": phase,
                    "detail": detail,
                    "row": row,
                    "total": total,
                }

        update_progress("reading", "Membaca file...")

        ext = os.path.splitext(filename)[1].lower()
        if ext in ('.xlsx', '.xls'):
            df = pd.read_excel(file_path, engine='openpyxl', nrows=5)
        elif ext == '.csv':
            df = pd.read_csv(file_path, nrows=5)
        elif ext == '.parquet':
            df = pd.read_parquet(file_path)
            df = df.head(5)
        else:
            raise ValueError(f"Unsupported file: {ext}")

        headers = list(df.columns)
        detection = detect_file_type(filename, headers)
        file_type = file_type_override if file_type_override != "auto" else detection["file_type"]

        _processing_jobs[job_id]["file_type"] = file_type

        if file_type == "unknown":
            raise ValueError("Tidak dapat mendeteksi tipe file. Silakan pilih tipe secara manual.")

        if file_type == "site_master":
            from backend.services.site_master_processor import process_site_master
            result = process_site_master(file_path)
            _log_site_master_import(filename, file_path, result)
        else:
            from backend.services.ticket_processor import process_ticket_file
            result = process_ticket_file(file_path, file_type, progress_callback=update_progress)

        _processing_jobs[job_id]["status"] = "completed"
        _processing_jobs[job_id]["result"] = result
        update_progress("completed", "Selesai", result.get("total", 0), result.get("total", 0))

    except Exception as e:
        logger.exception(f"Processing failed for job {job_id}")
        _processing_jobs[job_id]["status"] = "failed"
        _processing_jobs[job_id]["error"] = str(e)
        _processing_jobs[job_id]["progress"]["phase"] = "failed"
        _processing_jobs[job_id]["progress"]["detail"] = str(e)

        try:
            from backend.database import get_write_connection
            with get_write_connection() as conn:
                max_id = conn.execute("SELECT COALESCE(MAX(id), 0) FROM import_logs").fetchone()[0]
                conn.execute("""
                    INSERT INTO import_logs (id, filename, file_type, file_size_mb, period,
                        rows_total, rows_imported, rows_skipped, rows_error,
                        orphan_count, processing_time_sec, status, error_message)
                    VALUES (?, ?, ?, 0, NULL, 0, 0, 0, 0, 0, 0, 'failed', ?)
                """, [max_id + 1, filename, file_type or 'unknown', str(e)])
        except Exception:
            pass


def _log_site_master_import(filename, file_path, result):
    from backend.database import get_write_connection
    file_size_mb = round(os.path.getsize(file_path) / (1024 * 1024), 2) if os.path.exists(file_path) else 0

    with get_write_connection() as conn:
        max_id = conn.execute("SELECT COALESCE(MAX(id), 0) FROM import_logs").fetchone()[0]
        orphan_count = sum(v for v in result.get("orphans", {}).values() if isinstance(v, int))
        conn.execute("""
            INSERT INTO import_logs (id, filename, file_type, file_size_mb, period,
                rows_total, rows_imported, rows_skipped, rows_error,
                orphan_count, processing_time_sec, status, backup_created)
            VALUES (?, ?, 'site_master', ?, NULL, ?, ?, ?, 0, ?, ?, 'completed', FALSE)
        """, [
            max_id + 1, filename, file_size_mb,
            result["total"], result["inserted"], result["updated"],
            orphan_count, result["duration_sec"],
        ])

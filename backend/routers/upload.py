import os
from fastapi import APIRouter, UploadFile, File, Header, HTTPException, Request
from backend.config import SINGLE_UPLOAD_LIMIT_MB, CHUNK_SIZE_MB
from backend.services.upload_service import save_upload, save_chunk, assemble_chunks, get_chunk_status

router = APIRouter(prefix="/upload")

ALLOWED_EXTENSIONS = {".xlsx", ".csv", ".parquet"}


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

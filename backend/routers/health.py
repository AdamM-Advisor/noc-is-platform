import os
import time
import psutil
from fastapi import APIRouter, Request
from backend.config import DB_PATH, DATA_DIR, BACKUP_DIR, DUCKDB_MEMORY_LIMIT
from backend.database import get_connection

router = APIRouter()


@router.get("/health")
async def health_check(request: Request):
    status = "healthy"
    db_connected = False
    db_size_mb = 0.0
    table_count = 0

    try:
        if os.path.exists(DB_PATH):
            db_size_mb = round(os.path.getsize(DB_PATH) / (1024 * 1024), 2)

        with get_connection() as conn:
            conn.execute("SELECT 1")
            db_connected = True
            try:
                tables = conn.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
                ).fetchall()
                table_count = len(tables)
            except Exception:
                pass
    except Exception:
        status = "error"

    process = psutil.Process(os.getpid())
    memory_used_mb = round(process.memory_info().rss / (1024 * 1024), 2)

    data_dir_mb = 0.0
    try:
        for dirpath, _, filenames in os.walk(DATA_DIR):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                if os.path.exists(fp):
                    data_dir_mb += os.path.getsize(fp)
        data_dir_mb = round(data_dir_mb / (1024 * 1024), 2)
    except Exception:
        pass

    backup_count = 0
    try:
        backup_count = len([
            f for f in os.listdir(BACKUP_DIR)
            if f.endswith(".duckdb")
        ])
    except Exception:
        pass

    uptime = time.time() - request.app.state.start_time

    return {
        "status": status,
        "database": {
            "connected": db_connected,
            "size_mb": db_size_mb,
            "path": DB_PATH,
        },
        "memory": {
            "used_mb": memory_used_mb,
            "limit": DUCKDB_MEMORY_LIMIT,
        },
        "disk": {
            "data_dir_mb": data_dir_mb,
            "backup_count": backup_count,
        },
        "uptime_seconds": round(uptime, 2),
        "version": "1.0.0",
        "table_count": table_count,
    }

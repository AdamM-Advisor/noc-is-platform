import os
from fastapi import APIRouter, HTTPException
from backend.config import DB_PATH, BACKUP_DIR
from backend.database import get_connection, get_write_connection, init_database
from backend.services.backup_service import create_backup, list_backups, restore_backup

router = APIRouter(prefix="/admin")


@router.get("/backups")
async def get_backups():
    backups = list_backups()
    return {"backups": backups}


@router.post("/backup")
async def trigger_backup():
    result = create_backup()
    return result


@router.post("/restore")
async def trigger_restore(data: dict):
    backup_filename = data.get("backup_filename")
    if not backup_filename:
        raise HTTPException(status_code=400, detail="backup_filename is required")

    backup_path = os.path.join(BACKUP_DIR, backup_filename)
    if not os.path.exists(backup_path):
        raise HTTPException(status_code=404, detail="Backup file not found")

    result = restore_backup(backup_filename)
    return result


@router.get("/db-info")
async def db_info():
    db_size_mb = 0.0
    if os.path.exists(DB_PATH):
        db_size_mb = round(os.path.getsize(DB_PATH) / (1024 * 1024), 2)

    table_count = 0
    row_counts = {}

    try:
        with get_connection() as conn:
            tables = conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
            ).fetchall()
            table_count = len(tables)
            for (table_name,) in tables:
                count = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]
                row_counts[table_name] = count
    except Exception:
        pass

    return {
        "db_size_mb": db_size_mb,
        "table_count": table_count,
        "row_counts": row_counts,
    }


@router.post("/delete-data")
async def delete_data():
    try:
        create_backup()
    except Exception:
        pass

    try:
        with get_write_connection() as conn:
            tables = conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
            ).fetchall()
            for (table_name,) in tables:
                if not table_name.startswith("master_"):
                    conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return {"status": "success", "message": "Data berhasil dihapus"}


@router.post("/reset-database")
async def reset_database():
    try:
        create_backup()
    except Exception:
        pass

    try:
        with get_write_connection() as conn:
            tables = conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
            ).fetchall()
            for (table_name,) in tables:
                conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return {"status": "success", "message": "Database berhasil direset"}
